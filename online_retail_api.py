from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Optional
from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
import os
from redis import Redis
from pydantic import BaseModel

# FastAPI app initialization 
app = FastAPI()

# Database configuration
SQLALCHEMY_DATABASE_URL = "postgresql://user:password@localhost/retail_db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Redis configuration
redis_client = Redis(host='localhost', port=6379, db=0)

# JWT configuration
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Database Models
class RetailData(Base):
    __tablename__ = "retail_data"
    id = Column(Integer, primary_key=True, index=True)
    invoice_no = Column(String, index=True)
    stock_code = Column(String, index=True)
    description = Column(String)
    quantity = Column(Integer)
    unit_price = Column(Float)
    customer_id = Column(Integer, index=True)
    country = Column(String)
    invoice_date = Column(Date, index=True)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)

# Pydantic Models
class RetailDataBase(BaseModel):
    invoice_no: str
    stock_code: str
    description: str
    quantity: int
    unit_price: float
    customer_id: int
    country: str
    invoice_date: datetime

class UserCreate(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Authentication functions
def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = db.query(User).filter(User.username == username).first()
    if user is None:
        raise credentials_exception
    return user

# API Endpoints
@app.post("/signup")
def signup(user: UserCreate, db: Session = Depends(get_db)):
    hashed_password = pwd_context.hash(user.password)
    db_user = User(username=user.username, hashed_password=hashed_password)
    db.add(db_user)
    db.commit()
    return {"message": "User created successfully"}

@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == form_data.username).first()
    if not user or not pwd_context.verify(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/data")
def get_data(
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    cache_key = f"data:{skip}:{limit}"
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return eval(cached_data)
    
    data = db.query(RetailData).offset(skip).limit(limit).all()
    redis_client.setex(cache_key, 300, str([dict(d.__dict__) for d in data]))
    return data

@app.get("/data/{id}")
def get_data_by_id(
    id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    cache_key = f"data_id:{id}"
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return eval(cached_data)
    
    data = db.query(RetailData).filter(RetailData.id == id).first()
    if not data:
        raise HTTPException(status_code=404, detail="Record not found")
    redis_client.setex(cache_key, 300, str(dict(data.__dict__)))
    return data

@app.get("/data/filter")
def filter_data(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    country: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    cache_key = f"filter:{start_date}:{end_date}:{country}:{min_price}:{max_price}:{skip}:{limit}"
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return eval(cached_data)
    
    query = db.query(RetailData)
    if start_date:
        query = query.filter(RetailData.invoice_date >= start_date)
    if end_date:
        query = query.filter(RetailData.invoice_date <= end_date)
    if country:
        query = query.filter(RetailData.country == country)
    if min_price:
        query = query.filter(RetailData.unit_price >= min_price)
    if max_price:
        query = query.filter(RetailData.unit_price <= max_price)
    
    data = query.offset(skip).limit(limit).all()
    redis_client.setex(cache_key, 300, str([dict(d.__dict__) for d in data]))
    return data

# Create tables
Base.metadata.create_all(bind=engine)