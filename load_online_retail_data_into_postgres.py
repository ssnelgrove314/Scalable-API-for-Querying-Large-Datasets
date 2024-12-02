import pandas as pd
from sqlalchemy import create_engine

# Load the Excel file
file_path = 'online_retail_ii/online_retail_II.xlsx'
df_2009_2010 = pd.read_excel(file_path, sheet_name='Year 2009-2010')
df_2010_2011 = pd.read_excel(file_path, sheet_name='Year 2010-2011')

# Concatenate the two DataFrames
df = pd.concat([df_2009_2010, df_2010_2011])

# Database connection details
db_user = 'postgres'
db_password = 'password'
db_host = 'localhost'
db_port = '5432'
db_name = 'online_retail_data'

# Create a connection to the database
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load data into PostgreSQL
table_name = 'online_retail_data'
df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"Data loaded successfully into {table_name} table.")

# Show the head of the loaded DataFrame
print(df.head())
