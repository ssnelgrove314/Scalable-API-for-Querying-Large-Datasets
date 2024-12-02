#!/bin/bash

# Define the URL and the output file name
URL="https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
OUTPUT_FILE="online_retail_ii.zip"

# Download the zip file
curl -o $OUTPUT_FILE $URL

# Unpack the zip file
unzip $OUTPUT_FILE -d ./online_retail_ii

# Clean up by removing the downloaded zip file
rm $OUTPUT_FILE