'''Uploading the French bakery dataset on MySQL server using Python.
    This code has especially been designed to upload the French bakery dataset on MySQL server.

    Main steps:
        1. Importing packages, load the dataset;
        2. Processing dates and time;
        3. Creating MySQL queries (CREATE TABLE and INSERT INTO);
        4. Connecting to the server;
        5. Executing the queries;

NOTE: dates and time were processed using pd.to_datetime strftime function (datetime library).
'''


### 1. Import packages, load the dataset
import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime

df = pd.read_csv('C:/Users/***/Bakery sales.csv') # the real path is hidden

### 2. Processing dates and time arrays
df['date'] = df['date'].apply(pd.to_datetime, errors='raise', utc='True', dayfirst=False, format=('%Y-%m-%d'))
df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d')) # this is MySQL standard date format

df['time'] = df['time'].apply(pd.to_datetime, errors='raise', utc='True', format=('%H:%M'))
df['time'] = df['time'].apply(lambda x: x.strftime('%H:%M:%S'))

### 3. Creating MySQL queries

query_cr_tb = '''CREATE TABLE sales (
	id INT DEFAULT NULL,
    date DATE DEFAULT NULL,
    time TIME DEFAULT NULL,
    ticket_number BIGINT DEFAULT NULL,
    article VARCHAR(50) DEFAULT NULL,
    quantity INT DEFAULT NULL,
    unit_price VARCHAR(50) NULL
);
'''

query_ins = '''INSERT INTO sales (
				id,
                date,
                time,
                ticket_number,
                article,
                quantity,
                unit_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''

### 4. Connecting to MySQL server
# Note: due to the privacy reasons, the information related to the server access is hidden
connection = mysql.connector.connect(host='127.*.*.*',
            user='****',
            password='****',
            database='prj_frenchbaker'
)

cursor = connection.cursor()

### 5. Queries execution
# Creating table
cursor.execute(query_cr_tb)

# Inserting values
cursor.executemany(query_ins, list(df.values))
connection.commit()

# Closing the connection
connection.close()
cursor.close()
