import pyodbc
import pandas as pd
import os

conn_str = os.environ["Connection_SQL"]


# CSV file path
innfil = 'users_REST_241017.csv'

# Read the CSV file
df = pd.read_csv(innfil).dropna(axis=0, how='any') 

# Create a connection to Azure



query = """
    MERGE INTO [dbo].[akv_user_id_kobling] AS target 
    USING (VALUES (?, ?)) AS source (user_id, sis_user_id) 
    ON target.[user_id] = source.[user_id]
    WHEN MATCHED THEN
        UPDATE SET target.[sis_user_id] = source.[sis_user_id]
    WHEN NOT MATCHED THEN
        INSERT ([user_id], [sis_user_id]) VALUES (source.[user_id], source.[sis_user_id]);
"""
with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()
    # Upsert the data
    for index, row in df.iterrows():
        user_id = str(row[0])
        sis_user_id = str(row[1])
        cursor.execute(query, (user_id, sis_user_id))
    conn.commit()