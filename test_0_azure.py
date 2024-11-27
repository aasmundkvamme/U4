import pyodbc
import os

conn_str = os.environ["Connection_SQL"]


with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()
    query = """
    SELECT TOP (10) * FROM [stg].[Canvas_Courses]
    """
    cursor.execute(query)
    row = cursor.fetchmany(4)
    if row:
        print(row)