import requests
import json
import aasmund
import pandas as pd
import gzip
import shutil
from datetime import datetime, timedelta
import time
import re
import locale
import os
import pyodbc

base_url = aasmund.CD2_base_url
client_id = aasmund.CD2_client_id
client_secret = aasmund.CD2_client_secret
conn_str = os.environ["Connection_SQL"]

idag = datetime.now()
igår = idag - timedelta(days=1)


def akv_finn_sist_oppdatert(table_name):
    """
    Return the latest update time for the given table from the akv_sist_oppdatert table.
    """
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        try:
            query = """
            SELECT [sist_oppdatert] FROM [dbo].[akv_sist_oppdatert]
            WHERE [tabell] = ?
            """
            cursor.execute(query, (table_name,))
            row = cursor.fetchone()
            if row:
                return row[0].strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                return igår.strftime("%Y-%m-%dT%H:%M:%SZ")
        except pyodbc.Error as exc:
            return igår.strftime("%Y-%m-%dT%H:%M:%SZ")

print(akv_finn_sist_oppdatert("modules"))