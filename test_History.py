import logging
import requests
import pyodbc
import os
import io
from datetime import datetime, date, timedelta
import time
import json
import gzip
import pandas as pd
import traceback
import numpy as np

idag = datetime.now()
igår = idag - timedelta(days=1)
år = idag.year
måned = idag.month
dag = idag.day
if måned <= 7:
    termin = "VÅR"
else:
    termin = "HØST"

CD2_base_url = "https://api-gateway.instructure.com"
CD2_client_id = os.environ['CD2_client_id']
CD2_client_secret = os.environ['CD2_client_secret']
conn_str = os.environ["Connection_SQL"]


def timer_Canvas_History():
    start_Canvas_History = time.perf_counter()
    headers = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}
    url_template = 'https://hvl.instructure.com/api/v1/users/{0}/history'
    query = "DELETE FROM [stg].[Canvas_History]"
    with pyodbc.connect(conn_str) as cnxn:
        with cnxn.cursor() as cursor:
            # cursor.execute(query)
            cnxn.commit()
            query = """
                SELECT DISTINCT [stg].[Canvas_Users].[user_id]
                FROM [stg].[Canvas_Users]
                LEFT JOIN [stg].[Canvas_Enrollments]
                    ON [stg].[Canvas_Enrollments].[user_id] = [stg].[Canvas_Users].[user_id]
                    WHERE [stg].[Canvas_Enrollments].[user_id] is not null and (last_login > getdate()-3  or last_activity_at > getdate()-3)

            """
            cursor.execute(query)
            user_ids = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Henta {len(user_ids)} brukarar")
            for user_id in user_ids:
                url = url_template.format(user_id)
                response = requests.get(url, headers=headers)
                for item in response.json():
                    try:
                        visitedAt = item['visited_at']
                    except:
                        visitedAt = ''
                    try:
                        visitedURL = item['visited_url']
                    except:
                        visitedURL = ''
                    try:
                        assetReadableCategory = item['asset_readable_category']
                    except:
                        assetReadableCategory = ''
                    userId = user_id
                    query = "INSERT INTO [stg].[Canvas_History] (visited_at, visited_url, asset_readable_category, user_id) VALUES (?,?,?,?)"
                    cursor.execute(query, visitedAt, visitedURL,
                                   assetReadableCategory, userId)
                    cnxn.commit()
            query = "EXEC dbo.Populate_dbo_Canvas_History"
            cursor.execute(query)
            cnxn.commit()
            logger.debug("Data lasta opp til Canvas_History")
    logger.info(f"Tidsbruk Canvas_History: {time.perf_counter() - start_Canvas_History} s")

rutine="History"
if os.path.exists(f'loggfil-{rutine}.log'):
    os.remove(f'loggfil-{rutine}.log')

# Opprett logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)  # Sett ønsket loggnivå

# Opprett formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Opprett filhandler for å logge til fil
file_handler = logging.FileHandler(f'loggfil-{rutine}.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# Opprett konsollhandler for å logge til konsollen
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

# Legg til handlerne i loggeren
logger.addHandler(file_handler)
logger.addHandler(console_handler)



timer_Canvas_History()
