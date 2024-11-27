import logging
import requests
import pyodbc
import os
import io
# import azure.functions as func
from datetime import datetime, timedelta
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


def timer_Canvas_Users():
    start_Canvas_Users = time.perf_counter()
    headers = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}
    parametre = {'per_page': 100, 'page': 1}
    base_url = "https://hvl.instructure.com/api/v1/accounts/54/users"
    params = {"page": 1, "per_page": 100}
    with pyodbc.connect(conn_str) as cnxn:
        with cnxn.cursor() as cursor:
            page = 1
            while True:
                response = requests.get(base_url, headers=headers, params=params)
                data = response.json()
                while 'next' in response.links:
                    next_url = response.links['next']['url']
                    print(f"Hentar {next_url}")
                    response = requests.get(next_url, headers=headers)
                    data.extend(response.json())
                for item in data:
                    user_id = item['id']
                    sis_user_id = item['sis_user_id']
                    created_at = item['created_at']
                    root_account = item.get('root_account')
                    try:
                        last_login = item['last_login']
                    except:
                        last_login = ""
                    try:
                        login_id = item['login_id']
                    except:
                        login_id = ""
                    merge_query = """
                        MERGE INTO [stg].[Canvas_Users] WITH (HOLDLOCK) AS t \
                        USING (VALUES (?,?,?,?,?,?)) AS s(
                            [user_id],
                            [sis_user_id],
                            [created_at],
                            [root_account],
                            [last_login],
                            [login_id]
                        )
                        ON t.[user_id] = s.[user_id]
                        WHEN MATCHED THEN
                            UPDATE SET
                            t.[sis_user_id] = s.[sis_user_id],
                            t.[created_at] = s.[created_at],
                            t.[root_account] = s.[root_account],
                            t.[last_login] = s.[last_login],
                            t.[login_id] = s.[login_id]
                        WHEN NOT MATCHED THEN
                            INSERT (
                                [user_id],
                                [sis_user_id],
                                [created_at],
                                [root_account],
                                [last_login],
                                [login_id]
                            )
                            VALUES (
                                s.[user_id],
                                s.[sis_user_id],
                                s.[created_at],
                                s.[root_account],
                                s.[last_login],
                                s.[login_id]
                            );
                    """
                #     cursor.execute(merge_query, user_id, sis_user_id, created_at, root_account, last_login, login_id)
                # cnxn.commit()
            logging.debug("Data lasta opp til Canvas_Users")
    logging.info(f"Tidsbruk Canvas_Users: {time.perf_counter() - start_Canvas_Users} s")

logging.basicConfig(filename='timerRequest.log', encoding='utf-8', level=logging.INFO)
timer_Canvas_Users()
