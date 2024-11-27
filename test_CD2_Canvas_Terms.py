import logging
from pip._vendor import requests
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
# conn_str = os.environ["Connection_SQL"]


def hent_CD2_access_token():
    be_om_access_token = requests.request(
        "POST",
        f"{CD2_base_url}/ids/auth/login",
        data={'grant_type': 'client_credentials'},
        auth=(CD2_client_id, CD2_client_secret)
        )
    if be_om_access_token.status_code == 200:
        CD2_access_token = be_om_access_token.json()['access_token']
        print("Henta access_token OK")
        return CD2_access_token
    else:
        feilmelding = f"Klarte ikkje å skaffe access_token, feil {be_om_access_token.status_code}"
        print(feilmelding)
        return feilmelding


def hent_filar(innfil, token, svar):
    requesturl = f"{CD2_base_url}/dap/object/url"
    payload = f"{svar['objects']}"
    payload = payload.replace('\'', '\"')
    headers = {'x-instauth': token, 'Content-Type': 'text/plain'}
    r4 = requests.request("POST", requesturl, headers=headers, data=payload)
    if r4.status_code == 200:
        respons4 = r4.json()
        url = respons4['urls'][innfil]['url']
        data = requests.request("GET", url)
        buffer = io.BytesIO(data.content)
        with gzip.GzipFile(fileobj=buffer, mode='rb') as utpakka_fil:
            utpakka_data = utpakka_fil.read().decode()
    return utpakka_data

CD2_access_token = hent_CD2_access_token()
headers = {'x-instauth': CD2_access_token, 'Content-Type': 'text/plain'}

tabell = "enrollment_terms"
sist_oppdatert_dato = f"{igår.year}-{igår.month:02}-{igår.day:02}T{12}:00:00Z"
payload = '{"format": "csv", "since": \"%s\"}' % (sist_oppdatert_dato)

try:
    requesturl = f"{CD2_base_url}/dap/query/canvas/table/{tabell}/data"
    print(f"Sender søk til {requesturl}")
    r = requests.request("POST", requesturl, headers=headers, data=payload)
    if r.status_code == 200:
        respons = r.json()
        id = respons['id']
        vent = True
        while vent:
            requesturl = f"{CD2_base_url}/dap//job/{id}"
            r2 = requests.request("GET", requesturl, headers=headers)
            time.sleep(5)
            respons2 = r2.json()
            print(respons2)
            if respons2['status'] == "complete":
                vent = False
    else:
        print(f"Feil i spørjing mot CD2, kode {r.status_code}")
    antal = len(respons2['objects'])
    data_i_dag = []
    for i in range(antal):
        data = io.StringIO(hent_filar(
            respons2['objects'][i]['id'], CD2_access_token, respons2))
        df = pd.read_csv(data, sep=",")
        data_i_dag.append(df)
except RuntimeError:
    print(f"Får ikkje lasta data frå tabellen {tabell} i Canvas Data 2")
dataliste = [data_i_dag[0][['key.id', 'value.name', 'value.sis_source_id', 'value.term_code']]]
# for datasett in data_i_dag[1:]:
#     dataliste.append(datasett[['key.id', 'value.user_id', 'value.course_id', 'value.type', 'value.created_at', 'value.updated_at', 'value.start_at', 'value.end_at', 'value.workflow_state', 'value.total_activity_time', 'value.last_activity_at']])
# oppsamla = pd.concat(dataliste)
# oppsamla['sis_user_id'] = " "
# oppsamla['value.total_activity_time'] = oppsamla['value.total_activity_time'].fillna(0.0)
# oppsamla['value.total_activity_time'] = oppsamla['value.total_activity_time'].astype(int)
# # Ensure created_at, updated_at, start_at, end_at, and last_activity_at are datetime
# oppsamla['value.created_at'] = oppsamla['value.created_at'].apply(lambda x: None if np.isnan(x) else pd.to_datetime(x))
# oppsamla['value.updated_at'] = oppsamla['value.updated_at'].apply(lambda x: None if np.isnan(x) else pd.to_datetime(x))
# oppsamla['value.start_at'] = oppsamla['value.start_at'].apply(lambda x: None if np.isnan(x) else pd.to_datetime(x))
# oppsamla['value.end_at'] = oppsamla['value.end_at'].apply(lambda x: None if np.isnan(x) else pd.to_datetime(x))
# dikt = oppsamla.to_dict('records')
# logging.info(f"Har gjort klar {len(dikt)} enrollments til innlegging/oppdatering.")
