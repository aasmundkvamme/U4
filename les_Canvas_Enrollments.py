import logging
import requests
import pyodbc
import os
import io
# import azure.functions as func
from datetime import datetime, timedelta, date
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

CD2_base_url = os.environ['CD2_base_url']
CD2_client_id = os.environ['CD2_client_id']
CD2_client_secret = os.environ['CD2_client_secret']
conn_str = os.environ["Connection_SQL"]

idag = date.today().isoformat()
igår = (date.today() - timedelta(days=1)).isoformat()
tid_logg = {}

def akv_hent_CD2_access_token():
    try:
        respons = requests.request(
            "POST",
            f"{CD2_base_url}/ids/auth/login",
            data={'grant_type': 'client_credentials'},
            auth=(CD2_client_id, CD2_client_secret)
        )
        respons.raise_for_status()
        return respons.json()['access_token']
    except requests.exceptions.RequestException as exc:
        raise exc

def akv_finn_sist_oppdatert(tabell):
    """
    Return the latest update time for the given table from the akv_sist_oppdatert table.
    If no date is given, return yesterday.
    """
    with pyodbc.connect(conn_str) as connection:
        cursor = connection.cursor()
        try:
            query = """
            SELECT [sist_oppdatert] FROM [dbo].[akv_sist_oppdatert]
            WHERE [tabell] = ?
            """
            cursor.execute(query, (tabell,))
            row = cursor.fetchone()
            if row:
                return row[0].isoformat() + "Z"
        except pyodbc.Error as exc:
            return (date.today() - timedelta(days=1)).isoformat() + "Z"


def akv_hent_CD2_filar(innfil, token, svar):
    try:
        requesturl = f"{CD2_base_url}/dap/object/url"
        payload = f"{svar['objects']}"
        payload = payload.replace('\'', '\"')
        headers = {'x-instauth': token, 'Content-Type': 'text/plain'}
        respons = requests.request("POST", requesturl, headers=headers, data=payload)
        respons.raise_for_status()
        fil = respons.json()
        url = fil['urls'][innfil]['url']
        data = requests.request("GET", url)
        buffer = io.BytesIO(data.content)
        with gzip.GzipFile(fileobj=buffer, mode='rb') as utpakka_fil:
            utpakka_data = utpakka_fil.read().decode()
        return utpakka_data
    except requests.exceptions.RequestException as exc:
        raise exc

def akv_les_CD2_tabell(tabell):
    CD2_access_token = akv_hent_CD2_access_token()
    headers = {'x-instauth': CD2_access_token, 'Content-Type': 'text/plain'}
    forrige_oppdatering = akv_finn_sist_oppdatert(tabell)
    payload = '{"format": "csv", "since": \"%s\"}' % (forrige_oppdatering)
    requesturl = f"{CD2_base_url}/dap/query/canvas/table/{tabell}/data"
    print(f"Sender søk til {requesturl}")
    try:
        start_hente_oppdatert = time.perf_counter()
        r = requests.request("POST", requesturl, headers=headers, data=payload)
        r.raise_for_status()
        respons = r.json()
        id = respons['id']
        vent = True
        while vent:
            requesturl2 = f"{CD2_base_url}/dap//job/{id}"
            r2 = requests.request("GET", requesturl2, headers=headers)
            time.sleep(5)
            respons2 = r2.json()
            print(respons2)
            if respons2['status'] == "complete":
                vent = False
                filar = respons2['objects']
        tid_logg['hente_oppdatert'] = time.perf_counter() - start_hente_oppdatert
        dr_liste = []
        # print(filar)
        start_hente_filar = time.perf_counter()
        for fil in filar:
            data = io.StringIO(akv_hent_CD2_filar(fil['id'], CD2_access_token, respons2))
            df = pd.read_csv(data, sep=",")
            dr_liste.append(df)
        alledata = pd.concat(df for df in dr_liste if not df.empty)
        tid_logg['hente_filar'] = time.perf_counter() - start_hente_filar
        return alledata, forrige_oppdatering, respons2['until']
    except requests.exceptions.RequestException as exc:
        raise exc
        # return None, None


def akv_lagre_sist_oppdatert(tabell, sist_oppdatert):
    start_oppdatering = time.perf_counter()
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        try:
            query = """
            MERGE INTO [dbo].[akv_sist_oppdatert] AS target 
            USING (VALUES (?, ?)) AS source (tabell, sist_oppdatert) 
            ON target.[tabell] = source.[tabell]
            WHEN MATCHED THEN
                UPDATE SET target.[sist_oppdatert] = source.[sist_oppdatert]
            WHEN NOT MATCHED THEN
                INSERT ([tabell], [sist_oppdatert]) VALUES (source.[tabell], source.[sist_oppdatert]);
            """ 
            cursor.execute(query, (tabell, sist_oppdatert))
            conn.commit()
        except pyodbc.Error as e:
            print(f"Feil ved opplasting av sist oppdatert: {e}")
    tid_logg['sist_oppdatert'] = time.perf_counter() - start_oppdatering
    return None


def timer_Canvas_Enrollments_Ny():
    start_Canvas_Enrollments_Ny = time.perf_counter()
    tabell = "enrollments"
    alledata, forrige_oppdatering, sist_oppdatert = akv_les_CD2_tabell(tabell)
    akv_lagre_sist_oppdatert(tabell, sist_oppdatert)
    start_analyse = time.perf_counter()
    temp = alledata[alledata['value.created_at'] >= forrige_oppdatering]
    enrollments = temp[['key.id','value.user_id','value.course_id','value.type','value.created_at','value.updated_at','value.start_at','value.end_at','value.workflow_state','value.total_activity_time','value.last_activity_at']]
    # Sett inn kode for å oppdatere Azure her.
    
    enrollments.to_csv(f"enrollments_{sist_oppdatert}.csv", index=False)
    tid_logg['analyse'] = time.perf_counter() - start_analyse
    tid_logg['Canvas_Enrollments_Ny'] = time.perf_counter() - start_Canvas_Enrollments_Ny

logging.basicConfig(filename='timerRequest.log', encoding='utf-8', level=logging.INFO)
timer_Canvas_Enrollments_Ny()
df = pd.DataFrame.from_dict(tid_logg, orient='index')
df.to_csv('tid_logg.csv')