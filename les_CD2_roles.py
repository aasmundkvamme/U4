import requests
import os
import io
from datetime import datetime, timedelta, date
import time
import json
import gzip
import pandas as pd
import aasmund
import logging
import pyodbc

idag = datetime.now()
igår = idag - timedelta(days=1)
år = idag.year
måned = idag.month
dag = idag.day
if måned <= 7:
    termin = "VÅR"
else:
    termin = "HØST"

CD2_client_id = os.environ['CD2_client_id']
CD2_client_secret = os.environ['CD2_client_secret']
conn_str = os.environ["Connection_SQL"]


def akv_hent_CD2_access_token():
    try:
        respons = requests.request(
            "POST",
            f"https://api-gateway.instructure.com/ids/auth/login",
            data={'grant_type': 'client_credentials'},
            auth=(CD2_client_id, CD2_client_secret)
        )
        respons.raise_for_status()
        return respons.json()['access_token']
    except requests.exceptions.RequestException as exc:
        raise exc


def akv_hent_CD2_filar(innfil, token, svar):
    try:
        requesturl = f"https://api-gateway.instructure.com/dap/object/url"
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

def akv_finn_sist_oppdatert(tabell):
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
            cursor.execute(query, (tabell,))
            row = cursor.fetchone()
            if row:
                return row[0].isoformat() + "Z"
        except pyodbc.Error as exc:
            return (date.today() - timedelta(days=1)).isoformat() + "Z"


def akv_skriv_sist_oppdatert(tabell, sist_oppdatert):
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
            print(f"tabell: {tabell}, sist_oppdatert: {sist_oppdatert}")
            cursor.execute(query, (tabell, sist_oppdatert))
            conn.commit()
        except pyodbc.Error as e:
            print(f"Feil ved opplasting av sist oppdatert: {e}")

def akv_les_CD2_tabell(tabell):
    CD2_access_token = akv_hent_CD2_access_token()
    headers = {'x-instauth': CD2_access_token, 'Content-Type': 'text/plain'}
    sist_oppdatert = akv_finn_sist_oppdatert(tabell)
    payload = '{"format": "csv", "since": \"%s\"}' % (sist_oppdatert)
    print(payload)
    requesturl = f"https://api-gateway.instructure.com/dap/query/canvas/table/{tabell}/data"
    print(f"Sender søk til {requesturl}")
    try:
        r = requests.request("POST", requesturl, headers=headers, data=payload)
        r.raise_for_status()
        respons = r.json()
        id = respons['id']
        vent = True
        while vent:
            requesturl2 = f"https://api-gateway.instructure.com/dap//job/{id}"
            r2 = requests.request("GET", requesturl2, headers=headers)
            time.sleep(5)
            respons2 = r2.json()
            print(respons2)
            if respons2['status'] == "complete":
                print(respons2)
                vent = False
        filar = respons2['objects']
        sistoppdatert = respons2['at']
    except requests.exceptions.RequestException as exc:
        raise exc

    dr_liste = []
    for fil in filar:
        data = io.StringIO(akv_hent_CD2_filar(fil['id'], CD2_access_token, respons2))
        df = pd.read_csv(data, sep=",")
        dr_liste.append(df)
    alledata = pd.concat(df for df in dr_liste if not df.empty)
    return alledata, sistoppdatert

tabell = "roles"
alledata, sist_oppdatert = akv_les_CD2_tabell(tabell)

roller = alledata[['key.id', 'value.name']]
roller.to_csv("rollerliste_241010.csv", index=False)
