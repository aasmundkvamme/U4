import requests
import pandas as pd
import gzip
from datetime import datetime, timedelta, date
import time
import os
import logging
import io
import pyodbc


def main():
    start_CD2_pseudonyms = time.perf_counter()
    global CD2_tabell
    CD2_tabell = "pseudonyms"
    init()
    alledata, sist_oppdatert, denne_oppdateringa = akv_les_CD2_tabell(CD2_tabell)
    alle_nye = alledata[(alledata['value.created_at']>sist_oppdatert)]
    alle_nye.to_csv(f"{CD2_tabell}_nye_{denne_oppdateringa[0:10]}.csv", index=False)
    ekte_nye = alle_nye.dropna(subset='value.sis_user_id')
    oppdater_Azure(ekte_nye)
    akv_lagre_sist_oppdatert(CD2_tabell, denne_oppdateringa)
    print(f"Tabell: {CD2_tabell} er oppdatert {denne_oppdateringa}")
    print(f"Total tidsbruk: {time.perf_counter() - start_CD2_pseudonyms}")


def init():
    global CD2_base_url, CD2_client_id, CD2_client_secret, conn_str, idag, igår, logger
    CD2_base_url = "https://api-gateway.instructure.com"
    CD2_client_id = os.environ['CD2_client_id']
    CD2_client_secret = os.environ['CD2_client_secret']
    conn_str = os.environ["Connection_SQL"]
    idag = date.today().isoformat()
    igår = (date.today() - timedelta(days=1)).isoformat()
    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.DEBUG)  # Sett ønsket loggnivå
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(f'loggfil-{CD2_tabell}-{idag}.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def akv_hent_CD2_access_token():
    try:
        respons = requests.request(
            "POST",
            f"{CD2_base_url}/ids/auth/login",
            data={'grant_type': 'client_credentials'},
            auth=(CD2_client_id, CD2_client_secret)
        )
        respons.raise_for_status()
        logger.debug("Har henta access_token")
        return respons.json()['access_token']
    except requests.exceptions.RequestException as exc:
        raise exc


def akv_finn_sist_oppdatert(tabell):
    """
    Return the latest update time for the given table from the akv_sist_oppdatert table.
    If no date is given, return yesterday.
    """
    try:
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            query = """
            SELECT [sist_oppdatert] FROM [dbo].[akv_sist_oppdatert]
            WHERE [tabell] = ?
            """
            cursor.execute(query, (tabell,))
            row = cursor.fetchone()
            if row:
                logger.debug(f"{tabell} er sist oppdatert (Azure): {row[0].isoformat() + 'Z'}")
                return row[0].isoformat() + "Z"
            
    except pyodbc.Error as exc:
        logger.debug(f"{tabell} er sist oppdatert (lokal): {(date.today() - timedelta(days=1)).isoformat() + 'Z'}") 
        return (date.today() - timedelta(days=1)).isoformat() + "Z"


def akv_lagre_sist_oppdatert(tabell, dato):
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = """
            MERGE INTO [dbo].[akv_sist_oppdatert] AS target 
            USING (VALUES (?, ?)) AS source (tabell, sist_oppdatert) 
            ON target.[tabell] = source.[tabell]
            WHEN MATCHED THEN
                UPDATE SET target.[sist_oppdatert] = source.[sist_oppdatert]
            WHEN NOT MATCHED THEN
                INSERT ([tabell], [sist_oppdatert]) VALUES (source.[tabell], source.[sist_oppdatert]);
            """ 
            cursor.execute(query, (tabell, dato))
            conn.commit()
            logger.debug(f"{tabell} er sist oppdatert (Azure): {dato}")
    except pyodbc.Error as e:
        with open(f'sist_oppdatert_{tabell}.txt', 'w') as f_out:
            f_out.write(dato)
            logger.debug(f"{tabell} er sist oppdatert (lokal): {dato}")
    return None


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
    sist_oppdatert = akv_finn_sist_oppdatert(tabell)
    payload = '{"format": "csv", "since": \"%s\"}' % (sist_oppdatert)
    requesturl = f"{CD2_base_url}/dap/query/canvas/table/{tabell}/data"
    print(f"Sender søk til {requesturl}")
    try:
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
        dr_liste = []
        print(filar)
        for fil in filar:
            data = io.StringIO(akv_hent_CD2_filar(fil['id'], CD2_access_token, respons2))
            df = pd.read_csv(data, sep=",")
            dr_liste.append(df)
        alledata = pd.concat(df for df in dr_liste if not df.empty)
        return alledata, sist_oppdatert, respons2['until']
    except requests.exceptions.RequestException as exc:
        raise exc


def oppdater_Azure(data):
    query = """
        MERGE INTO [dbo].[akv_user_id_kobling] AS target 
        USING (VALUES (?, ?)) AS source (user_id, sis_user_id) 
        ON target.[user_id] = source.[user_id]
        WHEN MATCHED THEN
            UPDATE SET target.[sis_user_id] = source.[sis_user_id]
        WHEN NOT MATCHED THEN
            INSERT ([user_id], [sis_user_id]) VALUES (source.[user_id], source.[sis_user_id]);
    """
    nye = data[['value.user_id', 'value.sis_user_id']]
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        for index, row in nye.iterrows():
            user_id = str(row[0])
            sis_user_id = str(row[1])
            cursor.execute(query, (user_id, sis_user_id))
        conn.commit()


if __name__ == "__main__":
    main()