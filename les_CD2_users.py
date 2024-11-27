import requests
import pandas as pd
import gzip
from datetime import datetime, timedelta, date
import time
import os
import io
import pyodbc

CD2_base_url = os.environ['CD2_base_url']
CD2_client_id = os.environ['CD2_client_id']
CD2_client_secret = os.environ['CD2_client_secret']
conn_str = os.environ["Connection_SQL"]

idag = date.today().isoformat()
igår = (date.today() - timedelta(days=1)).isoformat()

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
    sist_oppdatert = akv_finn_sist_oppdatert(tabell)
    sist_oppdatert = "2024-10-30T00:00:00Z"
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
        return alledata, respons2['until']
    except requests.exceptions.RequestException as exc:
        # raise exc
        return None


def akv_lagre_sist_oppdatert(tabell, sist_oppdatert):
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
    return None


start_CD2_users = time.perf_counter()
tabell = "users"
alledata, sist_oppdatert = akv_les_CD2_tabell(tabell)
alledata.to_csv(f"users_{sist_oppdatert}.csv", index=False)
temp = alledata[alledata['value.created_at'] >= igår]
nye_brukarar = temp[~temp.apply(lambda row: row.astype(str).str.contains('student', case=False).any(), axis=1)]

# Finn sis_user_id for kvar av dei nye brukarane
nye_liste = []
for user_id in nye_brukarar['key.id']:
    url = f"https://hvl.instructure.com/api/v1/users/{user_id}"
    hodeCanvas = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}
    respons = requests.get(url, headers=hodeCanvas)
    if 200 <= respons.status_code < 300:
        data = respons.json()
        nye_liste.append([user_id, data['sis_user_id']])
nye = pd.DataFrame(nye_liste, columns=['user_id', 'sis_user_id'])

query = """
    MERGE INTO [dbo].[akv_user_id_kobling] AS target 
    USING (VALUES (?, ?)) AS source (user_id, sis_user_id) 
    ON target.[user_id] = source.[user_id]
    WHEN MATCHED THEN
        UPDATE SET target.[sis_user_id] = source.[sis_user_id]
    WHEN NOT MATCHED THEN
        INSERT ([user_id], [sis_user_id]) VALUES (source.[user_id], source.[sis_user_id]);
"""

# Oppdater tabellen akv_user_id_kobling 
with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()
    # Upsert the data
    for index, row in nye.iterrows():
        user_id = str(row[0])
        sis_user_id = str(row[1])
        cursor.execute(query, (user_id, sis_user_id))
    conn.commit()


# Lagre tidspunktet for siste oppdatering
akv_lagre_sist_oppdatert(tabell, sist_oppdatert)

# Lagre CSV med nye brukarar (sånn for sikkerhets skuld)
nye.to_csv(f"nye_users_{igår}.csv", index=False)

# Og gi ei kvittering om at eg er ferdig.
print(f"Tabell: {tabell}, Sist oppdatert: {sist_oppdatert}, Antall nye brukarar: {len(nye)}")
print(f"Totalt for CD2_Users: {time.perf_counter() - start_CD2_users}")

