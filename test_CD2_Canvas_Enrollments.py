import pandas as pd
import os
import requests
from datetime import datetime, date, timedelta
import time
import logging
import traceback
import io
import gzip

CD2_base_url = "https://api-gateway.instructure.com"
CD2_client_id = os.environ['CD2_client_id']
CD2_client_secret = os.environ['CD2_client_secret']
conn_str = os.environ["Connection_SQL"]


def akv_hent_CD2_access_token():
    be_om_access_token = requests.request(
        "POST",
        f"{CD2_base_url}/ids/auth/login",
        data={'grant_type': 'client_credentials'},
        auth=(CD2_client_id, CD2_client_secret)
        )
    if be_om_access_token.status_code == 200:
        CD2_access_token = be_om_access_token.json()['access_token']
        return CD2_access_token
    else:
        feilmelding = f"Klarte ikkje å skaffe access_token, feil {be_om_access_token.status_code}"
        logging.error(feilmelding)
        return feilmelding


def akv_finn_sist_oppdatert(tabell):
    if os.path.exists(f"sist_oppdatert_{tabell}.txt"):
        with open("sist_oppdatert_{tabell}.txt", "r") as f_in:
            return f_in.read()
    else:
        return (datetime.now()-timedelta(days=1)).isoformat() + "Z"


def akv_lagre_sist_oppdatert(tabell, sist_oppdatert):
    with open(f"sist_oppdatert_{tabell}.txt", "w") as f:
        f.write(sist_oppdatert)


def akv_hent_CD2_fil(innfil, token, svar):
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
    payload = '{"format": "csv", "since": \"%s\"}' % (forrige_oppdatering)
    requesturl = f"{CD2_base_url}/dap/query/canvas/table/{tabell}/data"
    logging.info(f"Sender søk til {requesturl}")
    try:
        start_finn_filar = time.perf_counter()
        r = requests.request("POST", requesturl, headers=headers, data=payload)
        r.raise_for_status()
        respons = r.json()
        id = respons['id']
        vent = True
        while vent:
            logging.info(f"Vent til fila blir tilgjengelig: {requesturl}/job/{id}")
            requesturl2 = f"{CD2_base_url}/dap//job/{id}"
            r2 = requests.request("GET", requesturl2, headers=headers)
            time.sleep(5)
            respons2 = r2.json()
            if respons2['status'] == "complete":
                logging.info(f"Fila(ne) er tilgjengeleg(e): {requesturl}/job/{id}")
                vent = False
                filer = respons2['objects']
                sist_oppdatert = respons2['until']
        logging.info(f"Totalt for {tabell}_finn_filer: {time.perf_counter() - start_finn_filar}")
    except requests.exceptions.RequestException as exc:
        raise exc
    dr_liste = []
    start_les_filer = time.perf_counter()
    for fil in filer:
        data = io.StringIO(akv_hent_CD2_fil(fil['id'], CD2_access_token, respons2))
        df = pd.read_csv(data, sep=",")
        dr_liste.append(df)
    alledata = pd.concat(df for df in dr_liste if not df.empty)
    logging.info(f"Totalt for {tabell}_les_filer: {time.perf_counter() - start_les_filer}")
    return alledata, sist_oppdatert

tabell = "enrollments"
logging.basicConfig(filename=f'{tabell}.log', encoding='utf-8', level=logging.INFO)
forrige_oppdatering = akv_finn_sist_oppdatert(tabell)
alledata, sist_oppdatert = akv_les_CD2_tabell(tabell)
akv_lagre_sist_oppdatert(tabell, sist_oppdatert)
enrollments = alledata[['key.id', 'value.user_id', 'value.course_id', 'value.type', 'value.created_at', 'value.updated_at', 'value.start_at', 'value.end_at', 'value.workflow_state', 'value.total_activity_time', 'value.last_activity_at']]
nye = enrollments[enrollments['value.created_at'] > forrige_oppdatering]

# For testing lokalt. Kan fjernes etter testing.
nye.to_csv(f"nye_{tabell}_{sist_oppdatert[0:10]}.csv", index=False)

for index, row in nye.iterrows():
    entity = {
        'PartitionKey': str(row['value.course_id']),
        'RowKey': str(row['key.id']),
        'user_id': str(row['value.user_id']),
        'type': str(row['value.type']),
        'created_at': str(row['value.created_at']),
        'updated_at': str(row['value.updated_at']),
        'start_at': str(row['value.start_at']),
        'end_at': str(row['value.end_at']),
        'workflow_state': str(row['value.workflow_state']),
        'total_activity_time': str(row['value.total_activity_time']),
        'last_activity_at': str(row['value.last_activity_at'])
    }
    try:
        table_service.insert_or_replace_entity(table_name, entity)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
