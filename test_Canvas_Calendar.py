import logging
from pip._vendor import requests
import pyodbc
import os
import io
from datetime import datetime, timedelta
import time
import json
# from itertools import product
import gzip
# import shutil
import pandas as pd
import traceback
import numpy as np

idag = datetime.now()
igår = idag - timedelta(days=1)
år = idag.year
måned = idag.month
dag = idag.day

CD2_base_url = "https://api-gateway.instructure.com"
CD2_client_id = os.environ['CD2_client_id']
CD2_client_secret = os.environ['CD2_client_secret']



def query_canvas_graphql(query: str, variables: dict):
    """Send a GraphQL query to Canvas and return the response."""
    try:
        svar = requests.post(
            "https://hvl.instructure.com/api/graphql",
            json={"query": query, "variables": variables},
            headers={"Authorization": f"Bearer {os.environ['tokenCanvas']}"},
        )
        svar.raise_for_status()
        return svar
    except requests.exceptions.RequestException as exc:
        raise exc


def query_FS_graphql(query, variable):
    """
    Send a GraphQL query to Felles Studentsystem and return the response.

    Parameters
    ----------
    query : str
        The GraphQL query to send.
    variable : dict
        A dictionary of variables to send with the query.

    Returns
    -------
    Response
        The response from the server.

    Raises
    ------
    requests.exceptions.RequestException
        If there is an error with the request.
    """
    hode = {
        'Accept': 'application/json;version=1',
        'Authorization': f'Basic {os.environ["tokenFS"]}',
        "Feature-Flags": "beta, experimental"
    }
    try:
        svar = requests.post(
            "https://api.fellesstudentsystem.no/graphql/", 
            json = {'query': query, 'variables': variable}, 
            headers=hode,
        )
        svar.raise_for_status()
        return svar
    except requests.exceptions.RequestException as exc:
        raise exc


def hent_CD2_access_token():
    """
    Hent access token frå Canvas Data 2.

    Returns
    -------
    str
        The access token or an error message if the request fails.

    Raises
    ------
    requests.exceptions.RequestException
        If there is an error with the request.
    """

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
    """
    Hent fila med namn innfil frå svar.

    Parameters
    ----------
    innfil : str
        Namnet på fila som skal hentast.
    token : str
        Access token for å hente fila.
    svar : dict
        Svar frå Canvas Data 2.

    Returns
    -------
    str
        Innholdet i fila.

    Raises
    ------
    requests.exceptions.RequestException
        If there is an error with the request.
    """
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


def timer_Canvas_Calendar():
    start_Canvas_Calendar = time.perf_counter()
    def finn_lærar(rekke):
        if type(rekke['value.description']) is str:
            streng = rekke['value.description']
            return streng[0:streng.find("<br")]

    def finn_timeedit_id(rekke):
        if type(rekke['value.description']) is str:
            streng = rekke['value.description']
            return streng[streng.find("ID:")+4:streng.find("<", streng.find("ID:"))]

    CD2_access_token = hent_CD2_access_token()
    headers = {'x-instauth': CD2_access_token, 'Content-Type': 'text/plain'}

    tabell = "calendar_events"

    # Den følgjande koden kan eg ikkje bruke dersom eg ikkje har skrivetilgang til filområdet
    # sist_oppdatert = f"sist_oppdatert_{tabell}.txt"
    # if os.path.exists(sist_oppdatert):
    #     with open(sist_oppdatert, "r") as f_in:
    #         sist_oppdatert_dato = f_in.read()
    #         payload = '{"format": "csv", "since": \"%s\"}' %(sist_oppdatert_dato)
    #         print("lest_fil OK")
    # else:
    #     payload = '{"format": "csv"}'

    # Her set eg sist_oppdatert til "i går" (meir presist "kl. 12 føremiddag i går")
    sist_oppdatert_dato = f"{igår.year}-{igår.month:02}-{igår.day:02}T{12}:00:00Z"
    payload = '{"format": "csv", "since": \"%s\"}' %(sist_oppdatert_dato)

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
                    print(respons2)
                    vent = False
        else:
            print(f"Feil i spørjing mot CD2, kode {r.status_code}")
        antal = len(respons2['objects'])
        data_i_dag = []
        for i in range(antal):
            data = io.StringIO(hent_filar(respons2['objects'][i]['id'], CD2_access_token, respons2))
            df = pd.read_csv(data, sep=",")
            data_i_dag.append(df)
        print(f"{idag}: har henta {len(data_i_dag)} filer med kalenderhendingar")
    except RuntimeError:
        print(f"{idag}: får ikkje lasta data frå kalenderen i Canvas")

    les_inn_startdata = data_i_dag[0]
    startdata = les_inn_startdata.loc[(les_inn_startdata['value.start_at'] > '2023') & (les_inn_startdata['value.workflow_state'] == 'active')][['key.id', 'value.title', 'value.start_at', 'value.end_at', 'value.location_name', 'value.description', 'value.context_code']]
    startdata['teacher'] = None
    # startdata['teacher'] = startdata.apply(finn_lærar, axis=1)
    startdata['timeedit_id'] = None
    # startdata['timeedit_id'] = startdata.apply(finn_timeedit_id, axis=1)
    liste_av_datarammer = [startdata]
    for les_inn_data in data_i_dag[1:]:
        data = les_inn_data.loc[(les_inn_data['value.start_at'] > '2023') & (les_inn_data['value.workflow_state'] == 'active')][['key.id', 'value.title', 'value.start_at', 'value.end_at', 'value.location_name', 'value.description', 'value.context_code']]
        liste_av_datarammer.append(data)
    behandla = pd.concat(liste_av_datarammer, ignore_index=True)
    behandla['teacher'] = behandla.apply(finn_lærar, axis=1)
    behandla['timeedit_id'] = behandla.apply(finn_timeedit_id, axis=1)
    dikt = behandla.to_dict('records')

    try:
        # conn_str = os.environ["Connection_SQL"]
        conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:hvl-data-db-server.database.windows.net,1433;Database=HVL-db;Uid=Admin-hvl;Pwd=BergenByErFin1;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
        with pyodbc.connect(conn_str) as cnxn:
            data_to_insert = []
            for hending in dikt[0:10]:
                print(f"Legg inn hending {hending['key.id']}")
                id = hending['key.id']
                title = hending['value.title']
                start_at = hending['value.start_at']
                end_at = hending['value.end_at']
                rom = hending['value.location_name']
                if "<" in rom:
                    n = rom.find("<")
                    location_name = rom[0:n-1]
                else:
                    location_name = rom
                description = hending['value.description']
                context_code = hending['value.context_code']
                teacher = hending['teacher']
                timeedit_id = hending['timeedit_id']
                data_to_insert.append((int(id),
                                    title,
                                    start_at,
                                    end_at,
                                    location_name,
                                    description,
                                    context_code,
                                    teacher,
                                    timeedit_id))
            query = """
                MERGE INTO [stg].[canvas_timeplan] AS t
                USING (VALUES (?,?,?,?,?,?,?,?,?)) AS s(
                    [id],
                    [title],
                    [start_at],
                    [end_at],
                    [location_name],
                    [description],
                    [context_code],
                    [teacher],
                    [timeedit_id]
                )
                ON t.[id] = s.[id]
                WHEN MATCHED THEN
                    UPDATE SET
                        t.[title] = s.[title],
                        t.[start_at] = s.[start_at],
                        t.[end_at] = s.[end_at],
                        t.[location_name] = s.[location_name],
                        t.[description] = s.[description],
                        t.[context_code] = s.[context_code],
                        t.[teacher] = s.[teacher],
                        t.[timeedit_id] = s.[timeedit_id]
                WHEN NOT MATCHED THEN
                    INSERT (
                        [id],
                        [title],
                        [start_at],
                        [end_at],
                        [location_name],
                        [description],
                        [context_code],
                        [teacher],
                        [timeedit_id]
                    )
                    VALUES (
                        s.[id],
                        s.[title],
                        s.[start_at],
                        s.[end_at],
                        s.[location_name],
                        s.[description],
                        s.[context_code],
                        s.[teacher],
                        s.[timeedit_id]
                    );
            """
            with cnxn.cursor() as cursor:
                for data in data_to_insert:
                    cursor.execute(query, data)
                cnxn.commit()
        print(f"Har lasta opp {len(dikt)} kalender-hendingar til databasen")
    except RuntimeError:
        print("Feil når eg skal legge timeplan-data inn i tabellen.")
    print(f"Tidsbruk Canvas_Calendar: {time.perf_counter() - start_Canvas_Calendar} s")

if os.path.exists("timerRequest.log"):
    os.remove("timerRequest.log")
logging.basicConfig(filename='timerRequest.log', encoding='utf-8', level=logging.INFO)
try:
    timer_Canvas_Calendar()
except:
    logging.exception("Feil i timer_Canvas_Calendar")