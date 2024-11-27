import logging
from pip._vendor import requests
import pyodbc
import os
import io
from datetime import datetime, timedelta
import time
import json
import gzip
import pandas as pd
import traceback

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
        logging.info("Henta access_token OK")
        return CD2_access_token
    else:
        feilmelding = f"Klarte ikkje å skaffe access_token, feil {be_om_access_token.status_code}"
        logging.error(feilmelding)
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


def timer_Canvas_Modules():
    start_Canvas_Modules = time.perf_counter()
    headers = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}
    url_template = 'https://hvl.instructure.com/api/v1/courses/{course_id}/modules?include[]=items&per_page=100&page={page_number}'
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        try:
            query = """
            SELECT ALL * FROM [stg].[Canvas_Terms]
            """
            cursor.execute(query)
            row = cursor.fetchall()
            terminar = []
            for t in row:
                try:
                    term_id = t[0]
                    name = t[1]
                    start_at = t[2]
                    end_at = t[3]
                    created_at = t[4]
                    terminar.append([term_id, name, start_at, end_at, created_at])
                except IndexError:
                    logging.error("Ingen data i denne raden: {t}")
        except pyodbc.Error as e:
            logging.error(f"Feil: {e}")
        aktuelle_terminar = []
        try:
            for t in terminar:
                desimal = int(år) + 0.5*(termin == 'HØST')
                if '-' in t[1]:
                    start_termin = t[1].split('-')[0]
                    slutt_termin = t[1].split('-')[1]
                    start_år = start_termin.split(' ')[0]
                    slutt_år = slutt_termin.split(' ')[0]
                    start_semester = start_termin.split(' ')[1]
                    slutt_semester = slutt_termin.split(' ')[1]
                    start_desimal = int(start_år) + 0.5*(start_semester == 'HØST')
                    slutt_desimal = int(slutt_år) + 0.5*(slutt_semester == 'HØST')
                    if start_desimal <= desimal <= slutt_desimal:
                        aktuelle_terminar.append(t[0])
                        logging.info(f"{t[1]} er i det aktuelle semesteret.")
                    else:
                        logging.info(f"{t[1]} er i det ikkje aktuelle semesteret.")
                        pass
                elif t[1] == f"{str(år)} {termin}":
                    aktuelle_terminar.append(t[0])
                    logging.info(f"{t[1]} er i det aktuelle semesteret.")
                else:
                    pass
        except:
            logging.error(f"Feil: {traceback.format_exc()}")

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        try:
            query = """
            SELECT ALL * FROM [stg].[Canvas_Courses]
            """
            cursor.execute(query)
            row = cursor.fetchall()
            aktuelle_emne = []
            for emne in row:
                try:
                    if emne[4] in aktuelle_terminar:
                        aktuelle_emne.append(emne[0])
                except IndexError:
                    pass
            logging.info(f"Det er {len(aktuelle_emne)} emne i dette semesteret.")
        except pyodbc.Error as e:
            logging.error(f"Feil: {e}")

    with pyodbc.connect(conn_str) as cnxn:
        cursor = cnxn.cursor()
        for emne in aktuelle_emne:
            page_number = 1
            while True:
                url = url_template.format(course_id=emne, page_number=page_number)
                response = requests.get(url, headers=headers)
                data = response.json()
                if not data or 'errors' in data:
                    break
                for item in data:
                    module_id = item['id']
                    name = item['name']
                    published = item['published']
                    items_count = item['items_count']
                    items_url = item['items_url']
                    course_id = emne
                    for items in item['items']:
                        module_item_id = items['id']
                        title = items['title']
                        type = items['type']
                        parent_module_id = items['module_id']
                        item_published = items['published']
                        if 'external_url' in item:
                            external_url = items['external_url']
                        else:
                            external_url = None
                        merge_query = """
                            MERGE INTO [stg].[Canvas_Modules] AS target
                            USING (SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS source (
                                [items_count],
                                [items_url],
                                [module_id],
                                [course_id],
                                [type],
                                [module_item_id],
                                [title],
                                [parent_module_id],
                                [external_url],
                                [item_published],
                                [name],
                                [published]
                            )
                            ON target.[module_id] = source.[module_id] and target.[module_item_id] = source.[module_item_id]
                            WHEN MATCHED THEN
                                UPDATE SET 
                                    target.[items_url] = source.[items_url],
                                    target.[items_count] = source.[items_count],
                                    target.[course_id] = source.[course_id],
                                    target.[type] = source.[type],
                                    target.[title] = source.[title],
                                    target.[parent_module_id] = source.[parent_module_id],
                                    target.[external_url] = source.[external_url],
                                    target.[item_published] = source.[item_published],
                                    target.[name] = source.[name],
                                    target.[published] = source.[published]
                            WHEN NOT MATCHED THEN
                                INSERT (
                                    [items_count],
                                    [items_url],
                                    [module_id],
                                    [course_id],
                                    [type],
                                    [module_item_id],
                                    [title],
                                    [parent_module_id],
                                    [external_url],
                                    [item_published],
                                    [name],
                                    [published]
                                )
                            VALUES (
                                source.[items_count],
                                source.[items_url],
                                source.[module_id],
                                source.[course_id],
                                source.[type],
                                source.[module_item_id],
                                source.[title],
                                source.[parent_module_id],
                                source.[external_url],
                                source.[item_published],
                                source.[name],
                                source.[published]
                            );
                        """
                        cursor.execute(merge_query, items_count, items_url, module_id, course_id, type,
                                        module_item_id, title, parent_module_id, external_url, item_published, name, published)
                    cnxn.commit()
                page_number += 1
            logging.debug("Data lasta opp til Canvas_Modules")
    logging.info(f"Tidsbruk Canvas_Modules: {time.perf_counter() - start_Canvas_Modules} s")

if os.path.exists("timerRequest.log"):
    os.remove("timerRequest.log")
logging.basicConfig(filename='timerRequest.log', encoding='utf-8', level=logging.INFO)
try:
    timer_Canvas_Modules()
except:
    logging.exception("Feil i timer_Canvas_Enrollments")
