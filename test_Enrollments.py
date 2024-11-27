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


def akv_finn_sist_oppdatert(tabell):
    """
    Returner den siste oppdateringstida for den gitte tabellen fra akv_sist_oppdatert-tabellen.
    Hvis ingen dato er gitt (eller vi ikkje får kontakt med databasen), returner igår.
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
    """
    Lagre datoen for siste oppdatering av tabell i Azure eller lokalt (dersom vi ikkje får kontakt med databasen).
    """
    
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
            logging.debug(f"{tabell} er sist oppdatert (Azure): {dato}")
    except pyodbc.Error as e:
        with open(f'sist_oppdatert_{tabell}.txt', 'w') as f_out:
            f_out.write(dato)
            logging.debug(f"{tabell} er sist oppdatert (lokal): {dato}")
    return None


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


def akv_hent_CD2_filar(innfil, token, svar):
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
        logger.debug(f"Pakker ut fil.")
        try:
            with gzip.GzipFile(fileobj=buffer, mode='rb') as utpakka_fil:
                utpakka_data = utpakka_fil.read().decode()
        except:
            print("Feilmelding")
    return utpakka_data


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
        logger.debug(f"Skal hente data frå {len(filar)} filar.")
        for fil in filar:
            data = io.StringIO(akv_hent_CD2_filar(fil['id'], CD2_access_token, respons2))
            df = pd.read_csv(data, sep=",")
            dr_liste.append(df)
        alledata = pd.concat(df for df in dr_liste if not df.empty)
        return alledata, sist_oppdatert, respons2['until']
    except requests.exceptions.RequestException as exc:
        raise exc

def timer_Canvas_Enrollments():
    start_Canvas_Enrollments = time.perf_counter()
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        try:
            logger.debug("Hentar terminar")
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
                else:
                    pass
            elif t[1] == f"{str(år)} {termin}":
                aktuelle_terminar.append(t[0])
            else:
                pass
    except:
        logging.error(f"Feil: {traceback.format_exc()}")
    logger.debug(f"Det er {len(aktuelle_terminar)} terminar")

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        try:
            logger.debug("Hentar emne")
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
        except pyodbc.Error as e:
            logging.error(f"Feil: {e}")
        logger.debug(f"Det er {len(aktuelle_emne)} emne")

    enrollments_data = []
    for emne in aktuelle_emne:
        variables = {"courseId": emne}
        try:
            tabell = "enrollments"
            resultat, sist_oppdatert = akv_les_CD2_tabell(tabell)
            akv_lagre_sist_oppdatert(tabell, sist_oppdatert)
            enrollments = resultat['key.id', 'value.user_id', 'value.course_id', 'value.type', 'value.created_at', 'value.updated_at', 'value.start_at', 'value.end_at', 'value.workflow_state', 'value.total_activity_time', 'value.last_activity_at', ]
            logger.debug(f"enrollments har type{type(enrollments)}")
            for enrollment in enrollments:
                enrollment_id = enrollment['_id']
                user_id = enrollment['user']['_id']
                sis_user_id = enrollment['user']['sisId']
                course_id = emne
                type = enrollment['type']
                try:
                    created_at = datetime.fromisoformat(str(enrollment['user']['createdAt']))
                except (KeyError, ValueError):
                    created_at = None
                try:
                    updated_at = datetime.fromisoformat(str(enrollment['user']['updatedAt']))
                except (KeyError, ValueError):
                    updated_at = None
                enrollment_state = enrollment['state']
                total_activity_time = enrollment['totalActivityTime']
                try:
                    last_activity_at = datetime.fromisoformat(str(enrollment['lastActivityAt']))
                except (KeyError, ValueError):
                    last_activity_at = None
                enrollments_data.append([enrollment_id, user_id, sis_user_id, course_id, type, created_at, updated_at, enrollment_state, total_activity_time, last_activity_at])
        except:
            logger(f"Feil i emne {emne}.")
            logging.error(f"Feil: {traceback.format_exc()}")

    with pyodbc.connect(conn_str) as cnxn:
        with cnxn.cursor() as cursor:
            try:
                for row in enrollments_data:
                    merge_query = """
                        MERGE INTO [stg].[Canvas_Enrollments] AS target
                        USING (SELECT ?, ?, ?, ?, ?, CONVERT(datetime, ?, 127), CONVERT(datetime, ?, 127), ?, ?, CONVERT(datetime, ?, 127)) AS source (
                            [enrollment_id],
                            [user_id],
                            [sis_user_id],
                            [course_id],
                            [type],
                            [created_at],
                            [updated_at],
                            [enrollment_state],
                            [total_activity_time],
                            [last_activity_at]
                        )
                        ON target.[enrollment_id] = source.[enrollment_id]
                        WHEN MATCHED THEN
                            UPDATE SET target.[user_id]= source.[user_id],
                                target.[sis_user_id] = source.[sis_user_id],
                                target.[course_id] = source.[course_id],
                                target.[type] = source.[type],
                                target.[created_at] = source.[created_at],
                                target.[updated_at] = source.[updated_at],
                                target.[enrollment_state] = source.[enrollment_state],
                                target.[total_activity_time] = source.[total_activity_time],
                                target.[last_activity_at] = source.[last_activity_at]
                        WHEN NOT MATCHED THEN
                            INSERT ([enrollment_id],
                                [user_id],
                                [sis_user_id],
                                [course_id],
                                [type],
                                [created_at],
                                [updated_at],
                                [enrollment_state],
                                [total_activity_time],
                                [last_activity_at]
                            )
                            VALUES (source.[enrollment_id],
                                source.[user_id],
                                source.[sis_user_id],
                                source.[course_id],
                                source.[type],
                                source.[created_at],
                                source.[updated_at],
                                source.[enrollment_state],
                                source.[total_activity_time],
                                source.[last_activity_at]
                            );
                    """
                    cursor.execute(merge_query, row)
            except pyodbc.Error as e:
                logging.error(f"Feil: {e}\n{row}")
            cnxn.commit()
    logging.info(f"Tidsbruk Canvas_Enrollments: {time.perf_counter() - start_Canvas_Enrollments} s")



logging.basicConfig(filename='timerRequest.log', encoding='utf-8', level=logging.INFO)
rutine="Enrollment"
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



try:
    timer_Canvas_Enrollments()
except:
    logging.exception("Feil i timer_Canvas_Enrollments")