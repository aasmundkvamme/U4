import logging
from pip._vendor import requests
import pyodbc
import os
from datetime import datetime, timedelta
import time
import json
import pandas as pd
import traceback
import numpy as np
import aasmund

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
headers = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}

def query_canvas_graphql(query: str, variables: dict):
    """Send a GraphQL query to Canvas and return the response."""
    try:
        response = requests.post(
            "https://hvl.instructure.com/api/graphql",
            json={"query": query, "variables": variables},
            headers = headers
        )
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as exc:
        logging.error(f"Feil: {exc}")
        raise exc


def akv_hent_aktuelle_emne():
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
                else:
                    pass
            elif t[1] == f"{str(år)} {termin}":
                aktuelle_terminar.append(t[0])
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

    return aktuelle_emne

def timer_Canvas_Courses_StudentSummaries():
    start_Canvas_StudentSummaries = time.perf_counter()

    queryCanvas = """
    query MyQuery($course_id: ID!) {
        course(id: $course_id) {
            usersConnection {
                pageInfo {
                    hasNextPage
                }
                nodes {
                    summaryAnalytics(courseId: $course_id) {
                        pageViews {
                            total
                            level
                            max
                        }
                        participations {
                            level
                            max
                            total
                        }
                        tardinessBreakdown {
                            late
                            missing
                            onTime
                            total
                        }
                    }
                sisId
                _id
            }
        }
    }
    }
    """
    merge_query = """
    MERGE INTO [stg].[Canvas_Courses_StudentSummaries] AS target
    USING (SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS source (
        page_views,
        id,
        max_page_views,
        page_views_level,
        participations,
        max_participations,
        participations_level,
        course_id,
        missing,
        late,
        on_time,
        total
    )
    ON target.id = source.id AND target.course_id = source.course_id
    WHEN MATCHED THEN UPDATE SET 
            target.max_page_views = source.max_page_views,
            target.page_views = source.page_views,
            target.page_views_level = source.page_views_level,
            target.participations = source.participations,
            target.max_participations = source.max_participations,
            target.participations_level = source.participations_level,
            target.missing = source.missing,
            target.late = source.late,
            target.on_time = source.on_time,
            target.total = source.total
    WHEN NOT MATCHED THEN INSERT (
            page_views,
            id,
            max_page_views,
            page_views_level,
            participations,
            max_participations,
            participations_level,
            course_id,
            missing,
            late,
            on_time,
            total
        ) VALUES (
            source.page_views,
            source.id,
            source.max_page_views,
            source.page_views_level,
            source.participations,
            source.max_participations,
            source.participations_level,
            source.course_id,
            source.missing,
            source.late,
            source.on_time,
            source.total
        );
    """

    with pyodbc.connect(conn_str) as cnxn:
        cursor = cnxn.cursor()
        start_hent_GraphQl = time.perf_counter()
        for emne in aktuelle_emne:
            variables = {"course_id": emne}
            logging.info(variables)
            resultat = query_canvas_graphql(queryCanvas, variables)
            data = resultat.json()
            for enrollment in data['data']['course']['usersConnection']['nodes']:
                if enrollment['summaryAnalytics'] != None:
                    user_id = enrollment['_id']
                    page_views = enrollment['summaryAnalytics']['pageViews']['total']
                    max_page_views = enrollment['summaryAnalytics']['pageViews']['max']
                    page_views_level = enrollment['summaryAnalytics']['pageViews']['level']
                    participations = enrollment['summaryAnalytics']['participations']['total']
                    max_participations = enrollment['summaryAnalytics']['participations']['max']
                    participations_level = enrollment['summaryAnalytics']['participations']['level']
                    missing = enrollment['summaryAnalytics']['tardinessBreakdown']['missing']
                    late = enrollment['summaryAnalytics']['tardinessBreakdown']['late']
                    on_time = enrollment['summaryAnalytics']['tardinessBreakdown']['onTime']
                    total = enrollment['summaryAnalytics']['tardinessBreakdown']['total']
                    course_id = emne
                    cursor.execute(merge_query, page_views, user_id, max_page_views, page_views_level, participations,
                                max_participations, participations_level, course_id, missing, late, on_time, total)
            print(f"Emne: {emne} Data lasta opp til Canvas_Courses_StudentSummaries")
        print(f"Tidsbruk hent_REST: {time.perf_counter() - start_hent_GraphQl}")
        cnxn.commit()
    print(f"Tidsbruk Canvas_StudentSummaries: {time.perf_counter() - start_Canvas_StudentSummaries} s")

# Delete logfile if it exists
if os.path.exists('timerRequest.log'):
    os.remove('timerRequest.log')

logging.basicConfig(filename='timerRequest.log', encoding='utf-8', level=logging.INFO)
try:
    aktuelle_emne = akv_hent_aktuelle_emne()
    try:
        timer_Canvas_Courses_StudentSummaries()
    except:
        print("Feil i timer_Canvas_Courses_StudentSummaries")
except:
    print("Feil i akv_hent_aktuelle_emne")