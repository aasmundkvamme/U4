import logging
from pip._vendor import requests
import pyodbc
import os
from datetime import datetime, timedelta
import time
import pandas as pd
import traceback
import numpy as np
import pandas as pd

start_Canvas_Enrollments = time.perf_counter()
idag = datetime.now()
igår = idag - timedelta(days=1)
år = idag.year
måned = idag.month
dag = idag.day
if måned <= 7:
    termin = "VÅR"
else:
    termin = "HØST"
conn_str = os.environ["Connection_SQL"]
hodeCanvas = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}


def query_canvas_graphql(query: str, variables: dict):
    """Send a GraphQL query to Canvas and return the response."""
    try:
        response = requests.post(
            "https://hvl.instructure.com/api/graphql",
            json={"query": query, "variables": variables},
            headers={"Authorization": f"Bearer {os.environ['tokenCanvas']}"},
        )
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as exc:
        raise exc

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
                if emne[4] == 1:
                    pass
                    # aktuelle_emne.append(emne[0])
                else:
                    if emne[3] is None:
                        pass
                    else:
                        emne_år = emne[3].split('_')[4]
                        emne_termin = emne[3].split('_')[5]
                        if (emne_år == str(år)) and (emne_termin == termin):
                            aktuelle_emne.append(emne[0])
            except IndexError:
                # print("Ingen data i denne raden")
                pass
    except pyodbc.Error as e:
        print(f"Feil: {e}")
    print(f"Det er {len(aktuelle_emne)} aktuelle emne.")

queryCanvas = """
query MyQuery($courseId: ID!) {
  course(id: $courseId) {
    enrollmentsConnection {
      nodes {
        user {
          _id
          sisId
          createdAt
          updatedAt
          name
        }
        type
        state
        _id
        totalActivityTime
        lastActivityAt
      }
    }
  }
}
"""
statistikk = []
enrollments_data = []
for emne in aktuelle_emne:
    variables = {"courseId": emne}
    print(f"Les påmeldingar i emnet {emne}: ", end="")
    try:
        resultat = query_canvas_graphql(queryCanvas, variables)
        data = resultat.json()
        enrollments = data['data']['course']['enrollmentsConnection']['nodes']
        print(len(enrollments))
        for enrollment in enrollments:
            enrollment_id = enrollment['_id']
            user_id = enrollment['user']['_id']
            sis_user_id = enrollment['user']['sisId']
            course_id = emne
            type = enrollment['type']
            try:
                created_at = datetime.fromisoformat(enrollment['user']['createdAt'])
            except (KeyError, ValueError):
                created_at = None
            try:
                updated_at = datetime.fromisoformat(enrollment['user']['updatedAt'])
            except (KeyError, ValueError):
                updated_at = None
            enrollment_state = enrollment['state']
            total_activity_time = enrollment['totalActivityTime']
            try:
                last_activity_at = datetime.fromisoformat(enrollment['user']['lastActivityAt'])
            except (KeyError, ValueError):
                last_activity_at = None
            enrollments_data.append([enrollment_id, user_id, sis_user_id, course_id, type, created_at, updated_at, enrollment_state, total_activity_time, last_activity_at])
        statistikk.append([course_id, len(enrollments)])
    except:
        # print(f"Feil: {traceback.format_exc()}\n{resultat.text}\n{resultat.status_code}\n{resultat.json()}")
        print(f"Feil: {traceback.format_exc()}")
pd.DataFrame(enrollments_data).to_csv("enrollments.csv", index=False)
print(f"Det er {len(enrollments_data)} enrollments i {len(aktuelle_emne)} emne.")


with pyodbc.connect(conn_str) as cnxn:
    with cnxn.cursor() as cursor:
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
        cnxn.commit()

















# url_template = 'https://hvl.instructure.com/api/v1/courses/{course_id}/users/?include[]=enrollments&per_page=100&page={page_number}'

# query = """
#     SELECT DISTINCT course_id FROM [stg].[Canvas_Courses]
#         LEFT JOIN (select term_id
#         ,CASE
#             when name not like '%-%' and name like '%[0-9]%' and LEFT(name,4) like '%[0-9]%' then LEFT(name,4)
#             when name not like '%-%' and name like '%[0-9]%' and RIGHT(name,4) like '%[0-9]%' then RIGHT(name,4)
#             when name like '%standard%' then YEAR(GETDATE())
#             when name like '%eveg vår%' then YEAR(GETDATE())
#             when name like '%-%' and name like '%[0-9]%' then LEFT(SUBSTRING(name,charindex('-',name)+1,4),4)
#             else null end as [end_year]
#         ,CASE
#             when RIGHT(name,3) like '%vår%' then 7
#             when RIGHT(name,4) like '%høst%' then 12
#             when name like '%vår%' then 7
#             when name like '%høst%' then 12
#             when name like '%standard%' and MONTH(GETDATE())<8 then 7
#             else 12 end as [end_semester]
#         FROM [stg].[Canvas_Terms]) t on t.term_id = [stg].[Canvas_Courses].enrollment_term_id
#         WHERE workflow_state not like '%unpub%' and t.end_year >= year(getdate()) and t.end_semester >= MONTH(getdate()) and (sis_course_id like '%UE_203%' or sis_course_id like '%UA_203%')
#         ORDER BY course_id asc
# """

# print(conn_str)
# print(headers)
# with pyodbc.connect(conn_str) as cnxn:
#     with cnxn.cursor() as cursor:
#         cursor.execute(query)
#         results = cursor.fetchall()
#         print(results)
#         for row in results:
#             course_id = row.course_id
#             page_number = 1
#             while True:
#                 url = url_template.format(
#                     course_id=course_id, page_number=page_number)
#                 parametre = {'per_page': 100, 'enrollment_state[]': [
#                     'active', 'invited', 'completed', 'creation_pending', 'deleted', 'rejected', 'inactive']}
#                 response = requests.get(
#                     url, headers=headers, params=parametre)
#                 data = response.json()
#                 if not data:
#                     break
#                 for item in data:
#                     sis_user_id = item['sis_user_id']
#                     for enrollment in item['enrollments']:
#                         enrollment_id = enrollment['id']
#                         user_id = enrollment['user_id']
#                         course_id = enrollment['course_id']
#                         type = enrollment['type']
#                         created_at = enrollment['created_at']
#                         updated_at = enrollment['updated_at']
#                         enrollment_state = enrollment['enrollment_state']
#                         total_activity_time = enrollment['total_activity_time']
#                         last_activity_at = enrollment['last_activity_at']
#                         merge_query = """
#                             MERGE INTO [stg].[Canvas_Enrollments] AS target
#                             USING (SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS source (
#                                 [enrollment_id],
#                                 [user_id],
#                                 [sis_user_id],
#                                 [course_id],
#                                 [type],
#                                 [created_at],
#                                 [updated_at],
#                                 [enrollment_state],
#                                 [total_activity_time],
#                                 [last_activity_at]
#                             )
#                             ON target.[enrollment_id] = source.[enrollment_id]
#                             WHEN MATCHED THEN
#                                 UPDATE SET target.[user_id]= source.[user_id],
#                                     target.[sis_user_id] = source.[sis_user_id],
#                                     target.[course_id] = source.[course_id],
#                                     target.[type] = source.[type],
#                                     target.[created_at] = source.[created_at],
#                                     target.[updated_at] = source.[updated_at],
#                                     target.[enrollment_state] = source.[enrollment_state],
#                                     target.[total_activity_time] = source.[total_activity_time],
#                                     target.[last_activity_at] = source.[last_activity_at]
#                             WHEN NOT MATCHED THEN
#                                 INSERT ([enrollment_id],
#                                     [user_id],
#                                     [sis_user_id],
#                                     [course_id],
#                                     [type],
#                                     [created_at],
#                                     [updated_at],
#                                     [enrollment_state],
#                                     [total_activity_time],
#                                     [last_activity_at]
#                                 )
#                             VALUES (source.[enrollment_id],
#                                 source.[user_id],
#                                 source.[sis_user_id],
#                                 source.[course_id],
#                                 source.[type],
#                                 source.[created_at],
#                                 source.[updated_at],
#                                 source.[enrollment_state],
#                                 source.[total_activity_time],
#                                 source.[last_activity_at]
#                             );
#                         """
#                         # cursor.execute(merge_query, enrollment_id, user_id, sis_user_id, course_id, type,
#                         #                created_at, updated_at, enrollment_state, total_activity_time, last_activity_at)
#                         # cnxn.commit()
#                 page_number += 1
#             logging.debug("Data lasta opp til Canvas_Enrollments")
# logging.info(f"Tidsbruk Canvas_Enrollments: {time.perf_counter() - start_Canvas_Enrollments} s")