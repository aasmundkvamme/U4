import pandas as pd
import os
import requests
from datetime import datetime, date, timedelta
import time
import pyodbc

conn_str = os.environ["Connection_SQL"]
tid_logg = {}

def akv_query_FS_graphql(query, variable):
    try:
        response = requests.post(
            "https://api.fellesstudentsystem.no/graphql/", 
            json={"query": query, "variables": variable},
            headers={'Authorization': f'Basic {os.environ["tokenFS"]}', "Feature-Flags": "beta, experimental"},
            )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        raise exc
    
query = """
query MyQuery($antal: Int, $start: String) {
  programStudieretter(filter: {eierOrganisasjonskode: "0203", aktivStatus: AKTIV}, 
    after: $start,
    first: $antal) {
    pageInfo {
        endCursor
        hasNextPage
    }
    nodes {
      personProfil {
        personlopenummer
      }
      studieprogram {
        kode
      }
      campus {
        navnAlleSprak {
          nb
        }
      }
      kull {
        termin {
          arstall
          betegnelse {
            navnAlleSprak {
              nb
            }
          }
        }
      }
    }
  }
}
"""


start_les_FS_programstudierettar = time.perf_counter()
programstudierettar = []
n = 0
hentmeir = True
antal_per_side = 1000
while hentmeir:
    if n == 0:
        start = None
    else:
        start = svar['data']['programStudieretter']['pageInfo']['endCursor']
    variable = {'antal': antal_per_side, 'start': start}
    n += 1
    print(f"Hentar side {n} ({antal_per_side*(n-1)}-{antal_per_side*n})")
    svar = akv_query_FS_graphql(query, variable)
    for pSr in svar['data']['programStudieretter']['nodes']:
        try:
            plnr = pSr['personProfil']['personlopenummer']
        except (TypeError, KeyError, ValueError):
            plnr = ''
        try:
            studieprogram = pSr['studieprogram']['kode']
        except (TypeError, KeyError, ValueError):
            studieprogram = ''
        try:
            campus = pSr['campus']['navnAlleSprak']['nb']
        except (TypeError, KeyError, ValueError):
            campus = ''
        try:
            åar = pSr['kull']['termin']['arstall']
        except (TypeError, KeyError, ValueError):
            åar = ''
        try:
            termin = pSr['kull']['termin']['betegnelse']['navnAlleSprak']['nb']
        except (TypeError, KeyError, ValueError):
            termin = ''
        programstudierettar.append([plnr, studieprogram, campus, åar, termin])
    hentmeir = svar['data']['programStudieretter']['pageInfo']['hasNextPage']
tid_logg['les_FS_programstudierettar'] = time.perf_counter() - start_les_FS_programstudierettar

df = pd.DataFrame(programstudierettar, columns=['plnr', 'studieprogram', 'campus', 'år', 'termin'])
df.to_csv('programstudierettar.csv', index=False)

tabell = "akv_programstudierett"

# plnr 
# studieprogram
# campus
# år
# termin

start_oppdater_FS_programstudieretter = time.perf_counter()
with pyodbc.connect(conn_str) as connection:
    try:
        query = """
        MERGE INTO [FS_ProgramStudieretter] AS target 
        USING (VALUES (?, ?, ?, ?, ?)) AS source (plnr, studieprogram, campus, år, termin) 
        ON target.[plnr] = source.[plnr]
        WHEN MATCHED THEN
            UPDATE SET 
                target.[termin] = source.[termin],
                target.[studieprogram] = source.[studieprogram],
                target.[campus] = source.[campus],
                target.[år] = source.[år]
        WHEN NOT MATCHED THEN
            INSERT ([plnr], [studieprogram], [campus], [år], [termin]) VALUES (source.[plnr], source.[studieprogram], source.[campus], source.[år], source.[termin]);
        """
        with connection.cursor() as cursor:
            for data in programstudierettar:
                cursor.execute(query, (data[0], data[1], data[2], data[3], data[4]))
            connection.commit()
    except pyodbc.Error as exc:
        print(f"Error: {exc}")
tid_logg['oppdater_FS_programstudieretter'] = time.perf_counter() - start_oppdater_FS_programstudieretter

print(tid_logg)