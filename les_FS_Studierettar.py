import logging
import requests
import pyodbc
import os
import io
# import azure.functions as func
from datetime import datetime, timedelta
import time
import json
import gzip
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
tokenFS = aasmund.tokenFS


def akv_query_FS_graphql(query, variable):
    hode = {
        'Accept': 'application/json;version=1',
        'Authorization': f'Basic {tokenFS}',
        "Feature-Flags": "beta, experimental"
    }
    GraphQLurl = "https://api.fellesstudentsystem.no/graphql/"
    svar = requests.post(
        GraphQLurl, 
        json = {
            'query': query,
            'variables': variable
        },
        headers=hode)
    if 200 <= svar.status_code < 300:
        return svar.json()
    else:
        raise Exception(f"Feil i spørjing med kode {svar.status_code}. {query}")


query = """
query studieprogram($antal: Int!, $start: String) {
  studenter(filter: {eierOrganisasjonskode: "0203"}, first: $antal, after: $start) {
    nodes {
      studieprogramISemester(filter: {}) {
        studieprogram {
          kode
          vekting {
            verdi
          }
        }
        termin {
          arstall
          betegnelse {
            kode
          }
        }
      }
      personProfil {
        personlopenummer
      }
      studentnummer
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"""

n = 0
hentmeir = True
antal_per_side = 1000
studierettarliste = []
while hentmeir:
    if n == 0:
        start = None
    else:
        start = svar['data']['studenter']['pageInfo']['endCursor']
    variable = {'antal': antal_per_side, 'start': start}
    n += 1
    print(f"Les side {n} studierettar, {(n-1)*antal_per_side}-{(n)*antal_per_side}")
    svar = akv_query_FS_graphql(query, variable)
    for student in svar['data']['studenter']['nodes']:
        personløpenummer = student['personProfil']['personlopenummer']
        studieprogram = []
        for sP in student['studieprogramISemester']:
            kode = sP['studieprogram']['kode']
            vekting = sP['studieprogram']['vekting']['verdi']
            semester = str(sP['termin']['arstall']) + "_" + sP['termin']['betegnelse']['kode']
            studieprogram.append([kode, vekting, semester])
        if len(studieprogram) != 0:
            studierettarliste.append([personløpenummer, studieprogram])
    hentmeir = svar['data']['studenter']['pageInfo']['hasNextPage']

studierettar = pd.DataFrame(studierettarliste, columns = ['Personløpenummer', 'Studierettar'])
studierettar.to_csv("studierettoversikt_241017_c.csv", index=False)