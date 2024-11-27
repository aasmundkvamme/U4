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
# import traceback
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


def akv_query_canvas_graphql(query: str, variables: dict):
    """Send a GraphQL query to Canvas and return the response."""
    try:
        respons = requests.post(
            "https://hvl.instructure.com/api/graphql",
            json={"query": query, "variables": variables},
            headers={"Authorization": f"Bearer {os.environ['tokenCanvas']}"},
        )
        respons.raise_for_status()
        return respons
    except requests.exceptions.RequestException as exc:
        raise exc


def akv_query_FS_graphql(query, variables):
    try:
        respons = requests.post(
            "https://api.fellesstudentsystem.no/graphql/", 
            json={"query": query, "variables": variables},
            headers={'Authorization': f'Basic {os.environ["tokenFS"]}', "Feature-Flags": "beta, experimental"},
            )
        respons.raise_for_status()
        return respons
    except requests.exceptions.RequestException as exc:
        raise exc


def timer_FS_emne():
    start_FS_Emne = time.perf_counter()
    query = """
    query emneoversikt($aar_emne: Int!, $termin_emne: EmneIkkeUtloptITerminTerminbetegnelse!, $aar_lub: Int!, $antal: Int, $start: String) {
    emner(
        filter: {
            eierInstitusjonsnummer: "0203", 
            ikkeUtloptITermin: {
                arstall: $aar_emne, 
                terminbetegnelse: $termin_emne
                }
            } 
            first: $antal, 
            after: $start) {
        pageInfo {
            endCursor
            hasNextPage
        }
        nodes {
      kode
      versjonskode
      navnAlleSprak {
        nn
        en
        nb
      }
      organisasjonsenhet {
        administrativtAnsvarlig {
          fakultet {
            fakultetsnummer
            navn {
              nb
            }
          }
          instituttnummer
          navnAlleSprak {
            nb
          }
        }
      }
      fagkoblinger {
        viktigsteFag {
          navnAlleSprak {
            nb
          }
        }
      }
      emnetype
      vekting {
        emnevekting {
          vektingstype {
            kode
          }
          verdi
        }
      }
      beskrivelser(
        filter: {gjelderFraTerminer: {arstall: $aar_lub, terminbetegnelse: "HØST"}, tekstkategorikoder: ["EBLUB", "EBARB"]}
      ) {
        innhold
        sprak {
          iso6392Kode
        }
        tekstkategori {
          kode
        }
      }
      personroller(filter: {kanPubliseres: true, rollekoder: "EMNEANSVAR"}) {
        personProfil {
          navn {
            etternavn
            fornavn
          }
        }
        fagperson {
          feideBruker
        }
      }
    }
  }
}
    """
    try:
        with pyodbc.connect(conn_str) as cnxn:
            if måned < 5:
                år_lub = år - 1
            else:
                år_lub = år
            if måned < 7:
                termin_emne = "VAR"
            else:
                termin_emne = "HOST"
            liste_utan_emneansvarlege = []
            liste_med_emneansvarlege = []
            n = 0
            hentmeir = True
            antal_per_side = 100
            try:
                while hentmeir:
                    if n == 0:
                        start = None
                    else:
                        start = svar['data']['emner']['pageInfo']['endCursor']
                    variable = {'aar_emne': år, 'termin_emne': termin_emne, 'aar_lub': år_lub, 'antal': antal_per_side, 'start': start}
                    n += 1
                    svar = graphql(query, variable)
                    for item in svar['data']['emner']['nodes']:
                        try:
                            emnekode = item['kode']
                            versjonskode = item['versjonskode']
                            unik_kode = emnekode + "_" + versjonskode
                            emnenavn_nob = item['navnAlleSprak']['nob']
                            emnenavn_nno = item['navnAlleSprak']['nno']
                            emnenavn_eng = item['navnAlleSprak']['eng']
                            fag = item['fagkoblinger']['viktigsteFag']['navnAlleSprak']['nob']
                            fakultetsnummer = item['organisasjonsenhet']['administrativtAnsvarlig']['fakultet']['fakultetsnummer']
                            instituttnummer = item['organisasjonsenhet']['administrativtAnsvarlig']['instituttnummer']
                            instituttnavn = item['organisasjonsenhet']['administrativtAnsvarlig']['navnAlleSprak']['nob']
                            emnetype = item['emnetype']
                            vekting = item['vekting']['emnevekting']['verdi']
                            vektingstype = item['vekting']['emnevekting']['vektingstype']['kode']
                            lubarb = {'ARB_NNO': "Ikkje registrert", 'ARB_NOB': "Ikkje registrert", 'ARB_ENG': "Ikkje registrert", 'LUB_NNO': "Ikkje registrert", 'LUB_NOB': "Ikkje registrert", 'LUB_ENG': "Ikkje registrert"}
                            try:
                                for b in item['beskrivelser']:
                                    kode = b['tekstkategori']['kode'][2:] + "_" + b['sprak']['iso6392Kode']
                                    lubarb[kode] = b['innhold']
                            except:
                                pass
                            ARB_NNO = lubarb['ARB_NNO']
                            ARB_NOB = lubarb['ARB_NOB']
                            ARB_ENG = lubarb['ARB_ENG']
                            LUB_NNO = lubarb['LUB_NNO']
                            LUB_NOB = lubarb['LUB_NOB']
                            LUB_ENG = lubarb['LUB_ENG']               
                            personroller = item['personroller']
                            emneansvarlege = []
                            try:
                                for person in personroller:
                                    etternavn = person['personProfil']['navn']['etternavn']
                                    fornavn = person['personProfil']['navn']['fornavn']
                                    feidebruker = person['fagperson']['feideBruker']
                                    emneansvarlege.append((fornavn, etternavn, feidebruker))
                            except:
                                emneansvarlege = 'Ikkje registrert'
                            emneansvarlege = json.dumps(emneansvarlege, ensure_ascii=False)
                            liste_utan_emneansvarlege.append((emnekode, versjonskode, unik_kode, emnenavn_nob, emnenavn_nno, emnenavn_eng, fag, fakultetsnummer, instituttnummer, instituttnavn, emnetype, vekting, vektingstype, LUB_NNO, LUB_NOB, LUB_ENG, ARB_NNO, ARB_NOB, ARB_ENG))
                            liste_med_emneansvarlege.append((unik_kode, emneansvarlege))
                        except:
                            logging.error(f"Feil i {item['kode']}")
                    hentmeir = svar['data']['emner']['pageInfo']['hasNextPage']
            except:
                raise Exception(f"Feil i henting av data om emnet. Side {n}")
            query = """MERGE INTO [stg].[FS_Emner] AS t
                USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS s(
                    [emnekode],
                    [versjonskode],
                    [unik_kode],
                    [emnenavn_nob],
                    [emnenavn_nno],
                    [emnenavn_eng],
                    [fag],
                    [fakultetsnummer],
                    [instituttnummer],
                    [instituttnavn],
                    [emnetype],
                    [vekting],
                    [vektingstype],
                    [LUB_NNO],
                    [LUB_NOB],
                    [LUB_ENG],
                    [ARB_NNO],
                    [ARB_NOB],
                    [ARB_ENG]
                )
                ON t.[unik_kode] = s.[unik_kode] 
                WHEN MATCHED THEN UPDATE SET
                    t.[emnekode] = s.[emnekode],
                    t.[versjonskode] = s.[versjonskode],
                    t.[emnenavn_nob] = s.[emnenavn_nob],
                    t.[emnenavn_nno] = s.[emnenavn_nno],
                    t.[emnenavn_eng] = s.[emnenavn_eng],
                    t.[fag] = s.[fag],
                    t.[fakultetsnummer] = s.[fakultetsnummer],
                    t.[instituttnummer] = s.[instituttnummer],
                    t.[instituttnavn] = s.[instituttnavn],
                    t.[emnetype] = s.[emnetype],
                    t.[vekting] = s.[vekting],
                    t.[vektingstype] = s.[vektingstype],
                    t.[LUB_NNO] = s.[LUB_NNO],
                    t.[LUB_NOB] = s.[LUB_NOB],
                    t.[LUB_ENG] = s.[LUB_ENG],
                    t.[ARB_NNO] = s.[ARB_NNO],
                    t.[ARB_NOB] = s.[ARB_NOB],
                    t.[ARB_ENG] = s.[ARB_ENG]
                WHEN NOT MATCHED THEN INSERT (
                    [unik_kode],
                    [emnekode],
                    [versjonskode],
                    [emnenavn_nob],
                    [emnenavn_nno],
                    [emnenavn_eng],
                    [fag],
                    [fakultetsnummer],
                    [instituttnummer],
                    [instituttnavn],
                    [emnetype],
                    [vekting],
                    [vektingstype],
                    [LUB_NNO],
                    [LUB_NOB],
                    [LUB_ENG],
                    [ARB_NNO],
                    [ARB_NOB],
                    [ARB_ENG])
                  VALUES (
                    s.[unik_kode],
                    s.[emnekode],
                    s.[versjonskode],
                    s.[emnenavn_nob],
                    s.[emnenavn_nno],
                    s.[emnenavn_eng],
                    s.[fag],
                    s.[fakultetsnummer],
                    s.[instituttnummer],
                    s.[instituttnavn],
                    s.[emnetype],
                    s.[vekting],
                    s.[vektingstype],
                    s.[LUB_NNO],
                    s.[LUB_NOB],
                    s.[LUB_ENG],
                    s.[ARB_NNO],
                    s.[ARB_NOB],
                    s.[ARB_ENG]);    
                """
            cursor = cnxn.cursor()
            for data in liste_utan_emneansvarlege:
                cursor.execute(query, data)
            cnxn.commit()
            cursor.close()
            logging.debug("Data lasta opp til FS_Emner")
            
            query = """
                MERGE INTO [stg].[FS_Emneansvarlige] AS t
                USING (VALUES (?, ?)) AS s(
                    [unik_kode],
                    [emneansvarlege]
                )
                ON t.[unik_kode] = s.[unik_kode] 
                WHEN MATCHED THEN UPDATE SET
                    t.[emneansvarlege] = s.[emneansvarlege]
                WHEN NOT MATCHED THEN INSERT (
                    [unik_kode],
                    [emneansvarlege])
                VALUES (
                    s.[unik_kode],
                    s.[emneansvarlege]);    
                """
            cursor = cnxn.cursor()
            for data in liste_med_emneansvarlege:
                cursor.execute(query, data)
            cnxn.commit()
            cursor.close()
            logging.debug("Data lasta opp til FS_Emneansvarlige")
    except:
        logging.error("Feil ved oppdatering av tabell FS_Emne.")
        logging.error(traceback.format_exc())
    logging.info(f"Tidsbruk FS_Emne: {time.perf_counter() - start_FS_Emne} s")

timer_FS_emne()