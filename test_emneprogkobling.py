import logging
import requests
import pyodbc
import os
import io
# import azure.functions as func
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

# Opprett logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)  # Sett ønsket loggnivå

# Opprett formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Opprett filhandler for å logge til fil
if os.path.exists('loggfil-emneprogkobling.log'):
    os.remove('loggfil-emneprogkobling.log')
file_handler = logging.FileHandler('loggfil-emneprogkobling.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# Opprett konsollhandler for å logge til konsollen
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

# Legg til handlerne i loggeren
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def akv_query_FS_graphql(query, variable):
    hode = {
        'Accept': 'application/json;version=1',
        'Authorization': f'Basic {os.environ["tokenFS"]}',
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
    logger.debug(f"Spørjinga returnerer kode {svar.status_code}")
    if 200 <= svar.status_code < 300:
        return svar.json()
    else:
        logger.debug(f"Feil i spørjing med kode {svar.status_code}.")
        return {}
        # raise Exception(f"Feil i spørjing med kode {svar.status_code}.")



def timer_FS_EmneProgKobling():
    start_FS_EmneProgKobling = time.perf_counter()
    query_FS_EmneProgKobling = """
    query studieprogramkobling($antal: Int, $start: String) {
        emner(filter: {eierInstitusjonsnummer: "0203"}, after: $start, first: $antal) {
            pageInfo {
                endCursor
                hasNextPage
            }
            nodes {
                kode
                versjonskode
                studieprogramkoblinger {
                    periode {
                        fraTermin {
                            arstall
                            betegnelse {
                                kode
                            }
                        }
                        tilTermin {
                            arstall
                            betegnelse {
                                kode
                            }
                        }
                    }
                    studieprogram {
                        kode
                    }
                }
                undervisesIPeriode {
                    forsteTermin {
                        arstall
                        betegnelse {
                            kode
                        }
                    }
                    sisteTermin {
                        arstall
                        betegnelse {
                            kode
                        }
                    }
                }
            }
        }
    }
    """

    emnekoblingar = []
    n = 0
    hentmeir = True
    antal_per_side = 500
    while hentmeir:
        if n == 0:
            start = None
        else:
            start = svar['data']['emner']['pageInfo']['endCursor']
        variable = {'antal': antal_per_side, 'start': start}
        n += 1
        logger.debug(f"Spør etter side {n}")
        svar = akv_query_FS_graphql(query_FS_EmneProgKobling, variable)
        if svar == {}:
            hentmeir = False  # Avbryt prosessen når det oppstår ein feil
        else:   
            for emner in svar['data']['emner']['nodes']:
                emnekode = emner['kode']
                versjonskode = emner['versjonskode']
                try:
                    emnestart_år = emner['undervisesIPeriode']['forsteTermin']['arstall']
                except:
                    emnestart_år = None
                try:
                    emnestart_termin = emner['undervisesIPeriode']['forsteTermin']['betegnelse']['kode']
                except:
                    emnestart_termin = None
                try:
                    emneslutt_år = emner['undervisesIPeriode']['sisteTermin']['arstall']
                except:
                    emneslutt_år = None
                try:
                    emneslutt_termin = emner['undervisesIPeriode']['sisteTermin']['betegnelse']['kode']
                except:
                    emneslutt_termin = None
                if (emneslutt_år is None) or (emneslutt_år > 2016):
                    for studieprogram in emner['studieprogramkoblinger']:
                        if studieprogram is not None:
                            studieprogramkode = studieprogram['studieprogram']['kode']
                            if studieprogram['periode']['fraTermin'] is None:
                                Undervises_forste_ar = emnestart_år
                                Undervises_forste_termin = emnestart_termin
                            else:
                                Undervises_forste_ar = studieprogram['periode']['fraTermin']['arstall']
                                Undervises_forste_termin = studieprogram['periode']['fraTermin']['betegnelse']['kode']
                            if studieprogram['periode']['tilTermin'] is None:
                                Undervises_siste_ar = emneslutt_år
                                Undervises_siste_termin = emneslutt_termin
                            else:
                                Undervises_siste_ar = studieprogram['periode']['tilTermin']['arstall']
                                Undervises_siste_termin = studieprogram['periode']['tilTermin']['betegnelse']['kode']
                            emnekoblingar.append({'Emnekode': emnekode,
                                                'Versjonskode': versjonskode,
                                                'Studieprogramkode': studieprogramkode,
                                                'Undervises_forste_ar': Undervises_forste_ar,
                                                'Undervises_forste_termin': Undervises_forste_termin,
                                                'Undervises_siste_ar': Undervises_siste_ar,
                                                'Undervises_siste_termin': Undervises_siste_termin})
                        else:
                            logger.debug(f"Ingen studieprogram: {emner}")
                            emnekoblingar.append({'Emnekode': emnekode,
                                                'Versjonskode': versjonskode,
                                                'Studieprogramkode': None,
                                                'Undervises_forste_ar': emnestart_år,
                                                'Undervises_forste_termin': emnestart_termin,
                                                'Undervises_siste_ar': emneslutt_år,
                                                'Undervises_siste_termin': emneslutt_termin})

            hentmeir = svar['data']['emner']['pageInfo']['hasNextPage']
            # hentmeir = False

    logger.debug(f"Ferdig med henting av data, har henta {len(emnekoblingar)} emneprogramkoblingar.")
    dataramme = pd.DataFrame(emnekoblingar, columns=['Emnekode', 'Versjonskode', 'Studieprogramkode', 'Undervises_forste_ar', 'Undervises_forste_termin', 'Undervises_siste_ar', 'Undervises_siste_termin'])
    dataramme['Emnekode2'] = dataramme['Emnekode'] + '_' + dataramme['Versjonskode']
    dataramme['programEmneKode'] = dataramme['Studieprogramkode'] + '_' + dataramme['Emnekode'] + '_' + dataramme['Versjonskode']
    dataramme.loc[dataramme['Undervises_siste_ar'].isna(), 'Undervises_siste_termin'] = None
    dataramme = dataramme.where(pd.notnull(dataramme), "")
    emnekoblingar = dataramme.values.tolist()
    dataramme.to_csv('test_emneprogkobling.csv', index=False)

    conn_str = os.environ["Connection_SQL"]
    with pyodbc.connect(conn_str) as cnxn:
        with cnxn.cursor() as cursor:
            merge_query = """
                MERGE INTO [dbo].[FS_EmneProgKobling] AS target
                USING (SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?) AS source (
                    [Emnekode],
                    [Versjonskode],
                    [Studieprogramkode],
                    [Undervises_forste_ar],
                    [Undervises_forste_termin],
                    [Undervises_siste_ar],
                    [Undervises_siste_termin],
                    [Emnekode2],
                    [programEmneKode]
                )
                ON target.[programEmneKode] = source.[programEmneKode]
                WHEN MATCHED THEN UPDATE SET 
                    target.[Studieprogramkode]= source.[Studieprogramkode],
                    target.[Emnekode] = source.[Emnekode],
                    target.[Versjonskode] = source.[Versjonskode],
                    target.[Undervises_forste_ar]= source.[Undervises_forste_ar],
                    target.[Undervises_forste_termin]= source.[Undervises_forste_termin],
                    target.[Undervises_siste_ar]= source.[Undervises_siste_ar],
                    target.[Undervises_siste_termin]= source.[Undervises_siste_termin],
                    target.[Emnekode2] = source.[Emnekode2]
                WHEN NOT MATCHED THEN INSERT (
                    [Emnekode],
                    [Versjonskode],
                    [Studieprogramkode],
                    [Undervises_forste_ar],
                    [Undervises_forste_termin],
                    [Undervises_siste_ar],
                    [Undervises_siste_termin],
                    [Emnekode2],
                    [programEmnekode])
                VALUES (
                    source.[Emnekode],
                    source.[Versjonskode],
                    source.[Studieprogramkode],
                    source.[Undervises_forste_ar],
                    source.[Undervises_forste_termin],
                    source.[Undervises_siste_ar],
                    source.[Undervises_siste_termin],
                    source.[Emnekode2],
                    source.[programEmneKode]
                );
            """
            for rad in emnekoblingar:
                Emnekode = rad[0]
                Versjonskode = rad[1]
                Studieprogramkode = rad[2]
                Undervises_forste_ar = rad[3]
                Undervises_forste_termin = rad[4]
                Undervises_siste_ar = rad[5]
                Undervises_siste_termin = rad[6]
                Emnekode2 = rad[7]
                programEmneKode = rad[8]
                logger.debug(f"Inserting {Emnekode} {Versjonskode} {Studieprogramkode} {Undervises_forste_ar} {Undervises_forste_termin} {Undervises_siste_ar} {Undervises_siste_termin} {Emnekode2} {programEmneKode}")
                cursor.execute(merge_query, (Emnekode, Versjonskode, Studieprogramkode, Undervises_forste_ar, Undervises_forste_termin, Undervises_siste_ar, Undervises_siste_termin, Emnekode2, programEmneKode))
            cnxn.commit()
    logging.info(f"Tidsbruk FS_EmneProgKobling: {time.perf_counter() - start_FS_EmneProgKobling} s")

logging.basicConfig(filename='timerRequest.log', encoding='utf-8', level=logging.INFO)
logger.debug("Start prosessen")
timer_FS_EmneProgKobling()