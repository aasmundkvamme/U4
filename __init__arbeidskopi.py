import logging
import requests
import pyodbc
import os
import io
import azure.functions as func
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
                logging.debug(f"{tabell} er sist oppdatert (Azure): {row[0].isoformat() + 'Z'}")
                return row[0].isoformat() + "Z"
            
    except pyodbc.Error as exc:
        logging.debug(f"{tabell} er sist oppdatert (lokal): {(date.today() - timedelta(days=1)).isoformat() + 'Z'}") 
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


def akv_query_canvas_graphql(query, variable):
    """
    Send a GraphQL query to Canvas and return the response.

    :param query: GraphQL query
    :type query: str
    :param variable: GraphQL variable
    :type variable: dict
    :return: JSON response
    :rtype: dict
    :raises Exception: if the request fails
    """
    hode = {
        'Authorization': f'Basic {os.environ["tokenCanvas"]}',
    }
    GraphQLurl = "https://hvl.instructure.com/api/graphql/"
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
    if 200 <= svar.status_code < 300:
        return svar.json()
    else:
        return {}


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
        with gzip.GzipFile(fileobj=buffer, mode='rb') as utpakka_fil:
            utpakka_data = utpakka_fil.read().decode()
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
        print(filar)
        for fil in filar:
            data = io.StringIO(akv_hent_CD2_filar(fil['id'], CD2_access_token, respons2))
            df = pd.read_csv(data, sep=",")
            dr_liste.append(df)
        alledata = pd.concat(df for df in dr_liste if not df.empty)
        return alledata, sist_oppdatert, respons2['until']
    except requests.exceptions.RequestException as exc:
        raise exc
    

def akv_les_CD2_pseudonyms():
    """
    Leser pseudonyms-tabellen fra Canvas Data 2, henter nye poster og oppdaterer Azure-tabellen "akv_user_id_kobling.
    """
    start_CD2_pseudonyms = time.perf_counter()
    CD2_tabell = "pseudonyms"
    alledata, sist_oppdatert, denne_oppdateringa = akv_les_CD2_tabell(CD2_tabell)
    alle_nye = alledata[(alledata['value.created_at']>sist_oppdatert)]
    alle_nye.to_csv(f"{CD2_tabell}_nye_{denne_oppdateringa[0:10]}.csv", index=False)
    ekte_nye = alle_nye.dropna(subset='value.sis_user_id')

    query = """
        MERGE INTO [dbo].[akv_user_id_kobling] AS target 
        USING (VALUES (?, ?)) AS source (user_id, sis_user_id) 
        ON target.[user_id] = source.[user_id]
        WHEN MATCHED THEN
            UPDATE SET target.[sis_user_id] = source.[sis_user_id]
        WHEN NOT MATCHED THEN
            INSERT ([user_id], [sis_user_id]) VALUES (source.[user_id], source.[sis_user_id]);
    """
    try:
        nye = ekte_nye[['value.user_id', 'value.sis_user_id']]
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            for index, row in nye.iterrows():
                user_id = str(row[0])
                sis_user_id = str(row[1])
                cursor.execute(query, (user_id, sis_user_id))
            conn.commit()
    except pyodbc.Error as e:
        with open(f'sist_oppdatert_{CD2_tabell}.txt', 'w') as f_out:
            f_out.write(idag)
            logging.debug(f"{CD2_tabell} er sist oppdatert (lokal): {idag}")
    akv_lagre_sist_oppdatert(CD2_tabell, denne_oppdateringa)
    print(f"Tabell: {CD2_tabell} er oppdatert {denne_oppdateringa}")
    print(f"Total tidsbruk: {time.perf_counter() - start_CD2_pseudonyms}")


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

    CD2_access_token = akv_hent_CD2_access_token()
    headers = {'x-instauth': CD2_access_token, 'Content-Type': 'text/plain'}

    tabell = "calendar_events"

    # Her set eg sist_oppdatert til "i går" (meir presist "kl. 12 føremiddag i går")
    sist_oppdatert_dato = f"{igår.year}-{igår.month:02}-{igår.day:02}T{12}:00:00Z"
    payload = '{"format": "csv", "since": \"%s\"}' %(sist_oppdatert_dato)

    try:
        requesturl = f"{CD2_base_url}/dap/query/canvas/table/{tabell}/data"
        logging.debug(f"Sender søk til {requesturl}")
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
                logging.debug(respons2)
                if respons2['status'] == "complete":
                    logging.debug(respons2)
                    vent = False
        else:
            logging.error(f"Feil i spørjing mot CD2, kode {r.status_code}")
        antal = len(respons2['objects'])
        data_i_dag = []
        for i in range(antal):
            data = io.StringIO(akv_hent_CD2_filar(respons2['objects'][i]['id'], CD2_access_token, respons2))
            df = pd.read_csv(data, sep=",")
            data_i_dag.append(df)
        logging.debug(f"{idag}: har henta {len(data_i_dag)} filer med kalenderhendingar")
    except RuntimeError:
        logging.error(f"{idag}: får ikkje lasta data frå kalenderen i Canvas")

    les_inn_startdata = data_i_dag[0]
    startdata = les_inn_startdata.loc[(les_inn_startdata['value.start_at'] > '2023') & (les_inn_startdata['value.workflow_state'] == 'active')][['key.id', 'value.title', 'value.start_at', 'value.end_at', 'value.location_name', 'value.description', 'value.context_code']]
    startdata['teacher'] = None
    startdata['timeedit_id'] = None
    liste_av_datarammer = [startdata]
    for les_inn_data in data_i_dag[1:]:
        data = les_inn_data.loc[(les_inn_data['value.start_at'] > '2023') & (les_inn_data['value.workflow_state'] == 'active')][['key.id', 'value.title', 'value.start_at', 'value.end_at', 'value.location_name', 'value.description', 'value.context_code']]
        liste_av_datarammer.append(data)
    behandla = pd.concat(liste_av_datarammer, ignore_index=True)
    behandla['teacher'] = behandla.apply(finn_lærar, axis=1)
    behandla['timeedit_id'] = behandla.apply(finn_timeedit_id, axis=1)
    dikt = behandla.to_dict('records')

    try:
        with pyodbc.connect(conn_str) as cnxn:
            data_to_insert = []
            for hending in dikt:
                logging.debug(f"Legg inn hending {hending['key.id']}")
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
        logging.debug(f"Har lasta opp {len(dikt)} kalender-hendingar til databasen")
    except RuntimeError:
        logging.error("Feil når eg skal legge timeplan-data inn i tabellen.")
    logging.info(f"Tidsbruk Canvas_Calendar: {time.perf_counter() - start_Canvas_Calendar} s")


def timer_FS_Studieprogram():
    start_FS_Studieprogram = time.perf_counter()
    query = """
    query studieprogram($antal: Int, $start: String ) {
        studieprogram(
            first: $antal
            after: $start
            filter: {eierInstitusjonsnummer: "0203"}
        ) {
            pageInfo {
                endCursor
                hasNextPage
            }
            nodes {
                navnAlleSprak {
                    nb
                }
                kode
                prosentHeltid
                organisasjonsenhet {
                    studieansvarlig {
                    fakultet {
                        fakultetsnummer
                    }
                    instituttnummer
                        navnAlleSprak {
                            nb
                        }
                    }
                }
                studieniva {
                    navnAlleSprak {
                        nb
                    }
                }
                undervisningsorganisering {
                    navnAlleSprak {
                        und
                    }
                }
                vekting {
                    vektingstype {
                        kode
                    }
                    verdi
                }
                finansieringstype {
                    navn {
                        und
                    }
                }
                prosentEgenfinansiering
                nusKode
            }
        }
    }
    """
    try:
        with pyodbc.connect(conn_str) as cnxn:
            data_to_insert = []
            n = 0
            hentmeir = True
            antal_per_side = 500
            while hentmeir:
                if n == 0:
                    start = None
                else:
                    start = studieprogramRespons['data']['studieprogram']['pageInfo']['endCursor']
                variable = {'antal': antal_per_side, 'start': start}
                n += 1
                logging.debug(f"Les side {n} studieprogram")
                studieprogramRespons = akv_query_FS_graphql(query, variable)
                logging.debug(f"Henta {len(studieprogramRespons['data']['studieprogram']['nodes'])} linjer frå FS")
                for item in studieprogramRespons['data']['studieprogram']['nodes']:
                    studieprogramkode = item['kode']
                    studieprogramnavn = item['navnAlleSprak']['nb']
                    fakultetsnummer = item['organisasjonsenhet']['studieansvarlig']['fakultet']['fakultetsnummer']
                    instituttnummer = item['organisasjonsenhet']['studieansvarlig']['instituttnummer']
                    instituttnavn = item['organisasjonsenhet']['studieansvarlig']['navnAlleSprak']['nb']
                    prosentAvHeltid = item['prosentHeltid']
                    studieniva = item['studieniva']['navnAlleSprak']['nb'] if item['studieniva'] is not None else ''
                    undervisningsorganisering = item['undervisningsorganisering']['navnAlleSprak']['und']
                    finansieringstype = item['finansieringstype']['navn']['und'] if item['finansieringstype'] is not None else ''
                    vekting = item['vekting']['verdi'] if item['vekting'] is not None else ''
                    vektingstype = item['vekting']['vektingstype']['kode'] if item['vekting'] is not None else ''
                    nuskode = item['nusKode'] if item['nusKode'] is not None else ''
                    prosentEgenfinansiering = item['prosentEgenfinansiering']
                    data_to_insert.append((studieprogramkode,
                                           studieprogramnavn,
                                           fakultetsnummer,
                                           instituttnummer,
                                           instituttnavn,
                                           int(prosentAvHeltid),
                                           studieniva,
                                           undervisningsorganisering,
                                           finansieringstype,
                                           float(vekting),
                                           vektingstype,
                                           nuskode if nuskode is not None else None,
                                           int(prosentEgenfinansiering)))
                hentmeir = studieprogramRespons['data']['studieprogram']['pageInfo']['hasNextPage']
                logging.debug(f"Variabelen 'hentmeir' er satt til {hentmeir}")
            query = """
                MERGE INTO [stg].[FS_Studieprogram] AS t
                USING (VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)) AS s(
                    [studieprogramkode],
                    [studieprogramnavn],
                    [fakultetsnummer],
                    [instituttnummer],
                    [instituttnavn],
                    [prosentAvHeltid],
                    [studieniva],
                    [undervisningsorganisering],
                    [finansieringstype],
                    [vekting],
                    [vektingstype],
                    [nuskode],
                    [prosentEgenfinansiering]
                )
                ON t.[studieprogramkode] = s.[studieprogramkode]
                WHEN MATCHED THEN
                    UPDATE SET
                        t.[studieprogramnavn] = s.[studieprogramnavn],
                        t.[fakultetsnummer] = s.[fakultetsnummer],
                        t.[instituttnummer] = s.[instituttnummer],
                        t.[instituttnavn] = s.[instituttnavn],
                        t.[prosentAvHeltid] = s.[prosentAvHeltid],
                        t.[studieniva] = s.[studieniva],
                        t.[undervisningsorganisering] = s.[undervisningsorganisering],
                        t.[finansieringstype] = s.[finansieringstype],
                        t.[vekting] = s.[vekting],
                        t.[vektingstype] = s.[vektingstype],
                        t.[nuskode] = s.[nuskode],
                        t.[prosentEgenfinansiering] = s.[prosentEgenfinansiering]
                WHEN NOT MATCHED THEN
                    INSERT (
                        [studieprogramkode],
                        [studieprogramnavn],
                        [fakultetsnummer],
                        [instituttnummer],
                        [instituttnavn],
                        [prosentAvHeltid],
                        [studieniva],
                        [undervisningsorganisering],
                        [finansieringstype],
                        [vekting],
                        [vektingstype],
                        [nuskode],
                        [prosentEgenfinansiering]
                    )
                    VALUES (
                        s.[studieprogramkode],
                        s.[studieprogramnavn],
                        s.[fakultetsnummer],
                        s.[instituttnummer],
                        s.[instituttnavn],
                        s.[prosentAvHeltid],
                        s.[studieniva],
                        s.[undervisningsorganisering],
                        s.[finansieringstype],
                        s.[vekting],
                        s.[vektingstype],
                        s.[nuskode],
                        s.[prosentEgenfinansiering]
                    );
            """
            with cnxn.cursor() as cursor:
                for data in data_to_insert:
                    cursor.execute(query, data)
                cnxn.commit()
                logging.debug("Data lasta opp til FS_Studieprogram")
    except (KeyError, TypeError):
        raise Exception("Feil i henting av FS-data.")
    logging.info(f"Tidsbruk FS_Studieprogram: {time.perf_counter() - start_FS_Studieprogram} s")



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
            nob
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
                    logging.debug(f"Les side {n} emneoversikt")
                    svar = akv_query_FS_graphql(query, variable)
                    for item in svar['data']['emner']['nodes']:
                        try:
                            emnekode = item['kode']
                            versjonskode = item['versjonskode']
                            unik_kode = emnekode + "_" + versjonskode
                            emnenavn_nob = item['navnAlleSprak']['nb']
                            emnenavn_nno = item['navnAlleSprak']['nn']
                            emnenavn_eng = item['navnAlleSprak']['en']
                            fag = item['fagkoblinger']['viktigsteFag']['navnAlleSprak']['nob']
                            fakultetsnummer = item['organisasjonsenhet']['administrativtAnsvarlig']['fakultet']['fakultetsnummer']
                            instituttnummer = item['organisasjonsenhet']['administrativtAnsvarlig']['instituttnummer']
                            instituttnavn = item['organisasjonsenhet']['administrativtAnsvarlig']['navnAlleSprak']['nb']
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
                            logging.debug(f"Feil i {item['kode']}")
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
        logging.debug("Feil ved oppdatering av tabell FS_Emne.")
        logging.debug(traceback.format_exc())
    logging.info(f"Tidsbruk FS_Emne: {time.perf_counter() - start_FS_Emne} s")


def timer_FS_ProgramStudieretter():
    query = """
    query MyQuery($antal: Int, $start: String) {
        programStudieretter(
            filter: {eierOrganisasjonskode: "0203", aktivStatus: AKTIV}, 
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
        try:
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
                    år = pSr['kull']['termin']['arstall']
                except (TypeError, KeyError, ValueError):
                    år = ''
                try:
                    termin = pSr['kull']['termin']['betegnelse']['navnAlleSprak']['nb']
                except (TypeError, KeyError, ValueError):
                    termin = ''
                programstudierettar.append([plnr, studieprogram, campus, år, termin])
        except (KeyError, TypeError):
            raise Exception("Feil i henting av FS-data.")
        hentmeir = svar['data']['programStudieretter']['pageInfo']['hasNextPage']

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
            logging.debug("Feil ved oppdatering av tabell FS_Emne.")
            logging.debug(traceback.format_exc())
    logging.info(f"Tidsbruk FS_ProgramStudieretter: {time.perf_counter() - start_les_FS_programstudierettar} s")


def timer_Canvas_Terms():
    start_Canvas_Terms = time.perf_counter()
    headers = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}
    url_template = 'https://hvl.instructure.com/api/v1/accounts/1/terms?sort=id&order=desc&per_page=100&page={0}'
    with pyodbc.connect(conn_str) as cnxn:
        with cnxn.cursor() as cursor:
            page = 1
            while True:
                url = url_template.format(page)
                response = requests.get(url, headers=headers)
                data = response.json()['enrollment_terms']
                if not data:
                    break
                for item in data:
                    term_id = item['id']
                    name = item['name']
                    start_at = item['start_at']
                    end_at = item['end_at']
                    created_at = item['created_at']
                    # (chatgpt)Removed unnecessary parameters and fixed the query
                    canvas_term_query = """
                        MERGE INTO [stg].[Canvas_Terms] AS t
                        USING (VALUES (?, ?, ?, ?, ?)) AS s (
                            [term_id],
                            [name],
                            [start_at],
                            [end_at],
                            [created_at]
                        )
                        ON t.[term_id] = s.[term_id]
                        WHEN MATCHED THEN
                            UPDATE SET
                                t.[name] = s.[name],
                                t.[start_at] = s.[start_at],
                                t.[end_at] = s.[end_at],
                                t.[created_at] = s.[created_at]
                        WHEN NOT MATCHED THEN
                            INSERT ([term_id], [name], [start_at], [end_at], [created_at])
                            VALUES (s.[term_id], s.[name], s.[start_at], s.[end_at], s.[created_at]);
                    """
                    cursor.execute(canvas_term_query, term_id, name, start_at, end_at, created_at)
                    cnxn.commit()
                page += 1
            logging.debug("Data lasta opp til Canvas_Terms")
    logging.info(f"Tidsbruk Canvas_Terms: {time.perf_counter() - start_Canvas_Terms} s")


def timer_Canvas_Users():
    start_Canvas_Users = time.perf_counter()
    headers = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}
    url_template = 'https://hvl.instructure.com/api/v1/accounts/54/users?sort=last_login&order=desc&per_page=100&page={0}'
    with pyodbc.connect(conn_str) as cnxn:
        with cnxn.cursor() as cursor:
            page = 1
            while True:
                url = url_template.format(page)
                response = requests.get(url, headers=headers)
                data = response.json()
                if not data:
                    break
                for item in data:
                    user_id = item['id']
                    sis_user_id = item['sis_user_id']
                    created_at = item['created_at']
                    root_account = item.get('root_account')
                    last_login = item['last_login']                    
                    query = """
                        MERGE INTO [stg].[Canvas_Users] WITH (HOLDLOCK) AS t \
                        USING (VALUES (?,?,?,?,?)) AS s(
                            [user_id],
                            [sis_user_id],
                            [created_at],
                            [root_account],
                            [last_login]
                        )
                        ON t.[user_id] = s.[user_id]
                        WHEN MATCHED THEN
                            UPDATE SET
                            t.[sis_user_id] = s.[sis_user_id],
                            t.[created_at] = s.[created_at],
                            t.[root_account] = s.[root_account],
                            t.[last_login] = s.[last_login]
                        WHEN NOT MATCHED THEN
                            INSERT (
                                [user_id],
                                [sis_user_id],
                                [created_at],
                                [root_account],
                                [last_login]
                            )
                            VALUES (
                                s.[user_id],
                                s.[sis_user_id],
                                s.[created_at],
                                s.[root_account],
                                s.[last_login]
                            );
                    """
                    cursor.execute(query, user_id, sis_user_id,
                                   created_at, root_account, last_login)
                    cnxn.commit()
                page += 1
            logging.debug("Data lasta opp til Canvas_Users")
    logging.info(f"Tidsbruk Canvas_Users: {time.perf_counter() - start_Canvas_Users} s")


def timer_Canvas_Courses():
    start_Canvas_Courses = time.perf_counter()
    headers = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}
    url_template = 'https://hvl.instructure.com/api/v1/accounts/54/courses?sort=created_at&order=desc&per_page=100&page={0}'
    with pyodbc.connect(conn_str) as cnxn:
        with cnxn.cursor() as cursor:
            page = 1
            while True:
                url = url_template.format(page)
                response = requests.get(url, headers=headers)
                data = response.json()
                if not data:
                    break
                for item in data:
                    course_id = item['id']
                    name = item['name']
                    course_code = item['course_code']
                    sis_course_id = item['sis_course_id']
                    enrollment_term_id = item['enrollment_term_id']
                    account_id = item['account_id']
                    start_at = item['start_at']
                    conclude_at = item['end_at']
                    created_at = item['created_at']
                    updated_at = item.get('updated_at')
                    root_account_id = item['root_account_id']
                    workflow_state = item['workflow_state']
                    login_id = item.get('login_id')
                    if (sis_course_id is not None) and ('_203_' in sis_course_id):
                        emnekode = sis_course_id.split("_")[2]
                        versjonskode = sis_course_id.split("_")[3]
                    else:
                        emnekode = ' '
                        versjonskode = ' '
                    query = """
                        MERGE INTO [stg].[Canvas_Courses] WITH (HOLDLOCK) AS t
                        USING (VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)) AS s(
                            [course_id],
                            [name],
                            [course_code],
                            [sis_course_id],
                            [enrollment_term_id],
                            [account_id],
                            [start_at],
                            [conclude_at],
                            [created_at],
                            [updated_at],
                            [root_account_id],
                            [workflow_state],
                            [login_id],
                            [emnekode],
                            [versjonskode]
                        )
                        ON t.[course_id] = s.[course_id]
                        WHEN MATCHED THEN
                            UPDATE SET 
                                t.[name] = s.[name],
                                t.[course_code] = s.[course_code],
                                t.[sis_course_id] = s.[sis_course_id],
                                t.[enrollment_term_id] = s.[enrollment_term_id],
                                t.[account_id] = s.[account_id],
                                t.[start_at] = s.[start_at],
                                t.[conclude_at] = s.[conclude_at],
                                t.[created_at] = s.[created_at],
                                t.[updated_at] = s.[updated_at],
                                t.[root_account_id] = s.[root_account_id],
                                t.[workflow_state] = s.[workflow_state],
                                t.[login_id] = s.[login_id],
                                t.[emnekode] = s.[emnekode],
                                t.[versjonskode] = s.[versjonskode]
                        WHEN NOT MATCHED THEN
                            INSERT ([course_id],
                                [name],
                                [course_code],
                                [sis_course_id],
                                [enrollment_term_id],
                                [account_id],
                                [start_at],
                                [conclude_at],
                                [created_at],
                                [updated_at],
                                [root_account_id],
                                [workflow_state],
                                [login_id],
                                [emnekode],
                                [versjonskode]
                            )
                            VALUES (s.[course_id],
                                s.[name],
                                s.[course_code],
                                s.[sis_course_id],
                                s.[enrollment_term_id],
                                s.[account_id],
                                s.[start_at],
                                s.[conclude_at],
                                s.[created_at],
                                s.[updated_at],
                                s.[root_account_id],
                                s.[workflow_state],
                                s.[login_id],
                                s.[emnekode],
                                s.[versjonskode]
                            );
                    """
                    try:
                        cursor.execute(query, course_id, name, course_code, sis_course_id, enrollment_term_id, account_id,
                                       start_at, conclude_at, created_at, updated_at, root_account_id, workflow_state, login_id, emnekode, versjonskode)
                        cnxn.commit()
                    except pyodbc.Error as feil:
                        logging.error(f"Noko gjekk galt med opplasting av {emnekode}: {feil}")
                page += 1
            logging.debug("Data lasta opp til Canvas_Courses")
    logging.info(f"Tidsbruk Canvas_Courses: {time.perf_counter() - start_Canvas_Courses} s")


def timer_Canvas_Enrollments_Ny():
    start_Canvas_Enrollments_Ny = time.perf_counter()

    CD2_access_token = akv_hent_CD2_access_token()
    headers = {'x-instauth': CD2_access_token, 'Content-Type': 'text/plain'}

    tabell = "enrollments"
    sist_oppdatert_dato = f"{igår.year}-{igår.month:02}-{igår.day:02}T{12}:00:00Z"
    payload = '{"format": "csv", "since": \"%s\"}' % (sist_oppdatert_dato)

    try:
        requesturl = f"{CD2_base_url}/dap/query/canvas/table/{tabell}/data"
        logging.debug(f"Sender søk til {requesturl}")
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
                logging.debug(respons2)
                if respons2['status'] == "complete":
                    vent = False
        else:
            logging.error(f"Feil i spørjing mot CD2, kode {r.status_code}")
        antal = len(respons2['objects'])
        data_i_dag = []
        for i in range(antal):
            data = io.StringIO(akv_hent_CD2_filar(
                respons2['objects'][i]['id'], CD2_access_token, respons2))
            df = pd.read_csv(data, sep=",")
            data_i_dag.append(df)
        # logging.info(f"Har henta {len(data_i_dag)} filer med enrollments")
    except RuntimeError:
        logging.error("Får ikkje lasta data frå enrollments i Canvas Data 2")
    dataliste = [data_i_dag[0][['key.id', 'value.user_id', 'value.course_id', 'value.type', 'value.created_at', 'value.updated_at', 'value.start_at', 'value.end_at', 'value.workflow_state', 'value.total_activity_time', 'value.last_activity_at']]]
    for datasett in data_i_dag[1:]:
        dataliste.append(datasett[['key.id', 'value.user_id', 'value.course_id', 'value.type', 'value.created_at', 'value.updated_at', 'value.start_at', 'value.end_at', 'value.workflow_state', 'value.total_activity_time', 'value.last_activity_at']])
    oppsamla = pd.concat(dataliste)
    oppsamla['sis_user_id'] = " "
    oppsamla['value.total_activity_time'] = oppsamla['value.total_activity_time'].fillna(0.0)
    oppsamla['value.total_activity_time'] = oppsamla['value.total_activity_time'].astype(int)
    # Ensure created_at, updated_at, start_at, end_at, and last_activity_at are datetime
    oppsamla['value.created_at'] = oppsamla['value.created_at'].apply(lambda x: None if np.isnan(x) else pd.to_datetime(x))
    oppsamla['value.updated_at'] = oppsamla['value.updated_at'].apply(lambda x: None if np.isnan(x) else pd.to_datetime(x))
    oppsamla['value.start_at'] = oppsamla['value.start_at'].apply(lambda x: None if np.isnan(x) else pd.to_datetime(x))
    oppsamla['value.end_at'] = oppsamla['value.end_at'].apply(lambda x: None if np.isnan(x) else pd.to_datetime(x))
    dikt = oppsamla.to_dict('records')

    data_to_insert = []
    for e in dikt:
        # Adding a default value for 'sis_user_id' if it is missing, sa ChatGPT for å sikre seg at denne vil ha innhold når listen skal fylles.
        e['sis_user_id'] = e.get('sis_user_id', " ")
        data_to_insert.append((
            e['key.id'],
            e['value.user_id'],
            e['sis_user_id'],
            e['value.course_id'],
            e['value.type'],
            e['value.created_at'],
            e['value.updated_at'],
            e['value.start_at'],
            e['value.end_at'],
            e['value.workflow_state'],
            e['value.total_activity_time'],
            e['value.last_activity_at']))
    query = """
        MERGE INTO [stg].[Canvas_Enrollments] AS target
        USING (SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS source (
            [enrollment_id],
            [user_id],
            [sis_user_id],
            [course_id],
            [type],
            [created_at],
            [updated_at],
            [start_at],
            [end_at],
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
                target.[start_at] = source.[start_at],
                target.[end_at] = source.[end_at],
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
                [start_at],
                [end_at],
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
            source.[start_at],
            source.[end_at],
            source.[workflow_state],
            source.[total_activity_time],
            source.[last_activity_at]
        );
    """
    try:
        første_feil = True

        def is_valid_bigint(value):
            try:
                # Convert to integer and check if it fits within the range of bigint
                int_value = int(value)
                if -(2**63) <= int_value <= (2**63 - 1):
                    return True
                else:
                    return False
            except ValueError:
                return False

        with pyodbc.connect(conn_str) as cnxn:
            with cnxn.cursor() as cursor:
                for data in data_to_insert:
                    if første_feil:
                        try:
                            cursor.execute(query, data)
                        except:
                            første_feil = False
                            logging.error(f"Feil ved opplasting av ein post.\nData: {data}")
                            logging.error(traceback.format_exc())
                cnxn.commit()
    except:
        logging.error(f"Feil når eg skal legge enrollments inn i tabellen.")
        logging.error(traceback.format_exc())
    logging.info(f"Tidsbruk Canvas_Enrollments_Ny: {time.perf_counter() - start_Canvas_Enrollments_Ny} s")


def timer_Canvas_Enrollments():
    start_Canvas_Enrollments = time.perf_counter()
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
        except pyodbc.Error as e:
            logging.error(f"Feil: {e}")

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
        try:
            resultat = akv_query_canvas_graphql(queryCanvas, variables)
            enrollments = resultat['data']['course']['enrollmentsConnection']['nodes']
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
            statistikk.append([course_id, len(enrollments)])
        except:
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

def timer_Canvas_Courses_StudentSummaries():
    start_Canvas_StudentSummaries = time.perf_counter()
    headers = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}
    url_template = 'https://hvl.instructure.com/api/v1/courses/{course_id}/analytics/student_summaries?per_page=100&page={page_number}'

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
                    user_id = item['id']
                    page_views = item['page_views']
                    max_page_views = item['max_page_views']
                    page_views_level = item['page_views_level']
                    participations = item['participations']
                    max_participations = item['max_participations']
                    participations_level = item['participations_level']
                    course_id = emne
                    missing = item['tardiness_breakdown']['missing']
                    late = item['tardiness_breakdown']['late']
                    on_time = item['tardiness_breakdown']['on_time']
                    floating = item['tardiness_breakdown']['floating']
                    total = item['tardiness_breakdown']['total']
                    merge_query = """
                        MERGE INTO [stg].[Canvas_Courses_StudentSummaries] AS target
                        USING (SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS source (
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
                            floating,
                            total
                        )
                        ON target.id = source.id AND target.course_id = source.course_id
                        WHEN MATCHED THEN
                            UPDATE SET 
                                target.max_page_views = source.max_page_views,
                                target.page_views = source.page_views,
                                target.page_views_level = source.page_views_level,
                                target.participations = source.participations,
                                target.max_participations = source.max_participations,
                                target.participations_level = source.participations_level,
                                target.missing = source.missing,
                                target.late = source.late,
                                target.on_time = source.on_time,
                                target.floating = source.floating,
                                target.total = source.total
                        WHEN NOT MATCHED THEN
                            INSERT (
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
                                floating,
                                total
                            )
                            VALUES (
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
                                source.floating,
                                source.total
                            );
                    """
                    cursor.execute(merge_query, page_views, user_id, max_page_views, page_views_level, participations,
                                    max_participations, participations_level, course_id, missing, late, on_time, floating, total)
                cnxn.commit()
                page_number += 1
            logging.debug("Data lasta opp til Canvas_Courses_StudentSummaries")
    logging.info(f"Tidsbruk Canvas_StudentSummaries: {time.perf_counter() - start_Canvas_StudentSummaries} s")


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

def timer_Canvas_History():
    start_Canvas_History = time.perf_counter()
    headers = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}
    url_template = 'https://hvl.instructure.com/api/v1/users/{0}/history'
    query = "DELETE FROM [stg].[Canvas_History]"
    with pyodbc.connect(conn_str) as cnxn:
        with cnxn.cursor() as cursor:
            cursor.execute(query)
            cnxn.commit()
            query = """
                SELECT DISTINCT [stg].[Canvas_Users].[user_id]
                FROM [stg].[Canvas_Users]
                LEFT JOIN [stg].[Canvas_Enrollments]
                    ON [stg].[Canvas_Enrollments].[user_id] = [stg].[Canvas_Users].[user_id]
                    WHERE [stg].[Canvas_Enrollments].[user_id] is not null and (last_login > getdate()-3  or last_activity_at > getdate()-3)

            """
            cursor.execute(query)
            user_ids = [row[0] for row in cursor.fetchall()]
            for user_id in user_ids:
                url = url_template.format(user_id)
                response = requests.get(url, headers=headers)
                for item in response.json():
                    visitedAt = item['visited_at']
                    visitedURL = item['visited_url']
                    assetReadableCategory = item['asset_readable_category']
                    userId = user_id
                    query = "INSERT INTO [stg].[Canvas_History] (visited_at, visited_url, asset_readable_category, user_id) VALUES (?,?,?,?)"
                    cursor.execute(query, visitedAt, visitedURL,
                                   assetReadableCategory, userId)
                    cnxn.commit()
            query = "EXEC dbo.Populate_dbo_Canvas_History"
            cursor.execute(query)
            cnxn.commit()
            logging.debug("Data lasta opp til Canvas_History")
    logging.info(f"Tidsbruk Canvas_History: {time.perf_counter() - start_Canvas_History} s")


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
    antal_per_side = 100
    while hentmeir:
        if n == 0:
            start = None
        else:
            start = svar['data']['emner']['pageInfo']['endCursor']
        variable = {'antal': antal_per_side, 'start': start}
        n += 1
        svar = akv_query_FS_graphql(query_FS_EmneProgKobling, variable)
        if svar == {}:
            hentmeir = False # Avbryt prosessen når ein feil oppstår, slik at resten av programmet ikkje kræsjer
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
                        emnekoblingar.append({'Emnekode': emnekode,
                                            'Versjonskode': versjonskode,
                                            'Studieprogramkode': None,
                                            'Undervises_forste_ar': emnestart_år,
                                            'Undervises_forste_termin': emnestart_termin,
                                            'Undervises_siste_ar': emneslutt_år,
                                            'Undervises_siste_termin': emneslutt_termin})

        hentmeir = svar['data']['emner']['pageInfo']['hasNextPage']

    dataramme = pd.DataFrame(emnekoblingar, columns=['Emnekode', 'Versjonskode', 'Studieprogramkode', 'Undervises_forste_ar', 'Undervises_forste_termin', 'Undervises_siste_ar', 'Undervises_siste_termin'])
    dataramme['Emnekode2'] = dataramme['Emnekode'] + '_' + dataramme['Versjonskode']
    dataramme['programEmneKode'] = dataramme['Studieprogramkode'] + '_' + dataramme['Emnekode'] + '_' + dataramme['Versjonskode']
    dataramme.loc[dataramme['Undervises_siste_ar'].isna(), 'Undervises_siste_termin'] = None
    dataramme = dataramme.where(pd.notnull(dataramme), "")
    emnekoblingar = dataramme.values.tolist()

    conn_str = os.environ["Connection_SQL"]
    with pyodbc.connect(conn_str) as cnxn:
        with cnxn.cursor() as cursor:
            merge_query = """
                MERGE INTO [stg].[FS_EmneProgKobling] AS target
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
                cursor.execute(merge_query, (Emnekode, Versjonskode, Studieprogramkode, Undervises_forste_ar, Undervises_forste_termin, Undervises_siste_ar, Undervises_siste_termin, Emnekode2, programEmneKode))
            cnxn.commit()
    logging.info(f"Tidsbruk FS_EmneProgKobling: {time.perf_counter() - start_FS_EmneProgKobling} s")


def main(mytimer: func.TimerRequest) -> None:
    logging.basicConfig(filename='timerRequest.log', encoding='utf-8', level=logging.INFO)
    # try:
    #     timer_Canvas_Calendar()
    # except:
    #     logging.exception("Feil i timer_Canvas_Calendar")
    try:
        timer_Canvas_Users()
    except:
        logging.exception("Feil i timer_Canvas_Users")
    if dag in [1]:            
        try:
            timer_Canvas_Terms()
        except:
            logging.exception("Feil i timer_Canvas_Terms")
        try:
            timer_FS_Studieprogram()
        except:
            logging.exception("Feil i timer_FS_Studieprogram")
        try:
            timer_FS_emne()
        except:
            logging.exception("Feil i timer_FS_emne")
    try:
        timer_FS_ProgramStudieretter()
    except:
        logging.exception("Feil i timer_FS_ProgramStudieretter")
    try:
        timer_Canvas_Courses() 
    except:
        logging.exception("Feil i timer_Canvas_Courses")
    try:
        timer_Canvas_Enrollments()
    except:
        logging.exception("Feil i timer_Canvas_Enrollments")
    try:
        timer_Canvas_Courses_StudentSummaries()
    except:
        logging.exception("Feil i timer_Canvas_Courses_StudentSummaries")
    try:
        timer_Canvas_Modules()
    except:
        logging.exception("Feil i timer_Canvas_Modules")
    try:
        timer_Canvas_History()
    except:
        logging.exception("Feil i timer_Canvas_History")
    try:
        timer_FS_EmneProgKobling()
    except:
        logging.exception("Feil i timer_FS_EmneProgKobling")
    # try:
    #     timer_Canvas_Enrollments_Ny()
    # except:
    #     logging.exception("Feil i timer_Canvas_Enrollments_Ny")