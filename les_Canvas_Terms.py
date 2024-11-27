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
desimalår = år + 0.5*(termin == "HØST")

conn_str = os.environ["Connection_SQL"]

def aktuell_termin(t):
    if t[1] == f"{str(år)} {termin}":
        utdata = t[0]
    elif "-" in t[1]:
        termin1 = t[1].split("-")[0]
        termin2 = t[1].split("-")[1]
        år1 = termin1.split(" ")[0]
        år2 = termin2.split(" ")[0]
        semester1 = termin1.split(" ")[1]
        semester2 = termin2.split(" ")[1]
        desimalår1 = int(år1) + 0.5*(semester1 == "HØST")
        desimalår2 = int(år2) + 0.5*(semester2 == "HØST")
        if desimalår1 <= desimalår <= desimalår2:
            utdata = t[0]
        else:
            utdata = 0
    else:
        utdata = 0    
    return utdata    

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
                print("Ingen data i denne raden")
    except pyodbc.Error as e:
        print(f"Feil: {e}")
pd.DataFrame(terminar).to_csv("terminar.csv", index=False)

aktuelle_terminar = []
for t in terminar:
    if aktuell_termin(t) != 0:
        aktuelle_terminar.append(aktuell_termin(t))

print(aktuelle_terminar)