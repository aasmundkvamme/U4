import pandas as pd
import os
import requests
from datetime import datetime, date, timedelta
import time


def finn_rel(link_header):
    link_header_dict = {}
    for link in link_header.split(","):
        url, rel = link.strip().split(";")
        rel = rel.split('=')[1]
        link_header_dict[rel.strip().replace('"', '')] = url.strip().replace('<', '').replace('>', '')
    return link_header_dict

parametreCanvas = {'per_page': '100'}
hodeCanvas = {'Authorization': 'Bearer ' + os.environ["tokenCanvas"]}

konto = 1
url = f"https://hvl.instructure.com/api/v1/accounts/{konto}/users"

dr_liste = []
hentmeir = True
while hentmeir:
    respons = requests.get(url, headers=hodeCanvas, params=parametreCanvas)
    if 200 <= respons.status_code < 300:
        data = respons.json()
        df = pd.DataFrame(data=[{'id': item['id'], 'sis_user_id': item['sis_user_id']} for item in data])
        dr_liste.append(df)
        hentmeir = "next" in respons.headers['link']
        if hentmeir:
            url = finn_rel(respons.headers['link'])['next']
            print(url)
alledata = pd.concat(dr_liste)
alledata.to_csv("users_REST_241017.csv", index=False)