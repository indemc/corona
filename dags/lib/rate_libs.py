


from sqlalchemy import create_engine
import pandas as pd
import requests
import json
import psycopg2
from bs4 import BeautifulSoup
import re

table_names = ['crv_1', 'crv_2', 'crv_3']

def _db_connect():
    conn = psycopg2.connect(dbname='postgres', user='postgres', host='pg-server', password='postgres')
    conn.autocommit = True
    return conn

def _engine_creation():
    engine = create_engine('postgresql+psycopg2://postgres:postgres@pg-server:5432/postgres')
    return  engine

def create_necessary_tables():
    conn = _db_connect()
    cursor_obj = conn.cursor()

    for i in table_names:
        query1 = f"DROP TABLE IF EXISTS {i};"
        query2 = f"CREATE TABLE IF NOT EXISTS {i}\
                   (Country_Other VARCHAR ( 70 ),\
                   TotalCases INT,\
                   NewCases INT,\
                   TotalDeaths INT,\
                   TotalRecovered INT,\
                   NewRecovered INT,\
                   ActiveCases INT);"

        cursor_obj.execute(query1)
        cursor_obj.execute(query2)

    cursor_obj.close()
    conn.close()

def get_data_from_source():
    engine = _engine_creation()

    u = 'https://www.worldometers.info/coronavirus/'
    r = requests.get(u)
    soup = BeautifulSoup(r.content, 'html.parser')
    s = soup.find('div', class_='main_table_countries_div')
    s = str(s)
    html_source = re.sub(r'\+|\:', r'', s)

    xx = pd.read_html(html_source)[0]
    xx.rename(columns={'Country,Other': 'Country_Other'}, inplace=True)
    xx = xx[['Country_Other', 'TotalCases', 'NewCases', 'TotalDeaths', 'NewDeaths', 'TotalRecovered', 'NewRecovered',
             'ActiveCases']]
    xx = xx.fillna(0)
    xx = xx.astype({'Country_Other': str,
                    'TotalCases': int,
                    'NewCases': int,
                    'TotalDeaths': int,
                    'NewDeaths': int,
                    'TotalRecovered': int,
                    'NewRecovered': int,
                    'ActiveCases': int})
                    
    xx.columns = xx.columns.str.lower()

    for i in table_names:
        xx.to_sql(i, con=engine, if_exists='replace', index=False)



def transform_loaded_data(ti):
    conn = _db_connect()
    cursor_obj = conn.cursor()
    query1 = f"DROP TABLE IF EXISTS public.new_table;"
    query2 = 'Select "country_other" as country, "totalrecovered", "totalcases" INTO TABLE public.new_table \
              FROM public.crv_1 ORDER BY "totalrecovered"/ "totalcases" ASC ;'

    cursor_obj.execute(query1)
    cursor_obj.execute(query2)
    
    cursor_obj.close()
    conn.close()



