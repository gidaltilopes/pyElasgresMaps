import googlemaps
from datetime import datetime
import psycopg2
import sys
import json
from elasticsearch import Elasticsearch

gmaps = googlemaps.Client(key='key')

es = Elasticsearch()
try:
    con = psycopg2.connect(host='127.0.0.1', database='db', user='user', password='pass')
    cur = con.cursor()
    cur.execute('select cep_column from table')
    version = cur.fetchall()
    n = 1
    for i in version:
       geocode_result = json.dumps(gmaps.geocode(', '.join(list(i)))).strip('[]')
       try:
           res = es.index(index="cep-index", doc_type='cep', id=n, body=geocode_result)
           print(res['result'])
           n += 1
       except Exception:
           print(i)
except psycopg2.DatabaseError as e:
    print(e.pgerror)
    sys.exit(1)
