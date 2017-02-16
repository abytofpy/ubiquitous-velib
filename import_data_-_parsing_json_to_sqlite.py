# -*- coding: utf-8 -*-
"""
Chargement des données Velib du site http://vlsstats.ifsttar.fr/rawdata/
Les données traitées correspondent aux fichiers .gz produits à une fréquence
mensuelle.
Il est nécessaire de télécharger les données pour les traiter avec ce script.
Ce script ne traite pas les données accessibles via l'API JCD temps réel.
"""

import json
import gzip
import pandas as pd
import sqlalchemy as sa
import os


def gzip_Json_to_str(jsonfilename):
    with gzip.GzipFile(jsonfilename, 'r') as file:        
        json_bytes = file.read()         
        json_str = json_bytes.decode('utf-8')#, errors='ignore')            
        data = json.loads(json.dumps(json_str))[1:-2]
        return(data) 


def gzip_Json_todf(jsonfilename):
    data = gzip_Json_to_str(jsonfilename)
    data1 = data.replace(']',',\n')
    data2 =  "[" + data1.replace('[', '') + "]"
    return(pd.read_json(data2, orient='records'))


def gzip_Json_to_sqlite(jsonfilename):
    data_frame = gzip_Json_todf(jsonfilename)
    columns = ['available_bike_stands', 'available_bikes', 'bike_stands',
           'download_date', 'last_update', 'number', 'status']

    for c in data_frame.columns:
        if c not in columns:
            data_frame = data_frame.drop(c, axis=1) 

    #TODO : engine = create_engine('mysql+mysqldb://meee:pass@localhost/foo')
    data_frame.to_sql('data', disk_engine, chunksize=1000, if_exists='append')
    # alternative : absolute path starting with an additional slash
   
if __name__ == "__main__":
    
    disk_engine = sa.create_engine('sqlite:///data/SQLiteData/Velib_raw_Data.sqlite')
    
    for file in os.listdir('data/data_all_Paris/'):
        file = 'data/data_all_Paris/' + file
        try :
            gzip_Json_to_sqlite(file)
        except :
            print('Error while parsing : ' + file)