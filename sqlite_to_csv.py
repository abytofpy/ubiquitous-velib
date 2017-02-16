# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 10:08:26 2017

@author: CEPE-S1-12
"""

import sqlite3
import csv

inpsql3 = sqlite3.connect('data/SQLiteData/Velib_raw_Data.sqlite')
sql3_cursor = inpsql3.cursor()
sql3_cursor.execute('SELECT * FROM data')
with open('data/SQLiteData/Velib_raw_Data.csv','w') as out_csv_file:
  csv_out = csv.writer(out_csv_file)
  # write header                        
  csv_out.writerow([d[0] for d in sql3_cursor.description])
  # write data                          
  for result in sql3_cursor:
    csv_out.writerow(result)
inpsql3.close()