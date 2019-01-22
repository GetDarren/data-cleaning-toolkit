import sqlalchemy as sa
import os
import time
import pyodbc
import pandas as pd
import urllib
server =
database =
username =
password = 
conn = pyodbc.connect(DRIVER='{SQL Server Native Client 11.0}', SERVER=server,
                        DATABASE=database, UID=username, PWD=password)
params = urllib.parse.quote("DRIVER={SQL Server Native Client 11.0};SERVER=server;DATABASE=db;UID=user;PWD=pwd")
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
df.to_sql("test_table",engine,if_exists="append",chunksize=100,index=False)

# Pandas connect MySQL
# #!/usr/bin/python3
import pymysql
import pandas as pd
import numpy as np

conn = pymysql.connect(host="127.0.0.1", port=3306, user="root", password="darrenzhou",db="world", charset = "utf8")
cursor=conn.cursor()

# cursor.execute("select * from city")

# row_1 = cursor.fetchone()
# print(row_1)
# row_3 = cursor.fetchall()
query = "select * from city limit 10"
df = pd.read_sql(query, conn)
conn.commit()
cursor.close()
conn.close() 