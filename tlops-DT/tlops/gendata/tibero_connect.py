##############
###
### 2. tibero에 연결하는 정보를 담은 class가 있는 파일
###
#############

import datetime
import os
import io
import pandas as pd
import numpy as np
from numpy import nan as NA

from typing import Iterator, Optional
import tempfile

import pyodbc

# from _00_util_ import util_


class TiberoDatabases():
    # def __init__(self, host='175.198.238.72', port=28629, dbname='itsio', user='signal', password='signal'):
    def __init__(self, dsn='Tibero', user='signal', password='signal'):
        # ## 인천 합사 volt db
        # self.host = '175.198.238.72'
        # self.user = 'icits'
        # self.password = 'icits'
        # self.port = 21212
        # self.dbname = 'itsio'

        # ## 인천 교통정보센터 tibero db
        # self.host = '192.168.6.10'
        # self.user = 'icsignal'
        # self.password = 'icsignal'
        # self.port = 8629
        # self.dbname = iomsprd


        # self.host = host
        # self.dbname = dbname
        # self.port = port

        self.dsn = dsn
        self.user = user
        self.password = password

        connection_string = f'DSN=Tibero;UID={user};PWD={password}'
        self.conn = pyodbc.connect(connection_string)
        self.cursor = self.conn.cursor()
#         self.engine = create_engine(f"tibero://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        
    # def __del__(self):
    #     self.cursor.close()
    #     self.conn.close()


    def commit(self):
        try:
            self.conn.commit()
            return 0
        except Exception as e:
            return e
        
    ## select 쿼리 제외하고 진행하는 쿼리
    def execute(self,query):
        self.cursor.execute(query)
        result = self.commit()
        return result

    ## select 쿼리만 쓸때 쿼리
    def execute_df(self,query):
        df = pd.read_sql(sql=query, con=self.conn)
        df.columns = df.columns.str.lower()
        return df
    


if __name__ == "__main__":
    
    # query= "insert into A (COLUMN1) values ('123');"

    db = TiberoDatabases(dsn='Tibero', user='signal', password='signal')


    # query = """insert into TEST_TABLE2 (GEOINFO, TEST) values (POINTFROMTEXT('POINT (126.652460680298 37.42404476629215)'), 'wow');"""
    
    # print(db.execute(query))


    query= "select * from soitsnode;"

    df = db.execute_df(query)
    print(df)