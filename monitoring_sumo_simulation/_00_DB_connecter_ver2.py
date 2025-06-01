import traceback

import psycopg2
import pandas as pd
import datetime
import os
import numpy as np
from numpy import nan as NA
from sqlalchemy import create_engine
import io

from typing import Iterator, Optional
import tempfile

from _00_util_ import util_
import _99_Contents as contents


class Databases:
    ## 안양시 sumo개발pc 서버
    # self.host = 'localhost'
    # self.dbname = 'anyang_traffic'
    # self.user = 'anyang_traffic'
    # self.password = 'anyang1234'
    # self.port = 5432

    ## 안양시 개발pc 서버(김한빈차장님)
    # self.host = '175.198.238.72'
    # self.dbname = 'anyang_traffic'
    # self.user = 'anyang_traffic'
    # self.password = 'anyang1234'
    # self.port = 15432

    ## 본사 개발팀과 함께쓰는 서버
    # self.host = 'midas.uinetworks.kr'
    # self.dbname = 'signalcontrol'
    # self.user = 'signal'
    # self.password = 'signal'
    # self.port = 15432

    # ## 컨설팅사업본부 서버
    # self.host = '1.223.235.149'
    # self.dbname = 'postgres'
    # self.user = 'postgres'
    # self.password = '0713'
    # self.port = 54325

    ## 컨본 새 서버
    host = '192.168.0.79'
    dbname = 'postgres'
    user = 'postgres'
    password = '0713'
    port = 54325

    db = None
    cursor = None
    engine = None
    schema_nm = contents.schema_nm if host == '192.168.0.79' else 'postgres'

    def __init__(self):
        if Databases.db is None:
            try:
                Databases.db = psycopg2.connect(host=Databases.host, dbname=Databases.dbname, user=Databases.user,
                                                password=Databases.password, port=Databases.port)
                Databases.cursor = Databases.db.cursor()
                Databases.engine = create_engine(
                    f"postgresql://{Databases.user}:{Databases.password}@{Databases.host}:{Databases.port}/{Databases.dbname}")
            except Exception as error:
                print("Error: Connection not established {}".format(error))
            else:
                print(Databases.db)

        self.connection = Databases.db
        self.cursor = Databases.cursor
        self.engine = Databases.engine
        self.schema_nm = Databases.schema_nm

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def execute(self, query, args={}):
        try:
            self.cursor.execute(query)
        except psycopg2.ProgrammingError as exc:
            self.db.rollback()
        except psycopg2.InterfaceError as exc:
            self.db = psycopg2.connect(host=Databases.host, dbname=Databases.dbname, user=Databases.user,
                                       password=Databases.password, port=Databases.port)
            self.cursor = self.db.cursor()
        except Exception as e:
            print(traceback.format_exc())

        self.db.commit()
        return 0

    def execute_df(self, query):
        df = pd.read_sql(query, self.db)
        return df

    def commit(self):
        self.db.commit()

    def read_sql_tmpfile(self, query):
        with tempfile.TemporaryFile() as tmpfile:
            copy_sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"

            try:
                self.cursor.copy_expert(copy_sql, tmpfile)
            except psycopg2.ProgrammingError as exc:
                self.db.rollback()
            except psycopg2.InterfaceError as exc:
                self.db = psycopg2.connect(host=Databases.host, dbname=Databases.dbname, user=Databases.user,
                                           password=Databases.password, port=Databases.port)
                self.cursor = self.db.cursor()
                self.cursor.copy_expert(copy_sql, tmpfile)
            except Exception as e:
                print(traceback.format_exc())

            tmpfile.seek(0)
            df = pd.read_csv(tmpfile)
            return df


########## 데이터 불러오는 코드, DB의 데이터에서 날짜로 필터링한 후 pandas의 dataframe형태로 불러옴
class Load_data(Databases):
    def __init__(self):
        Databases.__init__(self)
        self.util = util_()
        self.interval_sec = 300

    def change_interval(self, interval_sec):
        self.interval_sec = interval_sec

    def load_signal_data(self, begin, end):
        if type(begin) != datetime.datetime:
            begin = self.util.str2time(begin)
            end = self.util.str2time(end)

        # 5분 이후 정도 까지 데이터 더 불러오기
        end = end + datetime.timedelta(seconds=self.interval_sec)

        table_nm = 'signaldata' if self.host != 'localhost' else 'anyang_raw_signaldata'

        query = f'''
            select cycleseq, intersectionseq, cyclestartdate
            from {self.schema_nm}.{table_nm} 
            where cyclestartdate >= '{begin}' and cyclestartdate <= '{end}'
            '''
        # return pd.read_sql(query, self.db)
        return self.read_sql_tmpfile(query)

    def load_phase_data(self, begin, end):
        if type(begin) != datetime.datetime:
            begin = self.util.str2time(begin)
            end = self.util.str2time(end)

        table_nm = 'phasedata' if self.host != 'localhost' else 'anyang_raw_phasedata'

        query = f'''
            select cycleseq, phasepattern, aringstarttime
            from {self.schema_nm}.{table_nm}
            where aringstarttime >= '{begin}' and aringstarttime <= '{end}'
            '''
        # return pd.read_sql(query, self.db)
        return self.read_sql_tmpfile(query)

    def load_phase_table(self):

        query = f'''
            select * from {self.schema_nm}.phasetable
            '''
        # return pd.read_sql(query, self.db)
        return self.read_sql_tmpfile(query)

    def load_sumo_signal_data(self, begin, end):
        if type(begin) != datetime.datetime:
            begin = self.util.str2time(begin)
            end = self.util.str2time(end)

        query = f'''
            select *
            from {self.schema_nm}.sumo_signaldata 
            where aringstarttime >= '{begin}' and aringstarttime < '{end}'
            '''
        # return pd.read_sql(query, self.db)
        return self.read_sql_tmpfile(query)

    def load_collection_data(self, begin, end, num_inter=0):
        if type(begin) != datetime.datetime:
            begin = self.util.str2time(begin)
            end = self.util.str2time(end)

        table_nm1 = 'collectiondata' if self.host != 'localhost' else 'anyang_raw_collectiondata'
        table_nm2 = 'avenue'

        query = f'''
             select dataseq, c.avenueseq, intersectionseq, carmodeltype, movementtype, collecteddate
             from {self.schema_nm}.{table_nm1} as c
             left join {self.schema_nm}.{table_nm2} as a
             on c.avenueseq = a.avenueseq
             where collecteddate >= '{begin}' and collecteddate < '{end}'
             '''

        if num_inter != 0:
            query += f'''and intersectionseq in ({num_inter})'''

        # return pd.read_sql(query, self.db)
        return self.read_sql_tmpfile(query)

    # 네트워크 데이터
    def load_net_data(self, table_nm):
        query = f'''
            select *
            from {self.schema_nm}.{table_nm} 
            '''
        # from {self.schema_nm}.{table_nm}
        # return pd.read_sql(query, self.db)
        return self.read_sql_tmpfile(query)

    # 안양시 전체 데이터
    def load_anyang_data(self, table_nm):
        if self.host == 'localhost':
            table_nm = 'anyang_raw_' + table_nm

        query = f'''
            select *
            from {self.schema_nm}."{table_nm}"
            '''
        # return pd.read_sql(query, self.db)
        return self.read_sql_tmpfile(query)

    # 안양시 교통량 데이터
    def load_sumo_traffic_data(self, begin, end):
        if type(begin) != datetime.datetime:
            begin = int(self.util.str2unixtime(begin))
            end = int(self.util.str2unixtime(end))

        query = f'''
            select *
            from {self.schema_nm}.sumo_trafficdata 
            where depart >= '{begin}' and depart < '{end}'
            '''
        # return pd.read_sql(query, self.db)
        return self.read_sql_tmpfile(query)


class Insert_data(Databases):
    def __init__(self):
        Databases.__init__(self)

    # 기존에 있던 데이터를 지우고 새로운 데이터를 insert
    def delete_data(self, condition, table_nm, schema_nm=contents.schema_nm):
        query = f'''delete from {schema_nm}."{table_nm}" where {condition} ;'''
        try:
            self.execute(query)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print('delete DB err', e)

    # 실행파일 성공 여부 로고 쌓음
    def insert_log(self, s_time, seq, state, s_sumul_time, e_sumul_time, running_state):
        query = f''' insert into {self.schema_nm}.tb_log_ms (s_time, seq, state,  s_sumul_time, e_sumul_time, running_state) 
                        values ('{s_time}', {seq}, '{state}', '{s_sumul_time}', '{e_sumul_time}', '{running_state}') 
                        ; '''
        try:
            self.cursor.execute(query)
            self.db.commit()
        except:
            self.db.rollback()

            query = f''' insert into {self.schema_nm}.tb_log_ms (s_time, seq, state,  s_sumul_time, e_sumul_time, running_state) 
                values ('{s_time}', {seq}, '{state}', '{s_sumul_time}', '{e_sumul_time}', 'error') 
                ; '''
            self.cursor.execute(query)
            self.db.commit()

    # 데이터 테이블 INPUT
    def insert_db(self, data, table_nm, schema_nm=contents.schema_nm):
        data.to_sql(name=table_nm
                    , con=self.engine
                    , schema=schema_nm
                    , if_exists='append'  ## option : 'append','replace','fail'
                    , index=False
                    )

    # csv to DB
    def insert_bulk(self, data, table_nm, schema_nm=contents.schema_nm):
        csv_like_object = StringIteratorIO((
            '|'.join(data_row[1].fillna('').astype(str)) + '\n'
            for data_row in data.iterrows()
        ))
        schema_table = f'{schema_nm}.{table_nm}'
        query = f''' COPY {schema_table} FROM STDIN WITH CSV DELIMITER '|' ; '''
        self.cursor.copy_expert(query, csv_like_object)
        self.db.commit()


# 클래스 의미 파악 안됨
class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


if __name__ == "__main__":
    ##
    # query = f'''
    #         select "CycleSeq", "IntersectionSeq", "CycleStartDate"
    #         from anyang_second."SignalData" ;
    #         '''
    # query = f'''
    #         select cycleseq, intersectionseq, cyclestartdate
    #         from anyang_second."signaldata ;
    #         '''
    # ##

    # db2 = Databases()
    # db2.execute_df(query)

    begin = "20220202000000"
    end = "20220202001500"

    LD = Load_data()
    a = LD.load_collection_data(begin, end, num_inter=0)

    print(a)
