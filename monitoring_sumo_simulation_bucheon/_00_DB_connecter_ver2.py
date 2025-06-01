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

    ## 컨본 새 서버
    host = '192.168.0.79'
    dbname = 'postgres'
    user = 'postgres'
    password = '0713'
    port = 54325

    db = None
    cursor = None
    engine = None
    schema_nm = contents.schema_nm

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

    def node_list(self, int_lcno=None):
        table_nm = 'uid_sumo_node'
        query = f'''
            select node_id, sgnl_crsrd_no, sumo_node_id
            from {self.schema_nm}.{table_nm}
            where use_yn = 'Y'
        '''
        if int_lcno is not None:
            query += f'''
             and sgnl_crsrd_no in ({int_lcno})
        '''

        return self.read_sql_tmpfile(query)

    def node_edge_list(self):
        table_nm = 'uid_sumo_node_edge'
        query = f'''
            select node_id, acsr_id, drct_cd, sumo_from_edge_id, sumo_to_edge_id
            from {self.schema_nm}.{table_nm}
        '''

        return self.read_sql_tmpfile(query)

    def cd_lnfo_list_by_grp_cd(self, grp_cd):
        table_nm = 'm_cd_inf'
        query = f'''
            select cd, cd_nm
            from {self.schema_nm}.{table_nm}
            where grp_cd = '{grp_cd}' and use_yn = 'Y'
            order by grp_cd, sort_no
        '''
        return self.read_sql_tmpfile(query)

    def lane_sgnl_input_list(self, int_lcno_list):
        table_nm = 'uid_sumo_lane_sgnl_input'
        query = f'''
            select sgnl_crsrd_no, int_phaseno, yello_flag, array_to_string(array_agg(sgnl order by sgnl_crsrd_no, input_order), '') as sgnl 
            from {self.schema_nm}.{table_nm}
        '''
        if int_lcno_list is not None and len(int_lcno_list) != 0:
            query += f'''
                where sgnl_crsrd_no in {int_lcno_list}
            '''

        query += '''
            group by sgnl_crsrd_no, int_phaseno, yello_flag
            order by sgnl_crsrd_no
        '''

        return self.read_sql_tmpfile(query)

    def l_vhcl_data_proc_list(self, start, end, int_lcno=None):
        month = start.month
        table_nm = 'l_vhcl_data_proc'+str(month)

        node_table_nm = 'uid_sumo_node'
        query = f'''
                    select l.crt_dt, l.obj_id, n.sgnl_crsrd_no, l.node_id, l.acsr_id, l.drct_cd, l.lane_no, l.vknd_cd, l.avg_spd
                    from {self.schema_nm}.{table_nm} l
                    join {self.schema_nm}.{node_table_nm} n
                    on l.node_id = n.node_id
                    where crt_dt >= '{start}' and crt_dt < '{end}'
                 '''
        if int_lcno is not None and int_lcno != '':
            query += f'''
                        and n.sgnl_crsrd_no in ({int_lcno})
                     '''

        return self.read_sql_tmpfile(query)

    def l_sgnl_prst_list(self, start, end):
        table_nm = 'l_sgnl_prst'
        m_table_nm = 'm_crsrd_sgnl_inf'

        query = f'''
            select l.ocrn_dt, l.sgnl_ctrl_id, m.node_id, l.prst_no as int_phaseno, l.prst_ss
            from {self.schema_nm}.{table_nm} l
            join {self.schema_nm}.{m_table_nm} m
            on l.sgnl_ctrl_id = m.sgnl_ctrl_id 
            where ocrn_dt >= '{start}' and ocrn_dt < '{end}' and ring_cls = 'A'
            order by ocrn_dt
        '''

        return self.read_sql_tmpfile(query)

    def scs_m_intphase(self, int_lcno_list):
        table_nm = 'scs_m_intphase'
        query = f'''
            select int_lcno as sgnl_crsrd_no, int_phaseno, int_yellow
            from {self.schema_nm}.{table_nm}
        '''

        if int_lcno_list is not None and len(int_lcno_list) != 0:
            query += f'''
                       where int_lcno in {int_lcno_list}
                   '''

        return self.read_sql_tmpfile(query)

    def change_interval(self, interval_sec):
        self.interval_sec = interval_sec

    def load_signal_data(self, begin, end):
        if type(begin) != datetime.datetime:
            begin = self.util.str2time(begin)
            end = self.util.str2time(end)

        # 5분 이후 정도 까지 데이터 더 불러오기
        end = end + datetime.timedelta(seconds=self.interval_sec)

        table_nm = 'signaldata'

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

        table_nm = 'phasedata'

        query = f'''
            select cycleseq, phasepattern, aringstarttime
            from {self.schema_nm}.{table_nm}
            where aringstarttime >= '{begin}' and aringstarttime <= '{end}'
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

    # 단순 데이터 불러오기
    def load_net_data(self, table_nm):
        query = f'''
            select *
            from {self.schema_nm}.{table_nm} 
            '''
        # from {self.schema_nm}.{table_nm}
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


# 버퍼 사용으로 문자열 읽기
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
    begin = "20220901000000"
    end = "20220901001500"

    LD = Load_data()
    a = LD.l_vhcl_data_proc_list(begin, end, int_lcno=26)

    print(a)
