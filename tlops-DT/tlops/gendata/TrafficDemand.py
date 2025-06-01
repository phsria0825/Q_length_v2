##############
###
### 8. SUMO 최적화 input 교통량데이터 생성 파일
###    1) edge_count 생성 (db2xml)(차종별)
###    2) turn_count 생성 (db2xml)(차종별)
###    3) 교통량.rou.xml 파일 (routeSampler)(차종별)
###    4) 교통량.rou.xml 파일을 1개로 병합하여 생성
###
#############

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import pandas as pd
import numpy as np
from numpy import nan as NA

from tqdm import tqdm

import xml.etree.ElementTree as ET

import argparse

from QueryDic import query_dic
from re_naming_var import renaming

from tibero_connect import TiberoDatabases

# from _00_util_ import util_


# -u : 1660489200 = 2022-08-15 00:00:00
# -w : { 1 : '월'
#     , 2 : '화'
#     , 3 : '수'
#     , 4 : '목'
#     , 5 : '금'
#     , 6 : '토'
#     , 0 : '일'
#    }
# -sc : { 
#       1 : '자치구'
#     , 2 : '도로축'
#     , 3 : '연동그룹'
#    }


### python TrafficDemand.py -s scenario_id -u 1660489200 -w 1 -sc 1 -sdc 23050
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario-id', '-s', dest = 'scenario_id')
    parser.add_argument('--analysis-unixtime', '-u',  dest = 'anly_unixtime')
    parser.add_argument('--analysis-week-code', '-w', type=int, dest = 'anly_w_cd')
    parser.add_argument('--analysis-space-code', '-sc', dest = 'anly_s_cd')
    parser.add_argument('--analysis-space-detail-code', '-sdc', dest = 'anly_s_d_cd')
    args = parser.parse_args()
    return args

# def test_def():
#     print('test:이거 출력되면 제대로 된거지?')



class Traffic_demand():
    def __init__(self, scenario_id, anly_unixtime, anly_w_cd, anly_s_cd, anly_s_d_cd):
        # self.util = util_()
        self.scenario_id = scenario_id
        self.anly_unixtime = anly_unixtime
        self.anly_w_cd = anly_w_cd
        self.anly_s_cd = anly_s_cd
        self.anly_s_d_cd = anly_s_d_cd

        self.xml_string = ''
        self.def_xml_format()

        self.kncr_cd_list = ['LBUS','LTRUCK','MBUS','MOTOR','MTRUCK','PCAR']


##############
###
### 8-2. 인자값에 따른 이동가능경로 파일 경로 불러오기
###
#############
        # 임시 경로 지정 (possible_route, turn_count, edge_count, traffic_demand(차종별))
        self.temp_dir = os.path.join(self.scenario_id,'temp')
        self.possible_route = 'possible_routes.xml'

        self.possible_route_path = os.path.join(self.temp_dir, self.possible_route)

##############
###
### 8-1. 인자값에 따른 저장할 파일 경로 지정
###
#############
        self.base_dir = self.scenario_id
        self.out_dir = os.path.join(self.base_dir, 'inputs')
        self.out_file = os.path.join(self.out_dir, 'sumo.rou.xml')


        ## db연결
        self.db = TiberoDatabases()
        self.schema_nm = 'SIGNAL.'


##############
###
### 8-3. xml파일형식 정의
###
#############
    def def_xml_format(self):
        self.begin_xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
        self.start_xml = '<data>\n'

        self.turn_start = '  <interval id="generated" begin="%s" end="%s">\n'
        self.turn_end = '  </interval>\n'
        self.turn_counts = '    <edgeRelation from="%s" to="%s" count="%s"/>\n'

        self.edge_start = '  <interval id="generated" begin="%s" end="%s">\n'
        self.edge_end = '  </interval>\n'
        self.edge_entered = '    <edge id="%s" entered="%s"/>\n'

        self.end_xml = '</data>'

        self.xml_string += self.begin_xml


# #### 1. 엣지 카운트 파일 만들기
# ####   1.1. 쿼리 사용 -> 파일에서 불러오기 -> ok // self.traffic_query.edge_count_query // self.load_edge_count()
# ####   1.2. xml 파일 쓰기
# #### 2. 턴 카운트 파일 만들기
# ####   2.1. 쿼리 사용 -> 파일에서 불러오기 -> ok // self.traffic_query.turn_count_query // self.load_turn_count()
# ####   2.2. xml 파일 쓰기
# #### 3. 엣지카운트, 턴카운트 넣어서 차종별 traffic_demand.rou.xml 만들기
# #### 4. 만들어졌던 엣지카운트, 턴카운트, 차종별 traffic_demand는 temp 디렉토리에 저장
# #### 5. 차종별 traffic_demand파일을 sumo.rou.xml 파일 하나로 합치고 inputs 디렉토리에 저장
# #### 6. temp에 저장된 데이터 필요없으면 지우기(일단 두기)


##############
###
### 8-4. 실제 데이터 turn_count 생성을 위한 데이터 조회 (db에 query로 조회)
###
#############
    def load_turn_count(self):
        df = self.db.execute_df(renaming(query_dic['traffic'][f'anly_cd_{self.anly_s_cd}']['turn'] ).format(**{
                                                                                                                'anly_tm':self.anly_unixtime
                                                                                                                ,'anly_detail':self.anly_s_d_cd
                                                                                                            }))
        df.columns = df.columns.str.lower()

        # df = pd.read_csv(os.path.join('../test_data', 'opti_turn_data.csv'), dtype=str)
        # df.columns = df.columns.str.lower()

        df.loc[:, 'begin_time'] = df.apply( lambda x: int(x['tm_div']) * 900 , axis=1)
        df.loc[:, 'end_time'] = df.apply( lambda x: (int(x['tm_div']) + 1) * 900 , axis=1)


        return df


##############
###
### 8-6. 실제 데이터 edge_count 생성을 위한 데이터 조회 (db에 query로 조회)
###
#############
    def load_edge_count(self):
        df = self.db.execute_df(renaming(query_dic['traffic'][f'anly_cd_{self.anly_s_cd}']['edge'] ).format(**{
                                                                                                                'anly_tm':self.anly_unixtime
                                                                                                                ,'anly_detail':self.anly_s_d_cd
                                                                                                            }))
        df.columns = df.columns.str.lower()

        # df  = pd.read_csv(os.path.join('../test_data', 'opti_edge_data.csv'), dtype=str)
        df.columns = df.columns.str.lower()

        df.loc[:, 'begin_time'] = df.apply( lambda x: int(x['tm_div']) * 900 , axis=1)
        df.loc[:, 'end_time'] = df.apply( lambda x: (int(x['tm_div']) + 1) * 900 , axis=1)


        return df



    ## turn_count에 넣을 문자열 만들기
    def mk_turn_count(self, df):
        turn_xml = ''

        turn_xml += self.begin_xml
        turn_xml += self.start_xml
        
        ### interval의 시간범위 구분(최적화는 구분되어서 들어오기때문에 15분단위)
        b_e_time_list = []
        for row in df[['begin_time', 'end_time']].drop_duplicates().iterrows():
            row = row[1].to_list()
            b_e_time_list.append(row)
            
        for b_e_time in b_e_time_list:
            begin_time, end_time = b_e_time

            turn_xml += self.turn_start % (begin_time, end_time)
            
            df_filter = df[(df['begin_time'] == begin_time)&(df['end_time'] == end_time)]
            for row in df_filter.iterrows():
                row = row[1]
                turn_xml += self.turn_counts % (row.entr_edge_id, row.exit_edge_id, row.cnt)
            turn_xml += self.turn_end

        turn_xml += self.end_xml
        return turn_xml


##############
###
### 8-5. [차종]별 데이터 필터링 후 차종별_turn_count.xml 생성
###
#############
    ## turn_count파일 쓰기
    ## 파일명 : 분석일자_차종_turnCount.xml
    def write_turn_count(self):
        df = self.load_turn_count()
        
        for kncr_cd in self.kncr_cd_list:
            df_kncr_cd = df[df['kncr_cd'] == kncr_cd]
            
            if df_kncr_cd.shape[0] == 0:
                continue
            
            turn_xml = self.mk_turn_count(df_kncr_cd)

            with open(os.path.join(self.base_dir, 'temp', f'{self.anly_unixtime}_{kncr_cd}_turnCount.xml'), 'w') as f:
                f.write(turn_xml)


    ## edge_count에 넣을 문자열 만들기
    def mk_edge_count(self, df):
        edge_xml = ''

        edge_xml += self.begin_xml
        edge_xml += self.start_xml
        
        ## === 논의 필요
        ### interval의 시간범위 구분(현재는 시뮬레이션 시간으로 해둠(1시간 시뮬레이션이면 edge_count도 1시간)
        ###  -> 5분? 1분?으로 고정?
        b_e_time_list = []
        for row in df[['begin_time', 'end_time']].drop_duplicates().iterrows():
            row = row[1].to_list()
            b_e_time_list.append(row)

        for b_e_time in b_e_time_list:
            begin_time, end_time = b_e_time
            
            edge_xml += self.edge_start % (begin_time, end_time)
            
            df_filter = df[(df['begin_time'] == begin_time)&(df['end_time'] == end_time)]
            # df_filter = df[(df['begin_time'] == begin_time)&(df['end_time'] == end_time)&(df['kncr_cd'] == kncr_cd)].reset_index(drop=True)
            for row in df_filter.iterrows():
                row = row[1]
                edge_xml += self.edge_entered % (row.entr_edge_id, row.cnt)
            edge_xml += self.edge_end

        # print(len(edge_xml.split('\n')))
        # if len(edge_xml.split('\n')) == 3:
        #     edge_xml += self.edge_start % (self.begin_time, self.end_time)
        #     edge_xml += self.edge_end

        edge_xml += self.end_xml
        return edge_xml


##############
###
### 8-7. [차종]별 데이터 필터링 후 차종별_edge_count.xml 생성
###
#############
    ## edge_count파일 쓰기
    ## 파일명 : 분석일자_차종_edgeCount.xml
    def write_edge_count(self):
        df = self.load_edge_count()
        
        for kncr_cd in self.kncr_cd_list:
            df_kncr_cd = df[df['kncr_cd'] == kncr_cd]

            if df_kncr_cd.shape[0] == 0:
                continue

            edge_xml = self.mk_edge_count(df_kncr_cd)

            with open(os.path.join(self.base_dir, 'temp', f'{self.anly_unixtime}_{kncr_cd}_edgeCount.xml'), 'w') as f:
                f.write(edge_xml)


##############
###
### 8-8. 실제 데이터 [차종]별 교통량.rou.xml 생성
###
#############
    def write_kncr_cd_traffic_demand(self):
        self.write_turn_count()
        self.write_edge_count()

        get_file_list = [file for file in os.listdir(self.temp_dir) if file.find(f'{self.anly_unixtime}') != -1 ]

        edge_get_file_list = sorted([file for file in get_file_list if file.find('edgeCount.xml') != -1 ])
        turn_get_file_list = sorted([file for file in get_file_list if file.find('turnCount.xml') != -1 ])

        # begin_time = self.util.time2str( self.util.unixtime2time(self.begin_time) )
        # end_time = self.util.time2str( self.util.unixtime2time(self.end_time) )

        for turn_file, edge_file in zip(turn_get_file_list, edge_get_file_list):
            kncr_cd = edge_file.split('.')[0].split('_')[-2]
            demand_nm = f'{self.anly_unixtime}_{kncr_cd}.rou.xml'

            os.system(
                f'''python ../sumo_py/routeSampler.py -r {self.possible_route_path} --turn-files {self.temp_dir}/{turn_file} --edgedata-files {self.temp_dir}/{edge_file} -o {self.temp_dir}/{demand_nm} --prefix {kncr_cd}_  --attributes=type='{kncr_cd}'  '''
                )
        else:
            [os.remove(os.path.join(self.temp_dir, file))  for file in get_file_list]
        


##############
###
### 8-9. 교통량.rou.xml 파일 1개로 병합
###
#############
    def write_traffic_demand(self):
        self.write_kncr_cd_traffic_demand()

        rou_get_file_list = [ file for file in os.listdir(self.temp_dir) if file.find(f'.rou.xml') != -1 ]
        print(rou_get_file_list)
        for index, rou_file in enumerate(rou_get_file_list):
            globals()[f'tree_{index}'] = ET.parse(os.path.join(self.temp_dir, rou_file))
            globals()[f'root_{index}'] = globals()[f'tree_{index}'].getroot()
        else:
            [root_0.extend(globals()[f'root_{i}']) for i in range(1, len(rou_get_file_list))]

        root_0[:] = sorted(root_0, key=lambda vehicle: (vehicle.tag,float(vehicle.get('depart'))))

        tree_0.write(self.out_file)


        print('Done : traffic_demand')



def main(args):
    td = Traffic_demand(args.scenario_id, args.anly_unixtime, args.anly_w_cd, args.anly_s_cd, args.anly_s_d_cd)

    td.write_traffic_demand()



if __name__ == "__main__":
    
    args = parse_args()
    main(args)

