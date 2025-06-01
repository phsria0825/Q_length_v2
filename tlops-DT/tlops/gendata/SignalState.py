##############
###
### 6. SUMO 최적화 input 신호데이터 생성 파일
###
#############


import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import pandas as pd
import numpy as np
from numpy import nan as NA

from tqdm import tqdm

import argparse

from QueryDic import query_dic
from re_naming_var import renaming

from tibero_connect import TiberoDatabases


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


### python SignalState.py -s scenario_id -u 1660489200 -w 1 -sc 1 -sdc 23050
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

class Signal_data():
    def __init__(self, scenario_id, anly_unixtime, anly_w_cd, anly_s_cd, anly_s_d_cd):
        # self.util = util_()
        self.scenario_id = scenario_id
        self.anly_unixtime = anly_unixtime
        self.anly_w_cd = anly_w_cd
        self.anly_s_cd = anly_s_cd
        self.anly_s_d_cd = anly_s_d_cd

        self.xml_string = ''
        self.def_xml_format()
        self.yellow_phase_row = []


##############
###
### 6-1. 인자값에 따른 저장할 파일 경로 지정
###
#############
        # 기본경로 지정
        self.base_dir = os.path.join(f'{self.scenario_id}','inputs')
        self.out_file_nm = 'before'

        ## db연결
        self.db = TiberoDatabases()
        self.schema_nm = 'SIGNAL.'

        ## 녹색데이터 insert
        # self.insert_base_green_data()

##############
###
### 6-3. 현시TOD정보 (SOITSPHASTODINFO) 테이블에 데이터 생성 (db에 query로 조회)
###
#############
    def insert_base_green_data(self):
        # print(renaming(query_dic['signal'][f'anly_cd_{self.anly_s_cd}']))
        self.db.execute(renaming(query_dic['signal'][f'anly_cd_{self.anly_s_cd}']).format(**{'scen_id':self.scenario_id
                                                                                            ,'anly_wk':self.anly_w_cd
                                                                                            ,'anly_detail':self.anly_s_d_cd}) )

##############
###
### 6-2. xml파일형식 정의
###
#############
    def def_xml_format(self):
        self.begin_xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
        self.start_xml = '<tlLogics version="1.9" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/tllogic_file.xsd">\n'

        self.tlLogic_start = '  <tlLogic id="%s" type="static" programID="%s" offset="%s">\n'
        self.tlLogic_end = '  </tlLogic>\n'
        self.phase = '    <phase duration="%s" name="%s" minDur="%s" state="%s"/>\n'

        self.end_xml = '</tlLogics>'

        self.xml_string += self.begin_xml



##############
###
### 6-4. Tibero DB에서 3번에서 생성한 TOD기반 녹색시간 데이터 조회 및 데이터 가공
###
#############
    def load_soitsphastodinfo_green(self):
        query = f"""
                SELECT *
                FROM {self.schema_nm}SOITSPHASTODINFO
                WHERE SCEN_ID = {self.scenario_id}
                ;
        """

        self.soitsphastodinfo_green = self.db.execute_df(query)

        # self.soitsphastodinfo_green = pd.read_csv(os.path.join('../test_data', 'soitsphastodinfo_anyang_tod.csv'))
        
        self.soitsphastodinfo_green.columns = self.soitsphastodinfo_green.columns.str.lower()

        self.soitsphastodinfo_green = self.soitsphastodinfo_green.astype({'bgng_hh':int
                                                                            ,'bgng_mi':int
                                                                            , 'majr_road_se_cd':str})

        self.soitsphastodinfo_green['bgng_hhmi'] = [str('0'+ str(row[1].bgng_hh))[-2:] + str('0' + str(row[1].bgng_mi))[-2:] for row in self.soitsphastodinfo_green.iterrows()]

        ### tod시간계획 0600_1700 형태로 만들기 위한 추가적 파생변수 필요
        bgng_end_hhmi = self.soitsphastodinfo_green[['node_id', 'bgng_hhmi']].drop_duplicates().sort_values(['node_id','bgng_hhmi']).reset_index(drop=True)
        bgng_end_hhmi['end_hhmi'] = bgng_end_hhmi.groupby(['node_id'])['bgng_hhmi'].shift(-1)
        bgng_end_hhmi.loc[bgng_end_hhmi['end_hhmi'].isna(),'end_hhmi'] = bgng_end_hhmi.groupby(['node_id'])['bgng_hhmi'].first().to_list()

        self.soitsphastodinfo_green = self.soitsphastodinfo_green.merge(bgng_end_hhmi, how='left', on=['node_id','bgng_hhmi'])
        self.soitsphastodinfo_green['hhmi_hhmi'] = self.soitsphastodinfo_green['bgng_hhmi'] + "_" + self.soitsphastodinfo_green['end_hhmi']
        
        ### phas_no의 max값과 주현시 필요해서 추가적 파생변수 생성
        phas_max = self.soitsphastodinfo_green[['node_id', 'phas_no']].drop_duplicates()
        phas_max_temp = phas_max.groupby(['node_id'], as_index=False).max('phas_no').rename(columns={'phas_no':'max_phas_no'})
        phas_max = phas_max.merge(phas_max_temp, how='left', on='node_id')

        # phas_main = self.soitsphastodinfo_green[self.soitsphastodinfo_green['majr_road_se_cd']=='20'][['node_id','phas_no']].rename(columns={'phas_no':'main_phas_no'})

        # phas_max_main = phas_max.merge(phas_main, how='left', on ='node_id')
        self.soitsphastodinfo_green = self.soitsphastodinfo_green.merge(phas_max, how='left', on=['node_id','phas_no'])

        # phas_main = self.soitsphastodinfo_green[self.soitsphastodinfo_green['majr_road_se_cd']=='20'][['node_id','phas_no']]

        # print(self.soitsphastodinfo_green[self.soitsphastodinfo_green['majr_road_se_cd']=='20'][['node_id','phas_no']].rename(columns={'phas_no':'main_phas_no'}))

        self.soitsphastodinfo_green = self.soitsphastodinfo_green.merge(self.soitsphastodinfo_green[self.soitsphastodinfo_green['majr_road_se_cd']=='20'][['node_id','phas_no']].rename(columns={'phas_no':'main_phas_no'})
                                                                       , how='left', on='node_id')
        self.soitsphastodinfo_green['phas_no2'] = self.soitsphastodinfo_green.apply( lambda x:  (x['max_phas_no'] - x['main_phas_no'] + x['phas_no']) % x['max_phas_no'] + 1 , axis=1)
        self.soitsphastodinfo_green = self.soitsphastodinfo_green.drop_duplicates()
        # self.soitsphastodinfo_green.to_csv('test_11.csv', index=False)

        ### TOD시간대별로 A교차로의 주기가 달라짐 -> 시작시+시작분, node_id 가 구분자
        self.bgng_hhmi_intsersection_df = self.soitsphastodinfo_green[['hhmi_hhmi', 'node_id']].drop_duplicates()


    def mk_phase_no2(self, df):
        phase_no2_list = []
        for row in df.iterrows():
            for index, i in enumerate(range(row.main_phas_no-1, row.max_phas_no+row.main_phas_no-1)):
                phas_no2_val = i % max_phas_no

                phase_no2_list.append([row[1].node_id, row[1].main_phas_no, row[1].max_phas_no, index, phas_no2_val])


    # add_yellow_phases 함수에서 황색시간 return
    def find_yellowtime(self, phas_bgng_unix_tm, yellowtime, greentime = 0,  option ='mius'):
        if yellowtime in ['', NA, None]:
            yellowtime = 3
        
        if option == 'mius':
            return phas_bgng_unix_tm - yellowtime
        if option == 'plus':
            return phas_bgng_unix_tm + greentime - yellowtime

    # add_yellow_phases 함수에서 현재와 다음현시를 비교하여 변하는 현시를 황색불로
    def find_change_phases(self, cur_phase, next_phase):
        switch_reds = []
        switch_greens = []
        for i, (p0, p1) in enumerate(zip(cur_phase, next_phase)):
            if (p0 in 'Gg') and (p1 == 'r'):
                switch_reds.append(i)
            elif (p0 in 'r') and (p1 in 'Gg'):
                switch_greens.append(i)
        yellow_phase = list(cur_phase)
        for i in switch_reds:
            yellow_phase[i] = 'y'
        for i in switch_greens:
            yellow_phase[i] = 'r'
        return ''.join(yellow_phase)


    def add_yellow_phases(self, data):
        data = data.reset_index(drop=True)

        intersection_yellow_phase_row = []

        # spot_ints_id = data.loc[0,'spot_ints_id']

        prgrs_stts_cd = 3
        hhmi_hhmi = data.loc[0, 'hhmi_hhmi']
        node_id = data.loc[0,'node_id']
        for i in range(1, data.shape[0]+1):
            phas_no = data.loc[i-1, 'phas_no']
            
            ### (마지막 현시와 처음 현시를 비교하기 위해 if문 사용)
            ## i-1 : 현재 현시
            ## i : 다음 현시 
            if i == data.shape[0]:
                # print('이거 탐!')
                cur_phase = data.loc[i-1,'sgnl_stts']
                next_phase = data.loc[0,'sgnl_stts']

                phas_hr = data.loc[i-1,'phas_hr']
                yelw_hr = data.loc[i-1,'yelw_hr']
                phas_bgng_unix_tm = data.loc[i-1,'phas_bgng_unix_tm']

                yellow_phase = self.find_change_phases(cur_phase, next_phase)
                yellow_bgng_unix_tm = self.find_yellowtime(phas_bgng_unix_tm, yelw_hr, greentime=phas_hr, option='plus')
            else:
                cur_phase = data.loc[i-1,'sgnl_stts']
                next_phase = data.loc[i,'sgnl_stts']

                phas_bgng_unix_tm = data.loc[i,'phas_bgng_unix_tm']
                yelw_hr = data.loc[i-1,'yelw_hr']

                yellow_phase = self.find_change_phases(cur_phase, next_phase)
                yellow_bgng_unix_tm = self.find_yellowtime(phas_bgng_unix_tm, yelw_hr)
            
            intersection_yellow_phase_row.append([hhmi_hhmi, node_id, phas_no, yellow_phase, yellow_bgng_unix_tm, prgrs_stts_cd])
            
        return intersection_yellow_phase_row



    def mk_intersection_yellow_phase(self):
        self.load_soitsphastodinfo_green()

        for row in self.bgng_hhmi_intsersection_df.iterrows():
            row = row[1]

            temp_phassgnlinfo = self.soitsphastodinfo_green[self.soitsphastodinfo_green['node_id'] == row.node_id]
            temp_phassgnlinfo = temp_phassgnlinfo[temp_phassgnlinfo['hhmi_hhmi'] ==  row.hhmi_hhmi]
            temp_yellow_phase_row = self.add_yellow_phases(temp_phassgnlinfo)

            self.yellow_phase_row += temp_yellow_phase_row
        else:
            yellow_phase = pd.DataFrame( self.yellow_phase_row, columns = ['hhmi_hhmi','node_id','phas_no','sgnl_stts','phas_bgng_unix_tm', 'prgrs_stts_cd'] ).drop_duplicates()
            # yellow_phase['prgrs_stts_cd'] = 3
        
        return yellow_phase


##############
###
### 6-5. 황색 신호 데이터 생성
###
#############
    def mk_yellow_phase_table(self, write_file=False):
        ## 녹색시간 테이블 불러와서 황색시간 데이터 만들기
        yellow_phase = self.mk_intersection_yellow_phase()

        ## 녹색시간만 있는 테이블을 카피해서 황색시간 테이블 생성
        temp_phassgnlinfo = self.soitsphastodinfo_green.copy()
        temp_phassgnlinfo = temp_phassgnlinfo[temp_phassgnlinfo.columns.difference(['sgnl_stts','phas_bgng_unix_tm','prgrs_stts_cd'])]

        self.soitsphastodinfo_yellow = temp_phassgnlinfo.merge(yellow_phase, how='left', on=['hhmi_hhmi','node_id','phas_no'])

        if write_file:
            self.soitsphastodinfo_yellow[['clct_unix_tm','hhmi_hhmi','spot_ints_id','node_id','majr_ints_se_cd'
                                            ,'sgnl_stts','cycl_hr','ofst_hr','phas_no','yelw_hr', 'min_gren_hr'
                                            ,'majr_road_se_cd','prgrs_stts_cd','phas_hr','phas_bgng_unix_tm','inpt_dt','phas_no2']].to_csv('test2.csv', index=False)


##############
###
### 6-6. 녹색신호 데이터, 황색신호 데이터 병합
###
#############
    def concat_data(self):
        # df = pd.concat([self.soitsphastodinfo_green, self.soitsphastodinfo_yellow]).sort_values(['node_id','majr_road_se_cd','bgng_mi','phas_no','prgrs_stts_cd']).reset_index(drop=True)
        df = pd.concat([self.soitsphastodinfo_green, self.soitsphastodinfo_yellow])
        df['phase_no_gy'] = df.apply( lambda x: str(x['phas_no'])+'_y' if int(x['prgrs_stts_cd']) == 3 else str(x['phas_no']) +'_g', axis=1)

        return df


##############
###
### 6-7. Sumo input 신호데이터 생성
###
#############
    def mk_tlLogic(self, df):
        self.xml_string += self.start_xml
        for node_id in sorted(list(set(df['node_id']))):
            node_all_time = df.loc[df['node_id']==node_id, :].reset_index(drop=True)

            time_plan_set = set(node_all_time['hhmi_hhmi'])
            for time_plan in time_plan_set:
                tll = node_all_time.loc[node_all_time['hhmi_hhmi']==time_plan, :].sort_values(['node_id','phas_no2','prgrs_stts_cd'])

                self.xml_string += self.tlLogic_start % (node_id, time_plan, node_all_time['ofst_hr'][0])
                self.phas_xml(tll)
                self.xml_string += self.tlLogic_end

        self.xml_string += self.end_xml


    def phas_xml(self, df):
        for row in df.iterrows():
            row = row[1]
            if int(row.prgrs_stts_cd) == 1:
                self.xml_string += self.phase % (int(row.phas_hr) - int(row.yelw_hr), row.phase_no_gy, row.min_gren_hr-row.yelw_hr, row.sgnl_stts)
            else:
                self.xml_string += self.phase % (row.yelw_hr, row.phase_no_gy, row.yelw_hr, row.sgnl_stts)



    # 유닉스타임으로 시간 넣을 것 아니니 변환하는 함수 필요
    # 저 유닉스타임 어떻게 가져올 것인지 정의해야함
    def write_tll(self, write_file=False):
        self.mk_intersection_yellow_phase()
        soitsphastodinfo = self.concat_data()
        
        if write_file:
            soitsphastodinfo.to_csv('test3.csv', index=False)

        self.mk_tlLogic(soitsphastodinfo)

        path = os.path.join(self.base_dir, f'{self.out_file_nm}.tll.xml')
        print(path)

        with open(path, 'w') as f:
            f.write(self.xml_string)



def main(args, write_file=False):

    sd = Signal_data(args.scenario_id, args.anly_unixtime, args.anly_w_cd, args.anly_s_cd, args.anly_s_d_cd)
    
    sd.mk_yellow_phase_table(write_file=False)

    sd.write_tll(write_file=False)





if __name__ == "__main__":
    
    args = parse_args()

    main(args, write_file=False)

    # print('test:이거 출력되면 제대로 된거지?')

    # test_def()
    # print(os.path.dirname(__file__))














