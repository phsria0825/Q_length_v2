import os
import sys
import traceback

import pandas as pd
# import traci
import libsumo as traci

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from sumolib import checkBinary
import datetime
import time

import logging

from _00_util_ import util_
from _00_DB_connecter_ver2 import Load_data, Databases

# input part
from _01_sm2signal_state import Trans_signal
from _02_sm2counts import Trans_traffic

# simulation&output part
from _03_insert_data2sumo import Insert_sumo
from _05_sumo_output_vehicle import Vehicle
import _99_Contents as contents

class Simulation_sumo():
    def __init__(self):
        self.LD = Load_data()
        self.IS = Insert_sumo()

        self.db = Databases()
        self.net_path = './sumo_net'

        self.util = util_()
        self.net_file = contents.net_file
        self.util.createFolder(self.net_path)
        self.save_path = self.util.createFolder('./save_state')

        self.veh = Vehicle(traci)
        # self.veh

        self.logging_info()

    def logging_info(self):
        self.log = logging.getLogger(__name__)
        self.log.handlers = []
        self.log.setLevel(logging.INFO)
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = logging.Formatter('%(asctime)s - %(message)s')

        # streaming log
        # stream_handler = TqdmLoggingHandler()
        stream_handler = logging.StreamHandler()

        stream_handler.setFormatter(formatter)
        self.log.addHandler(stream_handler)

    ## set_sumo는 안드레아꺼 따라함
    def set_sumo(self, begin, step_len=1, screen_on=True):
        """
        Configure various parameters of SUMO
        """
        # sumo things - we need to import python modules from the $SUMO_HOME/tools directory
        if 'SUMO_HOME' in os.environ:
            tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
            sys.path.append(tools)
        else:
            sys.exit("please declare environment variable 'SUMO_HOME'")

        # setting the cmd mode or the visual mode
        if screen_on:
            sumoBinary = checkBinary('sumo-gui')
        else:
            sumoBinary = checkBinary('sumo')
            #  sumoBinary = checkBinary('sumo-gui')

        # setting the cmd command to run sumo at simulation time
        # "-a", ','.join([os.path.join(self.net_path, i) for i in ["exp.add_LA.xml","exp.add_LB.xml"]]),
        # a = ['all','bus','passenger','truck']

        sumo_cmd = [sumoBinary,
                    "-n", os.path.join(self.net_path, self.net_file),
                    "-r", os.path.join(self.net_path, "exp.rou.xml"),
                    # "-a", ','.join([os.path.join(self.net_path, i) for i in [f"add_e2_{j}.xml" for j in a]]),
                    "--step-length", str(step_len),
                    "-b", str(begin),
                    # "-S", "true",
                    # "--fcd-output", os.path.join(self.out_path, 'xml', f"carway.xml"),
                    # "--fcd-output.geo",
                    # "--lanechange-output", os.path.join(self.out_path, 'xml', f"lanechange.xml")
                    ]

        return sumo_cmd

    def traffic_simulator(self,
                          begin,  # 시뮬레이션 시작 시간
                          end,  # 시뮬레이션 종료 시간
                          # [begin, end)의 데이터를 가져옴
                          interval_sec=300,
                          step_len=1,  # 시뮬레이션 스텝 시간 (step_len = 1이면 1초 단위로 시뮬레이션이 진행됨)
                          get_past_status=True,  # 이전 상태값을 반영할 것인가?
                          save_last_state=True,  # 시뮬레이션 종료 후 마지막 상태값을 저장할 것인가?
                          screen_on=False,  # gui 실행 여부
                          file_path='./save_state',
                          insert_log=True,
                          s_time=''):

        file_path = self.util.createFolder(file_path)

        s_begin = begin

        ## prepare simulation
        data_signal_state, data_traffic_demands, unix_begin, unix_end = self.prepare_simulation(begin, end, step_len)
        # data_signal_state : sumo 신호 데이터
        # data_traffic_demands : sumo 교통수요 데이터
        # unix_begin : begin에 해당함, begin을 유닉스 타임으로 바꾼 것
        # unix_end : end를 유닉스 타임으로 바꾼 것
        # 현재 제외 변수 # past : unix_begin으로 부터 5분전 시간(이전 시뮬레이션 상태값을 불러오기 위해 필요)
        # 현재 제외 변수 # future : unix_begin으로 부터 5분 후(다음 시뮬레이션 데이터를 불러오기 위해 필요)

        ## start simulation
        traci.start(self.set_sumo(unix_begin, step_len, screen_on))

        if get_past_status:
            before_begin = self.util.str2time(begin) - datetime.timedelta(seconds=interval_sec)
            before_begin = self.util.time2str(before_begin)
            # before_begin = datetime.datetime.strftime(before_begin, '%Y%m%d%H%M%S')

            if os.path.exists(os.path.join(file_path, f"{contents.net_name}{before_begin}state.xml")):
                print(f"\n***** Access file : {contents.net_name}{before_begin}state.xml\n")
                traci.simulation.loadState(os.path.join(file_path, f"{contents.net_name}{before_begin}state.xml"))
            else:
                print(f"\n***** Error!! No file : {contents.net_name}{before_begin}state.xml\n")

        # 시뮬레이션 시작
        self.log.info(f'Simulate {begin}~{end} time.')
        # print(f"simulation time : {unix_begin} ~ {unix_end} == {begin} ~ {end}")

        # sumo에 차량 입력
        self.set_traffic_demands(traci, data_traffic_demands)

        # step_len으로 정의한 단위시간으로 시뮬레이션 진행(정의된 시간에 신호 발생)
        insert_log_list = [insert_log, s_time, begin, end]
        state = self.simulation_step(traci, unix_begin, unix_end, step_len, data_signal_state, insert_log_list)

        # 시뮬레이션 반복 후 마지막 상태값 저장 코드
        if save_last_state:
            traci.simulation.clearPending()
            traci.simulation.saveState(self.util.mk_path(self.save_path, f"{contents.net_name}{s_begin}state.xml"))

            self.log.info(f'')
            self.log.info(f'Save last state : "{contents.net_name}{s_begin}state.xml"')

        traci.close()

        self.log.info(f'End Simulate {s_begin}~{end} time.')

        return state

    def prepare_simulation(self, begin, end=None, step_len=1, interval_sec=300):
        unix_begin = self.util.str2unixtime(begin)
        unix_end = self.util.str2unixtime(end) - step_len

        demand_db_csv_folder = self.util.createFolder(os.getcwd() + '/counts2demands/db_csv')
        signal_csv_folder = self.util.createFolder(os.getcwd() + '/sm2signal/csv')

        if end is None:
            unix_end = unix_begin + interval_sec - step_len

        # 현재 db에 append로 데이터를 쌓고 있어서 중복으로 값이 들어갈 수 있음
        # data_signal_state = self.LD.load_sumo_signal_data(begin, end).drop_duplicates().reset_index(drop=True)
        # data_traffic_demands = self.LD.load_sumo_traffic_data(begin, end).drop_duplicates().reset_index(drop=True)

        data_signal_state = self.csv_to_data(signal_csv_folder, begin)
        data_traffic_demands = self.csv_to_data(demand_db_csv_folder, begin)

        return data_signal_state, data_traffic_demands, unix_begin, unix_end

    def csv_to_data(self, file_folder, begin_str):
        filelist = os.listdir(file_folder)
        get_filelist = []
        for i in filelist:
            if i.__contains__(contents.net_name + begin_str):
                get_filelist.append(i)

        return_data = None
        for file_nm in get_filelist:
            try:
                data = pd.read_csv(os.path.join(file_folder, file_nm), sep=',')
            except:
                pass
            else:
                if return_data is None:
                    return_data = data
                else:
                    return_data = pd.concat(objs=[return_data, data], axis=0, ignore_index=True)

        return return_data

    def simulation_step(self, sim, unix_begin, unix_end, step_len, data, insert_log_list=[]):
        dic = self.IS.set_dic(data)
        round_num = 1
        if len(str(step_len).split('.')) != 1:
            round_num = len(str(step_len).split('.')[1])
        s_unix_begin = unix_begin
        # s_unix_end = unix_end

        while unix_begin <= unix_end:
            self.IS.insert_signal_state(sim, dic, unix_begin)

            sim.simulationStep()

            self.veh.vehicle_subscribe_results(unix_begin)

            unix_begin += step_len
            unix_begin = round(unix_begin, round_num)
        else:
            # self.veh.vehicle_subscribe_results(unix_begin)
            # self.veh.get_vehicle_results(insert_db = True)
            self.log.info(f'End Simulate Step.')

            if insert_log_list[0]:
                insert_log, s_time, begin, end = insert_log_list

                end_time = self.util.unixtime2time(time.time())

                seq = 2
                query = f''' 
                update {self.db.schema_nm}.tb_log_ms 
                set end_time = '{end_time}'
                    , running_state = 'end'
                where s_time = '{s_time}'
                and seq = {seq}
                    ; '''
                self.db.execute(query)
                self.db.commit()

                seq = 3
                query = f''' insert into {self.db.schema_nm}.tb_log_ms (s_time, seq, state,  start_time,  s_sumul_time, e_sumul_time, running_state) 
                                values ('{s_time}', {seq}, 'vehicle_output', '{end_time}', '{begin}', '{end}', 'start') 
                                ; '''
                self.db.execute(query)
                self.db.commit()

        try:
            self.veh.get_vehicle_results(s_unix_begin, unix_end, insert_db=False)
            return 'end'
        except Exception as e:
            print(traceback.format_exc())
            return 'error'

    def set_traffic_demands(self, sim, data):
        self.IS.insert_traffic_demands(sim, data)

    # def insert_db(self, unix_begin, unix_end, data, table_nm, schema_nm = 'anyang'):
    #     ID = Insert_data()

    #     condition = f'''"Begin" = '{unix_begin}' and "End" = '{unix_end}' ''' if 'Begin'in list(data.columns) else f''' "Time" >= '{unix_begin}' and "Time" < '{unix_end}' '''

    #     ID.delete_data(condition, schema_nm='anyang', table_nm=table_nm)

    #     ID.insert_db(data, table_nm, schema_nm = 'anyang')


if __name__ == "__main__":

    ##
    # begin = "20211001070000"
    # end = "20211001080000"
    begin = "20220201000000"
    end = "20220201001500"

    interval_sec = 300  # 데이터 집계 간격(초)
    step_len = 1  # 시뮬레이션 스텝(초)
    step_len = round(step_len, 1)
    insert_data = True
    get_past_status = True
    save_last_state = True
    repeatable = False
    screen_on = False
    ## False

    log = logging.getLogger(__name__)
    log.handlers = []
    log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    log.addHandler(stream_handler)

    TT = Trans_traffic()
    TS = Trans_signal()

    ##### 실행코드
    util = util_()
    time_seq = util.get_datetime_list(begin, end)
    Simul_sumo = Simulation_sumo()

    # for i in range(1, len(time_seq)):
    for i in range(1, 2):
        begin = time_seq[i - 1]
        end = time_seq[i]

        # TT.insert_db(begin, end)
        # TS.sumo_signal_insert_db(begin, end)

        Simul_sumo.traffic_simulator(
            begin=begin,  # 시뮬레이션 시작 시간
            end=end,  # 시뮬레이션 종료 시간
            interval_sec=interval_sec,
            step_len=step_len,  # 시뮬레이션 스텝 시간 (step_len = 1이면 1초 단위로 시뮬레이션이 진행됨)
            # insert_data = insert_data,
            get_past_status=get_past_status,  # 이전 상태값을 반영할 것인가?
            save_last_state=save_last_state,  # 시뮬레이션 종료 후 마지막 상태값을 저장할 것인가?
            # repeatable = repeatable, # 시뮬레이션 반복이 가능하게 할 것인가?
            screen_on=screen_on,
            insert_log=False,  # db의 로그 테이블에 insert
            s_time='')  # gui 실행 여부

    # log.info(f'Completed All Simulate.')
    # keep = input("###################    keep going? or stop it? [y/n]")
    # if (keep == 'n') or (keep == 'N'):
    # break

#############################################################################
############## delete문 추가 안햇을때 실행 코드####################################
# ## db에 1시간치 데이터 input
# DB = Databases()
# TT = Trans_traffic()
# TS = Trans_signal()

# ##
# query = ['truncate table anyang."Sumo_SignalData";', 'truncate table anyang."Sumo_TrafficData";']
# ##
# for q in query:
#     DB.execute(q)
# else:
#     print('******* truncate table!!')

# for i in range(1, len(time_seq)):
#     begin = time_seq[i-1]
#     end = time_seq[i]

#     TT.insert_db(begin, end)
#     TS.insert_db(begin, end)

# for i in range(1, len(time_seq)):
# DB = Databases()
# table_li = ['Sumo_CarWay','Sumo_LaneArea','Sumo_LaneBase','Sumo_LaneChange']

# query = [f'truncate table anyang."{i}";' for i in table_li]
# ##
# for q in query:
#     DB.execute(q)
# else:
#     print('******* truncate table!!')
