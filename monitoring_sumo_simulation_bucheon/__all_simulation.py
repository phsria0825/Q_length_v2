import traceback

import traci
# import sumolib
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from sumolib import checkBinary
import datetime
import pandas as pd
import time
from numpy import nan as NA

import argparse

import logging

from _00_util_ import util_
from _00_DB_connecter_ver2 import Load_data, Databases, Insert_data

# input part
from _01_sm2signal_state import Trans_signal
from _02_sm2counts import Trans_traffic

import _99_Contents as contents

## simulation&output part
# from _03_insert_data2sumo import Insert_sumo
# from _05_sumo_output_vehicle import Vehicle
from _04_sumo_simulation_ver2 import Simulation_sumo


def input_part(begin, end, num_inter):
    util = util_()

    TT = Trans_traffic()
    TS = Trans_signal()

    start_time = util.unixtime2time(time.time())
    TT.make_sumo_traffic_demand(begin, end, num_inter)
    TS.make_sumo_tl_logic(begin, end)

    end_time = util.unixtime2time(time.time())
    return start_time, end_time, 'end'

    # try:
    #     TT.sumo_traffic_insert_db(begin, end)
    #     TS.sumo_signal_data2DB(begin, end)

    #     end_time = util.unixtime2time(time.time())

    #     return start_time, end_time, 'end'
    # except Exception as e:
    #     print('Error : ', e)
    #     error_time = util.unixtime2time(time.time())

    #     return start_time, error_time, 'error'


def simulation_output(begin
                      , end
                      , interval_sec=300  # 데이터 집계 간격(초)
                      , step_len=1  # 시뮬레이션 스텝(초)
                      # , insert_data = True
                      , get_past_status=True
                      , save_last_state=True
                      # , repeatable = False
                      , screen_on=False
                      , insert_log=True
                      , s_time=''
                      ):
    util = util_()
    Simul_sumo = Simulation_sumo()
    db = Databases()

    start_time = util.unixtime2time(time.time())

    if insert_log:
        seq = 2

        query = f''' 
        update {db.schema_nm}.tb_log_ms 
        set start_time = '{start_time}'
        where s_time = '{s_time}'
        and seq = {seq}
            ; '''
        db.execute(query)

    # running_state = Simul_sumo.traffic_simulator(
    #                   begin = begin, # 시뮬레이션 시작 시간
    #                   end = end, # 시뮬레이션 종료 시간
    #                   interval_sec = interval_sec,
    #                   step_len = step_len, # 시뮬레이션 스텝 시간 (step_len = 1이면 1초 단위로 시뮬레이션이 진행됨)
    #                   get_past_status = get_past_status, # 이전 상태값을 반영할 것인가?
    #                   save_last_state = save_last_state, # 시뮬레이션 종료 후 마지막 상태값을 저장할 것인가?
    #                   screen_on = screen_on,  # gui 실행 여부
    #                   insert_log = insert_log, # db의 로그 테이블에 insert
    #                   s_time = s_time)

    try:
        running_state = Simul_sumo.traffic_simulator(
            begin=begin,  # 시뮬레이션 시작 시간
            end=end,  # 시뮬레이션 종료 시간
            interval_sec=interval_sec,
            step_len=step_len,  # 시뮬레이션 스텝 시간 (step_len = 1이면 1초 단위로 시뮬레이션이 진행됨)
            # insert_data = insert_data,
            get_past_status=get_past_status,  # 이전 상태값을 반영할 것인가?
            save_last_state=save_last_state,  # 시뮬레이션 종료 후 마지막 상태값을 저장할 것인가?
            # repeatable = repeatable, # 시뮬레이션 반복이 가능하게 할 것인가?
            screen_on=screen_on,  # gui 실행 여부
            insert_log=insert_log,  # db의 로그 테이블에 insert
            s_time=s_time)
    except Exception as e:
        print(traceback.format_exc())
        print('error : something is wrong')
        running_state = 'error'

        if insert_log:
            seq = 2
            end_time = util.unixtime2time(time.time())

            query = f''' 
            update {db.schema_nm}.tb_log_ms 
            set end_time = '{end_time}'
                , running_state = '{running_state}'
            where s_time = '{s_time}'
            and seq = {seq}
                ; '''
            db.execute(query)

            if running_state == 'error':
                # print('!! "input_part" is error')
                raise Exception('!! "simulation_part" is error')

    end_time = util.unixtime2time(time.time())

    return end_time, running_state


def call_procedure(end, s_time, insert_log=True):
    end_tm = util.str2time(end)
    db = Databases()

    if insert_log:
        seq = 4
        start_time = util.unixtime2time(time.time())

        query = f''' 
        update {db.schema_nm}.tb_log_ms 
        set start_time = '{start_time}'
        where s_time = '{s_time}'
        and seq = {seq}
            ; '''
        db.execute(query)

    try:
        query = f''' CALL SP_DATA_PROCESSING('{end_tm}'); '''
        db.execute(query)
        print(query)
        running_state = 'end'
    except Exception as e:
        running_state = 'error'

    end_time = util.unixtime2time(time.time())
    return end_time, running_state


def logging_time(original_fn):
    def wrapper_fn(*args, **kwargs):
        start_time = time.time()
        result = original_fn(*args, **kwargs)
        end_time = time.time()
        print("WorkingTime[{}]: {} sec".format(original_fn.__name__, end_time - start_time))
        return result

    return wrapper_fn


@logging_time
def total_sumo_simulation(begin, end, num_inter, insert_log=True):
    log = logging.getLogger(__name__)
    log.handlers = []
    log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    log.addHandler(stream_handler)

    db = Databases()
    ID = Insert_data()

    log.info('Start : data_transform')

    s_time = util.unixtime2time(time.time())

    if insert_log:
        seq = 1
        ID.insert_log(s_time, seq, 'data_preprocessing', begin, end, 'start')

    ##### 변환부분 실행
    start_time, end_time, running_state = input_part(begin, end, num_inter)
    ##### 변환부분 완료

    if insert_log:
        query = f''' 
        update {db.schema_nm}.tb_log_ms 
            set start_time = '{start_time}'
                , end_time = '{end_time}'
                , running_state = '{running_state}'
            where s_time = '{s_time}'
            and seq = {seq}
            ; '''
        db.execute(query)
        db.commit()

        if running_state == 'error':
            # print('!! "input_part" is error')
            raise Exception('!! "input_part" is error')

    if insert_log:
        seq = 2
        ID.insert_log(s_time, seq, 'sumo_simulation', begin, end, 'start')

        # query = f''' insert into {db.schema_nm}.tb_log_ms (s_time, seq, state,  s_sumul_time, e_sumul_time)
        #                 values ('{s_time}', {seq}, 'sumo_simulation', '{begin}', '{end}')
        #                 ; '''
        # db.execute(query)

    ##### 시뮬레이션~아웃풋 부분 실행
    end_time, running_state = simulation_output(begin, end, s_time=s_time, get_past_status=False, insert_log=insert_log,
                                                screen_on=False)
    ##### 시뮬레이션~아웃풋 부분 완료

    if insert_log:
        seq = 3
        query = f''' 
        update {db.schema_nm}.tb_log_ms 
        set end_time = '{end_time}'
            , running_state = '{running_state}'
        where s_time = '{s_time}'
        and seq = {seq}
            ; '''
        db.execute(query)
        db.commit()

        if running_state == 'error':
            # print('!! "input_part" is error')
            raise Exception('!! "output_part" is error')

    if insert_log:
        seq = 4
        ID.insert_log(s_time, seq, 'call_procedure', begin, end, 'start')

    end_time, running_state = call_procedure(end, s_time, insert_log=True)

    if insert_log:
        seq = 4
        query = f''' 
        update {db.schema_nm}.tb_log_ms 
        set end_time = '{end_time}'
            , running_state = '{running_state}'
        where s_time = '{s_time}'
        and seq = {seq}
            ; '''
        db.execute(query)
        db.commit()

    e_time = util.unixtime2time(time.time())
    if insert_log:
        query = f''' 
            update {db.schema_nm}.tb_log_ms 
            set e_time = '{e_time}'
            where s_time = '{s_time}'
                ; '''
        db.execute(query)
        db.commit()


def parse_args():
    # USAGE = 'Usage: ' + sys.argv[0] + ' -b <time> <options>'
    # argParser = sumolib.options.ArgumentParser(usage=USAGE)
    argParser = argparse.ArgumentParser()
    argParser.add_argument('-n', '--network-file', dest='network_file', help='네트워크 파일 이름(*.net.xml)')
    argParser.add_argument('-b', '--begin-time', dest='begin_time', help='시뮬레이션 시작시간(YYYYMMDDHH24mmSS)')
    argParser.add_argument('-e', '--end-time', dest='end_time', help='시뮬레이션 종료시간(YYYYMMDDHH24mmSS)')
    argParser.add_argument('-d', '--during-time', dest='during_time', help='시뮬레이션 진행시간(sec)')
    argParser.add_argument('-i', '--intersection-seq', dest='num_inter', help='교차로 넘버')

    options = argParser.parse_args()

    if not options.end_time or not (options.begin_time or options.during_time):
        print('Missing arguments')
        argParser.print_help()
        exit()

    # folder = os.path.join(os.getcwd(), 'sumo_net')
    # f_list = os.listdir(folder)
    # net_folder = [file for file in f_list if file.endswith('.net.xml')]
    # if options.network_file not in net_folder:
    #     print('No network files')
    #     argParser.print_help()
    #     exit()

    return options


if __name__ == '__main__':
    args = parse_args()
    if args.network_file:
        contents.set_net_file(args.network_file)
    print(contents.net_file)
    util = util_()
    begin = args.begin_time if args.begin_time else util.time2str(
        util.str2time(args.end_time) - datetime.timedelta(seconds=int(args.during_time)))[:14]
    end = args.end_time
    num_inter = args.num_inter
    interval = args.during_time

    if num_inter:
        num = num_inter.split(',')
        contents.net_name = str(len(num)) + format(int(num[0]), '04') + "2"

    time_list = util.get_datetime_list(begin, end, int(interval))
    try:
        for i in range(1, len(time_list)):
            begin = time_list[i - 1]
            end = time_list[i]
            print(util.str2time(begin), util.str2time(end))
            if num_inter:
                total_sumo_simulation(begin, end, num_inter, insert_log=False)
            else:
                total_sumo_simulation(begin, end, None, insert_log=False)

    except Exception as e:
        print(traceback.format_exc())

    # input_part(begin, end)
    # total_sumo_simulation(begin, end, insert_log=True)

    # s_begin = '2022-02-01 00:00:00'
    # # s_end = '20220201001500'
    # s_end = '2022-02-02 03:10:00'
    # '2022-02-01 01:00:00'

    # s_begin = '20220201000000'
    # # s_end = '20220201001500'
    # s_end = '20220202010000'

    # time_list = util.get_datetime_list(s_begin, s_end, interval_sec = 300)

    # # for i in range(1, len(time_list)):
    # for i in range(1, 5):
    #     begin = time_list[i-1]
    #     end = time_list[i]

    #     print(util.str2time(begin), util.str2time(end))
    #     print(util.str2unixtime(begin), util.str2unixtime(end))

    #     total_sumo_simulation(begin, end, insert_log = True)
    #     # simulation_output(begin
    #     #             , end
    #     #             , interval_sec = 300 # 데이터 집계 간격(초)
    #     #             , step_len = 1 # 시뮬레이션 스텝(초)
    #     #             # , insert_data = True
    #     #             , get_past_status = True
    #     #             , save_last_state = True
    #     #             # , repeatable = False
    #     #             , screen_on = False
    #     #             , insert_log = False
    #     #             , s_time = ''
    #     #             )

    # print(parse_args())
    # print(begin, end)

    # strart_time = util.unixtime2time(time.time())
