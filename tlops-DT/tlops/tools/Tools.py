# 공통적으로 쓰이는 메소드들 모음
import os
import pandas as pd
from collections import OrderedDict

import sumolib


def load_traci(apply_libsumo, gui):
    if not apply_libsumo or gui:
        import traci
        print('"traci" has been imported.')
    else:
        try:
            import libsumo as traci
            print('"libsumo" has been imported.')
        except:
            import traci
            print('"libsumo" import failed, "traci" has been imported.')
    return traci


GREEN_SET = {'g', 'G'}
YELLOW_SET = {'y'}
RED_SET = {'r'}
def get_yellow_signal_state(prev, cur):

    ## ref : https://github.com/cts198859/deeprl_signal_control/blob/master/envs/env.py#L128-L152

    ## inputs
    # prev = 'gGGGrgrgGGGgrr'
    # cur  = 'gGGGGgrgrrrgrr'
    # GREEN_SET = {'g', 'G'}

    ## outputs
    # 'gGGGrgrgyyygrr', True

    # 이전신호와 같을때
    if prev == cur:
        return cur, False
    
    # 각 인덱스에 해당하는 문자를 비교
    switch_reds, switch_greens = [], []
    for i, (p0, p1) in enumerate(zip(prev, cur)):

        # 녹색에서 적색으로 바뀔 때
        if (p0 in GREEN_SET) and (p1 == 'r'):
            switch_reds.append(i)

        # 적색에서 녹색으로 바뀔 때
        elif (p0 in 'r') and (p1 in GREEN_SET):
            switch_greens.append(i)

    # 황색신호를 포함하지 않으면
    if not switch_reds:
        return cur, False

    # 황색신호를 포함하면
    mid = list(cur)
    for i in switch_reds:
        mid[i] = 'y'
    for i in switch_greens:
        mid[i] = 'r'

    return ''.join(mid), True


def get_passing_signal_state(prev, cur, signal_type='yellow'):

    # 'signal_type' has one of the following three values. ['yellow', 'red', 'green']
    
    # 이전신호와 같을때
    if prev == cur:
        return cur, False
    
    # 각 인덱스에 해당하는 문자를 비교
    switch_reds, switch_greens = [], []
    for i, (p0, p1) in enumerate(zip(prev, cur)):

        # 녹색에서 적색으로 바뀔 때
        if (p0 in GREEN_SET) and (p1 == 'r'):
            switch_reds.append(i)

        # 적색에서 녹색으로 바뀔 때
        elif (p0 in 'r') and (p1 in GREEN_SET):
            switch_greens.append(i)

    # 황색신호를 포함하지 않으면
    if (not switch_reds) and (signal_type == 'yellow'):
        return cur, False

    # 황색신호를 포함하면
    mid = list(cur)
    for i in switch_reds:
        if signal_type == 'yellow':
            mid[i] = 'y'
    for i in switch_greens:
        mid[i] = 'r'

    return ''.join(mid), True
    

def get_list_path(directory_name, file_format):
    out = []
    for file_name in os.listdir(directory_name):
        if file_format in file_name:
            out.append(os.path.join(directory_name, file_name))
    return out


def get_list_file(directory_name, file_format):
    out = []
    for p in get_list_path(directory_name, file_format):
        _, file_name = os.path.split(p)
        out.append(file_name)
    return out


def extract_time_plan_id_from_file_name(file_names):
    return [f.split('.')[0] for f in file_names]


def convert_hhmm_to_sec(hhmm):
    time_sec = 0

    # hour -> second
    time_sec += int(hhmm[:2]) * 60 * 60

    # minute -> second
    time_sec += int(hhmm[2:]) * 60

    return time_sec


def split_time_plan_id_to_hhmm(time_plan_id):

    # 시작/종료 시간(문자형)
    begin_hhmm, end_hhmm = time_plan_id.split('_')

    # 종료시간이 '0000'인 경우 '2400'으로 변경
    end_hhmm = end_hhmm if end_hhmm != '0000' else '2400'

    return begin_hhmm, end_hhmm


def split_time_plan_id_to_sec(time_plan_id):

    # 시작/종료 시간(문자형)
    begin_hhmm, end_hhmm = split_time_plan_id_to_hhmm(time_plan_id)

    # 단위는 초로 변환하고, 정수형으로 변경
    begin_sec = convert_hhmm_to_sec(begin_hhmm)
    end_sec = convert_hhmm_to_sec(end_hhmm)

    return begin_sec, end_sec


def convert_sec_to_hhmm(time_sec):

    sec_per_hour = 3600
    sec_per_minute = 60

    quotient_hour, remainder_sec = divmod(time_sec, sec_per_hour)
    quotient_minute, remainder_sec = divmod(remainder_sec, sec_per_minute)

    hh = str(quotient_hour).zfill(2)
    mm = str(quotient_minute).zfill(2)

    return hh + mm


def floor_time_sec(time_sec, interval_sec):
    floored = (time_sec // interval_sec) * interval_sec
    return int(floored)


def convert_element_to_list(element):
    
    if element is None:
        return None

    return element if isinstance(element, list) else [element]


def get_traffic_series_from_routes(path_rou, begin_sec, end_sec, interval_sec = 900):

    '''
    필요시 추가할 것
    route = vehicle.route[0].edges  # 경로
    vtype = vehicle.type  # 차종
    '''

    if not isinstance(path_rou, list):
        path_rou = [path_rou]

    traffic_series = OrderedDict()
    for time_sec in range(begin_sec, end_sec, interval_sec):
        traffic_series[time_sec] = 0

    for p in path_rou:
        for vehicle in sumolib.xml.parse(p, 'vehicle'):
            depart_floor = int((float(vehicle.depart) // interval_sec) * interval_sec)
            traffic_series[depart_floor] += 1

    return traffic_series


def replace_key_with_value(dic):
    dic_new = OrderedDict()
    for key, value in dic.items():
        dic_new[value] = key
    return dic_new


def set_time_plan_table(scenario_id, list_sub_area_id, list_time_plan_id):
    table = []
    for time_plan_id in list_time_plan_id:
        for sub_area_id in list_sub_area_id:
            begin_sec, end_sec = split_time_plan_id_to_sec(time_plan_id)
            sub = {
                'hr_plan_id' : time_plan_id,
                'lnkg_grup_id' : sub_area_id,
                'scnr_id' : scenario_id,
                'bgng_unix_tm' : str(begin_sec),
                'end_unix_tm' : str(end_sec)
                }
            table.append(sub)
    return pd.DataFrame(table)
