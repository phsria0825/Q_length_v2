import os
import pandas as pd
from config import Cfg
from collections import OrderedDict

import Tools as tl
import ToolsWriteLoad as twl

from InitNodes import InitNodes
from SetWorkingDirectory import make_dirs
from SetTimePlanToSimulate import SetTimePlanToSimulate


def load_sub_areas():

    # node 정보 불러오기
    d_type = {'sub_area_id':str, 'root_id':int, 'node_id':str, 'major_intersection_separating_code':int, 'sequence' : int}
    node_info = pd.read_csv(os.path.join('inputs', 'node.csv'), dtype=d_type)

    # 연동그룹과 연동그룹내 순서로 정렬
    node_info = node_info.sort_values(by=['sub_area_id', 'sequence'], ascending=True)

    # 딕셔너리 형태로 변경 (key : value = sub_area_id : node_id)
    dic = OrderedDict()
    for i in range(len(node_info)):

        row = node_info.iloc[i]
        key = row.sub_area_id
        node_id = row.node_id
        
        if key not in dic:
            dic[key] = []
            
        dic[key].append(node_id)

    return dic


def write_time_plan_table(scenario_id, time_plan_with_begin_sec):

    sub_areas = load_sub_areas()

    table = []
    for time_plan_id in time_plan_with_begin_sec.keys():
        for sub_area_id in list(sub_areas.keys()):
            begin_sec, end_sec = tl.split_time_plan_id_to_sec(time_plan_id)
            sub = {}
            sub['hr_plan_id'] = time_plan_id
            sub['lnkg_grup_id'] = sub_area_id
            sub['scnr_id'] = scenario_id
            sub['bgng_unix_tm'] = begin_sec
            sub['end_unix_tm'] = end_sec
            table.append(sub)
            
    path = os.path.join('outputs', 'soitsanlshrplancnfg.csv')
    twl.write_table(path, pd.DataFrame(table))


def main():
    
    ## 시나리오ID : 현재 폴더이름
    scenario_id = os.path.split(os.getcwd())[1]

    ## 필요한 디렉토리 생성
    make_dirs()

    ## 노드정보 준비
    InitNodes().main()

    ## 타임플랜
    stpts = SetTimePlanToSimulate()
    time_plan_with_begin_sec = stpts.main()

    # # 여기는 테스트때 타임플랜 선정을 위함
    # keys = list(time_plan_with_begin_sec.keys())
    # for i, key in enumerate(keys):
    #     if key in ['1700_2000']:
    #         continue
    #     del time_plan_with_begin_sec[key]

    ## 결과 산출을 위해 저장 필요함
    path = os.path.join('outputs', 'time_plan_with_begin_sec.pkl')
    twl.write_dic(path, time_plan_with_begin_sec)
    write_time_plan_table(scenario_id, time_plan_with_begin_sec)


if __name__ == '__main__':
    main()
