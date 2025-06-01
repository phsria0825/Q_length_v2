import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from scipy.stats import mode, gaussian_kde
from collections import Counter, defaultdict, OrderedDict

import Tools as tl
import ToolsWriteLoad as twl

import argparse


class PhaseConfig:
    def __init__(self, scenario_id, step_size):
        self.scenario_id = scenario_id
        self.step_size = step_size
        self.nodes = twl.load_dic(os.path.join('refined', 'nodes.pkl'))

        self.sub_areas = self._load_sub_areas()        
        self.time_plan_with_begin_sec = twl.load_dic(os.path.join('outputs', 'time_plan_with_begin_sec.pkl'))
        self.path_phase_table = os.path.join('outputs', 'soitsanlshrplanrslt.csv')

        self.list_node_id = list(self.nodes.keys())
        self.list_time_plan_id = list(self.time_plan_with_begin_sec.keys())
        self.rings = ['A', 'B']
        self.maximum_number_of_phase = 8  # 최대 8개의 현시를 가질 수 있음
        self.col_names = self._set_col_names()
        self.rep_type = 'kde'


    def _load_sub_areas(self):

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


    # 컬럼명 셋팅
    def _set_col_names(self):

        # 기본정보 컬럼
        col_names = ['node_id', 'hr_plan_id', 'lnkg_grup_id', 'scnr_id',
                     'ofst_hr', 'cycl_hr', 'majr_road_se_cd']

        # 녹색시간 컬럼
        for ring in self.rings:
            for i in range(self.maximum_number_of_phase):
                phase_no = i + 1
                col_name = f'{ring}_ring_{phase_no}_phas_gren_hr'
                col_names.append(col_name)

        # 황색시간 컬럼
        for ring in self.rings:
            for i in range(self.maximum_number_of_phase):
                phase_no = i + 1
                col_name = f'{ring}_ring_{phase_no}_phas_yelw_hr'
                col_names.append(col_name)

        return col_names
        

    # 대표값 구하기(최빈값, 평균값, 중앙값 중 택1)
    def _calc_representative_value(self, list_of_numbers, rep_type = 'mode'):

        # 최빈값
        if rep_type == 'mode':
            return int(mode(list_of_numbers)[0])

        # 평균값
        if rep_type == 'mean':
            return np.mean(list_of_numbers)

        # 중앙값
        if rep_type == 'median':
            return np.median(list_of_numbers)

        if rep_type == 'kde':
            kdeArr = gaussian_kde(list_of_numbers)(list_of_numbers)
            return list_of_numbers[np.argmax(kdeArr)]
    
    
    # 밀도함수 그리기
    def _plot(self, list_of_numbers):
        sns.kdeplot(list_of_numbers)
        plt.show()
    
    
    # tll_hist.csv 로드
    def _load_tll_hist(self, path):
        # 불러오고
        tll_hist = pd.read_csv(path)
        # 모든 데이터를 정수형으로 변경
        tll_hist = tll_hist.astype('int')
        # step으로 정렬
        tll_hist = tll_hist.sort_values(by = 'step')
        return tll_hist


    def _check_list_node_id(self, list_node_id):
        assert list_node_id == self.list_node_id


    '''
    # initialize
    phase_config 초기 셋팅
    (각 교차로만 고려한 셋팅 : 나중에 연동그룹 등 추가 고려하는 작업 진행)
    '''
    def _init_phase_config(self, path):

        # tll_hist.csv 불러오기
        tll_hist = self._load_tll_hist(path)
        tot_count = len(tll_hist)

        # 노드리스트 체크 : 누락된 교차로가 있는지 확인
        self._check_list_node_id(list(tll_hist.columns)[1:])

        dic_phase_config = {node_id : {} for node_id in self.list_node_id}
        for node_id in self.list_node_id:
            phase_config = dic_phase_config[node_id]
            
            # 해당 node_id의 현시순서
            phase_index_series = tll_hist[node_id].tolist()
            
            # 각 현시의 등장빈도
            phase_count = Counter(phase_index_series)
            
            # 가장 많이 나온 현시를 주현시로 정의
            phase_sorted = sorted(phase_count.most_common(), key = lambda x : (-x[1], x[0]))
            critical_phase_index, max_count = phase_sorted[0]
            
            # cycle 정의
            prev_phase_index, begin_step = -1, -1
            intervals, start_points = [], []
            for i, cur_phase_index in enumerate(phase_index_series):

                # 주현시가 처음 시작하는 지점 탐색
                if (cur_phase_index == critical_phase_index) and (cur_phase_index != prev_phase_index):
                    if begin_step > 0:
                        interval = (i - begin_step) * self.step_size
                        intervals.append(interval)  # 주현시 시작간격
                    start_points.append(i * self.step_size)  # 주현시 시간시간
                    begin_step = i
                prev_phase_index = cur_phase_index

            cycle = self._calc_representative_value(intervals, self.rep_type)
            # self._plot(intervals)
            
            # 녹색시간
            phase_durations = {}
            for key, value in phase_count.items():  # Key = phase_index
                # 비율계산
                ratio = value / tot_count
                # 비율에다가 주기를 곱하여 녹색시간을 구함
                phase_durations[key] = ratio * cycle
                
            # 오프셋
            remainders = [p % cycle for p in start_points]
            offset = self._calc_representative_value(remainders, self.rep_type)
            # self._plot(remainders)

            phase_config['critical_phase_index'] = critical_phase_index
            phase_config['cycle'] = sum(phase_durations.values())
            phase_config['phase_durations'] = phase_durations
            phase_config['offset'] = offset

        return dic_phase_config


    # Reprocess
    def _rep_phase_config(self):
        
        # 신호운영계획을 저장하기 위한 딕셔너리 생성
        dic_operation_plan = {}

        # 타임플랜마다
        for time_plan_id in self.list_time_plan_id:

            dic_phase_config = self._init_phase_config(os.path.join('save_tll_hist',  f'{time_plan_id}.csv'))

            # 각 연동그룹마다
            for sub_area_id, list_node_id_in_cs_group in self.sub_areas.items():
                
                # 현재 연동그룹의 최대주기 선정
                max_cycle = -1
                for node_id in list_node_id_in_cs_group:
                    cycle = dic_phase_config[node_id]['cycle']

                    # max_cycle = max(max_cycle, cycle)
                    if cycle > max_cycle:
                        max_cycle = cycle

                for node_id in list_node_id_in_cs_group:
                    phase_config = dic_phase_config[node_id]
                    cycle = phase_config['cycle']
                    diff = 0.0
                    for phase_index, duration in phase_config['phase_durations'].items():

                        # 녹색시간 조정
                        duration *= (max_cycle / cycle)
                        new_duration = np.round(duration, 0)
                        phase_config['phase_durations'][phase_index] = new_duration

                        # 보충해야할 양 누적합
                        diff += (duration - new_duration)

                    # 주현시에 보충
                    critical_phase_index = phase_config['critical_phase_index']
                    phase_config['phase_durations'][critical_phase_index] += np.round(diff, 0)

                    # 나머지 값들 저장
                    phase_config['cycle'] = max_cycle
                    phase_config['time_plan_id'] = time_plan_id
                    phase_config['sub_area_id'] = sub_area_id

            dic_operation_plan[time_plan_id] = dic_phase_config

        return dic_operation_plan


    def _convert_operation_plan_to_wider_table(self, dic_operation_plan):
        table = []
        for time_plan_id, dic_phase_config in dic_operation_plan.items():
            for node_id, phase_config in dic_phase_config.items():

                sub, sub_g_a, sub_g_b, sub_y_a, sub_y_b = [], [], [], [], []
                sub.append(node_id)
                sub.append(phase_config['time_plan_id'])
                sub.append(phase_config['sub_area_id'])
                sub.append(self.scenario_id)
                sub.append(phase_config['offset'])
                sub.append(phase_config['cycle'])
                sub.append(phase_config['critical_phase_index'] + 1)

                phase_durations = phase_config['phase_durations']
                for phase_index in range(self.maximum_number_of_phase):

                    # 결과 테이블에서의 녹색시간은 황색시간이 포함된 녹색시간인가?
                    duration = phase_durations.get(phase_index)
                    duration = duration if duration is None else int(duration)
                    yellow_duration = None if duration is None else self.step_size

                    for ring in self.rings:
                        if ring == 'A':
                            # 녹색시간
                            sub_g_a.append(duration)
                            # 황색시간
                            sub_y_a.append(yellow_duration)
                        elif ring == 'B':
                            sub_g_b.append(duration)
                            sub_y_b.append(yellow_duration)

                sub += sub_g_a
                sub += sub_g_b
                sub += sub_y_a
                sub += sub_y_b
                table.append(sub)

        table = pd.DataFrame(table)
        table.columns = self.col_names

        return table


    def _convert_operation_plan_to_xml(self, dic_operation_plan):
        tl_logics = ['<tlLogics>\n']
        for time_plan_id, dic_phase_config in dic_operation_plan.items():

            for node_id, phase_config in dic_phase_config.items():

                offset = phase_config['offset']
                critical_phase_index = phase_config['critical_phase_index']

                signal_states = self.nodes[node_id]['signal_states']
                phase_durations = phase_config['phase_durations']
                number_of_phase = len(signal_states)

                tl_logics.append(f'    <tlLogic id="{node_id}" type="static" programID="{time_plan_id}" offset="{offset}">\n')
                for i in range(number_of_phase):

                    phase_index = (critical_phase_index + i) % number_of_phase
                    next_phase_index = (phase_index + 1) % number_of_phase

                    signal_state = signal_states[phase_index]
                    next_signal_state = signal_states[next_phase_index]
                    yellow_signal_state, has_y = tl.get_yellow_signal_state(signal_state, next_signal_state)

                    duration = phase_durations.get(phase_index)
                    yellow_duration = self.step_size if has_y else 0
                    green_duration = duration - yellow_duration
                    
                    tl_logics.append(f'      <phase duration="{int(green_duration)}" state="{signal_state}"/>\n')

                    if yellow_duration > 0:
                        tl_logics.append(f'      <phase duration="{int(yellow_duration)}" state="{yellow_signal_state}"/>\n')

                tl_logics.append('    </tlLogic>\n')
        tl_logics.append('</tlLogics>')
        return ''.join(tl_logics)


    def main(self):
        dic_operation_plan = self._rep_phase_config()

        table = self._convert_operation_plan_to_wider_table(dic_operation_plan)
        twl.write_table(self.path_phase_table, table)

        tl_logics = self._convert_operation_plan_to_xml(dic_operation_plan)
        path_xml = os.path.join('outputs', 'after.tll.xml')
        twl.write_txt(path_xml, tl_logics)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario-id', '-i', dest = 'scenario_id', type = str)
    parser.add_argument('--step-size', '-s', dest = 'step_size', type = int)
    args = parser.parse_args()
    return args


def main(args):
    pc = PhaseConfig(args.scenario_id, args.step_size)
    pc.main()


# python PhaseConfig.py --scenario-id {scenario_id} --step-size {step_size}
if __name__ == '__main__':
    args = parse_args()
    main(args)
