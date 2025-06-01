import os
import pandas as pd
from heapq import heappush, heappop
from collections import defaultdict, OrderedDict

import sumolib


class DefPhaseSet:
    def __init__(self, path_tll, apply_min_duration = False, apply_max_duration = False):
        self.path_tll = path_tll
        self.apply_min_duration = apply_min_duration
        self.apply_max_duration = apply_max_duration


    def _load_tll_xml(self, path):
        return sumolib.xml.parse(path, 'tlLogic')


    # Check if there is 'y' or 'Y'
    def _check_signal_state(self, signal_state, color):
        
        # No yellow signal here
        if color == 'green':        
            assert 'y' not in signal_state.lower(), '녹색신호에 y가 있습니다.'

        # There should be a yellow signal here.
        if color == 'yellow':
            assert 'y' in signal_state.lower(), '황색신호에 y가 없습니다.'


    def main(self):

        ## =================================================================
        ## phase list : signal_states and min_duration
        ## =================================================================
        dic_phases = {}
        for tl_logic in self._load_tll_xml(self.path_tll):

            # 이미 처리되었다면 패스
            node_id = tl_logic.id
            if node_id in dic_phases:
                continue

            heap = []
            red_durations = {}
            yellow_durations = {}
            first_appeared = -1e+7
            for i, phase_info in enumerate(tl_logic.phase):

                phase_name = phase_info.attr_name
                if phase_name is not None:
                    phase_no, signal_color = phase_name.split('_')
                    phase_no = int(phase_no)

                    if phase_no <= 0:
                        raise Exception(f'현시번호는 0보다 커야 함. node_id: {node_id}')

                    if signal_color == 'y':
                        if phase_no in yellow_durations:
                            raise Exception(f'황색시간이 이미 정의되어 있음. node_id: {node_id}, phase_no: {phase_no}')
                        yellow_durations[phase_no] = int(phase_info.duration)
                        continue

                    if signal_color == 'r':
                        if phase_no in red_durations:
                            raise Exception(f'적색시간이 이미 정의되어 있음. node_id: {node_id}, phase_no: {phase_no}')
                        red_durations[phase_no] = int(phase_info.duration)
                        continue

                else:
                    phase_no = i + 1

                self._check_signal_state(phase_info.state, 'green')

                # 처음나타난 녹색현시가 주현시
                if first_appeared < 0:
                    is_critical = True
                    first_appeared = phase_no
                else:
                    is_critical = False

                # 최소, 최대 녹색시간
                min_duration, max_duration = phase_info.minDur, phase_info.maxDur

                # tll.xml에 min_dur, max_dur값이 없는 경우
                min_duration = -1 if min_duration is None else int(min_duration)
                max_duration = -1 if max_duration is None else int(max_duration)

                # 최대/최소 녹색시간 적용 여부 확인
                min_duration = min_duration if self.apply_min_duration else -1
                max_duration = max_duration if self.apply_max_duration else -1

                sub = (phase_no, is_critical, phase_info.duration, min_duration, max_duration, phase_info.state)
                heappush(heap, sub)

            dic = defaultdict(list)
            while heap:
                phase_no, is_critical, duration, min_duration, max_duration, signal_state = heappop(heap)
                red_duration = red_durations.get(phase_no, 0)
                yellow_duration = yellow_durations.get(phase_no, 0)
                dic['phase_numbers'].append(phase_no)
                dic['is_criticals'].append(is_critical)
                dic['min_durs'].append(min_duration)
                dic['max_durs'].append(max_duration)
                dic['red_durations'].append(red_duration)
                dic['yellow_durations'].append(yellow_duration)
                dic['signal_states'].append(signal_state)
            dic_phases[node_id] = dic

        ## =================================================================
        ## phase combination
        ## =================================================================
        dic_combs = {}
        for key, value in dic_phases.items():
            dic_combs[key] = OrderedDict()
            signal_states = value['signal_states']
            phase_num = len(signal_states)
            for i in range(phase_num):
                j = i + 1
                dic_combs[key][i] = [j if j < phase_num else j % phase_num]

        ## =================================================================
        ## summary
        ## =================================================================
        dic_phase_sets = {}
        for key in dic_phases.keys():

            dic_phase_sets[key] = {}
            phase_set = dic_phase_sets[key]

            # Adding signal_states and min_duration
            phase_set['phases'] = dic_phases[key]

            # Adding phase combination
            phase_set['combinations'] = dic_combs[key]

            # Adding critical phase : 주현시
            phase_set['critical_phase_index'] = dic_phases[key]['is_criticals'].index(True)

        return dic_phase_sets