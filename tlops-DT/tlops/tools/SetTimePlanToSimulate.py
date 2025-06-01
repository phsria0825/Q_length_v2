import os
import numpy as np
import pandas as pd

import sumolib
import matplotlib.pyplot as plt
from collections import OrderedDict

import Tools as tl
from config import Cfg
from ClusterAnalysis import ClusterAnalysis


class SetTimePlanToSimulate:
    def __init__(self, episode_sec = 3600, interval_sec = 900, min_time_plan_sec = 7200,
                       ratio_threshold = 1.5, weight_sec = 1.0, weight_traffic = 1.0, max_time_plan_count = 10):

        # 통행배정 데이터 경로 : 리스트가 올 수도 있음
        self.path_rou = os.path.join('inputs', 'sumo.rou.xml')

        # 시뮬레이션 시간 (ex : 3600초)
        self.episode_sec = Cfg.episode_sec

        # 교통량 집계 구간 시간 (ex : 900초)
        self.interval_sec = Cfg.interval_sec

        # 한 번의 에피소드를 위해 몇 개의 인터벌이 필요한가?
        self.interval_num = int(self.episode_sec / self.interval_sec)  # 3600 / 900 = 4회

        # 타임플랜의 최소 시간
        self.min_time_plan_sec = Cfg.min_time_plan_sec

        # 하루의 시작, 종료 시간(초)
        self.begin_sec_of_day = 0  # 하루의 시작
        self.end_sec_of_day = 24 * 60 * 60  # 하루의 종료

        ## 아래는 타임플랜을 새로 설정할 때 필요한 상수
        # 분산이 한 번에 급격하게 증가하면 중단하기 위한 임계값
        self.ratio_threshold = Cfg.ratio_threshold

        # 정규화 파라미터
        self.weight_sec = Cfg.weight_sec
        self.weight_traffic = Cfg.weight_traffic

        # 타임플랜 최대 갯수
        self.max_time_plan_count = Cfg.max_time_plan_count


    def _get_traffic_series_from_routes(self):

        '''
        필요시 추가할 것 : 이번엔 경로나 차종 상관하지 않음
        route = vehicle.route[0].edges  # 경로
        vtype = vehicle.type  # 차종
        '''

        traffic_series = OrderedDict()
        for time_sec in range(self.begin_sec_of_day, self.end_sec_of_day, self.interval_sec):
            traffic_series[time_sec] = 0

        for p in tl.convert_element_to_list(self.path_rou):
            for vehicle in sumolib.xml.parse(p, 'vehicle'):
                depart_floor = tl.floor_time_sec(float(vehicle.depart), self.interval_sec)
                traffic_series[depart_floor] += 1

        return traffic_series  # key : time_sec / value : traffic_count


    # 딕셔너리형태를 넘파이 어레이로 변경
    def _convert_to_array(self, traffic_series):
        input_array = []
        for key, value in traffic_series.items():
            input_array.append([key, value])
        return np.array(input_array, dtype = np.int32)

    
    def _get_time_plan_with_begin_sec(self, traffic_series, time_plan_with_period):

        saved = []
        for time_plan_id, list_sec in time_plan_with_period.items():
            
            max_count = -1
            for i, time_sec in enumerate(list_sec):
                
                subset = list_sec[i:(i + self.interval_num)]
                if len(subset) < self.interval_num:
                    break
                    
                count = 0
                for time_sec in subset:
                    count += traffic_series[time_sec]
                    
                if count > max_count:
                    temp = (subset[0], list_sec[0], time_plan_id)
                    max_count = count

            saved.append(temp)

        time_plan_with_begin_sec = OrderedDict()
        for train_begin_sec, time_plan_begin_sec, time_plan_id in sorted(saved, key = lambda x : x[0]):
            dic = {}
            dic['train_begin_sec'] = train_begin_sec
            dic['time_plan_begin_sec'] = time_plan_begin_sec
            time_plan_with_begin_sec[time_plan_id] = dic

        return time_plan_with_begin_sec


    def main(self):

        traffic_series = self._get_traffic_series_from_routes()

        # 군집분석을 위한 args 셋팅
        input_array = self._convert_to_array(traffic_series)
        interval_sec, min_time_plan_sec = self.interval_sec, self.min_time_plan_sec
        ratio_threshold, weight_sec, weight_traffic = self.ratio_threshold, self.weight_sec, self.weight_traffic
        max_time_plan_count = self.max_time_plan_count
        ca = ClusterAnalysis(input_array, interval_sec, min_time_plan_sec, ratio_threshold, weight_sec, weight_traffic, max_time_plan_count)

        time_plan2time_group = ca.main()
        time_plan_with_begin_sec = self._get_time_plan_with_begin_sec(traffic_series, time_plan2time_group)

        return time_plan_with_begin_sec
