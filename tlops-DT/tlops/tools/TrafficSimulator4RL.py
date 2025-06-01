import os
import sys
import pandas as pd
# import traci
# import libsumo as traci

from tqdm import tqdm
from sumolib import checkBinary
from collections import OrderedDict

import Tools as tl


class TrafficSimulator4RL:
    def __init__(self, sim, scenario_id, nodes,
                    time_plan_id, begin_sec, episode_sec,
                    path_segments, path_tll = None, path_waut = None, 
                    save_init_state = False, load_init_state = False,
                    save_results = False, gui = False):

        self.sim = sim
        self.scenario_id = scenario_id
        self.nodes = nodes
        self.list_node_id = list(self.nodes.keys())
        self.time_plan_id = time_plan_id
        self.begin_sec = begin_sec
        self.episode_sec = episode_sec
        self.path_segments = path_segments
        self.save_init_state = save_init_state
        self.load_init_state = load_init_state
        self.path_tll = path_tll
        self.path_waut = path_waut
        self.save_results = save_results
        self.gui = gui

        # self.sim = self._import_traci()

        # 파일 경로 입력
        self.path_network = os.path.join('refined', 'refined.net.xml')
        self.path_rou     = os.path.join('inputs', 'sumo.rou.xml')
        self.path_vtype   = os.path.join('inputs', 'vtype.add.xml')
        self.path_init_state = os.path.join('save_state', time_plan_id)

        # 수집할 세그먼트 데이터 종류
        self.list_data_to_collect_from_segment = [
                                                  18,  # tc.LAST_STEP_VEHICLE_ID_LIST
                                                 ]

        # 수집할 차량 데이터 종류
        self.list_data_to_collect_from_vehicle = [
                                                  79,  # tc.VAR_TYPE,
                                                  64,  # tc.VAR_SPEED,
                                                  183,  # tc.VAR_ALLOWED_SPEED,
                                                  57,  # tc.VAR_POSITION3D,
                                                  67,  # tc.VAR_ANGLE,
                                                  54,  # tc.VAR_SLOPE,
                                                  81,  # tc.VAR_LANE_ID,
                                                  86,  # tc.VAR_LANEPOSITION,
                                                  68  # tc.VAR_LENGTH
                                                 ]

        # 대기행렬 대상 여부
        self.check_queue = {True : 'Y', False : 'N'}
        self.speed_threshold = 5 / 3.6

        self.debug = False


    def _set_sumo(self):
        
        if 'SUMO_HOME' in os.environ:
            tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
            sys.path.append(tools)
        else:
            sys.exit("please declare environment variable 'SUMO_HOME'")

        sumo_cmd = [checkBinary('sumo-gui' if self.gui else 'sumo')]
        sumo_cmd += ['-b', str(self.begin_sec)]
        sumo_cmd += ['-n', self.path_network]
        sumo_cmd += ['-r', ','.join(tl.convert_element_to_list(self.path_rou))]

        path_additional = []
        if self.path_tll is not None:
            path_additional.append(self.path_tll)

        if self.path_waut is not None:
            path_additional.append(self.path_waut)

        if self.path_vtype is not None:
            path_additional.append(self.path_vtype)

        if self.path_segments is not None:
            path_additional.append(self.path_segments)

        if path_additional:
            sumo_cmd += ['-a', ','.join(path_additional)]

        sumo_cmd += ['--no-step-log', 'True']
        sumo_cmd += ['--duration-log.disable', 'True']
        sumo_cmd += ['--no-warnings', 'True']

        return sumo_cmd


    # 시뮬레이션 시작
    def sim_start(self):

        # 시뮬레이션 실행
        sumo_cmd = self._set_sumo()
        self.sim.start(sumo_cmd)

        # 저장
        if self.save_init_state:
            self.sim.simulation.saveState(self.path_init_state)

        # 불러오기
        if self.load_init_state:
            self.sim.simulation.loadState(self.path_init_state)


    # 시뮬레이션 종료
    def sim_close(self):
        self.sim.close()


    # 좌표변환 x, y -> lon, lat (느림. 대안 필요함)
    def _convert_geo(self, x, y):
        return self.sim.simulation.convertGeo(x, y)  # lon, lat

    
    # 데이터 수집
    def _set_junction_subscribe_context(self):
        # CMD_GET_VEHICLE_VARIABLE = 164
        # CMD_GET_LANEAREA_VARIABLE = 173
        self.sim.junction.subscribeContext(self.node0, 164, 1e+15, self.list_data_to_collect_from_vehicle)
        self.sim.junction.subscribeContext(self.node1, 173, 1e+15, self.list_data_to_collect_from_segment)


    def _get_junction_context_subscription_results(self):
        vehicle_values = self.sim.junction.getContextSubscriptionResults(self.node0)
        segment_values = self.sim.junction.getContextSubscriptionResults(self.node1)
        return vehicle_values, segment_values


    def _set_junction_unsubscribe_context(self):
        self.sim.junction.unsubscribeContext(self.node0, 164, 1e+15)  # vehicle
        self.sim.junction.unsubscribeContext(self.node1, 173, 1e+15)  # segment


    def _collect_data(self):
        self._set_junction_subscribe_context()
        vehicle_values, segment_values = self._get_junction_context_subscription_results()
        self._set_junction_unsubscribe_context()
        return vehicle_values, segment_values


    def _nodes_for_data_collection(self, num):
        set_node_id = set(self.list_node_id)
        out = []
        for node_id in self.sim.junction.getIDList():
            if node_id in set_node_id:
                continue
            out.append(node_id)
            if len(out) == num:
                return out
        return [None] * num


    def _get_program(self, node_id):
        return self.sim.trafficlight.getProgram(node_id)


    def run_sims(self, step_size = 5, aggregation_size = 300, is_before = None):

        print(f'Simulation start: {self.time_plan_id}')

        # init
        self.step_size = step_size
        self.aggregation_size = aggregation_size
        self.is_before = is_before
        self.analysis_type = 1 if is_before else 2

        # 데이터 수집을 위한 노드
        self.node0, self.node1 = self._nodes_for_data_collection(2)

        # 데이터를 저장할 공간
        self.observed = {}

        # 이전 정보 불러오기
        if self.load_init_state:
            self.sim.simulation.loadState(self.path_init_state)

        cur_sec = self.begin_sec
        for _ in tqdm(range(self.episode_sec)):

            # 한 스텝 진행
            self.sim.simulationStep()

            # 데이터 저장
            if self.save_results and (cur_sec % self.step_size == 0):

                # 차량, 세그먼트 데이터 수집
                vehicle_values, segment_values = self._collect_data()

                # 타임플랜 확인
                if self.debug:
                    programs = [self._get_program(node_id) for node_id in self.list_node_id]
                else:
                    programs = []

                # 적재
                self.observed[cur_sec] = vehicle_values, segment_values, self.time_plan_id, programs

            cur_sec += 1

        print(f'Simulation end: {self.time_plan_id}')


    # The trajectory of each vehicle every : sampling at predetermined time units
    # soitsanlsvhclmvmntowh
    def _get_vehicle_points(self):

        '''
        VAR_POSITION3D = 57
        VAR_SPEED = 64
        VAR_ALLOWED_SPEED = 183
        VAR_LENGTH = 68

        '''

        ## 데이터 저장공간 셋팅
        vehicle_points = []

        ## 데이터 가공
        for time_sec, values in tqdm(self.observed.items()):
            vehicle_values, segment_values, time_plan_id, programs = values

            for vehicle_id, value in vehicle_values.items():

                x, y, z = value[57]
                lon, lat = self._convert_geo(x, y)

                sub = {
                'scenario_id' : self.scenario_id,
                'analysis_type' : self.analysis_type,
                'time_plan_id' : time_plan_id,
                'time_group' : (time_sec // self.aggregation_size) * self.aggregation_size,
                'unix_time' : time_sec,
                'vehicle_id' : vehicle_id,
                'speed' : value[64],
                'allowed_speed' : value[183],
                'lon' : lon, 'lat' : lat,
                'loss_time' : 1 - value[64] / value[183],  # loss_time = 1 - speed / allowed_speed
                'vehicle_length' : value[68]
                }
                
                if self.debug:
                    # 프로그램이 정상적으로 바뀌는지 확인을 위해 저장
                    for i, node_id in enumerate(self.list_node_id):
                        sub[node_id] = programs[i]

                vehicle_points.append(sub)

        vehicle_points = pd.DataFrame(vehicle_points)
        col_names = ['scnr_id', 'anls_div_cd', 'hr_plan_id', 'tm_grup_no', 'locn_clct_unix_tm', 'vhcl_id', 'mmnt_sped', 'non_cngt_sped', 'vhcl_lot', 'vhcl_lat', 'dely_tm', 'vhcl_lngt']
        if self.debug:
            col_names += self.list_node_id

        vehicle_points.columns = col_names
        return vehicle_points


    # Aggregate traffic indicators at each intersection for all times
    # soitsanlsintsvalue
    def _get_traffic_indicators_from_each_intersection_for_all_times(self):

        '''
        LAST_STEP_VEHICLE_ID_LIST = 18
        VAR_SPEED = 64
        VAR_ALLOWED_SPEED = 183
        '''

        ## 데이터 저장공간 셋팅
        storages = {node_id : {'loss_time' : 0, 'vehicle_seen' : set()} for node_id in self.list_node_id}

        ## 데이터 가공
        for time_sec, values in tqdm(self.observed.items()):
            vehicle_values, segment_values, time_plan_id, programs = values

            for node_id, node in self.nodes.items():
                storage = storages[node_id]
                for segment_id in node['incoming']['segment_cluster']:
                    for vehicle_id in segment_values[segment_id][18]:
                        value = vehicle_values[vehicle_id]
                        storage['loss_time'] += (1 - value[64] / value[183])
                        if vehicle_id not in storage['vehicle_seen']:
                            storage['vehicle_seen'].add(vehicle_id)

        outs = []
        for node_id, storage in storages.items():

            # 총 지체시간 : step_size 단윌 샘플링했으므로 step_size 만큼 곱하여 보정
            tot_loss_time = storage['loss_time'] * self.step_size

            # 관측된 차량 수
            number_of_vehicle = len(storage['vehicle_seen'])
            avg_loss_time = 0 if number_of_vehicle == 0 else tot_loss_time / number_of_vehicle

            sub = {
            'scenario_id' : self.scenario_id,
            'time_plan_id' : self.time_plan_id,
            'analysis_type' : self.analysis_type,
            'node_id' : node_id,
            'tot_loss_time' : tot_loss_time,
            'avg_loss_time' : avg_loss_time,
            'vehicle_seen' : number_of_vehicle
            }
            outs.append(sub)

        outs = pd.DataFrame(outs)
        outs.columns = ['scnr_id', 'hr_plan_id', 'anls_div_cd', 'node_id', 'tot_dely_tm', 'avg_dely_tm', 'trvl']
        return outs


    # Aggregate traffic indicators from all road networks for all times
    # soitsanlsallvalue
    def _get_traffic_indicators_from_all_networks_for_all_times(self):

        ## 데이터 저장을 위한 공간
        simulation_sec = 0
        sampled_sec = 0
        sum_speed = 0  # 시뮬레이션이 1초 단위기 때문에 sum_distance랑 같음
        sum_loss_time = 0
        sum_queue_length = 0
        vehicle_seen = set()

        ## 데이터 가공
        for time_sec, values in tqdm(self.observed.items()):
            vehicle_values, segment_values, time_plan_id, programs = values
            simulation_sec += 1

            for vehicle_id, value in vehicle_values.items():

                if vehicle_id not in vehicle_seen:
                    vehicle_seen.add(vehicle_id)

                speed, allowed_speed = value[64], value[183]
                vehicle_length = value[68]

                sampled_sec += 1
                sum_speed += speed
                sum_loss_time += 1 - speed / allowed_speed
                sum_queue_length += vehicle_length if speed < self.speed_threshold else 0

        ## 최종 집계
        # 관측된 차량의 수
        number_of_vehicle = len(vehicle_seen)

        # 전체 지체시간
        sum_loss_time *= self.step_size
        # sum_queue_length *= self.step_size

        # 평균 통행속도
        avg_speed = sum_speed / sampled_sec

        # 차량별 평균 지체시간
        avg_loss_time = sum_loss_time / number_of_vehicle

        # 평균 대기행렬 길이
        avg_queue_length = sum_queue_length / simulation_sec

        out = {
        'scenario_id' : self.scenario_id,
        'analysis_type' : self.analysis_type,
        'time_plan_id' : self.time_plan_id,
        'number_of_vehicle' : number_of_vehicle,
        'avg_speed' : avg_speed,
        'avg_loss_time' : avg_loss_time,
        'avg_queue_length' : avg_queue_length
        }

        out = pd.DataFrame([out])
        out.columns = ['scnr_id', 'anls_div_cd', 'hr_plan_id', 'trvl', 'avg_sped', 'avg_dely_tm', 'avg_queu_lngt']
        return out


    # Aggregate time series traffic indicators from all road networks
    def _get_time_series_traffic_indicators_from_all_networks(self):

        ## 데이터 저장을 위한 공간
        storages = {}

        ## 데이터 가공
        for time_sec, values in tqdm(self.observed.items()):
            vehicle_values, segment_values, time_plan_id, programs = values
            time_group = (time_sec // self.aggregation_size) * self.aggregation_size

            if time_group not in storages:
                storages[time_group] = {}
                storage = storages[time_group]
                storage['simulation_sec'] = 0
                storage['sampled_sec'] = 0
                storage['sum_speed'] = 0
                storage['sum_loss_time'] = 0
                storage['sum_queue_length'] = 0
                storage['vehicle_seen'] = set()

            storage = storages[time_group]
            storage['simulation_sec'] += 1

            for vehicle_id, value in vehicle_values.items():

                if vehicle_id not in storage['vehicle_seen']:
                    storage['vehicle_seen'].add(vehicle_id)

                speed, allowed_speed = value[64], value[183]
                vehicle_length = value[68]

                storage['sampled_sec'] += 1
                storage['sum_speed'] += speed
                storage['sum_loss_time'] += 1 - speed / allowed_speed
                storage['sum_queue_length'] += vehicle_length if speed < self.speed_threshold else 0

        ## 최종 집계
        out = []
        for time_group, storage in storages.items():

            # 관측된 차량의 수
            number_of_vehicle = len(storage['vehicle_seen'])

            # 전체 지체시간
            storage['sum_loss_time'] *= self.step_size

            # 평균 통행속도
            if storage['sampled_sec'] != 0:
                avg_speed = storage['sum_speed'] / storage['sampled_sec']
            else:
                avg_speed = 0

            # 차량별 평균 지체시간
            if number_of_vehicle != 0:
                avg_loss_time = storage['sum_loss_time'] / number_of_vehicle
            else:
                avg_loss_time = 0

            # 평균 대기행렬 길이
            avg_queue_length = storage['sum_queue_length'] / storage['simulation_sec']

            sub = {
            'scenario_id' : self.scenario_id,
            'analysis_type' : self.analysis_type,
            'time_plan_id' : self.time_plan_id,
            'time_group' : time_group,
            'number_of_vehicle' : number_of_vehicle,
            'avg_speed' : avg_speed,
            'avg_loss_time' : avg_loss_time,
            'avg_queue_length' : avg_queue_length
            }
            out.append(sub)

        out = pd.DataFrame(out)
        # 컬러명 조정 : 인천시 양식에 맞춤
        out.columns = ['scnr_id', 'anls_div_cd', 'hr_plan_id', 'tm_grup_no', 'trvl', 'avg_sped', 'avg_dely_tm', 'avg_queu_lngt']
        return out


    def _process_data(self):

        ## before or after
        when = 'before' if self.is_before else 'after'

        # Vehicle points for the entire network
        vehicle_points = self._get_vehicle_points()
        vehicle_points.to_csv(os.path.join('outputs', f'{when}_{self.time_plan_id}_vehicle_points.csv'), index = False)

        # Aggregate traffic indicators at each intersection for all times
        values_for_each_inter = self._get_traffic_indicators_from_each_intersection_for_all_times()
        values_for_each_inter.to_csv(os.path.join('outputs', f'{when}_{self.time_plan_id}_values_for_each_inter.csv'), index = False)

        # Aggregate traffic indicators from all road networks for all times
        values_for_all = self._get_traffic_indicators_from_all_networks_for_all_times()
        values_for_all.to_csv(os.path.join('outputs', f'{when}_{self.time_plan_id}_values_for_all.csv'), index = False)

        # Get time series traffic indicators for entire network
        time_series_values_for_all = self._get_time_series_traffic_indicators_from_all_networks()
        time_series_values_for_all.to_csv(os.path.join('outputs', f'{when}_{self.time_plan_id}_time_series_values_for_all.csv'), index = False)
