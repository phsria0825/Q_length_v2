import os
import sys
import numpy as np
import pandas as pd
from collections import defaultdict

from config import Cfg
import Tools as tl


class Env:
    def __init__(self, time_plan_id, sim, nodes, state_types, episode_sec = -1, step_size = 4):
        
        # worker name
        self.time_plan_id = time_plan_id

        # simulator connection
        self.sim = sim

        # 각 노드의 모든 정보를 가지는 딕셔너리
        self.nodes = nodes
        self.list_node_id = list(self.nodes.keys())

        # state로 활용할 진입로와 종류
        self.state_types = state_types
        self.value_types = Cfg.value_types

        self.episode_sec = episode_sec
        self.check_expected_to_leave = Cfg.check_expected_to_leave
        self.step_size = step_size
        self.neighbor_discount_factor = Cfg.neighbor_discount_factor

        # 정규화 변수들
        self.dic_norm = {}
        self.dic_norm['wave'] = 100.0
        self.dic_norm['speed'] = 100.0 / 3.6  # 시속100km을 m/s로 변환

        self.dic_clip = {}
        self.dic_clip['wave'] = 1.0
        self.dic_clip['speed'] = 1.0

        # 리워드 스케일링 대상
        self.reward_scaling_target = Cfg.reward_scaling_target
        self.GAMMA = Cfg.GAMMA
        self.max_reward_beta = Cfg.max_reward_beta

        # 저장소 초기화
        self.storages = self._init_storages()

        self.dic_type_code = {}
        self.dic_type_code['wave'] = 19  # tc.LAST_STEP_OCCUPANCY
        self.dic_type_code['speed'] = 17  # tc.LAST_STEP_MEAN_SPEED
        self.dic_type_code['id'] = 18  # tc.LAST_STEP_VEHICLE_ID_LIST
        self.dic_type_code['running'] = 16  # tc.LAST_STEP_VEHICLE_NUMBER
        self.dic_type_code['halting'] = 20  # tc.LAST_STEP_VEHICLE_HALTING_NUMBER
        self.dic_type_code['jam_length'] = 25  # tc.JAM_LENGTH_METERS

        self.green_set = {'G', 'g'}
        self.inf = 1e+15
        self.deceleration_threshold = Cfg.deceleration_threshold

        self.penalty_jam = Cfg.penalty_jam
        self.reward_coefs = Cfg.reward_coefs
        self._check_reward_coefs()
        self.enable_vehicle_values = Cfg.enable_vehicle_values

        self.tracking_reward_values = Cfg.tracking_reward_values
        self.reward_values_scaled = []
        self.reward_values_org = []

        self.point1, self.point2 = self._get_node_id_for_measuring()
        self.path_init_state = os.path.join('save_state', time_plan_id)

        self.list_data_to_collect_from_segment = [19, 17]
        if 'jams' in self.reward_scaling_target:
            self.list_data_to_collect_from_segment.append(25)

        if self.enable_vehicle_values:
            self.list_data_to_collect_from_segment.append(18)

        self.list_data_to_collect_from_vehicle = [64]
        if 'delays' in self.reward_scaling_target:
            self.list_data_to_collect_from_vehicle.append(183)

        if 'decels' in self.reward_scaling_target:
            self.list_data_to_collect_from_vehicle.append(114)

        if 'waits' in self.reward_scaling_target:
            self.list_data_to_collect_from_vehicle.append(122)
        
        '''
        ## segment

        tc.LAST_STEP_OCCUPANCY = 19, for state
        tc.LAST_STEP_MEAN_SPEED = 17, for state
        tc.JAM_LENGTH_METERS = 25, for reward
        tc.JAM_LENGTH_VEHICLE = 24, for reward
        tc.LAST_STEP_VEHICLE_ID_LIST = 18, for reward
        tc.LAST_STEP_VEHICLE_HALTING_NUMBER = 20, for reward
        tc.LAST_STEP_VEHICLE_NUMBER 16 = for reward


        ## vehicle

        tc.VAR_WAITING_TIME = 122
        tc.VAR_SPEED = 64
        tc.VAR_ALLOWED_SPEED = 183
        tc.VAR_LANE_ID = 81
        tc.VAR_ACCUMULATED_WAITING_TIME = 135
        tc.VAR_ACCELERATION = 114

        # https://sumo.dlr.de/daily/pydoc/traci.constants.html
        '''

        
    def _get_node_id_for_measuring(self):
        '''
        데이터 수집을 위해 신호교차로에 해당하지 않는 노드 2개를 가져옴
        _observe_values에서 node_id로 사용됨
        '''

        outs = []
        set_node_id = set(self.list_node_id)
        for node_id in self.sim.junction.getIDList():
            if node_id not in set_node_id:
                outs.append(node_id)

            if len(outs) == 2:
                break

        return outs


    def _clip(self, x, norm, clip = -1):
        x = x / norm
        return x if clip < 0 else np.clip(x, 0, clip)


    def _observe_values(self, node_id, domain_int, list_data):

        # subscribeContext
        self.sim.junction.subscribeContext(node_id, domain_int, self.inf, list_data)

        # get data
        out = self.sim.junction.getContextSubscriptionResults(node_id)

        # unsubscribeContext
        self.sim.junction.unsubscribeContext(node_id, domain_int, self.inf)

        return out


    def _get_value_from_segment(self, segment_id, typ, segment_values, weights = None):

        # 필요한 데이터 불러오기
        type_code = self.dic_type_code[typ]
        val = segment_values[segment_id][type_code]

        # replacing nan to zero
        val = val if not np.isnan(val) else 0.0

        # 가중치 딕셔너리가 있다면 가중치를 곱해서 리턴
        return val if weights is None else val * weights[segment_id]


    def _init_storages(self):
        self.iter = 0
        storages = {node_id : {} for node_id in self.list_node_id}
        for node_id in self.list_node_id:
            storage = storages[node_id]
            for target in self.reward_scaling_target:
                storage[target] = {}
                storage[target]['R'] = 0
                storage[target]['E'], storage[target]['E_of_squares'] = 0, 0
        return storages


    def _update_beta(self):
        if self.max_reward_beta > 0:
            self.beta = min(1 - (1 / self.iter), self.max_reward_beta)
        else:
            self.beta = 1 - (1 / self.iter)


    def _update_mean(self, E, x):
        # E * (self.iter - 1) / self.iter + x / self.iter
        # E *= (1 - (1 / self.iter))
        # E += (x / self.iter)
        E *= self.beta
        E += x * (1 - self.beta)
        return E


    def _process_values(self):

        # tc.CMD_GET_LANEAREA_VARIABLE = 173
        segment_values = self._observe_values(self.point1, 173, self.list_data_to_collect_from_segment)

        # tc.CMD_GET_VEHICLE_VARIABLE = 164
        if self.enable_vehicle_values:
            vehicle_values = self._observe_values(self.point2, 164, self.list_data_to_collect_from_vehicle)

        for node_id in self.list_node_id:
            node, storage = self.nodes[node_id], self.storages[node_id]

            ## ===========================
            ## get state
            ## ===========================
            # movement : incoming, internal, outgoing
            # types : occupancy, speed
            dic_val = defaultdict(list)
            for movement, types in self.state_types.items():
                for typ in types:
                    for segment_id in node[movement]['segment_cluster']:
                        val = self._get_value_from_segment(segment_id, typ, segment_values)
                        dic_val[typ].append(val)

            for k, v in dic_val.items():
                value = np.array(v, dtype = np.float32)
                value = self._clip(value, self.dic_norm[k], self.dic_clip[k])  # 정규화
                storage[k] = value
            dic_val = None

            ## ===========================
            ## get values for reward
            ## ===========================
            # 리워드 구성요소 : 대기행렬, 대기시간, 급감속, 속도의 표준편차
            # 대기행렬은 차량사이의 간격(gap)도 포함하기 때문에 운행중인 차량의 총길이보다 길 수 있음

            # 후보요소
            #  - 신호변동(blinking or flickering)
            #  - 출발지연시간
            #  - 가속도 절대값(가/감속이 없을 수록 좋은거 거 아닌가?)

            # 대기시간은 진입로별 가장 큰 값만 활용
            # 지체시간은 대기행렬과 의미가 비슷함
            # 급감속은 4.5보다 큰 것만 활용? decelerations of more than 4.5m/s^2
            # 속도분산은 진입로별 속도들의 분산(혹은 표준편차)

            # 원활성 : 운행거리, 속도, 교통량
            # 혼잡성 : 대기행렬, 대기시간, 지체시간, 통행시간
            # 안정성 : 급감속, 속도분산, 신호변동(신호의 잦은 변동은 안정성을 떨어뜨림)
            # 공해 : 연료소비량, 소음, 배기가스

            # https://www.fransoliehoek.net/docs/VanDerPol16LICMAS.pdf

            # ['delays', 'jams', 'waits', 'decels', 'dev_speeds']
            for target in self.reward_scaling_target:
                storage[target]['r'] = 0
            storage['loss'] = 0
                
            for edge_id, list_segment_id in node['incoming']['edge_cluster'].items():

                delay, jam, max_wait, decel, list_speed = 0, 0, 0, 0, []
                for segment_id in list_segment_id:

                    # 대기행렬길이 : 멈춰있는 차량의 길이 + gap
                    # 지체시간으로 변경 가능
                    if 'jams' in self.reward_scaling_target:
                        jam += self._get_value_from_segment(segment_id, 'jam_length', segment_values)

                    # 차량리스트
                    if self.enable_vehicle_values:
                        for vehicle_id in segment_values[segment_id][18]:  # tc.LAST_STEP_VEHICLE_ID_LIST = 18

                            # 지체시간 (각 차량별 지체시간 : delay = 1 - speed / allowd_speed)
                            if 'delays' in self.reward_scaling_target:
                                spd = vehicle_values[vehicle_id][64]  # tc.VAR_SPEED = 64
                                allowed_spd = vehicle_values[vehicle_id][183]  # tc.VAR_ALLOWED_SPEED = 183
                                if not np.isnan(spd):
                                    delay += (1 - spd / allowed_spd)

                            # 급감속
                            if 'decels' in self.reward_scaling_target:
                                accel = vehicle_values[vehicle_id][114]  # tc.VAR_ACCELERATION = 114
                                if not np.isnan(accel):
                                    if accel <= self.deceleration_threshold:
                                        decel -= accel

                            # 대기시간
                            if 'waits' in self.reward_scaling_target:
                                wait = vehicle_values[vehicle_id][122]  # tc.VAR_WAITING_TIME = 122
                                if not np.isnan(wait):
                                    if wait > max_wait:
                                        max_wait = wait

                            # 속도편차 : deviation of speed
                            if 'dev_speeds' in self.reward_scaling_target:
                                spd = vehicle_values[vehicle_id][64]  # tc.VAR_SPEED = 64
                                if not np.isnan(spd):
                                    list_speed.append(spd)

                # 지체시간 합
                if delay:
                    storage['delays']['r'] += delay
                        
                # 대기행렬 합
                if jam:
                    storage['jams']['r'] += (jam ** self.penalty_jam)
                    storage['loss'] += jam

                # 대기시간 합
                if max_wait:
                    storage['waits']['r'] += max_wait

                # 급감속 합
                if decel:
                    storage['decels']['r'] += decel

                # 속도편차 합
                if list_speed:
                    storage['dev_speeds']['r'] += np.sqrt(np.var(list_speed) * len(list_speed))

            # 신호변동 : flickering
            # 황색이면 패널티를 부여함
            if (storage['steps_phase_lasted'] == 0) and ('flickering' in self.reward_scaling_target):
                storage['flickering']['r'] += 1

            ## ===========================
            ## PPO scaling optimization (https://arxiv.org/pdf/2005.12729.pdf)
            ## ===========================
            for target in self.reward_scaling_target:
                sub = storage[target]
                r, R = sub['r'], sub['R']
                E, E_of_squares = sub['E'], sub['E_of_squares']

                # R = R * GAMMA + r
                R = R * self.GAMMA + r

                # tracking E(x), E(x ** 2) for V(x)
                E = self._update_mean(E, R)
                E_of_squares = self._update_mean(E_of_squares, R ** 2)

                # V(x) = E(x ** 2) - E(x) ** 2
                V = E_of_squares - E ** 2

                # Std(x) = V(x) ** 0.5
                Std = np.sqrt(V) + 1e-20

                # scaling : r / np.std(RS)
                scaled = r / Std

                sub['R'] = R
                sub['E'], sub['E_of_squares'] = E, E_of_squares
                sub['scaled'] = scaled

                # if node_id == self.list_node_id[0]:
                #     print(f'{target} {self.iter} =========================')
                #     print('r:', round(r, 2), 'R:', round(R, 2), 'E:', round(E, 2), 'E_of_squares:', round(E_of_squares, 2), 'scaled:', round(scaled, 2))

            ## ===========================
            ## Reward Calculation
            ## ===========================
            storage['reward'] = 0
            for target in self.reward_scaling_target:
                storage['reward'] -= (storage[target]['scaled'] * self.reward_coefs[target])

    ## ==================================================
    ## 리워드 정규화 : PPO scaling optimization
    ## ==================================================
    '''
    R = 0
    RS = []
    while not done:
        s, r, ... = env.step(action)
        R = R * GAMMA + r
        RS.append(R)

        scaled_reward = r / np.std(RS)
    '''

    def _track_reward_values(self):
        if self.tracking_reward_values:
            for node_id in self.list_node_id:
                node, storage = self.nodes[node_id], self.storages[node_id]
                for target in self.reward_scaling_target:
                    storage[target]['scaled_cumulative'] += storage[target]['scaled']
                    storage[target]['org_cumulative'] += storage[target]['r']


    def save_reward_values(self):
        if self.tracking_reward_values:
            for node_id in self.list_node_id:
                node, storage = self.nodes[node_id], self.storages[node_id]
                sub = {'iter' : self.iter, 'node_id' : node_id}
                sub2 = {'iter' : self.iter, 'node_id' : node_id}
                for target in sorted(self.reward_scaling_target):
                    sub[target] = storage[target]['scaled_cumulative']
                    sub2[target] = storage[target]['org_cumulative']
                self.reward_values_scaled.append(sub)
                self.reward_values_org.append(sub2)
            path = os.path.join('save_weights', 'reward_values_scaled.csv')
            pd.DataFrame(self.reward_values_scaled).to_csv(path, index = False)
            path = os.path.join('save_weights', 'reward_values_org.csv')
            pd.DataFrame(self.reward_values_org).to_csv(path, index = False)


    def _check_reward_coefs(self):

        print(f'reward_coefs_for_values: {self.reward_coefs}')

        print('reward_coefs_for_neighbor')
        for node_id in self.list_node_id:
            node = self.nodes[node_id]
            print(f'{node_id} : {node["neighbors"]}')


    def _clear_state_values(self):
        for storage in self.storages.values():
            for typ in self.value_types:
                storage[typ] = None


    def _get_values_for_simulation(self):

        # 각 에이전트별로 데이터를 수집
        self.iter += 1
        self._update_beta()
        self._process_values()
        self._track_reward_values()
        
        states, masks, rewards = [], [], []
        total_loss = 0.0

        # 각 교차로 마다 집계
        for node_id in self.list_node_id:
            node, storage = self.nodes[node_id], self.storages[node_id]

            # state float
            wave, speed = [storage['wave']], [storage['speed']]

            # state int
            act, act_cnt = [[storage['prev_action']]], [[storage['steps_phase_lasted']]]

            # reward
            reward = storage['reward']
            sum_coef = 1
            total_loss -= storage['loss']

            # mask
            mask = storage['mask']

            # 이웃 교차로의 state와 reward도 같이 고려
            if self.neighbor_discount_factor > 0:

                for nnode_id, ndistance in node['neighbors'].items():
                    nnode, nstorage = self.nodes[nnode_id], self.storages[nnode_id]
                    coef_neighbor = 1 - ndistance / self.neighbor_discount_factor
                    
                    # state float
                    wave.append(nstorage['wave'] * coef_neighbor)
                    speed.append(nstorage['speed'] * coef_neighbor)

                    # state int
                    act.append([nstorage['prev_action']])
                    act_cnt.append([nstorage['steps_phase_lasted']])

                    # reward
                    reward += (nstorage['reward'] * coef_neighbor)
                    sum_coef += coef_neighbor

                    # # 중복집계되는 노드 보완 필요함
                    # for nnode_id2, ndistance2 in nnode['neighbors'].items():

                    #     if nnode_id2 in node['neighbors'] or nnode_id2 == node_id:
                    #         continue

                    #     nstorage2 = self.storages[nnode_id2]
                    #     coef_neighbor = 1 - (ndistance + ndistance2) / self.neighbor_discount_factor
                    #     reward += (nstorage2['reward'] * coef_neighbor)
                    #     sum_coef += coef_neighbor

            states.append(np.concatenate(wave + speed + act + act_cnt, dtype = np.float32))
            masks.append(mask)
            rewards.append(reward / sum_coef)
        
        self._clear_state_values()
        # print('rewards:', [round(r, 2) for r in rewards])
        # print('act:', act)
        # print('act_cnt:', act_cnt)
        wave, speed, act, act_cnt = None, None, None, None
        
        return states, masks, rewards, total_loss
        
        
    def _simulate(self, step_size):
        for _ in range(step_size):
            self.sim.simulationStep()
        self.cur_sec += step_size

    
    def reset(self, load_state = True):

        # 저장된 시뮬레이션 초기값 불러오기
        if load_state:
            self.sim.simulation.loadState(self.path_init_state)
        
        self.cur_sec = -1
        actions = []
        for node_id in self.list_node_id:
            node, storage = self.nodes[node_id], self.storages[node_id]
            storage['reward'] = 0
            storage['prev_action'] = node['critical_phase_index']
            storage['steps_phase_lasted'] = node['min_steps'][storage['prev_action']] - 1
            storage['red_state'] = None
            actions.append(node['critical_phase_index'])

            for target in self.reward_scaling_target:
                storage[target]['scaled_cumulative'] = 0
                storage[target]['org_cumulative'] = 0
                storage[target]['scaled'] = 0
                storage[target]['r'] = 0

        self._set_phase(actions, True)
        self._simulate(1)
        self._init_action_mask()
        
        states, masks, _, _ = self._get_values_for_simulation()
        return states, masks

    
    def _check_done(self):
        if self.check_expected_to_leave:
            return self.sim.simulation.getMinExpectedNumber() <= 0
        else:
            return self.cur_sec >= self.episode_sec
        
        
    def step(self, actions):
        
        self._set_phase(actions)
        self._simulate(self.step_size)
        
        states, masks, rewards, total_reward = self._get_values_for_simulation()
        done = self._check_done()
        
        return states, masks, rewards, total_reward, done
    
    
    def _set_phase(self, actions, initialization = False):
        for node_id, action in zip(self.list_node_id, list(actions)):

            # 입력할 신호 셋팅
            signal_state = self._set_node_phase(int(action), node_id)

            # 적용여부 판단
            if (self.storages[node_id]['steps_phase_lasted'] <= 2) or initialization:
                self.sim.trafficlight.setRedYellowGreenState(node_id, signal_state)
            
            
    def _set_node_phase(self, action, node_id):

        ## 준비
        node, storage = self.nodes[node_id], self.storages[node_id]
        new_state = node['signal_states'][action]
        prev_action = storage['prev_action']
        storage['prev_action'] = action

        ## action이 직전과 같다면
        if action == prev_action:

            storage['steps_phase_lasted'] += 1
            self._check_action_mask(action, node, storage)

            red_state = storage['red_state']
            storage['red_state'] = None

            return new_state if red_state is None else red_state

        ## action이 직전과 다르면 현시변경 준비
        # 황색이 없으면 바로 바뀌고, 황색이 있으면 황색시간 후 현시가 바뀜
        storage['steps_phase_lasted'] = 0
        prev_state = node['signal_states'][prev_action]
        mid_state, has_y = tl.get_yellow_signal_state(prev_state, new_state)

        # 전적색 확인
        red_duration = node['red_durations'][action]
        if red_duration > 0:
            storage['red_state'], _ = tl.get_passing_signal_state(prev_state, new_state, signal_type='red')

        # 황색이 없다면 count 1 증가
        if not has_y:
            storage['steps_phase_lasted'] += 1
            red_state = storage['red_state']
            storage['red_state'] = None
            mid_state = mid_state if red_state is None else red_state

        self._check_action_mask(action, node, storage)

        return mid_state


    def _check_action_mask(self, action, node, storage):

        # https://numpy.org/doc/stable/reference/generated/numpy.zeros.html
        # https://numpy.org/doc/stable/reference/generated/numpy.ones.html
        # https://www.tensorflow.org/api_docs/python/tf/keras/layers/Softmax

        # 처음엔 모든 행동을 드랍하는 마스크 정의
        mask = np.zeros(node['action_dim'], dtype=np.int32)

        # 최소녹색시간보다 작을 때
        if storage['steps_phase_lasted'] < node['min_steps'][action]:
            mask[action] = 1

        # 최소녹색시간보다 크면서 최대녹색시간보다 작을 때
        elif storage['steps_phase_lasted'] < node['max_steps'][action]:
            mask[action] = 1
            for valid_action in node['phase_combinations'][action]:
                mask[valid_action] = 1

        # 최대녹색시간 이상
        else:
            for valid_action in node['phase_combinations'][action]:
                mask[valid_action] = 1

        storage['mask'] = (mask == 1)


    # 최초 액션 마스크
    def _init_action_mask(self):
        for node_id in self.list_node_id:
            node, storage = self.nodes[node_id], self.storages[node_id]
            mask = np.ones(node['action_dim'], dtype=np.int32)
            storage['mask'] = (mask == 1)
