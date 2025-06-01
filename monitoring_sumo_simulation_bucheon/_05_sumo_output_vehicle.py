import libsumo as traci
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from sumolib import checkBinary
import datetime
import pandas as pd
from numpy import nan as NA

from _00_DB_connecter_ver2 import Load_data, Insert_data
from _00_util_ import util_
from _06_total_output import TotalOutput
import _99_Contents as contents


class Vehicle:
    def __init__(self, sim):
        self.ID = Insert_data()
        self.util = util_()
        self.total = TotalOutput()
        self.ui_time = self.util.unix_verUI
        self.vtype_dic = {'passenger': 1, 'bus': 2, 'truck': 3, 'trailer': 4, 'emergency': 5, 'motorcycle': 6}

        self.sim = sim
        self.veh_val_list = []

        self.read_require_net()
        # self.ds = self.segment_agg.groupby('seg_len', as_index=False).count().max()
        self.ds = 5
        self.stdr_speed = 0.1

    def read_require_net(self):
        # query = '''select * from anyang_network_agg.tb_segment_agg ;'''
        LD = Load_data()
        self.segment_agg = LD.load_net_data('tb_segment_agg')
        self.full_lane_table = LD.load_net_data('tb_lane')
        # self.segment_agg = self.segment_agg.merge(self.segment_agg[['lane_id','seg_len']].groupby('lane_id', as_index=False).sum().rename(columns = {'seg_len':'lane_len'}), on='lane_id', how='left')
        # del LD

    def hex2decimal(self, hex_val_list):
        return [int(hex_val, 16) for hex_val in hex_val_list]

    def vehicle_subscribe(self):
        for veh_id in self.sim.simulation.getDepartedIDList():
            # self.sim.vehicle.subscribe(veh_id, self.hex2decimal([
            #     '0x4f'  # type
            #     , '0x40'  # speed
            #     , '0xb7'  # allowed_speed
            #     , '0x43'  # angle
            #     , '0x39'  # position3d
            #     , '0x56'  # laneposition
            #     , '0x51'  # lane_id
            #     , '0x36'  # slope
            #     , '0x5b'  # signals
            #     , '0x70'  # next_tls
            # ]))
            self.sim.vehicle.subscribe(veh_id, self.hex2decimal([
                '0x36'  # slope
                , '0x39'  # position3d
                , '0x40'  # speed
                , '0x43'  # angle
                , '0x4f'  # type
                , '0x51'  # lane_id
                , '0x56'  # laneposition
                , '0x5b'  # signals
                , '0x70'  # next_tls
                , '0xb7'  # allowed_speed
            ]))

    ## 매초의 sim.simulationStep() 할때 함께 실행
    def vehicle_subscribe_results(self, cur_time):
        self.vehicle_subscribe()
        subscribe = {cur_time: self.sim.vehicle.getAllSubscriptionResults()}

        for time, veh in subscribe.items():
            for veh_id, val in veh.items():
                # v_type, v_speed, v_allowed_speed, v_angle, v_position3d, v_laneposition, v_lane_id, v_slope, v_signals, v_next_tls = list(
                #     val.values())
                # v_slope, v_position3d, v_speed, v_angle, v_type, v_lane_id, v_laneposition, v_signals, v_allowed_speed = list(
                #     val.values())
                v_slope = val[54] if 54 in val.keys() else NA
                v_position3d = val[57] if 57 in val.keys() else ()
                v_speed = val[64] if 64 in val.keys() else NA
                v_angle = val[67] if 67 in val.keys() else NA
                v_type = val[79] if 79 in val.keys() else NA
                v_lane_id = val[81] if 81 in val.keys() else NA
                v_laneposition = val[86] if 86 in val.keys() else NA
                v_signals = val[91] if 91 in val.keys() else NA
                v_next_tls = val[112] if 112 in val.keys() else []
                v_allowed_speed = val[183] if 183 in val.keys() else NA

                v_type = self.vtype_dic[v_type]

                v_lon, v_lat, v_alt = (0, 0, 0)
                if len(v_position3d) > 2:
                    v_lon, v_lat, v_alt = v_position3d
                else:
                    v_lon, v_lat = v_position3d
                v_lon, v_lat = self.sim.simulation.convertGeo(v_lon, v_lat)
                if len(v_next_tls) != 0:
                    v_next_inter, v_tls_index, v_dist2next_inter, v_signal_state = v_next_tls[0]
                else:
                    v_next_inter = v_tls_index = v_dist2next_inter = v_signal_state = NA
                    # v_next_inter = v_tls_index = v_dist2next_inter = v_signal_state = ''
                self.veh_val_list.append(
                    [time, veh_id, v_type, v_speed, v_allowed_speed, v_angle, v_lon, v_lat, v_alt, v_laneposition,
                     v_lane_id, v_slope, v_signals, v_next_inter, v_tls_index, v_dist2next_inter, v_signal_state])

    def find_dir(self, intersection_id, tls_index):  # v_next_inter, v_tls_index
        inter_lane = self.sim.trafficlight.getControlledLinks(intersection_id)[int(tls_index)][0][2]
        direction = self.sim.lane.getLinks(inter_lane)[0][6]
        #         return self.sim.lane.getLinks(inter_lane), self.sim.lane.getLinks(inter_lane)[0], direction
        return direction

    def get_vehicle_results_in_traci(self):
        # 초당 수집된 데이터 1차 정리
        self.vehicle_values = pd.DataFrame(self.veh_val_list,
                                           columns=['time', 'vehicle_id', 'vtype', 'speed', 'allowed_speed', 'angle',
                                                    'x', 'y', 'z', 'pos', 'lane_id', 'slope', 'signals',
                                                    'next_intersection', 'tls_index', 'dist2next_intersection',
                                                    'signal_state'])

        if self.ui_time != 0:
            self.vehicle_values['time'] = int(self.vehicle_values['time'] + self.ui_time)

        # 방향값 생성
        direction_table = self.vehicle_values[['next_intersection', 'tls_index']].drop_duplicates()
        direction_table['direction'] = [self.find_dir(direction_table.loc[i, 'next_intersection'],
                                                      int(direction_table.loc[i, 'tls_index'])) if not pd.isna(
            direction_table.loc[i, 'tls_index']) else NA for i in direction_table.index]

        # 방향값 조인
        self.vehicle_values = self.vehicle_values.merge(direction_table, on=['next_intersection', 'tls_index'],
                                                        how='left')

        # 지체시간
        self.vehicle_values['time_loss'] = 1 - (self.vehicle_values['speed'] / self.vehicle_values['allowed_speed'])
        return self.vehicle_values

    def time_col(self):
        unixtime_list = []
        for time in self.vehicle_values['time'].unique():
            stdr_time = self.util.unixtime2time(time)
            ymd = stdr_time.strftime('%Y%m%d')
            hm = stdr_time.strftime('%H%M')
            sec = stdr_time.strftime('%S')

            # unixtime_list.append([time, stdr_time, ymd, hm, sec])
            unixtime_list.append([time, ymd, hm, sec])

        else:
            time_table = pd.DataFrame(unixtime_list, columns=['time', 'ymd', 'hm', 'sec'])
            self.vehicle_values = self.vehicle_values.merge(time_table, on='time', how='left')

    def get_vehicle_results(self, unix_begin, unix_end, insert_db=False):
        self.get_vehicle_results_in_traci()

        # segment_agg테이블과 vehicle_values테이블 join
        self.vehicle_values = pd.merge(self.vehicle_values, self.full_lane_table[['lane_id', 'lane_len']], on='lane_id',
                                       how='left')
        self.vehicle_values['pos2'] = self.vehicle_values['lane_len'] - self.vehicle_values['pos']
        self.vehicle_values['seg_no2'] = self.vehicle_values['pos2'] // self.ds

        self.vehicle_values = pd.merge(self.vehicle_values, self.segment_agg[
            ['edge_grp_id', 'from_node_id', 'to_node_id', 'edge_id', 'lane_id', 'seg_no2', 'seg_id', 'dist2to_inter']],
                                       on=['lane_id', 'seg_no2'], how='left')

        self.time_col()

        self.vehicle_values['direction2'] = [
            self.vehicle_values.loc[i, 'direction'] if self.vehicle_values.loc[i, 'next_intersection'] ==
                                                       self.vehicle_values.loc[i, 'to_node_id'] else NA for i in
            range(self.vehicle_values.shape[0])]
        self.vehicle_values['tls_index2'] = [
            self.vehicle_values.loc[i, 'tls_index'] if self.vehicle_values.loc[i, 'next_intersection'] ==
                                                       self.vehicle_values.loc[i, 'to_node_id'] else NA for i in
            range(self.vehicle_values.shape[0])]

        self.vehicle_values['dist_start_200'] = self.vehicle_values['dist2to_inter'] <= 200
        self.vehicle_values['dist_200_500'] = (self.vehicle_values['dist2to_inter'] > 200) & (
                self.vehicle_values['dist2to_inter'] <= 500)
        self.vehicle_values['dist_500_end'] = self.vehicle_values['dist2to_inter'] > 500

        self.vehicle_values['queue_all'] = (self.vehicle_values['speed'] < self.stdr_speed).astype(int)
        self.vehicle_values['queue_200'] = [
            self.vehicle_values.loc[i, 'queue_all'] if self.vehicle_values.loc[i, 'dist_start_200'] else 2 for i in
            range(self.vehicle_values.shape[0])]
        self.vehicle_values['queue_200_500'] = [
            self.vehicle_values.loc[i, 'queue_all'] if self.vehicle_values.loc[i, 'dist_200_500'] else 2 for i in
            range(self.vehicle_values.shape[0])]
        self.vehicle_values['queue_500'] = [
            self.vehicle_values.loc[i, 'queue_all'] if self.vehicle_values.loc[i, 'dist_500_end'] else 2 for i in
            range(self.vehicle_values.shape[0])]

        self.change_colnames()

        self.vehicle_values['nxt_inter_phs_no'] = [str(i).split('.')[0] for i in
                                                   self.vehicle_values['nxt_inter_phs_no'].fillna('')]

        file_path = self.util.createFolder(os.getcwd() + '/vehicle_history')
        str_begin = self.util.time2str(self.util.unixtime2time(unix_begin))

        self.vehicle_values.to_csv(os.path.join(file_path, f'{contents.net_name}{str_begin}tra.csv'), index=False)

        self.total.save_od_data(f'{contents.net_name}{str_begin}tra.csv')
        self.total.save_summarize(f'{contents.net_name}{str_begin}tra.csv')
        if insert_db:
            self.insert_db(self.vehicle_values, table_nm='tb_vehicle_history_hist', schema_nm=self.ID.schema_nm)
            self.insert_ms_db(unix_begin, unix_end, self.vehicle_values, table_nm='tb_vehicle_history',
                              schema_nm=self.ID.schema_nm)

        return self.vehicle_values

    def change_colnames(self):
        self.vehicle_values = self.vehicle_values[['time', 'vehicle_id',
                                                   'ymd', 'hm', 'sec',
                                                   'x', 'y', 'z',
                                                   'speed', 'allowed_speed',
                                                   'angle', 'slope', 'vtype',
                                                   'from_node_id',
                                                   'to_node_id', 'dist2to_inter', 'direction2',
                                                   'edge_grp_id', 'edge_id', 'lane_id', 'seg_id',
                                                   'next_intersection', 'dist2next_intersection', 'direction',
                                                   'signal_state', 'tls_index',
                                                   'queue_all', 'queue_200', 'queue_200_500', 'queue_500', 'time_loss'
                                                   ]].rename(columns={
            'time': 'unix_time'
            # ,'stdr_time':'stdr_dt'
            , 'ymd': 'stdr_ymd'
            , 'hm': 'stdr_hm'
            , 'sec': 'stdr_ss'
            , 'vehicle_id': 'vhcl_id'
            , 'x': 'lon'
            , 'y': 'lat'
            , 'z': 'alt'
            , 'speed': 'spd'
            , 'allowed_speed': 'allowed_spd'
            , 'angle': 'agl'
            , 'slope': 'slp'
            , 'vtype': 'vhcl_typ'
            , 'edge_grp_id': 'edge_grp_id'
            , 'from_node_id': 'from_inter_id'
            , 'to_node_id': 'to_inter_id'
            , 'edge_id': 'edge_id'
            , 'lane_id': 'lane_id'
            , 'seg_id': 'seg_id'
            , 'dist2to_inter': 'dist2to_inter'
            # ,'tls_index2':'to_inter_phs_no'
            , 'next_intersection': 'nxt_inter_id'
            , 'direction': 'turn_typ2nxt_inter'
            , 'direction2': 'turn_typ2to_inter'
            , 'dist2next_intersection': 'dist2nxt_inter'
            , 'signal_state': 'nxt_inter_sig_st'
            , 'tls_index': 'nxt_inter_phs_no'
            , 'queue_all': 'que_all'
            , 'queue_200': 'que_200'
            , 'queue_200_500': 'que_200_500'
            , 'queue_500': 'que_500'
            , 'time_loss': 'tl'
        }) \
            .astype({'unix_time': 'int'
                     # ,'vhcl_id':'str'
                     # ,'stdr_ymd':'str'
                     # ,'stdr_hm':'str'
                     # ,'stdr_ss':'str'
                     # ,'lon':'float'
                     # ,'lat':'float'
                     # ,'alt':'float'
                     # ,'spd':'float'
                     # ,'allowed_spd':'float'
                     # ,'agl':'float'
                     # ,'slp':'float'
                     # ,'vhcl_typ':'int64'
                     # ,'from_inter_id':'str'
                     # ,'to_inter_id':'str'
                     # ,'dist2to_inter':'float'
                     # ,'turn_typ2to_inter':'str'
                     # ,'edge_grp_id':'str'
                     # ,'edge_id':'str'
                     # ,'lane_id':'str'
                     # ,'seg_id':'str'
                     # ,'nxt_inter_id':'str'
                     # ,'dist2nxt_inter':'float'
                     # ,'turn_typ2nxt_inter':'str'
                     # # ,'to_inter_phs_no':'int'
                     # ,'nxt_inter_sig_st':'str'
                     # ,'nxt_inter_phs_no':'int'
                     # ,'que_all':'int'
                     # ,'que_200':'int'
                     # ,'que_200_500':'int'
                     # ,'que_500':'int'
                     # ,'tl':'float'
                     })

    def insert_db(self, data, table_nm='tb_vehicle_history_hist', schema_nm=contents.schema_nm):
        ID = Insert_data()

        ### 같은 시간의 데이터가 있는 경우를 대비하여 기존데이터 delete 후 insert

        condition = f''' unix_time >= {data.unix_time.min()} and unix_time <= {data.unix_time.max()} '''
        ID.delete_data(condition, table_nm=table_nm, schema_nm=schema_nm)
        # ID.insert_db(data, table_nm = table_nm, schema_nm = schema_nm)
        ID.insert_bulk(data, table_nm=table_nm, schema_nm=schema_nm)

    def insert_ms_db(self, unix_begin, unix_end, data, table_nm='tb_vehicle_history', schema_nm=contents.schema_nm):
        ID = Insert_data()
        LD = Load_data()

        # unix_begin = unix_begin - datetime.timedelta(seconds = LD.interval_sec*2)
        # unix_end = unix_end - datetime.timedelta(seconds = LD.interval_sec*2)

        # print(unix_end - LD.interval_sec*2)
        unix_end = unix_end - LD.interval_sec * 2

        self.insert_db(data, table_nm=table_nm, schema_nm=schema_nm)

        ### 마스터 테이블은 최근 10분동안의 데이터만 가지고 있어야함
        condition = f''' unix_time <= {unix_end} '''
        ID.delete_data(condition, table_nm=table_nm, schema_nm=schema_nm)


#     # test를 위해 임의로 정의한 함수
#     def simulation_step(self, unix_begin, unix_end):
#         round_num = 1
#         unix_begin = int(unix_begin)
#         unix_end = int(unix_end)

#         while unix_begin <= unix_end:
#             self.sim.simulationStep()
#             self.vehicle_subscribe_results(unix_begin)

#             unix_begin = unix_begin + 1
#             unix_begin = round(unix_begin, 0)
#         else:
#             vehicle_values = self.get_vehicle_results()
#             return vehicle_values

#     def sumo_simulation(sumo_cmd, unix_begin, unix_end, write_file = True):
#         traci.start(sumo_cmd)
#         self.vehicle_values = veh.simulation_step(unix_begin, unix_end)
#         traci.close()

#         if write_file:
#             self.vehicle_values.to_csv('./test.csv')
#         return self.vehicle_values


# # test를 위해 임의로 정의한 함수
def set_sumo(gui=True):
    net_path = './sumo_net'
    net_file = 'anyang_20220119_ver03.net.xml'
    rou_path = './sumo_net'

    if 'SUMO_HOME' in os.environ:
        tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
        sys.path.append(tools)
    else:
        sys.exit("please declare environment variable 'SUMO_HOME'")

    sumo_binary = checkBinary('sumo-gui') if gui else checkBinary('sumo')

    sumo_cmd = [
        sumo_binary,
        "-n", f"{net_path}/{net_file}",
        "-r", ','.join([f'{rou_path}/{file}' for file in os.listdir(rou_path) if
                        (file.split('.')[-1] == 'xml') and (file.find('alt') == -1)]),
        "-b", unix_begin
    ]
    return sumo_cmd


if __name__ == "__main__":
    veh = Vehicle(traci)

    #####
    # begin = "20211001070000"
    # end = "20211001080000"
    begin = "20220202000000"
    end = "20220202001500"

    unix_begin = veh.util.str2unixtime(begin)
    unix_end = int(unix_begin) + 900
    #####

    traci.start(set_sumo())

    vehicle_values = veh.simulation_step(unix_begin, unix_end)

    traci.close()

    # vehicle_values.to_csv('./test.csv')
