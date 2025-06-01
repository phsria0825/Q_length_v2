from xml.etree.ElementTree import Element, ElementTree, SubElement

import pandas as pd
import datetime
import os
import numpy as np
from numpy import nan as NA
import _99_Contents as contents
# from sqlalchemy import create_engine


from _00_util_ import util_
from _00_DB_connecter_ver2 import Load_data, Insert_data


########## 데이터 불러오고, sumo에 넣기 위한 형식으로 변경 (+황색신호 추가)
class Trans_signal(Load_data):
    def __init__(self):
        Load_data.__init__(self)
        self.util = util_()

    # add_yellow_phases 함수에서 황색시간 return
    def find_yellowtime(self, aringstarttime, yellowtime, option='mius'):
        if yellowtime in ['', NA, None]:
            yellowtime = 3

        if option == 'mius':
            if type(aringstarttime) is pd.Timestamp:
                return aringstarttime - pd.DateOffset(seconds=int(yellowtime))
            elif type(aringstarttime) is datetime.datetime:
                return aringstarttime - datetime.timedelta(seconds=int(yellowtime))
            elif type(aringstarttime) is str:
                aringstarttime = aringstarttime.split('.')[0]
                aringstarttime = datetime.datetime.strptime(str(aringstarttime), "%Y-%m-%d %H:%M:%S")
                return aringstarttime - datetime.timedelta(seconds=int(yellowtime))
            else:
                return NA
        if option == 'plus':
            if type(aringstarttime) is pd.Timestamp:
                return aringstarttime + pd.DateOffset(seconds=int(yellowtime))
            elif type(aringstarttime) is datetime.datetime:
                return aringstarttime + datetime.timedelta(seconds=int(yellowtime))
            elif type(aringstarttime) is str:
                aringstarttime = datetime.datetime.strptime(str(aringstarttime), "%Y-%m-%d %H:%M:%S")
                return aringstarttime + datetime.timedelta(seconds=int(yellowtime))
            else:
                return NA

    # add_yellow_phases 함수에서 현재와 다음현시를 비교하여 변하는 현시를 황색불로
    def find_change_phases(self, cur_phase, next_phase):
        switch_reds = []
        switch_greens = []
        for i, (p0, p1) in enumerate(zip(cur_phase, next_phase)):
            if (p0 in 'Gg') and (p1 == 'r'):
                switch_reds.append(i)
            elif (p0 in 'r') and (p1 in 'Gg'):
                switch_greens.append(i)
        yellow_phase = list(cur_phase)
        for i in switch_reds:
            yellow_phase[i] = 'y'
        for i in switch_greens:
            yellow_phase[i] = 'r'
        return ''.join(yellow_phase)

    def add_yellow_phases(self, data, intersection_id):
        inter_data = data[data['interid'] == intersection_id].reset_index(drop=True)

        yellow_phase_list = []
        yellow_time_list = []

        for i in range(1, inter_data.shape[0]):
            cur_phase = inter_data.loc[i - 1, 'signalstate']
            next_phase = inter_data.loc[i, 'signalstate']

            aringstarttime = inter_data.loc[i, 'aringstarttime']
            yellowtime = inter_data.loc[i - 1, 'yellowtime']

            yellow_phase = self.find_change_phases(cur_phase, next_phase)
            yellowtime = self.find_yellowtime(aringstarttime, yellowtime)

            yellow_phase_list.append(yellow_phase)
            yellow_time_list.append(yellowtime)

        yellow_df = pd.DataFrame({'interid': intersection_id
                                     , 'aringstarttime': yellow_time_list
                                     , 'signalstate': yellow_phase_list
                                  })
        return yellow_df

    # def add_yellow_phases_df(self, data, yellowtime_type = 'end'):
    #     ## 시작시간 전에 황색시간 부여 방식일 경우, 녹색시간 시간을 뒤로 미루는 코드
    #     if yellowtime_type.lower() == 'front':
    #         data.aringstarttime = [self.find_yellowtime(data.loc[i,'aringstarttime'], data.loc[i,'yellowtime'], 'plus') for i in range(data.shape[0]) if data.loc[i,'aringstarttime'] == begin ]
    #     temp_data = data[['interid','aringstarttime','signalstate']]

    #     ### 황색시간 추가하는 코드
    #     for inter_id in list(data.interid.drop_duplicates()):
    #         temp_data = temp_data.append(self.add_yellow_phases(data, inter_id))
    #     else:
    #         # temp_data = temp_data[temp_data['']]
    #         temp_data = temp_data.reset_index(drop=True)
    #     return temp_data

    ## 최종 데이터 만드는 함수
    # 황색신호에 대한 데이터 만들어줘야함 -> add_yellow_phases 함수
    def sm2signal_state(self, begin, end, write_file=True, yellowtime_type='end'):
        begin = self.util.str2time(begin)
        end = self.util.str2time(end)

        # before begin : 시작시간 이전에 변경된 신호 데이터를 받아오는 역할
        b_b = begin - datetime.timedelta(seconds=self.interval_sec)
        b_e = begin

        # after end : 종료시간 이전에 황색시간으로 변경되어야할 사항 반영하기 위한 역할
        a_b = end
        a_e = end + datetime.timedelta(seconds=self.interval_sec)

        signal_data = self.load_signal_data(b_b, a_e)

        phase_table = self.load_phase_table()

        b_phase_data = self.load_phase_data(b_b, b_e)

        b_phase_data = b_phase_data.merge(signal_data[['cycleseq', 'intersectionseq']], how='left',
                                          on='cycleseq').dropna()
        b_phase_data = b_phase_data.sort_values('aringstarttime').groupby('intersectionseq').tail(1).astype(
            {'intersectionseq': 'int'})

        p_phase_data = self.load_phase_data(begin, end)

        p_phase_data = p_phase_data.merge(signal_data[['cycleseq', 'intersectionseq']], how='left',
                                          on='cycleseq')#.dropna().astype({'intersectionseq': 'int'})

        # e_phase_data = self.load_phase_data(a_b, a_e)
        # e_phase_data = e_phase_data.merge(signal_data[['cycleseq','intersectionseq']], how='left', on='cycleseq').dropna()
        # e_phase_data = e_phase_data.sort_values('aringstarttime').groupby('intersectionseq').head(1).astype({'intersectionseq':'int'})

        # sumo_signal_state = pd.concat([b_phase_data, p_phase_data, e_phase_data], ignore_index=True).astype({'intersectionseq':int}).drop_duplicates()
        sumo_signal_state = pd.concat([b_phase_data, p_phase_data], ignore_index=True).astype(
            {'intersectionseq': int}).drop_duplicates()

        sumo_signal_state = \
            sumo_signal_state.merge(phase_table, how='left', on=['intersectionseq', 'phasepattern']).dropna(
                subset=['interid', 'intersectionseq', 'signalstate', 'phasepattern'])[
                ['interid', 'aringstarttime', 'signalstate', 'yellowtime']].reset_index(drop=True)
        sumo_signal_state_2 = sumo_signal_state[['interid', 'aringstarttime', 'signalstate']]
        sumo_signal_state_2.loc[:, 'aringstarttime'] = [self.util.str2time(i) for i in
                                                        sumo_signal_state_2.loc[:, 'aringstarttime']]

        ## 시작시간 전에 황색시간 부여 방식일 경우, 녹색시간 시간을 뒤로 미루는 코드
        if yellowtime_type.lower() == 'front':
            sumo_signal_state_2['aringstarttime'] = [self.find_yellowtime(sumo_signal_state_2.loc[i, 'aringstarttime'],
                                                                          sumo_signal_state.loc[i, 'yellowtime'],
                                                                          'plus')
                                                     for i in range(sumo_signal_state.shape[0])]
            # self.sumo_signal_state_fin = sumo_signal_state[['interid','aringstarttime','signalstate']]

        ### 황색시간 추가하는 코드
        for inter_id in list(sumo_signal_state.interid.drop_duplicates()):
            sumo_signal_state_2 = pd.concat([sumo_signal_state_2, self.add_yellow_phases(sumo_signal_state, inter_id)],
                                            axis=0, ignore_index=True)
        else:
            ## 시작시간(begin)의 신호 데이터 정의
            b_sumo_signal_state = sumo_signal_state_2[sumo_signal_state_2['aringstarttime'] <= begin].groupby(
                'interid').tail(1)
            b_sumo_signal_state['aringstarttime'] = begin

            ## 시뮬레이션 기간 ~ 종료시간(end) 의 신호 데이터 정의
            a_sumo_signal_state = sumo_signal_state_2[
                (sumo_signal_state_2['aringstarttime'] > begin) & (sumo_signal_state_2['aringstarttime'] <= end)]

            self.sumo_signal_state_fin = pd.concat([b_sumo_signal_state, a_sumo_signal_state],
                                                   ignore_index=True).drop_duplicates()
            self.sumo_signal_state_fin = self.sumo_signal_state_fin.reset_index(drop=True)

        self.sumo_signal_state_fin['unix_time'] = [int(self.util.str2unixtime(i)) for i in
                                                   self.sumo_signal_state_fin.aringstarttime]

        #sorted_data = self.sumo_signal_state_fin.sort_values(by=['interid', 'unix_time']).reset_index(drop=True)

        self.sumo_signal_state_fin['phasepattern'] = ''

        if write_file:
            file_path = self.util.createFolder(os.getcwd() + '/sm2signal/csv')

            str_begin = self.util.time2str(begin)
            str_end = self.util.time2str(end)

            self.sumo_signal_state_fin.to_csv(os.path.join(file_path, f'{contents.net_name}{str_begin}s.csv'), index=False)
            self.csv_to_xml(self.sumo_signal_state_fin, str_begin, str_end)

        return self.sumo_signal_state_fin

    def csv_to_xml(self, org_data, begin_str, end_str):
        global tree
        data = org_data.sort_values(by=['interid', 'unix_time']).reset_index(drop=True)

        file_path = self.util.createFolder(os.getcwd() + '/sm2signal/xml')
        fileName = contents.net_name + begin_str + "s.xml"

        interid = data['interid'].drop_duplicates()
        root = list()
        root = Element("tlLogics")

        for i in range(len(interid.index)):
            element1 = Element("tlLogic")
            element1.set("offset", "0")
            element1.set("programID", str(i))
            element1.set("type", "static")
            element1.set("id", str(interid.iloc[i]))
            root.append(element1)

            for j in range(len(data.index)-1):
                if data['interid'].iloc[j] == interid.iloc[i]:
                    if data['interid'].iloc[j] == data['interid'].iloc[j+1]:
                        sub_element1 = SubElement(element1, "phase")
                        sub_element1.set("duration", str(data['unix_time'].iloc[j + 1] - data['unix_time'].iloc[j]))
                        sub_element1.set("state", str(data['signalstate'].iloc[j]))
                    else:
                        sub_element1 = SubElement(element1, "phase")
                        sub_element1.set("duration", str(int(self.util.str2unixtime(end_str) - data['unix_time'].iloc[j])))
                        sub_element1.set("state", str(data['signalstate'].iloc[j]))

            if interid.iloc[i] == data['interid'].iloc[len(data.index)-1]:
                sub_element1 = SubElement(element1, "phase")
                sub_element1.set("duration", str(int(self.util.str2unixtime(end_str) - data['unix_time'].iloc[len(data.index)-1])))
                sub_element1.set("state", str(data['signalstate'].iloc[len(data.index)-1]))

        self.util.indent(root, 0)

        tree = ElementTree(root)
    
        with open(file_path + "/" + fileName, "wb") as file:
            tree.write(file, encoding='utf-8', xml_declaration=True)


    def signal_state2phase_hist(self, begin, end, write_file=True):
        con_lane = self.load_net_data('tb_connect_lane')[
            ['node_id', 'con_no', 'con_lane_id', 'from_edge_grp_id', 'to_edge_grp_id']].rename(
            columns={'con_no': 'sig_sts_no'}).astype({'sig_sts_no': int})
        # con_lane = self.load_net_data('tb_connect_lane')[['node_id','con_no', 'con_lane_id']].rename(columns = {'con_no' : 'sig_sts_no'})

        phase_hist_list = []
        for i in range(self.sumo_signal_state_fin.shape[0]):
            unix_time = self.sumo_signal_state_fin.loc[i, 'unix_time']
            node_id = self.sumo_signal_state_fin.loc[i, 'interid']
            # count = 0
            # print(self.sumo_signal_state_fin.loc[i, 'signalstate'])
            for idx, sig_st in enumerate(self.sumo_signal_state_fin.loc[i, 'signalstate']):
                phase_hist_list.append([unix_time, node_id, idx, sig_st])
        else:
            self.phase_hist = pd.DataFrame(phase_hist_list, columns=['unix_time', 'node_id', 'sig_sts_no', 'sig_st'])
            self.phase_hist = self.phase_hist.merge(con_lane, on=['node_id', 'sig_sts_no'], how='left')
            self.phase_hist = self.phase_hist[
                ['con_lane_id', 'unix_time', 'sig_st', 'node_id', 'from_edge_grp_id', 'to_edge_grp_id']].rename(
                columns={'node_id': 'inter_id'})
            self.phase_hist = self.phase_hist[
                ['con_lane_id', 'unix_time', 'sig_st', 'inter_id', 'from_edge_grp_id', 'to_edge_grp_id']]

        if write_file:
            file_path = self.util.createFolder(os.getcwd() + '/sm2signal/hist')

            str_begin = self.util.time2str(begin)
            str_end = self.util.time2str(end)

            self.phase_hist.to_csv(os.path.join(file_path, f'{contents.net_name}{str_begin}s_hist.csv'), index=False)

    def sumo_signal_insert_db(self, data, begin, end, table_nm='sumo_signaldata_hist', schema_nm=contents.schema_nm):
        self.ID = Insert_data()

        unix_begin = self.sumo_signal_state_fin.unix_time.min()
        unix_end = self.sumo_signal_state_fin.unix_time.max()

        # print()
        condition = f'''"unix_time" >= {unix_begin} and "unix_time" <= {unix_end} '''

        # self.sumo_phase_hist_insert_db(begin, end)

        self.ID.delete_data(condition, schema_nm=schema_nm, table_nm=table_nm)
        self.ID.insert_bulk(data, schema_nm=schema_nm, table_nm=table_nm)

        print(f'insert {table_nm} Done.')

    def sumo_signal_insert_ms_db(self, data, begin, end, table_nm='sumo_signaldata', schema_nm=contents.schema_nm):
        unix_begin = self.sumo_signal_state_fin.unix_time.min()
        unix_end = self.sumo_signal_state_fin.unix_time.max()

        condition = f'''"unix_time" >= {unix_begin} and "unix_time" <= {unix_end} '''

        # self.sumo_phase_hist_insert_db(begin, end)

        unix_end = self.phase_hist.unix_time.max()
        unix_end = unix_end - self.interval_sec * 2

        self.ID.delete_data(condition, schema_nm=schema_nm, table_nm=table_nm)
        self.ID.insert_bulk(data, schema_nm=schema_nm, table_nm=table_nm)
        # self.ID.insert_bulk(data, table_nm=table_nm, schema_nm=schema_nm)

        ### 마스터 테이블은 최근 10분동안의 데이터만 가지고 있어야함
        condition = f''' unix_time <= {unix_end} '''
        self.ID.delete_data(condition, table_nm=table_nm, schema_nm=schema_nm)

        print(f'insert {table_nm} Done.')

    def sumo_signal_data2DB(self, begin, end, insert_DB=False):
        self.sm2signal_state(begin, end)
        self.signal_state2phase_hist(begin, end)

        if insert_DB:
            self.sumo_signal_insert_db(self.sumo_signal_state_fin, begin, end, table_nm='sumo_signaldata_hist',
                                       schema_nm=contents.schema_nm)
            self.sumo_signal_insert_db(self.phase_hist, begin, end, table_nm='tb_phase_history_hist',
                                       schema_nm=contents.schema_nm)

            self.sumo_signal_insert_ms_db(self.sumo_signal_state_fin, begin, end, table_nm='sumo_signaldata',
                                          schema_nm=contents.schema_nm)
            self.sumo_signal_insert_ms_db(self.phase_hist, begin, end, table_nm='tb_phase_history',
                                      schema_nm=contents.schema_nm)


if __name__ == "__main__":
    ##
    # begin = "20220201001000"
    # end = "20220201001500"
    begin = "20220202000000"
    end = "20220202001500"
    ##

    TS = Trans_signal()
    # ID = Insert_data()

    file_path = os.getcwd()
    file_path = TS.util.createFolder(file_path + '/sm2signal')

    # TS.sm2signal_state(begin, end)
    # TS.sumo_signal_insert_db(begin, end)
    # TS.sumo_phase_hist_insert_db(begin, end)
    # ID.insert_db(sumo_signal_state_fin, schema='anyang_second', table_nm='sumo_signaldata')
    # TS.add_yellow_phases()

    TS.sumo_signal_data2DB(begin, end)

    # unix_begin = TS.util.str2unixtime(begin)
    # unix_end = TS.util.str2unixtime(end)
    # sumo_signal_state.to_csv(os.path.join(file_path, f'{unix_begin}_{unix_end}.csv'), index=False)
