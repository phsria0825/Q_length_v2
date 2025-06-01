from xml.etree.ElementTree import Element, ElementTree, SubElement
import pandas as pd
import datetime
import os
import numpy as np
from numpy import nan as NA
import _99_Contents as contents

from _00_util_ import util_
from _00_DB_connecter_ver2 import Load_data, Insert_data


class Trans_signal(Load_data):
    def __init__(self):
        Load_data.__init__(self)
        self.sumo_signal_state_fin = None
        self.LD = Load_data()
        self.util = util_()

    def find_yellowtime(self, aringstarttime, yellowtime, yello_flag, option='mius'):
        if yello_flag == 'N':

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
        else:
            return aringstarttime

    def make_sumo_tl_logic(self, start, end, write_file=True):
        start_date_time = self.util.str2time(start)
        end_date_time = self.util.str2time(end)
        # self.change_interval()
        # # before begin : 시작시간 이전에 변경된 신호 데이터를 받아오는 역할
        # b_start_date_time = start_date_time - datetime.timedelta(seconds=self.interval_sec)
        # b_end_date_time = start_date_time

        # b_signal_list = self.find_signal_list(None, b_start_date_time, b_end_date_time)
        signal_list = self.find_signal_list(None, start_date_time, end_date_time)
        sumo_signal_state = signal_list.drop_duplicates()
        # sumo_signal_state = pd.concat([b_signal_list, signal_list], ignore_index=True).astype(
        #     {'sgnl_crsrd_no': int}).drop_duplicates()

        sumo_signal_state.loc[sumo_signal_state['yello_flag'] == 'Y', 'prst_ss'] = sumo_signal_state['int_yellow']
        sumo_signal_state.loc[sumo_signal_state['yello_flag'] == 'N', 'prst_ss'] = sumo_signal_state['prst_ss'] - \
                                                                                   sumo_signal_state['int_yellow']
        sumo_signal_state = sumo_signal_state.reset_index()

        sumo_signal_state['ocrn_dt'] = [self.find_yellowtime(
            sumo_signal_state.loc[i, 'ocrn_dt'],
            sumo_signal_state.loc[i, 'int_yellow'],
            sumo_signal_state.loc[i, 'yello_flag'],
            'mius') for i in range(sumo_signal_state.shape[0])]

        self.sumo_signal_state_fin = sumo_signal_state[['sumo_node_id', 'ocrn_dt', 'sgnl']] \
            .rename(columns={'sumo_node_id': 'interid', 'ocrn_dt': 'aringstarttime', 'sgnl': 'signalstate'})

        self.sumo_signal_state_fin['unix_time'] = [int(self.util.str2unixtime(i)) for i in
                                                   self.sumo_signal_state_fin.aringstarttime]
        if write_file:
            file_path = self.util.createFolder(os.getcwd() + '/sm2signal/csv')

            self.sumo_signal_state_fin.to_csv(os.path.join(file_path, f'{contents.net_name}{start}s.csv'), index=False)
            self.csv_to_xml(self.sumo_signal_state_fin, start, end)

        return self.sumo_signal_state_fin

    def find_signal_list(self, int_lcno, start, end):
        node_list = self.LD.node_list(int_lcno) if int_lcno is not None else self.LD.node_list()

        if len(node_list) != 0:
            int_lcno_list = tuple([i for i in node_list['sgnl_crsrd_no']])
            tl_logic_list = self.LD.lane_sgnl_input_list(int_lcno_list)
            l_sgnl_prst = self.LD.l_sgnl_prst_list(start, end)
            scs_m_intphase = self.LD.scs_m_intphase(int_lcno_list)

            l_sgnl_prst = pd.merge(l_sgnl_prst, node_list)
            tl_logic_list = pd.merge(tl_logic_list, l_sgnl_prst)
            tl_logic_list = pd.merge(tl_logic_list, scs_m_intphase)
            tl_logic_list = tl_logic_list.sort_values(by=['sgnl_crsrd_no', 'ocrn_dt'])

            return tl_logic_list

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

            if interid.iloc[i] == data['interid'].iloc[0]:
                sub_element1 = SubElement(element1, "phase")
                sub_element1.set("duration",
                                 str(int(data['unix_time'].iloc[0] - self.util.str2unixtime(begin_str)) if int(
                                     data['unix_time'].iloc[0] > self.util.str2unixtime(begin_str)) else 0))
                sub_element1.set("state", str(data['signalstate'].iloc[0]))

            for j in range(1, len(data.index)):
                if data['interid'].iloc[j] == interid.iloc[i]:
                    if data['interid'].iloc[j - 1] == data['interid'].iloc[j]:
                        sub_element1 = SubElement(element1, "phase")
                        sub_element1.set("duration", str(data['unix_time'].iloc[j] - data['unix_time'].iloc[j - 1]))
                        sub_element1.set("state", str(data['signalstate'].iloc[j]))
                    else:
                        sub_element1 = SubElement(element1, "phase")
                        sub_element1.set("duration",
                                         str(int(data['unix_time'].iloc[j] - self.util.str2unixtime(begin_str)) if int(
                                             data['unix_time'].iloc[j] > self.util.str2unixtime(begin_str)) else 0))
                        sub_element1.set("state", str(data['signalstate'].iloc[j]))

        self.util.indent(root, 0)

        tree = ElementTree(root)

        with open(file_path + "/" + fileName, "wb") as file:
            tree.write(file, encoding='utf-8', xml_declaration=True)


if __name__ == "__main__":
    sumo_config = Trans_signal()
    sumo_config.make_sumo_tl_logic('20220901000000', '20220901001500')
