import pandas as pd
from datetime import datetime
import os
import numpy as np
from tqdm import tqdm
from numpy import nan as NA
import _99_Contents as contents

from _00_util_ import util_
from _00_DB_connecter_ver2 import Load_data, Insert_data


class Trans_traffic(Load_data):
    def __init__(self):
        Load_data.__init__(self)
        self.movement_type = None
        self.carmodel_type = None
        self.collection_data = None
        self.LD = Load_data()
        self.util = util_()
        self.net_file = contents.net_file

    def make_sumo_traffic_demand(self, begin, end, num_inter):

        unix_begin = self.util.str2unixtime(begin)
        unix_end = self.util.str2unixtime(end)
        demand_csv_folder, demand_db_csv_folder = self.turn_count_to_traffic_demand(begin, end, num_inter)

        filelist = os.listdir(demand_csv_folder)

        get_filelist = []
        for i in filelist:
            if i.__contains__(contents.net_name + begin):
                get_filelist.append(i)

        for file_nm in get_filelist:
            try:
                data = pd.read_csv(os.path.join(demand_csv_folder, file_nm), sep=',')
            except:
                pass
            else:
                # print(data)
                # data['vtypeid'] = data.loc[:,'vehicle_id']
                data['vtypeid'] = [i.split('_')[0] for i in data.loc[:, 'vehicle_id']]
                data['vehicleid'] = [i.split('_')[0] + '_' + str(int(unix_begin)) + '_' + i.split('_', 1)[1] for i in
                                     data.loc[:, 'vehicle_id']]
                vtypeid = data.loc[0, 'vtypeid']

                data = data.rename(columns={'vehicle_depart': 'depart', 'route_edges': 'route'})[
                    ['vehicleid', 'vtypeid', 'depart', 'route']]

                unix_begin = data['depart'].min()
                unix_end = data['depart'].max()

                data.to_csv(os.path.join(demand_db_csv_folder, file_nm), index=False)
                with open('./cnt_traffic.csv', 'a') as f:
                    f.write(f'{begin},{vtypeid},{data.shape[0]}\n')

        else:
            print('Done.')

    def turn_count_to_traffic_demand(self, start, end, int_lcno=None, interval_sec=60):
        if interval_sec == 0:
            interval_sec = self.LD.interval_sec
        rou_path = self.possible_route(self.net_file, file_path='./sumo_net').replace('//', '/')

        count_path = self.make_turn_count_data(start, end, int_lcno).replace('//', '/')

        count_folder = count_path.split('/')[-1]
        file_path = '/'.join(count_path.split('/')[:-1])
        print(file_path)
        os.chdir(file_path)

        filelist = os.listdir(count_path)

        # begin = datetime.datetime.strftime(begin, '%Y%m%d%H%M%S')
        # end = datetime.datetime.strftime(end, '%Y%m%d%H%M%S')

        get_filelist = []
        for i in filelist:
            if i.__contains__(contents.net_name + start):
                get_filelist.append(i)

        demand_csv_folder = self.util.createFolder(file_path + '/counts2demands/csv')
        demand_xml_folder = self.util.createFolder(file_path + '/counts2demands/xml')
        demand_mismatch_xml_folder = self.util.createFolder(file_path + '/counts2demands/mismatch_xml')
        demand_db_csv_folder = self.util.createFolder(file_path + '/counts2demands/db_csv')

        carmodel_name_dic = {'p': 'passenger', 'b': 'bus', 't': 'truck', 'r': 'trailer', 'e': 'emergency',
                             'm': 'motorcycle'}

        for file_nm in get_filelist:
            vclass = carmodel_name_dic[file_nm.split('.')[0].strip()[-1]]
            demand_nm = ''.join([contents.net_name, start, file_nm.split('.')[0].strip()[-1]])
            os.system(
                f'''python sumo_py/routeSampler.py -r {rou_path} --turn-files {count_folder}/{file_nm} -o {demand_xml_folder}/{demand_nm}.xml --prefix {vclass}_  --attributes=type='{vclass}'  -i {interval_sec} --mismatch-output {demand_mismatch_xml_folder}/{demand_nm}.mismatch.xml '''
            )
            os.system(
                f"python sumo_py/xml2csv.py {demand_xml_folder}/{demand_nm}.xml -s , -o {demand_csv_folder}/{demand_nm}")
        return demand_csv_folder, demand_db_csv_folder

    def make_turn_count_data(self, start, end, int_lcno=None, interval_sec=60):
        self.movement_type, self.carmodel_type = self.load_bucheon_data()
        get_time_list = self.util.get_datetime_list(start, end, interval_sec)

        start_date_time = self.util.str2time(start)
        end_date_time = self.util.str2time(end)

        self.collection_data = self.find_traffic_demand_data(start_date_time, end_date_time, int_lcno)
        self.collection_data['crt_dt'] = [self.util.str2time(i) for i in self.collection_data['crt_dt']]

        group_time = [0 for _ in range(self.collection_data.shape[0])]
        if not self.collection_data.empty:
            for j in range(1, len(get_time_list)):
                for i in list(self.collection_data[(self.collection_data['crt_dt'] < get_time_list[j]) & (
                        self.collection_data['crt_dt'] >= get_time_list[j - 1])].index):
                    group_time[i] = get_time_list[j - 1]

        self.collection_data.loc[:, 'grouptime'] = group_time

        self.collection_data = self.collection_data.merge(self.carmodel_type[['vknd_cd', 'type']], how='left',
                                                          on='vknd_cd')

        self.collection_data = self.collection_data.merge(self.movement_type[['drct_cd', 'dir']], how='left',
                                                          on='drct_cd')

        self.collection_data = self.collection_data.rename(
            columns={'sumo_from_edge_id': 'fromedge', 'sumo_to_edge_id': 'toedge'})

        self.collection_data = self.collection_data.dropna().astype({'drct_cd': 'int16'})
        self.turn_counts = \
            self.collection_data.groupby(['grouptime', 'type', 'fromedge', "toedge"], as_index=False).count()[
                ['grouptime', 'type', 'fromedge', "toedge", 'obj_id']].rename(columns={'obj_id': 'cnt'})

        file_path = self.util.createFolder('./sm2counts')

        for c_type in list(set(self.turn_counts.type)):
            df = self.turn_counts[self.turn_counts['type'] == c_type].reset_index(drop=True)
            self.w_xml(start, end, interval_sec, df, c_type, file_path)

        return file_path

    def load_bucheon_data(self):
        movement_type_dic = {1: 's', 2: 'l', 3: 'r', 4: 'u', 5: 'sr'}
        carmodel_type_dic = {1: 'passenger', 2: 'passenger', 4: 'passenger', 8: 'passenger', 10: 'truck', 20: 'bus',
                             40: 'motorcycle'}

        movement_type = self.LD.cd_lnfo_list_by_grp_cd('DRCT_CD')
        carmodel_type = self.LD.cd_lnfo_list_by_grp_cd('VHCL_ATTR_CD')

        movement_type['dir'] = [movement_type_dic[i] for i in movement_type.cd]
        carmodel_type['type'] = [carmodel_type_dic[i] for i in carmodel_type.cd]

        return movement_type.rename(columns={'cd': 'drct_cd'}), carmodel_type.rename(columns={'cd': 'vknd_cd'})

    def find_traffic_demand_data(self, start, end, int_lcno):
        node_list = self.LD.node_list(int_lcno) if int_lcno is not None else self.LD.node_list()

        if len(node_list) != 0:
            node_edge_list = self.LD.node_edge_list()
            traffic_demand_list = self.LD.l_vhcl_data_proc_list(start, end, int_lcno)
            traffic_demand_list = pd.merge(traffic_demand_list, node_edge_list)
            return traffic_demand_list.sort_values(['crt_dt']).reset_index(drop=True)

    def w_xml(self, begin, end, interval_sec, df, c_type, file_path='./'):
        carmodel_name_dic = {'passenger': 'p', 'bus': 'b', 'truck': 't', 'trailer': 'tr', 'emergency': 'e',
                             'motorcycle': 'm'}

        f = open(file_path + f'/{contents.net_name}{begin}{carmodel_name_dic[c_type]}.xml', 'w')
        f.write(f'<data>\n')

        pbar = tqdm(sorted(list(set(df.grouptime))), desc=c_type)
        for gt in pbar:
            df_gt = df[df['grouptime'] == gt].reset_index(drop=True)
            # print(df_gt)
            [f.write(i) for i in self.interval_xml(begin, interval_sec, df_gt)]

        f.write(f'</data>\n')

        f.close()

    def interval_xml(self, begin, interval_sec, df):
        interval_list = []
        for idx in range(df.shape[0]):
            df_line = df.loc[idx, :]
            unix_begin = self.util.str2unixtime(df_line.grouptime)
            unix_end = unix_begin + interval_sec

            interval_list.append(f'  <interval id="generated" begin="{unix_begin}" end="{unix_end}">\n')
            interval_list.append(
                f'    <edgeRelation from="{df_line.fromedge}" to="{df_line.toedge}" count="{df_line.cnt}"/>\n')
            interval_list.append(f'  </interval>\n')
        return interval_list

    def possible_route(self, net_nm, file_path='.'):
        rou_nm = self.util.possible_routes
        trip_nm = self.util.possible_trip

        # if rou_nm not in os.listdir(file_path):
        #     os.system(
        #         f'python sumo_py/randomTrips.py -n {file_path}/{net_nm} -r {file_path}/{rou_nm} -o {file_path}/{trip_nm} --seed 0 --fringe-factor 5  '
        #     )
        # else:
        #     print('we already have "rou.xml"')

        os.system(
            f'python sumo_py/randomTrips.py -n {file_path}/{net_nm} -r {file_path}/{rou_nm} -o {file_path}/{trip_nm} --seed 500 --fringe-factor 5  '
        )

        return os.path.join(file_path, rou_nm)


if __name__ == "__main__":
    sumo_config = Trans_traffic()
    sumo_config.make_sumo_traffic_demand('20220901000000', '20220901001500', '26')
