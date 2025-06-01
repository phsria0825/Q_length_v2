import pandas as pd
import os
import datetime

from tqdm import tqdm
import _99_Contents as contents
from _00_util_ import util_
from _00_DB_connecter_ver2 import Load_data, Insert_data

SUMO_HOME = os.environ['SUMO_HOME']
# sys.path.append(join(SUMO_HOME, "tools"))

from sumolib import checkBinary  # noqa


class Trans_traffic():
    def __init__(self):
        self.LD = Load_data()
        self.ID = Insert_data()
        self.util = util_()
        self.net_file = contents.net_file

    def load_net_data(self):
        self.edge_table = self.LD.load_net_data('tb_edge_sm_avenue').astype(
            {'edge_id': str, 'avenueseq': int})  # sumo에서 나오는 edge 데이터 load

        edge_id = ['' for i in range(self.edge_table.shape[0])]
        for i in list(self.edge_table['edge_id'].index):
            edge_id[i] = self.edge_table.loc[:, 'edge_id'][i].replace('.0', '')
        self.edge_table.loc[:, 'edge_id'] = edge_id

        self.connection_table = self.LD.load_net_data('tb_connect_edge')  # sumo에서 나오는 connection 데이터 load

    def load_anyang_data(self):
        movement_type_dic = {1: 's', 2: 'l', 3: 'r', 4: 'u'}
        carmodel_type_dic = {1: 'passenger', 2: 'bus', 3: 'truck', 4: 'trailer', 5: 'emergency', 6: 'motorcycle'}

        self.movement_type = self.LD.load_anyang_data('movementtype')  # 4번데이터
        self.carmodel_type = self.LD.load_anyang_data('carmodel')  # 5번데이터

        self.movement_type['dir'] = [movement_type_dic[i] for i in self.movement_type.movementtype]
        self.carmodel_type['type'] = [carmodel_type_dic[i] for i in self.carmodel_type.carmodeltype]

        return self.movement_type, self.carmodel_type

    def sm2counts(self, begin, end, num_inter, interval_sec=60, file_path='./'):
        self.load_net_data()
        self.load_anyang_data()
        get_time_list = self.util.get_datetime_list(begin, end, interval_sec)

        self.collection_data = self.LD.load_collection_data(begin, end, num_inter).astype({'avenueseq': int})
        self.collection_data['collecteddate'] = [self.util.str2time(i) for i in self.collection_data['collecteddate']]

        group_time = [0 for i in range(self.collection_data.shape[0])]
        if not self.collection_data.empty:
            for j in range(1, len(get_time_list)):
                for i in list(self.collection_data[(self.collection_data['collecteddate'] < get_time_list[j]) & (
                        self.collection_data['collecteddate'] >= get_time_list[j - 1])].index):
                    group_time[i] = get_time_list[j - 1]

        self.collection_data.loc[:, 'grouptime'] = group_time

        self.collection_data = self.collection_data.merge(self.carmodel_type[['carmodeltype', 'type']], how='left',
                                                          on='carmodeltype')

        self.collection_data = self.collection_data.merge(self.edge_table[['edge_id', 'avenueseq']], how='left',
                                                          on='avenueseq').rename(columns={'edge_id': 'fromedge'})

        # self.collection_data = self.collection_data.merge(self.edge_table[['edge_id','avenueseq']], how='left',
        # left_on='avenueseq', right_on='avenueseq').rename(columns = {'edge_id' : 'fromedge'})
        self.collection_data = self.collection_data.merge(self.movement_type[['movementtype', 'dir']], how='left',
                                                          on='movementtype')

        self.collection_data = self.collection_data.merge(
            self.connection_table[['from_edge_id', 'to_edge_id', 'turn_typ']], how='left', left_on=['fromedge', 'dir'],
            right_on=['from_edge_id', 'turn_typ']).rename(columns={'to_edge_id': 'toedge'})

        self.collection_data = self.collection_data.dropna().astype({'movementtype': 'int16'})
        self.turn_counts = \
            self.collection_data.groupby(['grouptime', 'type', 'fromedge', "toedge"], as_index=False).count()[
                ['grouptime', 'type', 'fromedge', "toedge", 'dataseq']].rename(columns={'dataseq': 'cnt'})

        # unix_begin = self.util.str2unixtime(begin)
        # unix_end = self.util.str2unixtime(end)

        file_path = self.util.createFolder(file_path + '/sm2counts')

        for c_type in list(set(self.turn_counts.type)):
            df = self.turn_counts[self.turn_counts['type'] == c_type].reset_index(drop=True)
            self.w_xml(begin, end, interval_sec, df, c_type, file_path)

        return file_path

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

        # unix_begin = self.util.str2unixtime(begin)
        # unix_end = unix_begin + interval_sec

        # interval_list.append(f'  <interval id="generated" begin="{unix_begin}" end="{unix_end}">\n')

        # for idx in range(df.shape[0]):
        #     df_line = df.loc[idx,:]
        #     interval_list.append(f'    <edgeRelation from="{df_line.fromedge}" to="{df_line.toedge}" count="{df_line.cnt}"/>\n')

        # interval_list.append(f'  </interval>\n')
        # return interval_list

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

    def counts2demands(self, begin, end, num_inter, interval_sec=0):
        if interval_sec == 0:
            interval_sec = self.LD.interval_sec
        rou_path = self.possible_route(self.net_file, file_path='./sumo_net').replace('//', '/')

        count_path = self.sm2counts(begin, end, num_inter).replace('//', '/')

        count_folder = count_path.split('/')[-1]
        file_path = '/'.join(count_path.split('/')[:-1])
        print(file_path)
        os.chdir(file_path)

        filelist = os.listdir(count_path)

        # begin = datetime.datetime.strftime(begin, '%Y%m%d%H%M%S')
        # end = datetime.datetime.strftime(end, '%Y%m%d%H%M%S')

        get_filelist = []
        for i in filelist:
            if i.__contains__(contents.net_name + begin):
                get_filelist.append(i)

        demand_csv_folder = self.util.createFolder(file_path + '/counts2demands/csv')
        demand_xml_folder = self.util.createFolder(file_path + '/counts2demands/xml')
        demand_mismatch_xml_folder = self.util.createFolder(file_path + '/counts2demands/mismatch_xml')
        demand_db_csv_folder = self.util.createFolder(file_path + '/counts2demands/db_csv')

        carmodel_name_dic = {'p': 'passenger', 'b': 'bus', 't': 'truck', 'r': 'trailer', 'e': 'emergency',
                             'm': 'motorcycle'}

        for file_nm in get_filelist:
            vclass = carmodel_name_dic[file_nm.split('.')[0].strip()[-1]]
            demand_nm = ''.join([contents.net_name, begin, file_nm.split('.')[0].strip()[-1]])
            os.system(
                f'''python sumo_py/routeSampler.py -r {rou_path} --turn-files {count_folder}/{file_nm} -o {demand_xml_folder}/{demand_nm}.xml --prefix {vclass}_  --attributes=type='{vclass}'  -i {interval_sec} --mismatch-output {demand_mismatch_xml_folder}/{demand_nm}.mismatch.xml '''
            )
            os.system(
                f"python sumo_py/xml2csv.py {demand_xml_folder}/{demand_nm}.xml -s , -o {demand_csv_folder}/{demand_nm}")
        return demand_csv_folder, demand_db_csv_folder

    def insert_hist_db(self, data, condition, schema_nm=contents.schema_nm, table_nm='sumo_trafficdata_hist'):
        # ID = Insert_data()

        self.ID.delete_data(condition, schema_nm=schema_nm, table_nm=table_nm)
        self.ID.insert_bulk(data, schema_nm=schema_nm, table_nm=table_nm)

    def insert_ms_db(self, data, condition, unix_end, end, schema_nm=contents.schema_nm, table_nm='sumo_trafficdata'):
        # ID = Insert_data()

        self.ID.delete_data(condition, schema_nm=schema_nm, table_nm=table_nm)
        self.ID.insert_bulk(data, schema_nm=schema_nm, table_nm=table_nm)

        # unix_end = min(unix_end, self.util.str2unixtime(end)) - self.LD.interval_sec*2
        unix_end = self.util.str2unixtime(end) - self.LD.interval_sec * 2
        # print(unix_end)
        condition = f''' depart <= {unix_end} '''

        self.ID.delete_data(condition, table_nm=table_nm, schema_nm=schema_nm)

    def sumo_traffic_insert_db(self, begin, end, num_inter, insert_DB=False):

        unix_begin = self.util.str2unixtime(begin)
        unix_end = self.util.str2unixtime(end)
        demand_csv_folder, demand_db_csv_folder = self.counts2demands(begin, end, num_inter)

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

                condition = f'''"depart" >= '{unix_begin}' and "depart" <= '{unix_end}' and vtypeid = '{vtypeid}' '''

                if insert_DB:
                    self.insert_hist_db(data, condition, schema_nm=contents.schema_nm, table_nm='sumo_trafficdata_hist')
                    self.insert_ms_db(data, condition, unix_end, end, schema_nm=contents.schema_nm,
                                      table_nm='sumo_trafficdata')

                data.to_csv(os.path.join(demand_db_csv_folder, file_nm), index=False)
                with open('./cnt_traffic.csv', 'a') as f:
                    f.write(f'{begin},{vtypeid},{data.shape[0]}\n')


        else:
            print('Done.')


if __name__ == "__main__":
    ##
    # begin = "20220201001000"
    # end = "20220201001500"
    begin = "20220202000000"
    end = "20220202001500"
    interval_sec = 300
    ##
    sumo_traffic = Trans_traffic()

    # turn_counts = sumo_traffic.sm2counts(begin, end)
    # turn_counts.to_csv('turn_counts.csv', index=False)

    # sumo_traffic.sm2counts(begin, end, interval_sec)
    # sumo_traffic.insert_db(begin, end)

    # sumo_traffic.counts2demands(begin, end)

    sumo_traffic.sumo_traffic_insert_db(begin, end)
