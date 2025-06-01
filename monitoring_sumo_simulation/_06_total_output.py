import os

import pandas as pd
from _00_util_ import util_
import _99_Contents as contents

class TotalOutput:
    def __init__(self):
        self.util = util_()

    def save_od_data(self, filenm):
        vehicle_history_folder = self.util.createFolder(os.getcwd() + '/vehicle_history')
        veh_df = pd.read_csv(os.path.join(vehicle_history_folder, filenm), sep=',', encoding='utf8')

        start = veh_df.loc[veh_df.groupby(veh_df['vhcl_id'])['unix_time'].idxmin()]
        end = veh_df.loc[veh_df.groupby(veh_df['vhcl_id'])['unix_time'].idxmax()]

        result = pd.concat([start, end])
        result = result.sort_values(['vhcl_id', 'unix_time'])

        file_path = self.util.createFolder(os.getcwd() + '/od_data')
        outputnm = str(filenm).replace('tra', 'od')
        result.to_csv(file_path + '/' + outputnm, index=False)

    def save_summarize(self, filenm):  # 파일명 아래 넣기
        vehicle_history_folder = self.util.createFolder(os.getcwd() + '/vehicle_history')
        veh_df = pd.read_csv(os.path.join(vehicle_history_folder, filenm), sep=',', encoding='utf8')

        avg_spd = veh_df['spd'].groupby(veh_df['edge_grp_id']).mean()  # 평균 속도
        total_time_loss = (1 - veh_df['spd'] / veh_df['allowed_spd']).groupby(veh_df['edge_grp_id']).sum()  # 총 지체시간

        vdf = veh_df[['edge_grp_id', 'vhcl_id']].drop_duplicates()
        cnt_veh = vdf['vhcl_id'].groupby(vdf['edge_grp_id']).count()  # 차량 고유 개수
        avg_time_loss = total_time_loss / cnt_veh  # 차량당 평균 지체시간
        avg_travle_time = veh_df['vhcl_id'].groupby(veh_df['edge_grp_id']).count() / cnt_veh  # 차량당 평균 통행시간

        summarize_df = pd.concat([avg_spd, total_time_loss, avg_time_loss, avg_travle_time], axis=1)
        summarize_df.columns = ['avg_spd', 'total_time_loss', 'avg_time_loss', 'avg_travle_time']

        file_path = self.util.createFolder(os.getcwd() + '/total_output')
        outputnm = str(filenm).replace('tra', 'total')
        summarize_df.to_csv(file_path + '/' + outputnm)


if __name__ == "__main__":
    total = TotalOutput()
    total.save_od_data('40001120220202000000tra.csv')
