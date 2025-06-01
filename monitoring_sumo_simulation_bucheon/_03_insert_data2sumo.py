import pandas as pd
import os

from _00_util_ import util_
from _00_DB_connecter_ver2 import Load_data

class Insert_sumo():
    def __init__(self):
        self.LD = Load_data()
        self.util = util_()

    def set_dic(self, data):
        unix_time_list = sorted(data.unix_time.unique())
        dic = {}

        for unix_time in unix_time_list:
            idx = data.unix_time == unix_time
            data_sub = data.loc[idx, ["interid", "signalstate"]]

            dic_sub = {data_sub.iloc[i, 0] : data_sub.iloc[i, 1] for i in range(len(data_sub))}
            dic[unix_time] = dic_sub

        return dic

    def insert_signal_state(self, sim, dic, cur_time):
        if cur_time in dic:
            dic_sub = dic.get(cur_time)
            # inter_id_list = dic_sub.keys()
            for inter_id, phase in dic_sub.items():
                sim.trafficlight.setRedYellowGreenState(inter_id, phase)

    # def insert_signal_state(self, sim, dic, cur_time):
    #     dic_sub = dic.get(cur_time)
    #     inter_id_list = dic_sub.keys()

    #     if dic_sub is not None:
    #         for inter_id in inter_id_list:
    #             phase = dic_sub.get(inter_id)
    #             if phase is not None:
    #                 sim.trafficlight.setRedYellowGreenState(inter_id, phase)




    def insert_traffic_demands(self, sim, data):
        for i in range(data.shape[0]):
            data_sub = data.loc[i]

            vehicle_id = data_sub['vehicleid']
            vtype_id = data_sub['vtypeid']
            depart = data_sub['depart']
            route = data_sub['route'].split()

            sim.vehicle.addLegacy(vehID = vehicle_id, routeID = "", depart = depart, typeID = vtype_id)
            sim.vehicle.setRoute(vehicle_id, route)


if __name__ == "__main__":
    print("Show me the money")