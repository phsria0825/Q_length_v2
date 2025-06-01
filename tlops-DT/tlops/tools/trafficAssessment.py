import os
import pandas as pd

from config import Cfg
import Tools as tl
from Tools import load_traci
import ToolsWriteLoad as twl
from DefWAUT import DefWAUT
from TrafficSimulator4RL import TrafficSimulator4RL

import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--is-before', '-b', dest = 'is_before', type = str2bool)
    args = parser.parse_args()
    return args


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def main(args):

    ## 시나리오ID : 현재 폴더이름
    scenario_id = os.path.split(os.getcwd())[1]

    # nodes 불러오기
    nodes = twl.load_dic(os.path.join('refined', 'nodes.pkl'))

    ## 학습 시뮬레이터 셋팅
    list_node_id = list(nodes.keys())
    path_segments = os.path.join('refined', 'segments4rl.add.xml')

    is_before = args.is_before
    if is_before:
        path_tll = os.path.join('inputs', 'before.tll.xml')
        dw = DefWAUT(path_tll, is_before)
        dw.main()
        path_waut = os.path.join('refined', 'waut_before.add.xml')
    else:
        ## after.tll.xml 생성
        os.system(f'python PhaseConfig.py --scenario-id {scenario_id} --step-size {Cfg.step_size}')
        path_tll = os.path.join('outputs', 'after.tll.xml')
        dw = DefWAUT(path_tll, is_before)
        dw.main()
        path_waut = os.path.join('refined', 'waut_after.add.xml')

    save_init_state = False
    load_init_state = False
    save_results = True

    traci = load_traci(Cfg.apply_libsumo, Cfg.gui)
    time_plan_with_begin_sec = twl.load_dic(os.path.join('outputs', 'time_plan_with_begin_sec.pkl'))
    vehicle_points, values_from_each_inter, values_from_all, time_series_values_from_all = [], [], [], []
    for time_plan_id, value in time_plan_with_begin_sec.items():
        print(f'start{time_plan_id}')
        begin_sec = value['train_begin_sec']

        # 시뮬레이션 셋팅
        ts4rl = TrafficSimulator4RL(traci, scenario_id, nodes,
                                    time_plan_id, begin_sec, Cfg.episode_sec,
                                    path_segments, path_tll, path_waut,
                                    save_init_state, load_init_state,
                                    save_results, Cfg.gui)

        # 시뮬레이션 on
        ts4rl.sim_start()

        # 시뮬레이션 run
        ts4rl.run_sims(Cfg.step_size_for_results, Cfg.aggregation_size, is_before)

        # Vehicle points for the entire network
        sub1 = ts4rl._get_vehicle_points()
        vehicle_points.append(sub1)

        # Aggregate traffic indicators at each intersection for all times
        sub2 = ts4rl._get_traffic_indicators_from_each_intersection_for_all_times()
        values_from_each_inter.append(sub2)

        # Aggregate traffic indicators from all road networks for all times
        sub3 = ts4rl._get_traffic_indicators_from_all_networks_for_all_times()
        values_from_all.append(sub3)

        # Get time series traffic indicators for entire network
        sub4 = ts4rl._get_time_series_traffic_indicators_from_all_networks()
        time_series_values_from_all.append(sub4)

        # 시뮬레이션 off
        ts4rl.sim_close()

    vehicle_points = pd.concat(vehicle_points, axis = 0)
    values_from_each_inter = pd.concat(values_from_each_inter, axis = 0)
    values_from_all = pd.concat(values_from_all, axis = 0)
    time_series_values_from_all = pd.concat(time_series_values_from_all, axis = 0)

    atype = 'before' if is_before else 'after'
    vehicle_points.to_csv(os.path.join('outputs', f'{atype}_soitsanlsvhclmvmntowh.csv') ,index = False)
    values_from_each_inter.to_csv(os.path.join('outputs', f'{atype}_soitsanlsintsvalue.csv') ,index = False)
    values_from_all.to_csv(os.path.join('outputs', f'{atype}_soitsanlsallvalue.csv') ,index = False)
    time_series_values_from_all.to_csv(os.path.join('outputs', f'{atype}_soitsanls5minvalue.csv') ,index = False)


if __name__ == '__main__':
    args = parse_args()
    main(args)
