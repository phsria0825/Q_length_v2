import os

from config import Cfg
import ToolsWriteLoad as twl
from Tools import load_traci
from TrafficSimulator4RL import TrafficSimulator4RL

from Env import *
from Ppo import PPOagent

import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', dest = 'time_plan_id', type = str)
    parser.add_argument('-b', dest = 'begin_sec', type = int)
    parser.add_argument('-i', dest = 'iteration', type = int, default = 200)
    args = parser.parse_args()
    return args


def main(args):

    ## 시나리오ID : 현재 폴더이름
    scenario_id = os.path.split(os.getcwd())[1]

    # nodes 불러오기
    nodes = twl.load_dic(os.path.join('refined', 'nodes.pkl'))

    ## 학습 시뮬레이터 셋팅
    list_node_id = list(nodes.keys())
    path_segments = os.path.join('refined', 'segments4rl.add.xml')
    path_tll = None
    path_waut = None

    save_init_state = True
    load_init_state = False
    save_results = False

    traci = load_traci(Cfg.apply_libsumo, Cfg.gui)
    time_plan_id = args.time_plan_id
    begin_sec = args.begin_sec

    # 시뮬레이션 셋팅
    ts4rl = TrafficSimulator4RL(traci, scenario_id, nodes,
                                time_plan_id, begin_sec, Cfg.episode_sec,
                                path_segments, path_tll, path_waut,
                                save_init_state, load_init_state,
                                save_results, Cfg.gui)

    # 시뮬레이션 on
    ts4rl.sim_start()

    # 환경 셋팅
    env = Env(time_plan_id, traci, nodes, Cfg.state_types, Cfg.episode_sec, Cfg.step_size)

    # 에이전트 셋팅
    agent = PPOagent(scenario_id, env, nodes)
    # agent.load_weights()

    # 학습 시작
    agent.run(args.iteration, Cfg.save_tll_hist)

    # 시뮬레이션 off
    ts4rl.sim_close()


if __name__ == '__main__':
    args = parse_args()
    main(args)
