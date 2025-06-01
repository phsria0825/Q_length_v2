import os
import sys
import time
import pandas as pd
from collections import defaultdict

from tools import Cfg
from tools import connectDB
from tools import write_dic, load_dic

import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario-id', '-s', dest = 'scenario_id', type = str)
    parser.add_argument('--iteration', '-i', dest = 'iteration', type = int, default = 200)
    args = parser.parse_args()
    return args


def track(time_plan_id):
    path = os.path.join('save_weights', f'reward_{time_plan_id}.pkl')
    if os.path.exists(path):
        return load_dic(path)
    return None


def main(args):

    scenario_id = args.scenario_id

    # 경로설정
    path_main = os.path.dirname(os.path.abspath(__file__))
    os.chdir(os.path.join(path_main, scenario_id))

    db = connectDB()

    time_plan_with_begin_sec = load_dic(os.path.join('outputs', 'time_plan_with_begin_sec.pkl'))
    time_plans = list(time_plan_with_begin_sec.keys())
    N = len(time_plans)
    max_episode_num = args.iteration

    done = False
    done_set = set()
    hist = {}
    table = []
    path = os.path.join('save_weights', 'reward.csv')
    while not done:

        time.sleep(1)
        for i, time_plan_id in enumerate(time_plans):
            last_value = track(time_plan_id)

            if last_value is None:
                continue
            
            episode = last_value['episode']
            if episode in done_set:
                continue

            if episode not in hist:
                hist[episode] = {}
            hist[episode][time_plan_id] = last_value['rel_reward']
        
        for episode in list(hist):
            if len(hist[episode]) == N:
                reward = sum(hist[episode].values()) / len(hist[episode].values())
                done_set.add(episode)
                del hist[episode]
                db.insert_reward(scenario_id, episode, reward)
                sub = {'scenario_id' : scenario_id, 'episode' : episode, 'reward' : reward}
                table.append(sub)
                pd.DataFrame(table).to_csv(path, index = False)

                if episode == max_episode_num:
                    done = True


if __name__ == '__main__':
    args = parse_args()
    main(args)
    sys.exit(0)
