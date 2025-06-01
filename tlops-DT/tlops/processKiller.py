import os
import pickle
import psutil
import subprocess

from tools import connectDB
from tools import write_dic, load_dic

import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest = 'scenario_id', type = str)
    parser.add_argument('-p', dest = 'pid', type = int, default = None)
    args = parser.parse_args()
    return args


def kill_process_using_pid(pid, including_children = False):
    parent = psutil.Process(pid)
    if including_children:
        for child in parent.children(recursive = True):
            # child.kill()
            child.terminate()
    # parent.kill()
    parent.terminate()


def main(args):

    if args.pid is not None:
        kill_process_using_pid(args.pid, True)
        return

    ## 경로설정
    path_main = os.path.dirname(os.path.abspath(__file__))
    os.chdir(path_main)

    path = os.path.join(args.scenario_id, 'record', 'status.pkl')
    pid_training = load_dic(path)
    scenario_id, pid, training = pid_training['scenario_id'], pid_training['pid'], pid_training['training']

    if not training:
        print(f'{scenario_id} : 학습 중이 아니므로 모든 프로세스를 종료합니다.')
        kill_process_using_pid(pid, True)
    else:
        print(f'{scenario_id} : 학습 중이므로 현재 프로세스를 종료하고, 후분석을 진행합니다.')
        kill_process_using_pid(pid, True)
        subprocess.Popen(f'python postAnalysis.py -s {args.scenario_id} -t f')

    ## DB 셋팅
    db = connectDB()
    db.update_status('6', args.scenario_id)


if __name__ == '__main__':
    args = parse_args()
    main(args)
    print('완료')
