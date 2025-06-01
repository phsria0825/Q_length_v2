import os
import sys
from distutils.dir_util import copy_tree

import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario-id', '-s', dest = 'scenario_id', type = str)
    args = parser.parse_args()
    return args


def main(args):

    # 시나리오id
    print(f'pid : {os.getpid()}')
    scenario_id = args.scenario_id

    # 경로설정
    path_main = os.path.dirname(os.path.abspath(__file__))
    os.chdir(os.path.join(path_main, scenario_id))

    # test 시작
    print('test 시작')
    os.system('python runAll.py -t test')
    print('test 종료')


if __name__ == '__main__':
    args = parse_args()
    main(args)
