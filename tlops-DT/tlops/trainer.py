import os
import sys
import subprocess
from shutil import copy2
from distutils.dir_util import copy_tree

import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario-id', '-s', dest = 'scenario_id', type = str)
    parser.add_argument('--iteration', '-i', dest = 'iteration', type = int, default = 200)
    args = parser.parse_args()
    return args


def load_data(args):
    '''최적화를 위한 입력데이터 준비'''
    print('loading data')

    # 시나리오 폴더 만들기
    os.makedirs(args.scenario_id, exist_ok = True)
    os.makedirs(os.path.join(args.scenario_id, 'inputs'), exist_ok = True)

    # 입력 데이터 복사
    path = 'target_anyang'
    # path = 'target_inch'
    files2copy = ['before.tll.xml', 'node.csv', 'sumo.net.xml', 'sumo.rou.xml', 'vtype.add.xml']
    for file_name in files2copy:
        copy2(os.path.join(path, file_name), os.path.join(args.scenario_id, 'inputs', file_name))


def main(args):

    # 시나리오id
    print(f'pid : {os.getpid()}')
    scenario_id = args.scenario_id

    # 경로설정
    path_main = os.path.dirname(os.path.abspath(__file__))
    os.chdir(path_main)

    # 데이터 불러오기
    load_data(args)

    # 대상폴터에 모듈복사
    copy_tree('tools', os.path.join(scenario_id))
    print(f'{scenario_id}에 복사 완료')

    # 경로 재설정
    os.chdir(os.path.join(path_main, scenario_id))

    # 전처리 시작
    print('전처리 시작')
    os.system('python Preprocess.py')
    print('전처리 종료')

    # train 시작
    print('train 시작')
    subprocess.Popen(f'python ../trainingTracker.py -s {args.scenario_id} -i {args.iteration}', shell = True)
    os.system(f'python runAll.py -t train -i {args.iteration}')
    print('train 종료')


if __name__ == '__main__':
    args = parse_args()
    main(args)
