import os
import sys
from distutils.dir_util import copy_tree
from shutil import copy2

import argparse


### python preAnalysis.py -s scenario_id -u 1660489200 -w 1 -sc 1 -sdc 23050
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario-id', '-s', dest = 'scenario_id', type=str)
    parser.add_argument('--analysis-unixtime', '-u',  dest = 'anly_unixtime')
    parser.add_argument('--analysis-week-code', '-w', type=int, dest = 'anly_w_cd')
    parser.add_argument('--analysis-space-code', '-sc', dest = 'anly_s_cd')
    parser.add_argument('--analysis-space-detail-code', '-sdc', dest = 'anly_s_d_cd')
    args = parser.parse_args()
    return args


def copy_tools(path1, path2):
    copy_tree(path1, path2)


def get_base_file(network_path = '../input_network'):
    network_file = [file for file in os.listdir(network_path) if (file.find('.net.xml') != -1) and (file.find('all') != -1)][0]
    segment_file = [file for file in os.listdir(network_path) if (file.find('segments') != -1) and (file.find('add.xml') != -1)][0]
    vtype_file = [file for file in os.listdir(network_path) if (file.find('vtyp') != -1) and (file.find('rou.xml') != -1)][0]

    return network_path, network_file, segment_file, vtype_file


def mk_dir(scenario_id):
    dir_list = []

    temp_dir = os.path.join(scenario_id, 'temp')
    inputs_dir = os.path.join(scenario_id, 'inputs')

    dir_list.append(temp_dir)
    dir_list.append(inputs_dir)

    for i in dir_list:
        os.makedirs(i, exist_ok=True)


def main(args):

    # 시나리오id
    scenario_id = args.scenario_id

    # 경로설정
    path_main = os.path.dirname(os.path.abspath(__file__))
    os.chdir(path_main)

##############
###
### 1-1. 인자값에 따른 디렉토리 생성
###
#############
    os.makedirs(scenario_id, exist_ok=True)
    mk_dir(scenario_id)
    copy_tools('tools', os.path.join(scenario_id))
    print(f'{scenario_id}에 복사 완료')

##############
###
### 1-2. 기본 input 자료 복사
###
#############
    ## 네트워크(xml) 복사(../input_network) - inputs 폴더
    network_path, network_file, segment_file, vtype_file = get_base_file('../input_network')
    copy2(os.path.join(network_path, network_file), os.path.join(scenario_id, 'inputs', network_file))
    copy2(os.path.join(network_path, segment_file), os.path.join(scenario_id, 'inputs', segment_file))
    copy2(os.path.join(network_path, vtype_file), os.path.join(scenario_id, 'inputs', vtype_file))


##############
###
### 1-3. 신호 데이터 생성(별도 페이지로 설명)
###
#############
    os.system( f'python gendata/SignalState.py -s {args.scenario_id} -u {args.anly_unixtime} -w {args.anly_w_cd} -sc {args.anly_s_cd} -sdc {args.anly_s_d_cd}' )


##############
###
### 1-4. 교통량 데이터 생성(별도 페이지로 설명)
###
#############
    os.system('python gendata/PossibleRoutesGenerator.py -n %s -o %s -d 500' % (os.path.join(args.scenario_id, 'inputs', network_file)
                                                                                ,os.path.join(args.scenario_id, 'temp', 'possible_routes.xml')))
    os.system(f'python gendata/TrafficDemand.py -s {args.scenario_id} -u {args.anly_unixtime} -w {args.anly_w_cd} -sc {args.anly_s_cd} -sdc {args.anly_s_d_cd}' )


##############
###
### 1-5. 추가 전처리 및 전(前)분석 시작
###
#############
    # 경로 재설정
    os.chdir(os.path.join(path_main, scenario_id))

    # 전처리 시작
    os.system('python Preprocess.py')

    # 전분석 시작
    when = 'before'
    print(f'{scenario_id} : {when}-trafficAssessment has started')
    os.system(f'python trafficAssessment.py -w {when}')
    print(f'{scenario_id} : {when}-trafficAssessment has ended')


if __name__ == '__main__':
    args = parse_args()
    main(args)