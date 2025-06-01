import os
import sys
from distutils.dir_util import copy_tree
from shutil import copy2

from tools import connectDB
from tools import write_dic, load_dic

import argparse


def parse_args():
    parser = argparse.ArgumentParser()

    # 시나리오id - 폴더이름
    parser.add_argument('--scenario-id', '-s', dest = 'scenario_id', type = str)

    # 요청유닉스 시간
    parser.add_argument('--analysis-unixtime', '-u',  dest = 'anly_unixtime', type = int, default = None)

    # 분석요일 구분 코드 - 1:일요일, 6:금요일, 7:토요일, 8:월요일 ~ 목요일
    parser.add_argument('--analysis-week-code', '-w', dest = 'anly_w_cd', type = str, default = None)

    # 공간구분코드 - 2:도로, 5:행정구역(네트워크), 6:연동그룹
    parser.add_argument('--analysis-space-code', '-sc', dest = 'anly_s_cd', type = str, default = None)

    # 공간구분id - 행정구ID, 도로명, 연동그룹ID
    parser.add_argument('--analysis-space-detail-code', '-sdc', dest = 'anly_s_d_cd', type = str, default = None)

    return parser.parse_args()


'''
진행상태 코드

2 : done
6 : not done

11 : loading data
12 : preprocessing
13 : pre analyzing

16 : training
17 : post analyzing
'''


def load_data(args):
    '''최적화를 위한 입력데이터 준비'''
    print('loading data')

    # 상태코드 업데이트
    connectDB().update_status('11', args.scenario_id)

    # 시나리오 폴더 만들기
    os.makedirs(args.scenario_id, exist_ok = True)
    os.makedirs(os.path.join(args.scenario_id, 'inputs'), exist_ok = True)

    # 입력 데이터 복사
    path = 'target_anyang'  # 'target_inch'
    # path = 'target_inch'
    files2copy = ['before.tll.xml', 'node.csv', 'sumo.net.xml', 'sumo.rou.xml', 'vtype.add.xml']
    for file_name in files2copy:
        copy2(os.path.join(path, file_name), os.path.join(args.scenario_id, 'inputs', file_name))


def pre_process(args):
    '''입력된 데이터 전처리'''
    print('pre-processing')

    # 상태코드 업데이트
    connectDB().update_status('12', args.scenario_id)

    # 최적화 도구 복사
    copy_tree('tools', os.path.join(args.scenario_id))

    # 경로 재설정
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), args.scenario_id))

    if os.system('python Preprocess.py') != 0:
        raise Exception('전처리 에러')


def pre_analyze(args):
    '''전분석'''
    print('pre-analyzing')

    # 상태코드 업데이트
    connectDB().update_status('13', args.scenario_id)

    if os.system(f'python trafficAssessment.py -b t') != 0:
       raise Exception('전분석 에러')


def main(args):
    '''
    - loading data

    - pre-processing

    - pre-analyzing
    '''

    ## =============================================================================
    ## ready
    ## =============================================================================

    # 경로설정 : 스크립트가 존재하는 폴더를 작업 폴더로 설정함
    path_main = os.path.dirname(os.path.abspath(__file__))
    os.chdir(path_main)

    # training 여부 로컬에 저장
    pid_training = {'scenario_id' : args.scenario_id, 'pid' : os.getpid(), 'training' : False}
    os.makedirs(os.path.join(args.scenario_id, 'record'), exist_ok = True)
    path = os.path.join(args.scenario_id, 'record', 'status.pkl')
    write_dic(path, pid_training)
    print(load_dic(path))


    ## =============================================================================
    ## loading data
    ## =============================================================================
    try:
        load_data(args)
    except:
        try:
            connectDB().update_error('11', args.scenario_id)
        except:
            print('DB 업데이트 에러')
        finally:
            print('데이터로드 중 에러 발생으로 프로그램 종료')
            sys.exit(11)


    ## =============================================================================
    ## pre-processing
    ## =============================================================================
    try:
        pre_process(args)
    except:
        try:
            connectDB().update_error('12', args.scenario_id)
        except:
            print('DB 업데이트 에러')
        finally:
            print('전처리 중 에러 발생으로 프로그램 종료')
            sys.exit(12)

    ## =============================================================================
    ## pre-analyzing
    ## =============================================================================
    try:
        pre_analyze(args)
    except:
        try:
            connectDB().update_error('13', args.scenario_id)
        except:
            print('DB 업데이트 에러')
        finally:
            print('전분석 중 에러 발생으로 프로그램 종료')
            sys.exit(13)

    ## =============================================================================
    ## 완료처리
    ## =============================================================================
    try:
        print('done')
        connectDB().update_status('2', args.scenario_id)
    except:
        print('DB 업데이트 에러')
    finally:
        sys.exit()


if __name__ == '__main__':
    args = parse_args()
    main(args)
