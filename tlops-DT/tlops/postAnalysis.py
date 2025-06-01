import os
import sys
import subprocess

from tools import connectDB
from tools import write_dic

import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario-id', '-s', dest = 'scenario_id', type = str)
    parser.add_argument('--training', '-t', dest = 'training', type = str2bool, default = 't')
    parser.add_argument('--iteration', '-i', dest = 'iteration', type = int, default = 200)
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


def train(args):
    '''학습'''
    print('train')

    # 상태코드 업데이트
    connectDB().update_status('16', args.scenario_id)

    if os.system(f'python runAll.py -t train -i {args.iteration}') != 0:
        raise Exception('train 에러')


def post_analyze(args):
    '''후분석'''
    print('post-analyzing')

    # 상태코드 업데이트
    connectDB().update_status('17', args.scenario_id)

    if os.system(f'python trafficAssessment.py -b f') != 0:
       raise Exception('후분석 에러')


def main(args):

    ## =============================================================================
    ## ready
    ## =============================================================================

    # 경로설정
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), args.scenario_id))

    # training 여부 로컬에 저장
    path_record = os.path.join('record', 'status.pkl')
    pid_training = {'scenario_id' : args.scenario_id, 'pid' : os.getpid(), 'training' : False}
    print(pid_training)


    ## =============================================================================
    ## train
    ## =============================================================================
    if args.training:
        try:
            # training 여부 True로 설정
            pid_training['training'] = True
            write_dic(path_record, pid_training)
            
            # 각 worker의 reward 확인
            subprocess.Popen(f'python ../trainingTracker.py -s {args.scenario_id} -i {args.iteration}', shell = True)
            
            # 학습시작
            train(args)
        except:
            try:
                connectDB().update_error('16', args.scenario_id)
            except:
                print('DB 업데이트 에러')
            finally:
                print('학습 중 에러 발생으로 프로그램 종료')
                sys.exit(16)


    ## =============================================================================
    ## post-analyzing
    ## =============================================================================
    try:
        # training 여부 False로 설정
        pid_training['training'] = False
        write_dic(path_record, pid_training)

        # 후분석 시작
        post_analyze(args)
    except:
        try:
            connectDB().update_error('17', args.scenario_id)
        except:
            print('DB 업데이트 에러')
        finally:
            print('후분석 중 에러 발생으로 프로그램 종료')
            sys.exit(17)


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
