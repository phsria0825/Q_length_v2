import os
from multiprocessing import Process
import ToolsWriteLoad as twl

import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-type', '-t', dest = 'rtype', type = str)
    parser.add_argument('--iteration', '-i', dest = 'iteration', type = int, default = 200)
    return parser.parse_args()


def train(time_plan_id, begin_sec, args=None):
    print('train start:', time_plan_id)
    os.system(f'python Train.py -t {time_plan_id} -b {begin_sec} -i {args.iteration}')


def test(time_plan_id, begin_sec, args=None):
    print('test start:', time_plan_id)
    os.system(f'python Test.py -t {time_plan_id} -b {begin_sec}')


def main(args):

    if args.rtype == 'train':
        target_fun = train
    else:
        target_fun = test

    workers = []
    time_plan_with_begin_sec = twl.load_dic(os.path.join('outputs', 'time_plan_with_begin_sec.pkl'))
    for time_plan_id, value in time_plan_with_begin_sec.items():
        worker = Process(target = target_fun, args = (time_plan_id, value['train_begin_sec'], args,))
        workers.append(worker)

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()


if __name__ == '__main__':
    args = parse_args()
    main(args)
