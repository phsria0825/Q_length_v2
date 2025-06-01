import os
import pandas as pd
from sumolib.net import readNet
from copy import deepcopy
from collections import OrderedDict, defaultdict

from config import Cfg
from RefNetwork import RefNetwork
from DefAlongTheRoad import DefAlongTheRoad
from DefPhaseSet import DefPhaseSet
import ToolsWriteLoad as twl

import argparse


class InitNodes:
    def __init__(self):
        self.step_size = Cfg.step_size # --step-size
        self.distance_neighbor = Cfg.distance_neighbor # --dist-nei
        self.distance_incoming = Cfg.distance_incoming # --dist-inc
        self.distance_outgoing = Cfg.distance_outgoing # --dist-out
        self.angle = Cfg.angle # --angle
        self.gap = Cfg.gap # --gap
        self.apply_min_duration = Cfg.apply_min_duration
        self.apply_max_duration = Cfg.apply_max_duration
        self.neighbor_discount_factor = Cfg.neighbor_discount_factor

        # 부모자식관계 설정
        self.apply_parent_child_relationship = Cfg.apply_parent_child_relationship

        self.path_tll = os.path.join('inputs', 'before.tll.xml')
        self.path_org_network = os.path.join('inputs', 'sumo.net.xml')
        self.path_nod = os.path.join('refined', 'refined.nod.xml')
        self.path_ref_network = os.path.join('refined', 'refined.net.xml')
        self.path_segment = os.path.join('refined', 'segments4rl.add.xml')
        self.path_node = os.path.join('refined', 'nodes.pkl')

        self.state_types = Cfg.state_types
        self.movement_types = list(self.state_types.keys())


    def _refine_network(self):
        rn = RefNetwork(self.path_tll, self.path_nod, self.path_org_network, self.path_ref_network)
        rn.main()


    def _get_list_node_id(self):
        out = []
        for n in self.net.getNodes():
            if n.getType() == 'traffic_light':
                out.append(n.getID())
        return sorted(out)


    def _def_phase_set(self):
        dp = DefPhaseSet(self.path_tll, self.apply_min_duration, self.apply_max_duration)
        return dp.main()


    def _def_along_the_road(self):
        dat = DefAlongTheRoad(self.net, self.list_node_id, self.movement_types)
        return dat.main(self.distance_neighbor, self.distance_incoming, self.distance_outgoing, self.angle, self.gap)


    def _init_nodes(self):
        nodes = OrderedDict()
        for node_id in self.list_node_id:
            nodes[node_id] = {}
        return nodes


    def _set_nodes(self):

        nodes = self._init_nodes()
        for node_id in self.list_node_id:
            node = nodes[node_id]

            neighbor_info = []
            for key, value in self.dic_along_road[node_id]['neighbors'].items():
                # value = (cum_distance, list_edge_id)
                neighbor_info.append((key, value[0]))

            dic_neighbor_sorted = OrderedDict()
            for nnode_id, distance in sorted(neighbor_info):
                dic_neighbor_sorted[nnode_id] = distance

            min_steps = []
            for min_dur in self.dic_phases[node_id]['phases']['min_durs']:

                if (min_dur <= 0) or (min_dur is None):
                    min_step = 1
                else:
                    mod = divmod(min_dur, self.step_size)
                    min_step = mod[0]
                    if mod[1] != 0:
                        min_step = mod[0] + 1
                min_steps.append(min_step)

            max_steps = []
            for max_dur in self.dic_phases[node_id]['phases']['max_durs']:

                if (max_dur <= 0) or (max_dur is None):
                    max_step = 120 / self.step_size # int(1e15)
                else:
                    mod = divmod(max_dur, self.step_size)
                    max_step = mod[0]
                max_steps.append(int(max_step))

            ## neighbor
            node['neighbors'] = dic_neighbor_sorted

            ## phase related
            node['signal_states'] = self.dic_phases[node_id]['phases']['signal_states']
            node['action_dim'] = len(node['signal_states'])

            # duration
            node['min_steps'], node['max_steps'] = min_steps, max_steps
            node['red_durations'] = self.dic_phases[node_id]['phases']['red_durations']
            node['yellow_durations'] = self.dic_phases[node_id]['phases']['yellow_durations']

            # 현시의 가능한 조합
            node['phase_combinations'] = self.dic_phases[node_id]['combinations']

            # 주현시에 해당하는 인덱스
            node['critical_phase_index'] = self.dic_phases[node_id]['critical_phase_index']

            ## road related
            node['incoming'] = self.dic_along_road[node_id]['incoming']
            node['internal'] = self.dic_along_road[node_id].get('internal')
            node['outgoing'] = self.dic_along_road[node_id].get('outgoing')

        return nodes


    def _apply_parent_child_relationship(self):

        for node in self.nodes.values():
            node['parent_node_id'], node['children'] = None, OrderedDict()

        if self.apply_parent_child_relationship:
            self._merge_parent_child_relationship()


    def _is_unique(self, ls):
        '''
        리스트값의 중복이 있는지 확인
        '''
        assert len(ls) == len(set(ls))


    def _check_equal(self, ls1, ls2):
        assert ls1 == ls2


    def _merge_parent_child_relationship(self):

        # =============================================================
        # 데이터 불러옴
        # =============================================================
        d_type = {'node_id' : str, 'spot_ints_id' : str, 'majr_ints_se_cd' : str}
        node_info = pd.read_csv(os.path.join('inputs', 'node_info.csv'), dtype = d_type)

        # =============================================================
        # node_id 확인
        # =============================================================
        # 중복확인
        self._is_unique(list(node_info['node_id']))
        # 일치여부확인 (빠진 노드가 있는지 확인함)
        self._check_equal(self.list_node_id, sorted(set(list(node_info['node_id']))))

        # =============================================================
        # 부모-자식관계 정리
        # =============================================================
        list_root_id = set(node_info['spot_ints_id'])
        root2parent2children = {root_id : {'parent' : None, 'children' : []} for root_id in list_root_id}
        for index, row in node_info.iterrows():
            
            if row.majr_ints_se_cd == '1':
                # 부모가 이미 정의되어 있다면 에러(중복)
                if root2parent2children[row.spot_ints_id]['parent'] is not None:
                    raise Exception('Duplicate parent!!')
                root2parent2children[row.spot_ints_id]['parent'] = row.node_id
                
            if row.majr_ints_se_cd == '2':
                root2parent2children[row.spot_ints_id]['children'].append(row.node_id)
        
        # =============================================================
        # 병합
        # =============================================================
        for value in root2parent2children.values():

            parent_node_id = value['parent']
            for child_node_id in value['children']:
                child_node = self.nodes[child_node_id]

                # 부모노드에 자식정보 추가
                self.nodes[parent_node_id]['children'][child_node_id] = child_node
                # 자식노드에 부모id 추가
                self.nodes[child_node_id]['parent_node_id'] = parent_node_id
                # 자식노드를 nodes에서 삭제
                del self.nodes[child_node_id]

        # =============================================================
        # self.list_node_id 업데이트
        # =============================================================
        self.list_node_id = list(self.nodes.keys())
        print(self.list_node_id)

        # =============================================================
        # neighbors 업데이트
        # =============================================================
        for node_id, node in self.nodes.items():
            for child_node_id, child_node in node['children'].items():
                if child_node_id in node['neighbors']:
                    del node['neighbors'][child_node_id]
                for nnode_id, cum_distance in child_node['neighbors'].items():
                    if cum_distance < node['neighbors'].get(nnode_id, 1e+20):
                        node['neighbors'][nnode_id] = cum_distance

        # =============================================================
        # incoming, internal, outgoing 업데이트
        # =============================================================
        for node_id, node in self.nodes.items():
            for child_node_id, child_node in node['children'].items():
                for movement in self.movement_types:

                    # edge_cluster
                    # dic1.update(dic2)를 써도 상관없으나 dic2의 내용이 dic1에 덮어씌어지므로
                    # 혹시 edge에 속한 semgent가 누락되는 일이 생길까봐 아래처럼 진행함
                    for key, value in child_node[movement]['edge_cluster'].items():
                        if key in node[movement]['edge_cluster']:
                            node[movement]['edge_cluster'][key] = (node[movement]['edge_cluster'][key] | value)
                        else:
                            node[movement]['edge_cluster'][key] = value

                    # segment_cluster
                    node[movement]['segment_cluster'] += child_node[movement]['segment_cluster']
                    node[movement]['segment_cluster'] = sorted(set(node[movement]['segment_cluster']))


    def _get_embedding_info(self, node_id):
        '''https://www.tensorflow.org/api_docs/python/tf/keras/layers/Embedding'''
        node = self.nodes[node_id]
        input_dim = node['action_dim']
        max_step = max(node['max_steps'])
        return (input_dim, max_step)


    def _set_dims(self):
        # wave, speed, action
        for node_id in self.list_node_id:
            node = self.nodes[node_id]
            node['dims'] = {}
            node['dims']['wave'] = 0
            node['dims']['speed'] = 0
            node['dims']['act'] = 1
            node['dims']['act_cnt'] = 1

            for movement, types in self.state_types.items():
                for typ in types:
                    node['dims'][typ] += len(node[movement]['segment_cluster'])

            for nnode_id in node['neighbors']:
                nnode = self.nodes[nnode_id]
                node['dims']['act'] += 1
                node['dims']['act_cnt'] += 1
                for movement, types in self.state_types.items():
                    for typ in types:
                        node['dims'][typ] += len(nnode[movement]['segment_cluster'])

        for node_id in self.list_node_id:
            node = self.nodes[node_id]
            cnt = 0
            for value in node['dims'].values():
                cnt += value
            node['dims']['state_dim'] = cnt

        for node_id in self.list_node_id:
            node = self.nodes[node_id]
            node['dims']['embedding_info'] = [self._get_embedding_info(node_id)]
            for nnode_id in node['neighbors']:
                node['dims']['embedding_info'].append(self._get_embedding_info(nnode_id))


    def main(self):

        self._refine_network()
        self.net = readNet(self.path_ref_network, withInternal = True, withPrograms = True)
        self.list_node_id = self._get_list_node_id()

        self.dic_phases = self._def_phase_set()
        self.dic_along_road, strs_segments = self._def_along_the_road()

        self.nodes = self._set_nodes()
        self._apply_parent_child_relationship()
        self._set_dims()

        twl.write_txt(self.path_segment, strs_segments)
        twl.write_dic(self.path_node, self.nodes)


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('--step-size', dest = 'step_size', type = int)
    parser.add_argument('--dist-nei', dest = 'dist_nei', type = float)
    parser.add_argument('--dist-inc', dest = 'dist_inc', type = float)
    parser.add_argument('--dist-out', dest = 'dist_out', type = float)
    parser.add_argument('--angle', dest = 'angle', type = float)
    parser.add_argument('--gap', dest = 'gap', type = float)
    parser.add_argument('--apply-min-duration', dest = 'apply_min_duration', type = str2bool)
    parser.add_argument('--apply-max-duration', dest = 'apply_max_duration', type = str2bool)

    args = parser.parse_args()
    return args


def main(args):

    step_size = int(args.step_size)
    distance_neighbor = float(args.dist_nei)
    distance_incoming = float(args.dist_inc)
    distance_outgoing = float(args.dist_out)
    angle = float(args.angle)
    gap = float(args.gap)
    apply_min_duration = args.apply_min_duration
    apply_max_duration = args.apply_max_duration
       
    init_nodes = InitNodes(step_size,
                           distance_neighbor, distance_incoming, distance_outgoing, angle, gap,
                           apply_min_duration, apply_max_duration)
    init_nodes.main()


if __name__ == '__main__':
    args = parse_args()
    main(args)
