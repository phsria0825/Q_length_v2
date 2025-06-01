##############
###
### 7. 이동가능경로 파일 생성 (박산하 과장님 코드)
###
#############

import sumolib
from copy import copy
import argparse

from tqdm import tqdm

### python gendata/PossibleRoutesGenerator.py -n inch_all.net.xml -o scenario_id/temp/possible_routes.xml -d 500
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--network-file', '-n', dest = 'net')
    parser.add_argument('--output-file', '-o', dest = 'out')
    parser.add_argument('--distance_threshold', '-d', dest = 'dist')
    
    args = parser.parse_args()
    
    return args


class PossibleRoutesGenerator:
    def __init__(self, path_network, path_output, distance_threshold = 100.0):
        self.path_network = path_network
        self.path_output = path_output
        self.distance_threshold = float(distance_threshold)

        self.str_route = '  <route id="%s" edges="%s"/>\n'

        self.load_sumolib_net()
        self.get_intersection_id_list()


##############
###
### 7-1. Sumo네트워크 불러오기
###
#############
    def load_sumolib_net(self):
        self.net = sumolib.net.readNet(
            self.path_network,
            withPrograms = True,
            withConnections = True,
            withFoes = True,
            withInternal = True
        )


##############
###
### 7-2. 신호교차로 node_id 리스트 생성
###
#############
    def get_intersection_id_list(self):
        intersection_id_list = []
        for node in self.net.getNodes():
            if node.getType() == 'traffic_light':
                intersection_id_list.append(node.getID())
        self.intersection_id_set = set(intersection_id_list)
        

##############
###
### 7-3. 반복문으로 차량이 갈 수 있는 경로 지정
###
#############
    # 깊이 우선 검색
    def depth_first_search(self, edge, routes, route, visited, cum_distance):
        
        # 경로 제한 거리보다 길면 경로 탐색 종료
        if cum_distance > self.distance_threshold:
            routes.append(' '.join(route))
            return
        
        # 다음 엣지리스트 확인
        next_edges = []
        for next_edge in edge.getOutgoing().keys():
            if next_edge.getID() not in visited:
                next_edges.append(next_edge)
        
        # 다음에 갈 엣지가 없으면 종료
        if not next_edges:
            routes.append(' '.join(route))
            return
        
        # 각 엣지로 이동하며 경로 탐색
        for next_edge in next_edges:

        	# 경로와 방문기록, 누적이동거리 복사
            route_c, visited_c = copy(route), copy(visited)
            cum_distance_c = cum_distance
            
            next_edge_id = next_edge.getID()
            internal_edge = self.get_internal_edge(edge, next_edge)
            
            visited_c.add(next_edge_id) # 방문리스트에 다음엣지 추가
            route_c.append(next_edge_id) # 경로에 다음엣지 추가
            cum_distance_c += internal_edge.getLength() # 중간엣지 길이 추가
            cum_distance_c += next_edge.getLength() # 엣지 길이 추가
            
            # 다음엣지로 이동하여 탐색
            self.depth_first_search(next_edge, routes, route_c, visited_c, cum_distance_c)
            

    def get_internal_edge(self, from_edge, to_edge):
        shortest_path = self.net.getShortestPath(from_edge, to_edge, withInternal = True)
        internal_edge = shortest_path[0][1]
        assert internal_edge.getFunction() == 'internal'
        return internal_edge
    
    
    def get_downstream_edges(self, edge):
        distance = self.distance_threshold / 2
        stopOnTLS = True
        stopOnTurnaround = False
        out = self.net.getDownstreamEdges(edge, distance, stopOnTLS, stopOnTurnaround)
        return out
    
    
    # 출발지 탐색
    def get_origins(self, edge):
        ds_results = self.get_downstream_edges(edge)
        origins = []
        for ds in ds_results:
            from_node = ds[0].getFromNode()
            from_node_id = from_node.getID()

            if from_node_id not in self.intersection_id_set:
                # 종료지점이 신호교차로가 아니라면 마지막 엣지를 출발엣지 목록에 추가
                candi = ds[0]
                while ds[2] and candi.getFunction() == 'internal':
                    candi = ds[2].pop()
                origins.append(candi)
            else: # 종료지점이 신호교차로라면 중간지점의 엣지를 출발엣지 목록에 추가
                distance_limit = ds[1] / 2.0
                cum_distance = 0.0
                for candi in ds[2]:
                    cum_distance += candi.getLength()
                    if candi.getFunction() != 'internal':
                        if cum_distance >= distance_limit:
                            origins.append(candi)
                            break
        return origins
            

    def get_incoming_edges(self, node):
        outs = []
        for edge in node.getIncoming():
            if edge.getFunction() != 'internal':
                outs.append(edge)
        return outs
    
    
    def get_possible_routes(self):
        origins = []
        for node_id in self.intersection_id_set:
            node = self.net.getNode(node_id)
            outgoing_edges = self.get_incoming_edges(node)
            for edge in outgoing_edges:
                origins += self.get_origins(edge)

        index = 0

        for origin in tqdm(set(origins)):
            routes = []
            strs = ''

            edge_id = origin.getID()
            route = [edge_id]
            visited = {edge_id}
            cum_distance = origin.getLength()
            self.depth_first_search(origin, routes, route, visited, cum_distance)

            for route in routes:
                index += 1
                strs += self.str_route % (str(index), route)
            
            # index += 1
            self.re_write_file(self.path_output, strs)

            # print(routes[0])
            # input()

        return routes


    def re_write_file(self, path, content):
        with open(path, 'a') as f:
            f.write(content)
    
    
    def write_file(self, path, state='start'):
        
        if state.lower() == 'start':
            with open(path, 'w') as f:
                f.write('<routes>\n')
        elif state.lower() == 'end':
            with open(path, 'a') as f:
                f.write('</routes>')
        
##############
###
### 7-4. 이동가능경로(possible routes.xml) 파일 생성
###
#############    
    def save_possible_routes(self):
        self.write_file(self.path_output, state='start')
        self.get_possible_routes()
        self.write_file(self.path_output, state='end')
        # possible_routes = self.get_possible_routes(f)
        # str_route = '  <route id="%s" edges="%s"/>\n'
        
        # strs = '<routes>\n'
        # for i, route in enumerate(possible_routes):
        #     strs += str_route % (str(i), route)
        # strs += '</routes>'
        
        # self.write_file(self.path_output, strs)
        
        
def main(agrs):
    
    path_network = args.net
    path_output = args.out
    distance_threshold = args.dist
    
    prg = PossibleRoutesGenerator(path_network, path_output, distance_threshold)
    prg.save_possible_routes()
    
if __name__=="__main__":
    args = parse_args()
    main(args)