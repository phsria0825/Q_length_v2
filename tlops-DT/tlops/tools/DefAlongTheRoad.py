import math
import numpy as np
from copy import copy
from collections import defaultdict
from sumolib.miscutils import euclidean


class DefAlongTheRoad:
    def __init__(self, net, list_node_id, movement_types = None):
        self.net = net
        self.list_node_id = list_node_id
        self.set_node_id = set(list_node_id)
        self.movement_types = self._def_movement_types(movement_types)
        self.str_segment = '  <laneAreaDetector file="NUL" freq="86400" id="%s" lane="%s"  endPos="%s"  pos="%s"/>\n'
        self.min_segment_length = 0.1


    def _def_movement_types(self, movement_types):
        if movement_types is None:
            return ['incoming', 'outgoing', 'internal']
        return movement_types


    def _extract_id(self, objs):
        out = []
        for obj in objs:
            out.append(obj.getID())
        return out
        
        
    def _get_connected_edges_from_node(self, node, movement):

        connected_edges = []
        if movement == 'incoming':
            connected_edges = node.getIncoming()

        if movement == 'outgoing':
            connected_edges = node.getOutgoing()

        out = []
        for connected_edge in connected_edges:
            if connected_edge.getFunction() == 'internal':
                continue
            out.append(connected_edge)
        return out


    def _get_outgoing_edges_from_edge(self, edge):
        outgoing, internal = [], []
        for outgoing_edge in edge.getOutgoing():
            outgoing.append(outgoing_edge)
            internal_road, _ = self.net.getInternalPath(edge.getConnections(outgoing_edge))
            if internal_road is not None:
                for internal_edge in internal_road:
                    internal.append(internal_edge)                       

        if internal:
            return internal

        return outgoing
    
    
    def _get_incoming_edges_from_edge(self, edge):
        incoming, internal = [], []
        for incoming_edge in edge.getIncoming():
            if incoming_edge.getFunction() == 'internal':
                internal.append(incoming_edge)
            else:
                incoming.append(incoming_edge)
                
        if internal:
            return internal
        
        return incoming


    def _get_connected_edges_from_edge(self, edge, movement):

        if movement == 'incoming':
            return self._get_incoming_edges_from_edge(edge)

        if movement == 'outgoing':
            return self._get_outgoing_edges_from_edge(edge)

        return []


    def _get_direction_between_edges(self, from_edge, to_edge):
        conns = from_edge.getConnections(to_edge)
        if conns:
            return conns[0].getDirection().lower()
        return None


    def _get_next_node_id(self, edge, movement):
        if movement == 'incoming':
            return edge.getFromNode().getID()
        elif movement == 'outgoing':
            return edge.getToNode().getID()
        else:
            return None


    def _get_roads_from_edge(self, edge, distance, movement = 'outgoing'):
        roads, road = [], []
        visited = set()
        cum_distance = 0.0
        self._depth_first_search(edge, roads, road, visited, cum_distance, distance, movement)
        return roads # (road, cum_distance, next_node_id, is_traffic_light, done)


    def _depth_first_search(self, edge, roads, road, visited, cum_distance, distance, movement):

        edge_id = edge.getID()
        next_node_id = self._get_next_node_id(edge, movement)
        if next_node_id is None:
            return

        if edge_id in visited:
            return
        visited.add(edge_id)
        road.append(edge)

        cum_distance += edge.getLength()
        if cum_distance > distance:
            roads.append((road, cum_distance, next_node_id, False, True))
            return

        if next_node_id in self.set_node_id:
            roads.append((road, cum_distance, next_node_id, True, False))
            return

        next_edges = self._get_connected_edges_from_edge(edge, movement)
        if not next_edges:
            roads.append((road, cum_distance, next_node_id, False, False))
            return

        for next_edge in next_edges:

            if movement == 'incoming':
                if self._get_direction_between_edges(next_edge, edge) == 't':
                    continue
            elif movement == 'outgoing':
                if self._get_direction_between_edges(edge, next_edge) == 't':
                    continue
            else:
                continue

            road_c, visited_c = copy(road), copy(visited)

            self._depth_first_search(next_edge, roads, road_c, visited_c, cum_distance, distance, movement)


    def get_neighbors(self, distance):
        dic_neighbors = {node_id : {} for node_id in self.list_node_id}
        for node_id in self.list_node_id:
            node = self.net.getNode(node_id)
            coord1 = node.getCoord()
            incoming_edges = self._get_connected_edges_from_node(node, movement = 'incoming')
            for incoming_edge in incoming_edges:
                incoming_roads = self._get_roads_from_edge(incoming_edge, distance, movement = 'incoming')
                for incoming_road in incoming_roads:

                    # (incoming_road, cum_distance, first_node_id, is_traffic_light)
                    if not incoming_road[3]:
                        continue

                    cum_distance = incoming_road[1]
                    first_node_id = incoming_road[2]
                    coord2 = self.net.getNode(first_node_id).getCoord()

                    if first_node_id == node_id:
                        continue

                    if first_node_id not in self.set_node_id:
                        continue

                    if (euclidean(coord1, coord2) / cum_distance) <= 0.9:
                        continue

                    list_edge_id = self._extract_id(incoming_road[0])
                    if first_node_id not in dic_neighbors[node_id]:
                        dic_neighbors[node_id][first_node_id] = (cum_distance, list_edge_id)
                    else:
                        if cum_distance < dic_neighbors[node_id][first_node_id][0]:
                            dic_neighbors[node_id][first_node_id] = (cum_distance, list_edge_id)
        return dic_neighbors


    def _get_dic_internal_info(self):
        # dic = {node_id : {} for node_id in self.list_node_id}
        infos = []
        for node_id in self.list_node_id:
            node = self.net.getNode(node_id)
            
            for edge in self.net.getEdges():
                if (edge.getFromNode() == node) and (edge.getToNode() == node):
                    edge_id = edge.getID()
                    for lane in edge.getLanes():
                        lane_id = lane.getID()
                        # node_id, movement, edge_id, lane_id, end_pos, pos, all_covered
                        infos.append((node_id, 'internal', edge_id, lane_id, lane.getLength(), 0, True))
        return infos


    def _get_dic_info(self, movement, distance = 200.0, angle = 100.0):
        # output : (node_id, movement, edge_id, lane_id, end_pos, pos)

        if movement == 'internal':
            return self._get_dic_internal_info()

        infos = []
        for node_id in self.list_node_id:
            node = self.net.getNode(node_id)
            for edge in self._get_connected_edges_from_node(node, movement):
                edge_id = edge.getID()
                for road, cum_dist, _, _, done in self._get_roads_from_edge(edge, distance, movement):
                    last_edge = road[-1]
                    cum_angle = 0.0
                    for e in road:
                        if cum_angle >= angle:
                            break
                        cum_angle += self._get_road_angle_diff(e)

                        for lane in e.getLanes():

                            # 마지막 엣지가 아니거나 distance에 도달하지 못했을 때
                            if (e != last_edge) or (not done):
                                end_pos, pos, all_covered = lane.getLength(), 0, True
                            else: # (e == last_edge) and (done)
                                if movement == 'incoming':
                                    end_pos, pos, all_covered = lane.getLength(), cum_dist - distance, False
                                elif movement == 'outgoing':
                                    end_pos, pos, all_covered = lane.getLength() - (cum_dist - distance), 0, False

                            lane_id = lane.getID()
                            infos.append((node_id, movement, edge_id, lane_id, end_pos, pos, all_covered))
        return infos


    def _get_road_angle_diff(self, edge):

        cum_diff = 0.0
        edge_shape = edge.getShape()
        if len(edge_shape) <= 2:
            return cum_diff

        v1 = np.array((0, 1))
        v2 = np.array(edge_shape[1]) - np.array(edge_shape[0])
        prev_angle = self._get_angle(v1, v2)
        
        for i in range(2, len(edge_shape)):
            v2 = np.array(edge_shape[i]) - np.array(edge_shape[i - 1])
            angle = self._get_angle(v1, v2)
            cum_diff += abs(angle - prev_angle)
            prev_angle = angle

        return cum_diff


    def _get_angle(self, v1, v2):
        dot_product = np.dot(v1, v2)

        v1_len = np.sqrt(np.sum(v1 ** 2))
        v2_len = np.sqrt(np.sum(v2 ** 2))

        cosine = dot_product / v1_len / v2_len
        cosine = np.clip(cosine, a_min = -1.0, a_max = 1.0)
        angle = math.acos(cosine) * (180.0 / np.pi)

        return angle


    def main(self, distance_neighbor, distance_incoming, distance_outgoing, angle, gap = 0.0):

        infos = []
        # movement, distance = 200.0, angle = 100.0
        if 'incoming' in self.movement_types:
            infos += self._get_dic_info('incoming', distance_incoming)

        if 'outgoing' in self.movement_types:
            infos += self._get_dic_info('outgoing', distance_outgoing)

        if 'internal' in self.movement_types:
            infos += self._get_dic_info('internal')
        
        dic = {node_id : {} for node_id in self.list_node_id}
        for node_id in self.list_node_id:
            for movement in self.movement_types:
                dic[node_id][movement] = {}
                dic[node_id][movement]['edge_cluster'] = defaultdict(set)
                dic[node_id][movement]['segment_cluster'] = set()

        segment_info = {}
        for node_id, movement, edge_id, lane_id, end_pos, pos, all_covered in infos:

            pos = pos + gap
            segment_length = end_pos - pos
            if segment_length <= self.min_segment_length:
                continue

            if all_covered:
                segment_id = lane_id
            else:
                segment_id = f'{lane_id}_endpos{round(end_pos, 3)}_pos{round(pos, 3)}'

            if segment_id not in segment_info:
                segment_info[segment_id] = (segment_length, lane_id, end_pos, pos)            

            dic[node_id][movement]['edge_cluster'][edge_id].add(segment_id)
            dic[node_id][movement]['segment_cluster'].add(segment_id)

        # ['edge_cluster', 'segment_cluster']
        for node_id in dic.keys():
            for movement in self.movement_types:  # [incoming, internal, outgoing]
                for key, value in dic[node_id][movement].items():
                    if key == 'segment_cluster':
                        dic[node_id][movement][key] = sorted(list(value))

        for key, value in self.get_neighbors(distance_neighbor).items():
            dic[key]['neighbors'] = value

        strs = ['<additional>\n']
        for key, value in segment_info.items():
            segment_length, lane_id, end_pos, pos = segment_info[key]
            strs.append(self.str_segment % (key, lane_id, end_pos, pos))
        strs.append('</additional>')
    
        return dic, ''.join(strs)