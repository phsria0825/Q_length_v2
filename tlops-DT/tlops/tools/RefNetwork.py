import os
import pandas as pd

import sumolib
from sumolib.net import readNet


class RefNetwork:
    def __init__(self, path_tll, path_nod, path_org_network, path_ref_network):
        self.path_tll = path_tll
        self.path_nod = path_nod
        self.path_org_network = path_org_network
        self.path_ref_network = path_ref_network
        
        self.net = readNet(self.path_org_network, withInternal = True, withPrograms = True)
        self.tll_xml = self._load_tll_xml()
                

    def _load_tll_xml(self):
        return sumolib.xml.parse(self.path_tll, 'tlLogic')


    def _get_list_target_node_id(self):
        return [tll.id for tll in self.tll_xml]
    
    
    def _write_file(self, path, content):
        with open(path, 'w') as f:
            f.write(content)

    
    def _write_nod_xml(self):

        set_target_node_id = set(self._get_list_target_node_id())
        str_node = '    <node id="%s" type="%s" tl="%s"/>\n'
        str_nodes = ['<nodes>\n']

        for node in self.net.getNodes():
            node_id = node.getID()

            # 타겟에 해당하면 신호교차로 아니면 일반정션으로 변경
            node_type = 'traffic_light' if node_id in set_target_node_id else 'priority'
            str_nodes.append(f'    <node id="{node_id}" type="{node_type}" tl="{node_id}"/>\n')

        str_nodes.append('</nodes>')
        str_nodes = ' '.join(str_nodes)
        self._write_file(self.path_nod, str_nodes)
        
        
    def main(self):
        
        self._write_nod_xml()
        
        script = ['netconvert']
        script += ['-s', self.path_org_network]
        # script += ['--tls.discard-loaded']
        script += ['-n', self.path_nod]
        script += ['-o', self.path_ref_network]
        script = ' '.join(script)

        os.system(script)
