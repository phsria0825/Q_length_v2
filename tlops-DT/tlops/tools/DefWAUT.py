# https://sumo.dlr.de/docs/Simulation/Traffic_Lights.html#defining_program_switch_times_and_procedure
import os
import sumolib
from collections import defaultdict

import Tools as tl
import ToolsWriteLoad as twl


class DefWAUT:
    def __init__(self, path_tll, is_before):
        self.path_tll = path_tll
        self.when = 'before' if is_before else 'after'


    def _load_tll_xml(self, path):
        return sumolib.xml.parse(path, 'tlLogic')


    def main(self):

        dic = defaultdict(list)
        strs = ['<additional>\n\n']

        for tl_logic in self._load_tll_xml(self.path_tll):
            node_id = tl_logic.id
            time_plan_id = tl_logic.programID
            begin_sec, _ = tl.split_time_plan_id_to_sec(time_plan_id)
            dic[node_id].append((begin_sec, time_plan_id))

        for key, value in dic.items():
            strs.append(f'  <WAUT startProg="0" refTime="0" id="{key}">\n')
            for begin_sec, time_plan_id in sorted(value, key = lambda x : x[0]):
                strs.append(f'    <wautSwitch to="{time_plan_id}" time="{begin_sec}"></wautSwitch>\n')
            strs.append('  </WAUT>\n\n')

        for key, value in dic.items():
            strs.append(f'  <wautJunction junctionID="{key}" wautID="{key}"></wautJunction>\n')
        strs.append('\n</additional>')

        twl.write_txt(os.path.join('refined', f'waut_{self.when}.add.xml'), ''.join(strs))
