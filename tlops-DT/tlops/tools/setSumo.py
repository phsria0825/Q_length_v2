import os
import sys

from sumolib import checkBinary


def set_sumo(path_network, path_routes,
             path_tll = None, path_waut = None,
             path_e2 = None, path_vtype = None,
             begin_sec = 0, gui = False):

    if 'SUMO_HOME' in os.environ:
        tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
        sys.path.append(tools)
    else:
        sys.exit("please declare environment variable 'SUMO_HOME'")

    sumo_cmd = [checkBinary('sumo-gui' if gui else 'sumo')]
    sumo_cmd.append(f'-n {path_network}')
    sumo_cmd.append(f'-r {path_routes}')
    sumo_cmd.append(f'-b {begin_sec}')

    path_additional = []
    if path_tll is not None:
        path_additional.append(path_tll)

    if path_waut is not None:
        path_additional.append(path_waut)

    if path_e2 is not None:
        path_additional.append(path_e2)

    if path_vtype is not None:
        path_additional.append(path_vtype)

    if path_additional:
        path_additional = ','.join(path_additional)
        sumo_cmd.append(f'-a {path_additional}') 

    return sumo_cmd  # ' '.join(sumo_cmd)
