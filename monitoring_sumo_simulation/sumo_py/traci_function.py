import traci
import numpy as np

norm_wave = 5
norm_wait = 120
norm_reward = 1000

clip_wave = 2
clip_wait = 2
clip_reward = 2

def simulation_step():
    traci.simulationStep()

def norm_clip_state(x, norm, clip):
    x = x / norm
    return np.clip(x, 0, clip)

def get_values(list_node_id, list_controlled_lanes):

    out_wave = []
    out_wait = []
    out_queue = []

    for node in list_node_id:

        wave = []
        wait = []
        queue = []

        for lane in list_controlled_lanes[node]:

            # wave.append(traci.lanearea.getLastStepVehicleNumber(lane))
            wave.append(traci.lanearea.getLastStepOccupancy(lane))
            queue.append(traci.lanearea.getLastStepHaltingNumber(lane))

            max_pos = 0
            car_wait = 0
            cur_cars = traci.lanearea.getLastStepVehicleIDs(lane)
            for vid in cur_cars:
                car_pos = traci.vehicle.getLanePosition(vid)
                if car_pos > max_pos:
                    max_pos = car_pos
                    car_wait = traci.vehicle.getWaitingTime(vid)
            wait.append(car_wait)
			
        wave = np.array(wave)
        wait = np.array(wait)
        queue = np.array(queue)

        out_wave.append(wave)
        out_wait.append(wait)
        out_queue.append(queue)

    return out_wave, out_wait, out_queue