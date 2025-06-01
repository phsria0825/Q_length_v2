import math
from collections import OrderedDict


class Cfg:
    distance_neighbor = 1000.0
    distance_incoming = 150.0
    distance_outgoing = 30.0
    angle = 100.0
    gap = 0.0

    interval_sec = 900
    episode_sec = 3600 * 2
    check_expected_to_leave = False
    # max_episode_num = 2
    step_size = 4

    apply_min_duration = True
    apply_max_duration = True
    apply_parent_child_relationship = False

    step_size_for_results = 5
    aggregation_size = 300

    min_time_plan_sec = 3600 * 2
    ratio_threshold = 1.2
    weight_sec = 5.0
    weight_traffic = 1.0
    max_time_plan_count = 10

    gui = False
    test_gui = True
    save_tll_hist = True
    apply_libsumo = True

    state_types = OrderedDict()
    state_types['incoming'] = ['wave', 'speed']
    state_types['internal'] = ['wave', 'speed']
    state_types['outgoing'] = ['wave', 'speed']
    
    value_types = ['wave', 'speed']

    batch_size = 128
    rnn_dim = 64
    action_embedding_dim = 64
    wave_reduced_dim = 64
    speed_reduced_dim = 64
    action_reduced_dim = 64

    GAMMA = 0.95  # 
    GAE_LAMBDA = 0.7  # 0.5, 0.7, 0.9, 0.95, 0.97, 0.99

    ratio_clipping = 0.05
    epochs = 5

    enable_lr_decay = False
    learning_rate = 0.0005
    initial_learning_rate = 0.005
    first_decay_steps = 1000
    t_mul = 1.0
    m_mul = 0.9
    min_learning_rate = 0.001  # alpha

    step_per_episode = episode_sec // step_size
    step_per_episode = math.ceil(step_per_episode / batch_size)
    step_per_episode = step_per_episode * epochs

    entropy_decay = 'linear'
    initial_coef_entropy = 0.03
    min_coef_entropy = 0.001

    norm_reward = 1.0
    clip_reward = 1.0
    coef_critic = 0.3
    enable_rnn = True
    enable_position = True

    max_reward_beta = 0.9999

    ## Available Values
    # Among these, you can choose which values to use for reward.
    # delays, 'jams', 'waits', 'decels', 'dev_speeds', 'flickering'
    reward_coefs = {}
    reward_coefs['jams'] = 1
    # reward_coefs['flickering'] = 0.05
    # reward_coefs['waits'] = 0.05
    reward_scaling_target = list(reward_coefs.keys())

    coef_sum = 0
    for target in reward_scaling_target:
        coef_sum += reward_coefs[target]
    for target in reward_scaling_target:
        reward_coefs[target] /= coef_sum

    penalty_jam = 1.1
    deceleration_threshold = -4.5  # decelerations of more than 4.5m/s^2
    neighbor_discount_factor = 1500
    tracking_reward_values = True
    enable_vehicle_values = False
