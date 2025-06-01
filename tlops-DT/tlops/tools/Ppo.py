import os
import numpy as np
import pandas as pd
from time import time

import tensorflow as tf
from tensorflow.keras.layers import Input
from tensorflow.keras.optimizers import Adam

from config import Cfg
from Model_test import AC
# from Model import AC
from ToolsWriteLoad import write_dic, load_dic
from scheduler import entropyScheduler

## learning_rate Scheduler
def set_learning_rate():
    if not Cfg.enable_lr_decay:
        print('learning_rate Scheduler: constant')
        return Cfg.learning_rate
    else:
        print('learning_rate Scheduler: cosineDecay per episode')
        from tensorflow.keras.optimizers.schedules import CosineDecayRestarts
        args = Cfg.initial_learning_rate, Cfg.first_decay_steps, Cfg.t_mul, Cfg.m_mul, Cfg.min_learning_rate, None
        return CosineDecayRestarts(*args)
        
        # from scheduler import CosineDecayRestartsForRL
        # return CosineDecayRestartsForRL(Cfg.initial_learning_rate, Cfg.first_decay_steps, Cfg.step_per_episode,
        #                                 Cfg.t_mul, Cfg.m_mul, Cfg.min_learning_rate, name = None)


def set_models(nodes, list_node_id):

    models, model_opts, action_dims, state_dims = {}, {}, {}, {}
    for node_id in list_node_id:
        node = nodes[node_id]

        wave_dim = node['dims']['wave']
        speed_dim = node['dims']['speed']
        action_dim = node['action_dim']
        embedding_info = node['dims']['embedding_info']
        state_dim = node['dims']['state_dim']
        rnn_dim = Cfg.rnn_dim

        # wave_dim, speed_dim, embedding_info, rnn_dim, output_dim
        model = AC(wave_dim, speed_dim, embedding_info, rnn_dim, action_dim)
        state_in = tf.zeros((1, state_dim))
        rnn_state_in = tf.zeros((1, rnn_dim * 2))
        mask_in = tf.zeros((1, action_dim), dtype = tf.int32) == 0
        model([state_in, rnn_state_in, mask_in])
        model_opt = Adam(learning_rate = set_learning_rate())
        # model.summary()
        
        models[node_id] = model
        model_opts[node_id] = model_opt
        action_dims[node_id] = action_dim
        state_dims[node_id] = state_dim

    return models, model_opts, action_dims, state_dims


## PPO 에이전트 클래스
class PPOagent(object):

    def __init__(self, scenario_id, env, nodes):

        # 환경
        self.scenario_id = scenario_id
        self.env = env
        self.time_plan_id = env.time_plan_id
        self.nodes = nodes
        self.list_node_id = list(nodes.keys())
        self.batch_size = Cfg.batch_size
        self.rnn_dim = Cfg.rnn_dim

        self.GAMMA = Cfg.GAMMA
        self.GAE_LAMBDA = Cfg.GAE_LAMBDA

        self.RATIO_CLIPPING = Cfg.ratio_clipping
        self.EPOCHS = Cfg.epochs

        self.norm_reward = Cfg.norm_reward
        self.clip_reward = Cfg.clip_reward
        self.coef_critic = Cfg.coef_critic

        model_params = (nodes, self.list_node_id)
        self.models, self.model_opts, self.action_dims, self.state_dims = set_models(*model_params)


    ## 저장된 가중치 불러오기
    def load_weights(self):
        for node_id in self.list_node_id:
            path = self._get_path_model(node_id)
            self.models[node_id].load_weights(path)


    ## 모델 가중치 저장
    def _save_weights(self):
        for node_id in self.list_node_id:
            path = self._get_path_model(node_id)
            self.models[node_id].save_weights(path)


    def _get_path_model(self, node_id):
        return os.path.join('save_weights', f'model_{self.time_plan_id}_{node_id}.h5')
        

    ## 각 에이전트로 부터 액션 가져오기
    def _get_action(self, agent_model, state, rnn_state, mask, policy_type):
        
        policy, v_value, next_rnn_state = agent_model([state, rnn_state, mask])
        policy, v_value, next_rnn_state = policy.numpy()[0], v_value.numpy()[0], next_rnn_state.numpy()[0]
        
        if policy_type != "deterministic":
            action = np.random.choice(np.arange(len(policy)), p = policy)
        else:
            action = np.argmax(np.array(policy))
            
        return action, policy, v_value, next_rnn_state


    def get_actions(self, states, rnn_states, masks, policy_type = "default"):

        actions, policys, v_values, next_rnn_states = [], [], [], []
        for i, node_id in enumerate(self.list_node_id):
            state = tf.convert_to_tensor([states[i]], dtype = tf.float32)
            rnn_state = tf.convert_to_tensor([rnn_states[i]], dtype = tf.float32)
            mask = np.reshape(masks[i], (1, -1))
            action, policy, v_value, next_rnn_state = self._get_action(self.models[node_id], state, rnn_state, mask, policy_type)
            actions.append(action)
            policys.append(policy)
            v_values.append(v_value)
            next_rnn_states.append(next_rnn_state)
        return actions, policys, v_values, next_rnn_states


    ## 모델학습
    def model_learn(self, agent_model, model_opt, action_dim, log_old_policys, states, rnn_state, masks, actions, gaes, td_targets, node_id, epoch):

        with tf.GradientTape() as tape:

            policys, td_hat, _ = agent_model([states, rnn_state, masks], training = True)

            # # debug
            # if node_id == self.list_node_id[1] and epoch in [0, 1]:
            #     print('train:', policys)
            #     print('train:', td_hat)

            # 행동 원핫인코딩
            a_one_hot = tf.one_hot(actions, action_dim)

            ## 로그폴리시와 엔트로피 계산
            # 엔트로피
            log_policys = tf.math.log(tf.clip_by_value(policys, 1e-10, 1.0))
            entropy = -tf.reduce_sum(policys * log_policys, axis = 1)
            
            # 로그 폴리시
            log_policys = tf.reduce_sum(log_policys * a_one_hot, axis = 1, keepdims = True)

            # 이전 정책과의 비율
            ratio = tf.exp(log_policys - log_old_policys)
            # debug
            # print(epoch, ratio, node_id)

            clipped_ratio = tf.clip_by_value(ratio, 1.0 - self.RATIO_CLIPPING, 1.0 + self.RATIO_CLIPPING)

            # 대리목표 함수
            surrogate = -tf.minimum(ratio * gaes, clipped_ratio * gaes)  # 그냥 clipped_ratio * gaes 이것만 쓰면 안되나?
            
            # 각 로스값들
            actor_loss   = tf.reduce_mean(surrogate)
            entropy_loss = -tf.reduce_mean(entropy) * self.coef_entropy
            critic_loss  = tf.reduce_mean(tf.square(td_hat - td_targets)) * self.coef_critic

            # 총 로스 : 각 로스값들 가중치 정해야 함
            loss = actor_loss + critic_loss + entropy_loss

        # 그래디언트 계산
        grads = tape.gradient(loss, agent_model.trainable_variables)

        # 그래디언트 클리핑
        grads, _ = tf.clip_by_global_norm(grads, 10)
        
        # 학습
        model_opt.apply_gradients(zip(grads, agent_model.trainable_variables))

        return actor_loss.numpy(), critic_loss.numpy(), entropy_loss.numpy()

        
    ## GAE, 시간차 계산
    def gae_target(self, rewards, v_values, next_v_value, done):
        n_step_targets = np.zeros_like(rewards)
        gae = np.zeros_like(rewards)
        gae_cumulative = 0
        forward_val = 0

        if not done:
            forward_val = next_v_value

        for k in reversed(range(0, len(rewards))):
            delta = rewards[k] + self.GAMMA * forward_val - v_values[k]
            gae_cumulative = self.GAMMA * self.GAE_LAMBDA * gae_cumulative + delta
            gae[k] = gae_cumulative
            forward_val = v_values[k]
            n_step_targets[k] = gae[k] + v_values[k]
        return gae, n_step_targets


    def unpack_batch(self, batch):
        unpack = batch[0]
        for idx in range(len(batch) - 1):
            unpack = np.append(unpack, batch[idx + 1], axis = 0)
        return unpack


    def init_rnn_state(self):
        return np.zeros([self.rnn_dim * 2], dtype = np.float32)


    def _init_storages(self):
        storages = {}
        for node_id in self.list_node_id:
            storages[node_id] = {}
            storage = storages[node_id]

            storage['batch'] = {}
            storage['batch']['state'] = []
            storage['batch']['action'] = []
            storage['batch']['mask'] = []
            storage['batch']['reward'] = []
            storage['batch']['log_old_policy'] = []
            storage['batch']['v_value'] = []
            storage['batch_count'] = 0

        return storages


    def _clear_storages(self):
        for node_id in self.list_node_id:
            storage = self.storages[node_id]
            for key in storage['batch'].keys():
                storage['batch'][key] = []
            storage['batch_count'] = 0


    ## 에이전트 학습
    def run(self, max_episode_num, save_tll_hist):

        episode_count = 0
        episode_rewards = []
        episode_losses = []
        max_reward = -1e+20
        es = entropyScheduler(Cfg.initial_coef_entropy, Cfg.min_coef_entropy, max_episode_num, Cfg.entropy_decay)
        print(self.time_plan_id, "starts ---")

        # 에피소드마다 다음을 반복
        while episode_count < int(max_episode_num):

            # 에피소드 시작시간
            start = time()

            # 저장소 초기화
            self.storages = self._init_storages()
            batch_count = 0

            # entropy decay
            self.coef_entropy = es.get()
            print(f'entropy decay - {es.n} : {self.coef_entropy}')

            step, episode_reward, done = 0, 0, False
            episode_actor_loss, episode_critic_loss, episode_entropy_loss = 0, 0, 0
            
            # 시뮬레이션 초기화 및 초기 상태 관측
            states, masks = self.env.reset()
            start_rnn_states = [self.init_rnn_state() for _ in self.list_node_id]
            rnn_states = start_rnn_states

            # tll_hist저장 공간
            tll_hist = []
            
            # done and batch_count == 0 이면 종료
            while (not done) or (batch_count != 0):

                # 행동, 정책, 추정된 가치, rnn_state가져오기
                actions, policys, v_values, next_rnn_states = self.get_actions(states, rnn_states, masks)
                if save_tll_hist:
                    tll_hist.append(actions)

                # 한 스텝 진행하고 다음 상태, 보상 등 가져오기
                next_states, next_masks, rewards, total_reward, done = self.env.step(actions)
                
                # 각 에이전트 들의 데이터 변환
                for i, node_id in enumerate(self.list_node_id):

                    action_dim = self.action_dims[node_id]
                    state_dim = self.state_dims[node_id]
                    storage = self.storages[node_id]

                    state = np.reshape(states[i], [1, state_dim])
                    action = np.reshape(actions[i], [1, 1])
                    v_value = np.reshape(v_values[i], [1, 1])
                    mask = np.reshape(masks[i], [1, action_dim])
                    reward = np.reshape(rewards[i], [1, 1])

                    log_old_policy = np.log(np.clip(policys[i][action], 1e-10, 1.0))
                    log_old_policy = np.reshape(log_old_policy, [1, 1])

                    train_reward = np.clip(reward / self.norm_reward, a_min = -self.clip_reward, a_max = self.clip_reward)

                    # # debug
                    # if node_id == self.list_node_id[1]:
                    #     # print(reward)
                    #     print(policys[i], v_values[i])

                    storage['batch']['state'].append(state)
                    storage['batch']['action'].append(action)
                    storage['batch']['mask'].append(mask)
                    storage['batch']['reward'].append(train_reward)
                    storage['batch']['log_old_policy'].append(log_old_policy)
                    storage['batch']['v_value'].append(v_value)
                    storage['batch_count'] += 1
                batch_count += 1

                # 배치가 채워지면 학습 진행
                # 배치에서 데이터 추출
                if batch_count >= self.batch_size:
                    batch_count = 0
                    for i, node_id in enumerate(self.list_node_id):

                        storage = self.storages[node_id]
                        agent_model = self.models[node_id]
                        model_opt = self.model_opts[node_id]
                        action_dim = self.action_dims[node_id]

                        train_states = self.unpack_batch(storage['batch']['state'])
                        train_actions = self.unpack_batch(storage['batch']['action'])
                        train_masks = self.unpack_batch(storage['batch']['mask'])
                        train_rewards = self.unpack_batch(storage['batch']['reward'])
                        train_log_old_policys = self.unpack_batch(storage['batch']['log_old_policy'])
                        train_v_values = self.unpack_batch(storage['batch']['v_value'])
                        
                        train_rnn_state = tf.convert_to_tensor([start_rnn_states[i]], dtype = tf.float32)
                        train_next_state = tf.convert_to_tensor([next_states[i]], dtype = tf.float32)
                        train_next_mask = np.reshape(next_masks[i], (1, -1))
                        train_next_rnn_state = tf.convert_to_tensor([next_rnn_states[i]], dtype = tf.float32)

                        # GAE와 시간차 타깃 계산
                        _, next_v_value, _ = agent_model([train_next_state, train_next_rnn_state, train_next_mask])
                        gaes, y_i = self.gae_target(train_rewards, train_v_values, next_v_value.numpy(), done)

                        # 에포크만큼 반복
                        for epoch in range(self.EPOCHS):
                            # 액터 신경망 업데이트
                            a, c, e = self.model_learn(agent_model, model_opt, action_dim,
                                                       tf.convert_to_tensor(train_log_old_policys, dtype = tf.float32),
                                                       tf.convert_to_tensor(train_states, dtype = tf.float32),
                                                       train_rnn_state, train_masks, train_actions.flatten(),
                                                       tf.convert_to_tensor(gaes, dtype = tf.float32),
                                                       tf.convert_to_tensor(y_i, dtype = tf.float32), node_id, epoch)
                            episode_actor_loss += a  # 
                            episode_critic_loss += c  # 
                            episode_entropy_loss += e  #

                    # 다음학습에 활용할 rnn_state저장(나중에 학습된 이후 값을 받아오게 변경 예정)
                    start_rnn_states = next_rnn_states

                    # 배치에 저장된 데이터 삭제
                    self._clear_storages()

                # 다음 스텝 준비
                states, masks, rnn_states = next_states, next_masks, next_rnn_states
                episode_reward += total_reward
                step += 1

            # 최초 리워드 저장
            if episode_count == 0:
                initial_reward_abs = abs(episode_reward)

            # 에피소드 종료 시간
            end = time()

            # 에피소드 카운트 세기
            episode_count += 1

            ## 에피소드마다 결과 출력
            print('time_plan:', self.time_plan_id, 'Episode: ', episode_count, 'Step: ', step,
                  'Rel_reward: ', round(episode_reward / initial_reward_abs, 3),
                  'Reward: ', round(episode_reward, 3), 'episode_time: ', round(end - start, 3))

            episode_rel_reward = episode_reward / initial_reward_abs

            # 누적된 리워드 지표 저장
            self.env.save_reward_values()

            # worker rewards 저장
            episode_rewards.append((self.scenario_id, episode_count, episode_reward, episode_rel_reward))
            path = os.path.join('save_weights', f'reward_{self.time_plan_id}.csv')
            df_rewards = pd.DataFrame(episode_rewards)
            df_rewards.columns = ['scenario_id', 'episode', 'reward', 'rel_reward']
            df_rewards.to_csv(path, index = False)

            # for tracking
            last_value = {'scenario_id' : self.scenario_id,
                          'time_plan_id' : self.time_plan_id,
                          'episode' : episode_count,
                          'rel_reward' : episode_rel_reward,
                          'reward' : episode_reward
                          }
            path = os.path.join('save_weights', f'reward_{self.time_plan_id}.pkl')
            write_dic(path, last_value)

            # worker loss 저장
            episode_losses.append((episode_count, episode_actor_loss, episode_critic_loss, episode_entropy_loss))  # 
            path = os.path.join('save_weights', f'loss_{self.time_plan_id}.csv') #
            df_loss = pd.DataFrame(episode_losses)  #
            df_loss.columns = ['episode', 'actor_loss', 'critic_loss', 'entropy_loss']  #
            df_loss.to_csv(path, index = False)  #

            # 리워드가 높으면 저장
            if episode_reward > max_reward:
                max_reward = episode_reward
                # tll_hist 저장
                if save_tll_hist:
                    tll_hist = pd.DataFrame(tll_hist)
                    tll_hist.columns = self.list_node_id
                    tll_hist.to_csv(os.path.join('save_tll_hist', f'{self.time_plan_id}.csv'), index_label = 'step')

                # 모델 가중치 저장
                self._save_weights()
