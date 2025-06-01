from config import Cfg
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, Softmax, Embedding, LeakyReLU, LSTMCell
from layers import LstmCell, LstmCell_normal


class AC(Model):
    def __init__(self, wave_dim, speed_dim, embedding_info, rnn_dim, output_dim):
        super(AC, self).__init__()
        
        self.wd = wave_dim
        self.sd = speed_dim
        self.rd = rnn_dim
        self.cd = len(embedding_info)  # 자기자신과 이웃의 숫자
        self.ad = Cfg.action_embedding_dim  # action embedding dim
        self.wrd = Cfg.wave_reduced_dim
        self.srd = Cfg.speed_reduced_dim
        self.ard = Cfg.action_reduced_dim

        self.softmax_temp = tf.constant(output_dim ** 0.5, dtype = tf.float32)
        self.relu_alpha = 0.01
        self.enable_position = Cfg.enable_position
        self.enable_rnn = Cfg.enable_rnn

        self.embeddings_action, tot_dim, max_step = self._set_action_lookuptable(embedding_info)
        if self.enable_position:
            self.embedding_action_count = Embedding(max_step + 1, self.ad)

        self.wave00  = Dense(self.wrd)
        self.speed00 = Dense(self.srd)
        self.act00   = Dense(self.ard)

        # rnn layer
        if self.enable_rnn:
            # self.rnncell00 = LstmCell(self.rd)
            # self.rnncell00 = LstmCell_normal(self.rd)
            self.rnncell00 = LSTMCell(self.rd)

        # outputs
        self.logit  = Dense(output_dim)
        self.policy = Softmax()
        self.value  = Dense(1)

        # state슬라이싱을 위한 인수
        self.s0 = self.wd
        self.s1 = self.wd + self.sd
        self.s2 = self.wd + self.sd + self.cd
        self.s3 = self.wd + self.sd + self.cd + self.cd

        
    def call(self, inputs, training = None):

        state, rnn_state, mask, inputs = inputs[0], inputs[1], inputs[2], None

        ## 슬라이싱
        wave  = state[:, :self.s0]  # 점유율
        speed = state[:, self.s0:self.s1]  # 속도
        act_i = state[:, self.s1:self.s2]  # 액션
        act_cnt_i = state[:, self.s2:self.s3] if self.enable_position else None
        state = None

        # 액션 임베딩 : https://www.tensorflow.org/api_docs/python/tf/math/round
        # act_i = tf.cast(tf.round(act_i), tf.int32)
        # if act_cnt_i is not None:
        #     act_cnt_i = tf.cast(tf.round(act_cnt_i), tf.int32)
        act = self._vectorize_action(act_i, act_cnt_i)
        # print(act_i, act_cnt_i)
        act_i, act_cnt_i = None, None

        wave  = self.wave00(wave)
        speed = self.speed00(speed)
        act   = self.act00(act)

        wave  = tf.nn.leaky_relu(wave, self.relu_alpha)
        speed = tf.nn.leaky_relu(speed, self.relu_alpha)
        act   = tf.nn.leaky_relu(act, self.relu_alpha)

        # xs = tf.concat([wave, speed, act], axis = -1)
        xs = wave + speed + act
        wave, speed, act = None, None, None

        if self.enable_rnn:
            rnn_state = [rnn_state[:, :self.rd], rnn_state[:, self.rd:]]
            xs, rnn_state = self._get_rnn_forward(self.rnncell00, xs, rnn_state, training)

        policy = self.policy(self.logit(xs) / self.softmax_temp, mask)
        v_value = self.value(xs)

        return policy, v_value, tf.concat(rnn_state, axis = -1)


    def _set_action_lookuptable(self, embedding_info):

        max_step = -1
        embeddings = []
        tot_dim = self.wd + self.sd

        for i, m in embedding_info:
            embeddings.append(Embedding(i, self.ad))
            tot_dim += self.ad
            if m > max_step:
                max_step = m
        return embeddings, tot_dim, max_step


    def _vectorize_action(self, act_i, act_cnt_i = None):
        act = []
        for i, emb in enumerate(self.embeddings_action):

            # 액션 임배딩
            action_vector = emb(act_i[:, i])

            # positional encoding
            if act_cnt_i is not None:
                position_vector = self.embedding_action_count(act_cnt_i[:, i])
                act.append(action_vector + position_vector)
            else:
                act.append(action_vector)

        return tf.concat(act, axis = -1)


    def _get_rnn_forward(self, rnncell, xs, rnn_state, training):
        xs = tf.expand_dims(xs, 0)
        outs = []
        for x in tf.unstack(xs, axis = 1):
            out, rnn_state = rnncell(x, rnn_state, training)
            outs.append(out)
        outs = tf.concat(outs, axis = 0)  # tf.stack(outs, axis = 1)
        return outs, rnn_state
