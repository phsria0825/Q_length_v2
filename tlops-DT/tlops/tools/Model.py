from config import Cfg
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, Softmax, Embedding, LSTMCell, LeakyReLU

# import tensorflow_addons as tfa
# from tensorflow_addons.rnn import PeepholeLSTMCell


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

        self.softmax_temp = tf.constant(output_dim, dtype = tf.float32)
        self.relu_alpha = 0.01  # Swish를 써보는건 어떨까?
        self.enable_position = Cfg.enable_position
        self.enable_rnn = Cfg.enable_rnn

        self.embeddings_action, tot_dim, max_step = self._set_action_lookuptable(embedding_info)
        if self.enable_position:
            self.embedding_action_count = Embedding(max_step + 1, self.ad)

        # 차원축소 : activation을 빼면?
        self.wave00  = Dense(self.wrd, activation = 'linear')
        self.speed00 = Dense(self.srd, activation = 'linear')
        self.act00   = Dense(self.ard, activation = 'linear')

        # 아래는 wave, speed, act의 res_block 을 위함
        self.wave01  = Dense(self.wrd * 2, activation = 'linear')
        self.wave02  = Dense(self.wrd, activation = 'linear')

        self.speed01 = Dense(self.srd * 2, activation = 'linear')
        self.speed02 = Dense(self.srd, activation = 'linear')

        self.act01   = Dense(self.ard * 2, activation = 'linear')
        self.act02   = Dense(self.ard, activation = 'linear')

        # rnn
        # 검토 필요사항 : GRU 적용 / rnn layer 층 수를 늘리기
        if self.enable_rnn:
            # self.rnncell00 = PeepholeLSTMCell(self.rd)
            self.rnncell00 = LSTMCell(self.rd)
        self.hidden00 = Dense(self.rd, activation = LeakyReLU(alpha = self.relu_alpha))

        # hidden before outputs
        self.pout01 = Dense(self.rd * 2, activation = 'linear')
        self.pout02 = Dense(self.rd, activation = 'linear')

        self.vout01 = Dense(self.rd * 2, activation = 'linear')
        self.vout02 = Dense(self.rd, activation = 'linear')

        # outputs
        self.logit  = Dense(output_dim, activation = 'linear')
        self.policy = Softmax()
        self.value  = Dense(1, activation = 'linear')

        # state슬라이싱을 위한 인수
        self.s0 = self.wd
        self.s1 = self.wd + self.sd
        self.s2 = self.wd + self.sd + self.cd
        self.s3 = self.wd + self.sd + self.cd + self.cd

        
    def call(self, inputs, training = None):

        state, rnn_state, mask, inputs = inputs[0], inputs[1], inputs[2], None
        rnn_state = [rnn_state[:, :self.rd], rnn_state[:, self.rd:]]

        ## 슬라이싱
        wave  = state[:, :self.s0]  # 점유율
        speed = state[:, self.s0:self.s1]  # 속도
        act_i = state[:, self.s1:self.s2]  # 액션
        act_cnt_i = state[:, self.s2:self.s3] if self.enable_position else None
        state = None

        # 액션 임베딩 : https://www.tensorflow.org/api_docs/python/tf/math/round
        act_i = tf.cast(tf.round(act_i), tf.int32)
        if act_cnt_i is not None:
            act_cnt_i = tf.cast(tf.round(act_cnt_i), tf.int32)
        act = self._vectorize_action(act_i, act_cnt_i)
        act_i, act_cnt_i = None, None

        # print('=============')
        # print('wave', wave)
        # print('speed', speed)
        # print('act_i', act_i)
        # print('act_cnt_i', act_cnt_i)
        # print('=============')

        wave  = self.wave00(wave)
        speed = self.speed00(speed)
        act   = self.act00(act)

        # wave  = tf.nn.leaky_relu(wave, self.relu_alpha)
        # speed = tf.nn.leaky_relu(speed, self.relu_alpha)
        # act   = tf.nn.leaky_relu(act, self.relu_alpha)

        wave  = self._res_block(wave, self.wave01, self.wave02, self.relu_alpha)
        speed = self._res_block(speed, self.speed01, self.speed02, self.relu_alpha)
        act   = self._res_block(act, self.act01, self.act02, self.relu_alpha)

        xs = tf.concat([wave, speed, act], axis = -1)
        wave, speed, act = None, None, None
        xsh = self.hidden00(xs)

        if self.enable_rnn:
            rnn_out, rnn_state = self._get_rnn_forward(self.rnncell00, xs, rnn_state, training)
            out = rnn_out + xsh
            xs, rnn_out = None, None
        else:
            out = xsh
        
        pout = self._res_block(out, self.pout01, self.pout02, self.relu_alpha)
        vout = self._res_block(out, self.vout01, self.vout02, self.relu_alpha)
        out, xsh = None, None

        logit = self.logit(pout)
        policy = self.policy(logit / self.softmax_temp, mask)
        v_value = self.value(vout)

        # print('=============')
        # print('policy', policy)
        # print('v_value', v_value)
        # print('=============')

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


    def _res_block(self, x, layer1, layer2, relu_alpha = 0.2):

        # 첫 번째 레이어
        fx = layer1(x)
        fx = tf.nn.leaky_relu(fx, relu_alpha)

        # 두 번째 레이어 
        fx = layer2(fx)

        return tf.nn.leaky_relu(x + fx, relu_alpha)
