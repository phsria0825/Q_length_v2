import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense


# https://www.tensorflow.org/tutorials/customization/custom_layers?hl=ko
# https://notebook.community/googleinterns/loop-project/notebooks/basic_resnet_with_custom_block
class ResBlock(Model):
    def __init__(self, units, relu_alpha=0.01, add_skip_layer=False):
        super(ResBlock, self).__init__()
        units1, units2 = units
        self.relu_alpha = relu_alpha
        self.layer1 = Dense(units1)
        self.layer2 = Dense(units2)
        self.skip_layer = Dense(units2) if add_skip_layer else None


    def skip(self, x):
        return x if self.skip_layer is None else self.skip_layer(x)


    def call(self, input_tensor, training=False):

        x = self.layer1(input_tensor)
        x = tf.nn.leaky_relu(x, self.relu_alpha)

        x = self.layer2(x)
        x += self.skip(input_tensor)
        
        return tf.nn.leaky_relu(x, self.relu_alpha)


class LstmCell_normal(Model):
    def __init__(self, units, peephole=False):
        super(LstmCell_normal, self).__init__()
        self.units = units

        self.xf = Dense(units)
        self.xi = Dense(units)
        self.xo = Dense(units)
        self.xg = Dense(units)
        self.hf = Dense(units, use_bias=False)
        self.hi = Dense(units, use_bias=False)
        self.ho = Dense(units, use_bias=False)
        self.hg = Dense(units, use_bias=False)


    def call(self, x, state, training=False):

        h, c = state
        f = tf.nn.sigmoid(self.xf(x) + self.hf(h))
        i = tf.nn.sigmoid(self.xi(x) + self.hi(h))
        o = tf.nn.sigmoid(self.xo(x) + self.ho(h))
        g = tf.nn.tanh(self.xg(x) + self.hg(h))

        c_new = (f * c) + (i * g)
        h_new = o * tf.nn.tanh(c_new)

        return h_new, [h_new, c_new]


class LstmCell(Model):
    def __init__(self, units):
        super(LstmCell, self).__init__()

        self.units = units
        self.x = Dense(units * 4)
        self.h = Dense(units * 4, use_bias=False)


    def call(self, x, state, training=False):

        h, c = state
        H = self.units
        A = self.x(x) + self.h(h)

        f = tf.nn.sigmoid(A[:, :H])
        i = tf.nn.sigmoid(A[:, H:2*H])
        o = tf.nn.sigmoid(A[:, 2*H:3*H])
        g = tf.nn.tanh(A[:, 3*H:])

        c_new = (f * c) + (i * g)
        h_new = o * tf.nn.tanh(c_new)

        return h_new, [h_new, c_new]
