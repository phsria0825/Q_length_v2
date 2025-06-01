from tensorflow.keras.optimizers.schedules import LearningRateSchedule, CosineDecayRestarts
# from tensorflow.keras.experimental import CosineDecayRestarts


'''
- CosineDecayRestarts
- https://www.tensorflow.org/api_docs/python/tf/keras/optimizers/schedules/LearningRateSchedule
- https://www.tensorflow.org/api_docs/python/tf/keras/optimizers/schedules/CosineDecayRestarts
- https://github.com/keras-team/keras/blob/v2.10.0/keras/optimizers/schedules/learning_rate_schedule.py#L750-L802
'''

class CosineDecayRestartsForRL(LearningRateSchedule):
    def __init__(self, initial_learning_rate, first_decay_steps, step_per_episode,
                       t_mul = 2.0, m_mul = 1.0, alpha = 0.0, name = None):
        self.step_per_episode = step_per_episode
        self.gen = CosineDecayRestarts(initial_learning_rate, first_decay_steps, t_mul, m_mul, alpha, name)


    def __call__(self, step):
        episode = step // self.step_per_episode
        remainder = step % self.step_per_episode

        # lr check
        if remainder == 0:
            print(f'episode: {episode}  ---  step: {step}   ---   lr: {self.gen(episode).numpy()}')
            
        return self.gen(episode)


class entropyScheduler:
    def __init__(self, init_val, min_val=0, max_step=200, decay='linear'):
        self.val = init_val
        self.min_val = min_val
        self.N = max_step
        self.decay = decay
        self.n = -1

    def get(self):
        self.n += 1
        if self.decay == 'linear':
            return max(self.min_val, self.val * (1 - self.n / self.N))
        else:
            return self.val
