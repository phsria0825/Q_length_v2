import os


## SetWorkingDirectory
def make_dirs():
    os.makedirs(os.path.join('outputs'), exist_ok = True)
    os.makedirs(os.path.join('intermediate'), exist_ok = True)
    os.makedirs(os.path.join('save_state'), exist_ok = True)
    os.makedirs(os.path.join('save_weights'), exist_ok = True)
    os.makedirs(os.path.join('refined'), exist_ok = True)
    os.makedirs(os.path.join('save_tll_hist'), exist_ok = True)
