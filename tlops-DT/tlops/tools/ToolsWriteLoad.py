import pickle
import shutil
import pandas as pd


## data save and load
def write_txt(path, content):
    with open(path, 'w') as f:
        f.write(content)


def read_txt(path):
    with open(path, 'r') as f:
        return f.readline()
        
        
def write_dic(path, dic):
    with open(path, 'wb') as f:
        pickle.dump(dic, f)
        
        
def load_dic(path):
    with open(path, 'rb') as f:
        return pickle.load(f)
    
    
def copy_file(path1, path2):
    shutil.copy(path1, path2)


def write_table(path, tb):
    tb.to_csv(path, index = False)


def load_table(path):
    return pd.read_csv(path)
