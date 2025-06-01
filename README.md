# Traffic-Light-Optimization
## Multi-Agent Reinforcement Learning
PPO : Proximal policy optimization algorithms


## Requirements
* Anaconda(https://www.anaconda.com/products/distribution#download-section)
* Python==3.8.13
* Tensorflow==2.8.0
* Sumo==1.14.1 (https://www.eclipse.org/sumo/)

  
## Usages
1. 환경설정
~~~
## 가상환경 생성 코드
conda create -n rl_signal python=3.8

## 가상환경 활성화 
activate rl_signal 

## 필요 패키지 설치 
python -m pip install --upgrade pip
pip install tensorflow-cpu==2.7
pip install tensorflow-addons
pip install sumolib 
pip install pandas
pip install tqdm
pip install sklearn
pip install matplotlib
pip install pyodbc
~~~

2. Config 설정
~~~
# tools/config.py에서 gui = True를 하시면 시뮬레이션 스크린을 띄울 수 있습니다.
# 주요항목만 표시함

# 학습횟수
max_episode_num = 200

# 최대/최소 녹색시간 반영 설정
apply_min_duration = False
apply_max_duration = False

# 배치사이즈
batch_size = 128

# LSTM의 유닛 수
rnn_dim = 128

# 시뮬레이션 스크린 온/오프
gui = True
~~~

3. train
~~~
# python trainer.py -s [시나리오 폴더 이름]
python trainer.py -s ver_sample
~~~

4. test
~~~
# python tester.py -s [시나리오 폴더 이름]
python tester.py -s ver_sample
~~~
