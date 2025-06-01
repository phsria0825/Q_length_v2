import os
import numpy as np

import Tools as tl
from collections import OrderedDict, Counter, defaultdict
from sklearn.metrics import adjusted_rand_score
from sklearn.cluster import AgglomerativeClustering
# from scipy.cluster.hierarchy import dendrogram, linkage


class ClusterAnalysis:
    def __init__(self, input_array, interval_sec = 900, min_time_plan_sec = 7200,
                       ratio_threshold = 1.5, weight_sec = 1.0, weight_traffic = 1.0, max_time_plan_count = 10):

        # 군집분석 대상 데이터
        self.input_array = input_array

        # 교통량 집계 구간 시간 (ex : 900초)
        self.interval_sec = interval_sec

        # 타임플랜의 최소 시간 (ex : 3600초 = 1시간)
        self.min_time_plan_sec = min_time_plan_sec

        # interval_sec를 기준으로 몇개의 단위가 필요한지
        self.min_count = int(self.min_time_plan_sec / self.interval_sec)

        # 분산이 한 번에 급격학 증가하면 중단하기 위한 임계값
        self.ratio_threshold = ratio_threshold

        # 정규화 파라미터
        self.weight_sec = weight_sec
        self.weight_traffic = weight_traffic

        # 타임플랜 최대 갯수
        self.max_time_plan_count = max_time_plan_count


    # 정규화 : x를 normalize후 weight를 곱하여 범위가 [0, weight]가 되게 함
    def _normalize(self, x, weight):

        # 최소/최대값
        x_min, x_max = min(x), max(x)

        # normalize : [0, 1]
        x_normalized = (x - x_min) / (x_max - x_min + 1e-20)

        # 스케일링(범위조정) : [0, weight]
        return x_normalized * weight


    def _normalize_array(self, input_array):

        # 정규화된 값을 저장할 공간 생성
        normalized = np.zeros_like(input_array, dtype = np.float32)

        # 시간 축 정규화
        normalized[:, 0] = self._normalize(input_array[:, 0], self.weight_sec)

        # 교통량 축 정규화
        normalized[:, 1] = self._normalize(input_array[:, 1], self.weight_traffic)

        return normalized


    def _fit(self, n_clusters, input_array):

        # linkage = {‘ward’, ‘complete’, ‘average’, ‘single’}, default=’ward’
        # affinity = {“euclidean”, “l1”, “l2”, “manhattan”, “cosine”, “precomputed”}
        # 더 자세한 내용은 https://bit.ly/2HiasAl

        # 추후 밀집도기반 군집 적용 검토 필요
        # from sklearn.cluster import DBSCAN
        # 출처: https://hyunse0.tistory.com/50

        cluster = AgglomerativeClustering(n_clusters = n_clusters,  affinity = 'euclidean', linkage = 'ward')
        labels = cluster.fit_predict(input_array).tolist()  # 리스트로 변환
        return self._redefine_labels(labels)


    def _redefine_labels(self, labels):

        '''
        input  : [1, 1, 1, 2, 2, 2, 3, 3, 3, 2, 2, 1, 1, 3, 2]
        output : [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 6]
        '''

        redefined, prev, new_labels = 0, labels[0], []
        for cur in labels:

            if cur != prev:
                redefined += 1

            new_labels.append(redefined)
            prev = cur

        return new_labels


    def _get_variance(self, labels, sequence):

        grouped = {label : [] for label in set(labels)}
        for i, label in enumerate(labels):
            grouped[label].append(sequence[i])

        variance_sum = 0.0
        for value in grouped.values():
            variance_sum += np.var(value)

        return variance_sum + 1e-20


    def _set_labels(self, input_array):

        # 가장 많은 클러스트 숫자부터 시작
        prev = None
        saved = None, None
        for n_clusters in range(self.max_time_plan_count, 0, -1):

            labels = self._fit(n_clusters, input_array)
            label_count = Counter(labels)
            variance_sum = self._get_variance(labels, input_array[:, 1])

            # 군집안에 있는 요소의 최소 갯수가 임계최소 갯수보다 작으면 패스
            if min(label_count.values()) < self.min_count:
                continue

            # 이전 값과의 비율
            ratio = 1.0 if prev is None else variance_sum / prev

            # 이전값보다 분산이 급격하게 상승했으면 이전값 리턴
            if ratio >= self.ratio_threshold:
                return saved
            
            saved = labels, n_clusters
            prev = variance_sum

        return saved


    def _get_time_plan_to_time_group(self, labels):

        time_series = self.input_array[:, 0].tolist()
        label2time_group = defaultdict(list)
        for label, time_sec in zip(labels, time_series):
            label2time_group[label].append(time_sec)

        time_plan2time_group = {}
        for value in label2time_group.values():
            begin_hhmm = tl.convert_sec_to_hhmm(value[0])
            end_hhmm = tl.convert_sec_to_hhmm(value[-1] + self.interval_sec)
            end_hhmm = end_hhmm if end_hhmm != '2400' else '0000'

            time_plan_id = f'{begin_hhmm}_{end_hhmm}'
            time_plan2time_group[time_plan_id] = value

        return time_plan2time_group


    def main(self):

        # 데이터 정규화
        array_normalized = self._normalize_array(self.input_array.astype(np.float32))

        # 클러스터링
        labels, n_clusters = self._set_labels(array_normalized)

        # 재가공
        time_plan2time_group = self._get_time_plan_to_time_group(labels)
        
        return time_plan2time_group
