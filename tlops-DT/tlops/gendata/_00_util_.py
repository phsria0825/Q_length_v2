##############
###
### 9. 공통적으로 활용되는 함수들이 정의되어 있는 파일
###
#############


import pandas as pd
import datetime
import os
import numpy as np
from numpy import nan as NA


class util_():
    def __init__(self):
        self.net_file = 'refined_02.net.xml'
        self.unix_verUI = 0 
        # self.unix_verUI = 1435687200 # 2015년 07월 01일 00시 00분
        self.KST = datetime.timezone(datetime.timedelta(hours=9))

    def str2time(self, string):
        if type(string) == str:
            if string.find('-') != -1:
                return datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
                # return datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S").astimezone( self.KST)
            else:
                return datetime.datetime.strptime(string, "%Y%m%d%H%M%S")
                # return datetime.datetime.strptime(string, "%Y%m%d%H%M%S").astimezone( self.KST)
        elif type(string) == pd.Timestamp:
            return datetime.datetime.strptime(str(string), "%Y-%m-%d %H:%M:%S")
            # return datetime.datetime.strptime(str(string), "%Y-%m-%d %H:%M:%S").astimezone( self.KST)

    def time2str(self, time):
        import re
        return re.sub(r"[^0-9]","",str(time))

    def str2unixtime(self, string):
        if type(string) == str:
            if string.find('-') != -1:
                if string.find('.') != -1:
                    string_ymdhms = datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
                else:
                    string_ymdhms = datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S.%f")
            else:
                string_ymdhms = datetime.datetime.strptime(string, "%Y%m%d%H%M%S")
        elif type(string) == pd.Timestamp:
            string_ymdhms = datetime.datetime.strptime(str(string), "%Y-%m-%d %H:%M:%S")
        elif type(string) == datetime.datetime:
            string_ymdhms = string
        else:
            return None

        print(string_ymdhms)

        string_ymdhms = string_ymdhms.timestamp()
        # string_ymdhms = string_ymdhms.astimezone( self.KST).timestamp()

        return string_ymdhms - self.unix_verUI

    def unixtime2ymd(self, unix_time):
        if type(unix_time) != float:
            unix_time = float(unix_time)
        
        stdr_time = self.time2str(datetime.datetime.fromtimestamp(unix_time))[:8]
        
        return stdr_time

    def unixtime2time(self, unix_time):
        if type(unix_time) != float:
            unix_time = float(unix_time)

        stdr_time = datetime.datetime.fromtimestamp(unix_time)
        # stdr_time = self.time2str(datetime.datetime.fromtimestamp(unix_time))
            
        return stdr_time



    ## 시간 구분하는 함수
    def get_datetime_list(self, begin, end, interval_sec = 300):
        if end is None:
            end = begin

        begin = self.str2time(begin)
        end = self.str2time(end)

        if begin == end:
            end += datetime.timedelta(seconds = interval_sec)

        assert end >= begin
        
        cur = begin
        target_list = []
        
        while cur <= end:
            cur_time = datetime.datetime.strftime(cur, '%Y%m%d%H%M%S')
            target_list.append(cur_time)
            cur += datetime.timedelta(seconds = interval_sec)
        return target_list


    def mk_path(self, *path):    # os.path.join() 이라는 함수가 이미 있었음
        dir_path = ''
        for i in path:
            dir_path = dir_path + i + '/'
        return dir_path[:-1]


    def createFolder(self, directory):
        # tm = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        # directory = os.path.join(directory + '_' + tm)
        os.makedirs(directory, exist_ok=True)
        return directory


    # def delFolder(self, directory):
    #     os.makedirs(directory, exist_ok=True)
    #     return directory

    def mk_dir(self, base_dir, ymd_dir):
        dir_list = []
        
        base_1h_dir = os.path.join(base_dir, '1h', ymd_dir)

        base_5m_meta_act_ymd_dir = os.path.join(base_dir, '5m', 'meta_poc', 'actual_measure', ymd_dir)
        base_5m_meta_pre_ymd_dir = os.path.join(base_dir, '5m', 'meta_poc', 'predictive_measure', ymd_dir)
        base_5m_real_act_ymd_dir = os.path.join(base_dir, '5m', 'real_time', 'actual_measure', ymd_dir)
        base_5m_real_pre_ymd_dir = os.path.join(base_dir, '5m', 'real_time', 'predictive_measure', ymd_dir)

        dir_list.append(base_1h_dir)
        dir_list.append(base_5m_meta_act_ymd_dir)
        dir_list.append(base_5m_meta_pre_ymd_dir)
        dir_list.append(base_5m_real_act_ymd_dir)
        dir_list.append(base_5m_real_pre_ymd_dir)


        for i in dir_list:
            os.makedirs(i, exist_ok=True)
        

    def set_analysis_time(self, call_time, batch_time):
        ## 07시 10분 스크립트 실행

        ### 배치시간 05분 기준
        ## 실시간 : 07시 00분 ~ 07시 05분
        ## 메타 : 07시 00분 ~ 07시 05분

        ### 배치시간 1시간 기준
        ## 실시간 : 06시 00분 ~ 07시 00분
        ## 메타 : 06시 00분 ~ 07시 00분

        call_time = int(call_time)
        batch_time = int(batch_time)

        if batch_time == 300:
            # begin_time = call_time - 600
            # end_time = call_time - 300

            begin_time = call_time - batch_time - 300
            end_time = call_time - 300
            return begin_time, end_time
        elif batch_time == 3600:
            begin_time = call_time - batch_time - 600
            end_time = call_time - 600
            return begin_time, end_time
        else:
            raise print('')
















