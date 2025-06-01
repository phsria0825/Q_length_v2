import pandas as pd
import datetime
import os
import _99_Contents as contents

import numpy as np
from numpy import nan as NA


class util_:
    def __init__(self):
        self.possible_routes = 'possible_routes.rou.xml'
        self.possible_trip = 'possible_trip.rou.xml'
        self.unix_verUI = 0
        # self.unix_verUI = 1435687200 # 2015년 07월 01일 00시 00분
        self.KST = datetime.timezone(datetime.timedelta(hours=9))

    # 보기 좋게 xml 만드는 함수, 줄바꿈, 들여쓰기 작업
    def indent(self, elem, level=0):  # 자료 출처 https://goo.gl/J8VoDK
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for elem in elem:
                self.indent(elem, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i

    def str2time(self, string):
        if type(string) == str:
            if string.find('-') != -1:
                if string.find('.') != -1:
                    string = string.split('.')[0]
                    return datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
                else:
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
        return re.sub(r"[^0-9]", "", str(time))

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

        string_ymdhms = string_ymdhms.timestamp()
        # string_ymdhms = string_ymdhms.astimezone( self.KST).timestamp()

        return string_ymdhms - self.unix_verUI

    def unixtime2time(self, unix_time):
        # KST = datetime.timezone(datetime.timedelta(hours=9))

        if type(unix_time) != float:
            unix_time = float(unix_time)

        stdr_time = datetime.datetime.fromtimestamp(unix_time)

        # if stdr_time.tzinfo == None:
        #     stdr_time = stdr_time.astimezone(self.KST)
        # elif stdr_time.tzinfo == datetime.timezone.utc:
        #     stdr_time = stdr_time.replace(tzinfo = self.KST)

        return stdr_time

    ## 시간 구분하는 함수
    def get_datetime_list(self, begin, end, interval_sec=300):
        if end is None:
            end = begin

        begin = self.str2time(begin)
        end = self.str2time(end)

        if begin == end:
            end += datetime.timedelta(seconds=interval_sec)

        assert end >= begin

        cur = begin
        target_list = []

        while cur <= end:
            cur_time = datetime.datetime.strftime(cur, '%Y%m%d%H%M%S')
            target_list.append(cur_time)
            cur += datetime.timedelta(seconds=interval_sec)
        return target_list

    def mk_path(self, *path):  # os.path.join() 이라는 함수가 이미 있었음
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
