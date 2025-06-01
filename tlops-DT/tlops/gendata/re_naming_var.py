##############
###
### 4. QueryDic의 쿼리에서 사용되는 인자값에 대해 var_df.csv를 사용하여 변환하는 것을 하는 역할의 코드
###
#############

import os

import pandas as pd
import numpy as np
from tqdm import tqdm

import argparse

from QueryDic import query_dic


var_df = pd.DataFrame({'code_nm': ['◆v_in_scen_id','◆v_in_anly_wk','◆v_in_anly_detail','◆v_in_anly_tm','◆v_in_batch_1','◆v_in_batch_5','◆v_in_rgn_prttn_cd','◆v_in_batch_60']
		  ,'python_code_nm': ['{scen_id}','{anly_wk}','{anly_detail}','{anly_tm}','{self.call_time}','{self.call_time}','{self.target_area}','{self.call_time}']})



def renaming(query):
	global var_df

	for row in var_df.iterrows():
		row = row[1]

		query = query.replace(row.code_nm, row.python_code_nm)

	return query


if __name__ == '__main__':
##############
###
### 6-3. 현시TOD정보 (SOITSPHASTODINFO) 테이블 조회(db에 query로 조회)
###
#############
##############
###
### 소스 : 8-4 & 8-6. 교통량정보 (SOITGRTMDTINFO) 테이블 조회(db에 query로 조회)
###
#############
	try:
		var_df
	except NameError:
		var_df = pd.read_csv(os.path.join('./', 'var_df.csv'), dtype=str)


	for key in query_dic:
		if key == 'signal':
			for anly_cd in query_dic[key]:
				query_dic[key][anly_cd] = renaming(query_dic[key][anly_cd])
		else:
			for anly_cd in query_dic[key]:
				for count_type in query_dic[key][anly_cd]:
					query_dic[key][anly_cd][count_type] = renaming(query_dic[key][anly_cd][count_type])

	print(query_dic.items())
