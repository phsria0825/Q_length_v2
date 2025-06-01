##############
###
### 3. 고정적으로 활용할 쿼리를 정의해둔 파일(최적화용 쿼리)
###
#############

# import os
# query_dic = {
# 	'signal':{
# 		  'anly_cd_1': signal_anly_cd_1
# 		, 'anly_cd_2': signal_anly_cd_2
# 		, 'anly_cd_3': signal_anly_cd_3
# 	},
# 	'traffic':{
# 		  'anly_cd_1': {'turn': traffic_anly_cd_1_turn
# 		  				, 'edge' : traffic_anly_cd_1_edge
# 		  				}
# 		, 'anly_cd_2': {'turn': traffic_anly_cd_2_turn
# 		  				, 'edge' : traffic_anly_cd_2_edge
# 		  				}
# 		, 'anly_cd_3': {'turn': traffic_anly_cd_3_turn
# 		  				, 'edge' : traffic_anly_cd_3_edge
# 		  				}
# 	}
# }






signal_anly_cd_1 = """
	INSERT INTO SOITSPHASTODINFO
		(SCEN_ID,CLCT_UNIX_TM,BGNG_UNIX_TM,SPOT_INTS_ID,NODE_ID
		,TM_PLAN_NO,PHAS_OPER_PLAN_NO,BGNG_HH,BGNG_MI
		,MAJR_INTS_SE_CD,SGNL_STTS,CYCL_HR,OFST_HR,PHAS_NO
		,YELW_HR,MIN_GREN_HR,MAJR_ROAD_SE_CD,PRGRS_STTS_CD
		,PHAS_HR,PHAS_BGNG_UNIX_TM,INPT_DT)
	SELECT ◆v_in_scen_id
		, A.CLCT_UNIX_TM					-- 수집시각 (동인에서 신호이력 수집된 시각)
		, A.CLCT_UNIX_TM					-- 시작시각
		, A.SPOT_INTS_ID					-- 교차로ID
		, A.NODE_ID							-- 노드ID
		, A.TM_PLAN_NO						-- 시각계획번호
		, A.PHAS_OPER_PLAN_NO               -- 현시운영계획번호
		, A.BGNG_HH                         -- 시작시
		, A.BGNG_MI                         -- 시작분
		, A.MAJR_INTS_SE_CD					-- 주교차로여부
		, A.SGNL_STTS						-- 신호상태
		, A.CYCL_HR							-- 주기
		, A.OFST_HR                    		-- 옵셋
		, A.PHAS_NO 						-- 현시
		, A.YELW_HR							-- 황색시간
		, A.MIN_GREN_HR						-- 녹색시간
		, A.MAJR_ROAD_SE_CD					-- 주도로구분코드
		, '1'								-- 진행상태코드 (1:신호이력insert, 3:황색시간insert)
		, (CASE WHEN A.PHAS_NO = 1 THEN PHAS_TM_1
				WHEN A.PHAS_NO = 2 THEN PHAS_TM_2
				WHEN A.PHAS_NO = 3 THEN PHAS_TM_3
				WHEN A.PHAS_NO = 4 THEN PHAS_TM_4
				WHEN A.PHAS_NO = 5 THEN PHAS_TM_5
				WHEN A.PHAS_NO = 6 THEN PHAS_TM_6
				WHEN A.PHAS_NO = 7 THEN PHAS_TM_7
				WHEN A.PHAS_NO = 8 THEN PHAS_TM_8
			END) AS PHAS_TM			-- 현시시간
		, (CASE WHEN A.PHAS_NO = 1 THEN PHAS_TM_START_1
				WHEN A.PHAS_NO = 2 THEN PHAS_TM_START_2
				WHEN A.PHAS_NO = 3 THEN PHAS_TM_START_3
				WHEN A.PHAS_NO = 4 THEN PHAS_TM_START_4
				WHEN A.PHAS_NO = 5 THEN PHAS_TM_START_5
				WHEN A.PHAS_NO = 6 THEN PHAS_TM_START_6
				WHEN A.PHAS_NO = 7 THEN PHAS_TM_START_7
				WHEN A.PHAS_NO = 8 THEN PHAS_TM_START_8
			END) AS PHAS_TM_START	-- 현시시작시간
		, SYSDATE					-- 입력일시
	FROM
		(
			SELECT A.*
				, B.NODE_ID, B.MAJR_INTS_SE_CD, B.SGNL_STTS, B.MIN_GREN_HR, B.YELW_HR, B.MAJR_ROAD_SE_CD
			FROM
				(
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 1 							AS PHAS_NO
						, A.A_RING_1_PHAS_HR 			AS PHAS_TM_1
						, 0								AS PHAS_TM_2
						, 0								AS PHAS_TM_3
						, 0								AS PHAS_TM_4
						, 0								AS PHAS_TM_5
						, 0								AS PHAS_TM_6
						, 0								AS PHAS_TM_7
						, 0								AS PHAS_TM_8
						, A.CLCT_UNIX_TM - A.CYCL_HR	AS PHAS_TM_START_1
						, 0								AS PHAS_TM_START_2
						, 0								AS PHAS_TM_START_3
						, 0								AS PHAS_TM_START_4
						, 0								AS PHAS_TM_START_5
						, 0								AS PHAS_TM_START_6
						, 0								AS PHAS_TM_START_7
						, 0								AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.PBADMS_DSTT_CD = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_1_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 2 							AS PHAS_NO
						, 0								AS PHAS_TM_1
						, A.A_RING_2_PHAS_HR 			AS PHAS_TM_2
						, 0								AS PHAS_TM_3
						, 0								AS PHAS_TM_4
						, 0								AS PHAS_TM_5
						, 0								AS PHAS_TM_6
						, 0								AS PHAS_TM_7
						, 0								AS PHAS_TM_8
						, 0								AS PHAS_TM_START_1
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR		AS PHAS_TM_START_2
						, 0								AS PHAS_TM_START_3
						, 0								AS PHAS_TM_START_4
						, 0								AS PHAS_TM_START_5
						, 0								AS PHAS_TM_START_6
						, 0								AS PHAS_TM_START_7
						, 0								AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.PBADMS_DSTT_CD = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_2_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 3 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, A.A_RING_3_PHAS_HR 							AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, A.CLCT_UNIX_TM - A.CYCL_HR
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR	AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.PBADMS_DSTT_CD = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_3_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 4 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, A.A_RING_4_PHAS_HR 							AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR
							+ A.A_RING_3_PHAS_HR						AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.PBADMS_DSTT_CD = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_4_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 5 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, A.A_RING_5_PHAS_HR 							AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR	AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.PBADMS_DSTT_CD = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_5_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 6 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, A.A_RING_6_PHAS_HR 							AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR
							+ A.A_RING_5_PHAS_HR						AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.PBADMS_DSTT_CD = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_6_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 7 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, A.A_RING_7_PHAS_HR 							AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR 
							+ A.A_RING_5_PHAS_HR + A.A_RING_6_PHAS_HR	AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.PBADMS_DSTT_CD = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_7_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 8												AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, A.A_RING_8_PHAS_HR 							AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR 
							+ A.A_RING_5_PHAS_HR + A.A_RING_6_PHAS_HR
							+ A.A_RING_7_PHAS_HR						AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.PBADMS_DSTT_CD = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_8_PHAS_HR > 0
				) A
				, ICSIGNAL.SOITSPHASINFO B
			WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
			AND A.PHAS_NO = B.PHAS_NO
		) A
	;
	"""

signal_anly_cd_2 = """
	INSERT INTO SOITSPHASTODINFO
		(SCEN_ID,CLCT_UNIX_TM,BGNG_UNIX_TM,SPOT_INTS_ID,NODE_ID
		,TM_PLAN_NO,PHAS_OPER_PLAN_NO,BGNG_HH,BGNG_MI
		,MAJR_INTS_SE_CD,SGNL_STTS,CYCL_HR,OFST_HR,PHAS_NO
		,YELW_HR,MIN_GREN_HR,MAJR_ROAD_SE_CD,PRGRS_STTS_CD
		,PHAS_HR,PHAS_BGNG_UNIX_TM,INPT_DT)
	SELECT ◆v_in_scen_id
		, A.CLCT_UNIX_TM					-- 수집시각 (동인에서 신호이력 수집된 시각)
		, A.CLCT_UNIX_TM					-- 시작시각
		, A.SPOT_INTS_ID					-- 교차로ID
		, A.NODE_ID							-- 노드ID
		, A.TM_PLAN_NO						-- 시각계획번호
		, A.PHAS_OPER_PLAN_NO               -- 현시운영계획번호
		, A.BGNG_HH                         -- 시작시
		, A.BGNG_MI                         -- 시작분
		, A.MAJR_INTS_SE_CD					-- 주교차로여부
		, A.SGNL_STTS						-- 신호상태
		, A.CYCL_HR							-- 주기
		, A.OFST_HR                    		-- 옵셋
		, A.PHAS_NO 						-- 현시
		, A.YELW_HR							-- 황색시간
		, A.MIN_GREN_HR						-- 녹색시간
		, A.MAJR_ROAD_SE_CD					-- 주도로구분코드
		, '1'								-- 진행상태코드 (1:신호이력insert, 3:황색시간insert)
		, (CASE WHEN A.PHAS_NO = 1 THEN PHAS_TM_1
				WHEN A.PHAS_NO = 2 THEN PHAS_TM_2
				WHEN A.PHAS_NO = 3 THEN PHAS_TM_3
				WHEN A.PHAS_NO = 4 THEN PHAS_TM_4
				WHEN A.PHAS_NO = 5 THEN PHAS_TM_5
				WHEN A.PHAS_NO = 6 THEN PHAS_TM_6
				WHEN A.PHAS_NO = 7 THEN PHAS_TM_7
				WHEN A.PHAS_NO = 8 THEN PHAS_TM_8
			END) AS PHAS_TM			-- 현시시간
		, (CASE WHEN A.PHAS_NO = 1 THEN PHAS_TM_START_1
				WHEN A.PHAS_NO = 2 THEN PHAS_TM_START_2
				WHEN A.PHAS_NO = 3 THEN PHAS_TM_START_3
				WHEN A.PHAS_NO = 4 THEN PHAS_TM_START_4
				WHEN A.PHAS_NO = 5 THEN PHAS_TM_START_5
				WHEN A.PHAS_NO = 6 THEN PHAS_TM_START_6
				WHEN A.PHAS_NO = 7 THEN PHAS_TM_START_7
				WHEN A.PHAS_NO = 8 THEN PHAS_TM_START_8
			END) AS PHAS_TM_START	-- 현시시작시간
		, SYSDATE					-- 입력일시
	FROM
		(
			SELECT A.*
				, B.NODE_ID, B.MAJR_INTS_SE_CD, B.SGNL_STTS, B.MIN_GREN_HR, B.YELW_HR, B.MAJR_ROAD_SE_CD
			FROM
				(
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 1 							AS PHAS_NO
						, A.A_RING_1_PHAS_HR 			AS PHAS_TM_1
						, 0								AS PHAS_TM_2
						, 0								AS PHAS_TM_3
						, 0								AS PHAS_TM_4
						, 0								AS PHAS_TM_5
						, 0								AS PHAS_TM_6
						, 0								AS PHAS_TM_7
						, 0								AS PHAS_TM_8
						, A.CLCT_UNIX_TM - A.CYCL_HR	AS PHAS_TM_START_1
						, 0								AS PHAS_TM_START_2
						, 0								AS PHAS_TM_START_3
						, 0								AS PHAS_TM_START_4
						, 0								AS PHAS_TM_START_5
						, 0								AS PHAS_TM_START_6
						, 0								AS PHAS_TM_START_7
						, 0								AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, (
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											UNION
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											) B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_1_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 2 							AS PHAS_NO
						, 0								AS PHAS_TM_1
						, A.A_RING_2_PHAS_HR 			AS PHAS_TM_2
						, 0								AS PHAS_TM_3
						, 0								AS PHAS_TM_4
						, 0								AS PHAS_TM_5
						, 0								AS PHAS_TM_6
						, 0								AS PHAS_TM_7
						, 0								AS PHAS_TM_8
						, 0								AS PHAS_TM_START_1
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR		AS PHAS_TM_START_2
						, 0								AS PHAS_TM_START_3
						, 0								AS PHAS_TM_START_4
						, 0								AS PHAS_TM_START_5
						, 0								AS PHAS_TM_START_6
						, 0								AS PHAS_TM_START_7
						, 0								AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, (
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											UNION
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											) B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_2_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 3 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, A.A_RING_3_PHAS_HR 							AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, A.CLCT_UNIX_TM - A.CYCL_HR
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR	AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, (
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											UNION
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											) B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_3_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 4 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, A.A_RING_4_PHAS_HR 							AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR
							+ A.A_RING_3_PHAS_HR						AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, (
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											UNION
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											) B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_4_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 5 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, A.A_RING_5_PHAS_HR 							AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR	AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, (
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											UNION
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											) B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_5_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 6 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, A.A_RING_6_PHAS_HR 							AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR
							+ A.A_RING_5_PHAS_HR						AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, (
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											UNION
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											) B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_6_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 7 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, A.A_RING_7_PHAS_HR 							AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR 
							+ A.A_RING_5_PHAS_HR + A.A_RING_6_PHAS_HR	AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, (
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											UNION
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											) B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_7_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 8												AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, A.A_RING_8_PHAS_HR 							AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR 
							+ A.A_RING_5_PHAS_HR + A.A_RING_6_PHAS_HR
							+ A.A_RING_7_PHAS_HR						AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, (
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											UNION
												SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
												WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
											) B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_8_PHAS_HR > 0
				) A
				, ICSIGNAL.SOITSPHASINFO B
			WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
			AND A.PHAS_NO = B.PHAS_NO
		) A
	;
"""	



signal_anly_cd_3 = """
	INSERT INTO SOITSPHASTODINFO
		(SCEN_ID,CLCT_UNIX_TM,BGNG_UNIX_TM,SPOT_INTS_ID,NODE_ID
		,TM_PLAN_NO,PHAS_OPER_PLAN_NO,BGNG_HH,BGNG_MI
		,MAJR_INTS_SE_CD,SGNL_STTS,CYCL_HR,OFST_HR,PHAS_NO
		,YELW_HR,MIN_GREN_HR,MAJR_ROAD_SE_CD,PRGRS_STTS_CD
		,PHAS_HR,PHAS_BGNG_UNIX_TM,INPT_DT)
	SELECT ◆v_in_scen_id
		, A.CLCT_UNIX_TM					-- 수집시각 (동인에서 신호이력 수집된 시각)
		, A.CLCT_UNIX_TM					-- 시작시각
		, A.SPOT_INTS_ID					-- 교차로ID
		, A.NODE_ID							-- 노드ID
		, A.TM_PLAN_NO						-- 시각계획번호
		, A.PHAS_OPER_PLAN_NO               -- 현시운영계획번호
		, A.BGNG_HH                         -- 시작시
		, A.BGNG_MI                         -- 시작분
		, A.MAJR_INTS_SE_CD					-- 주교차로여부
		, A.SGNL_STTS						-- 신호상태
		, A.CYCL_HR							-- 주기
		, A.OFST_HR                    		-- 옵셋
		, A.PHAS_NO 						-- 현시
		, A.YELW_HR							-- 황색시간
		, A.MIN_GREN_HR						-- 녹색시간
		, A.MAJR_ROAD_SE_CD					-- 주도로구분코드
		, '1'								-- 진행상태코드 (1:신호이력insert, 3:황색시간insert)
		, (CASE WHEN A.PHAS_NO = 1 THEN PHAS_TM_1
				WHEN A.PHAS_NO = 2 THEN PHAS_TM_2
				WHEN A.PHAS_NO = 3 THEN PHAS_TM_3
				WHEN A.PHAS_NO = 4 THEN PHAS_TM_4
				WHEN A.PHAS_NO = 5 THEN PHAS_TM_5
				WHEN A.PHAS_NO = 6 THEN PHAS_TM_6
				WHEN A.PHAS_NO = 7 THEN PHAS_TM_7
				WHEN A.PHAS_NO = 8 THEN PHAS_TM_8
			END) AS PHAS_TM			-- 현시시간
		, (CASE WHEN A.PHAS_NO = 1 THEN PHAS_TM_START_1
				WHEN A.PHAS_NO = 2 THEN PHAS_TM_START_2
				WHEN A.PHAS_NO = 3 THEN PHAS_TM_START_3
				WHEN A.PHAS_NO = 4 THEN PHAS_TM_START_4
				WHEN A.PHAS_NO = 5 THEN PHAS_TM_START_5
				WHEN A.PHAS_NO = 6 THEN PHAS_TM_START_6
				WHEN A.PHAS_NO = 7 THEN PHAS_TM_START_7
				WHEN A.PHAS_NO = 8 THEN PHAS_TM_START_8
			END) AS PHAS_TM_START	-- 현시시작시간
		, SYSDATE					-- 입력일시
	FROM
		(
			SELECT A.*
				, B.NODE_ID, B.MAJR_INTS_SE_CD, B.SGNL_STTS, B.MIN_GREN_HR, B.YELW_HR, B.MAJR_ROAD_SE_CD
			FROM
				(
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 1 							AS PHAS_NO
						, A.A_RING_1_PHAS_HR 			AS PHAS_TM_1
						, 0								AS PHAS_TM_2
						, 0								AS PHAS_TM_3
						, 0								AS PHAS_TM_4
						, 0								AS PHAS_TM_5
						, 0								AS PHAS_TM_6
						, 0								AS PHAS_TM_7
						, 0								AS PHAS_TM_8
						, A.CLCT_UNIX_TM - A.CYCL_HR	AS PHAS_TM_START_1
						, 0								AS PHAS_TM_START_2
						, 0								AS PHAS_TM_START_3
						, 0								AS PHAS_TM_START_4
						, 0								AS PHAS_TM_START_5
						, 0								AS PHAS_TM_START_6
						, 0								AS PHAS_TM_START_7
						, 0								AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.LNKG_GRUP_ID = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_1_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 2 							AS PHAS_NO
						, 0								AS PHAS_TM_1
						, A.A_RING_2_PHAS_HR 			AS PHAS_TM_2
						, 0								AS PHAS_TM_3
						, 0								AS PHAS_TM_4
						, 0								AS PHAS_TM_5
						, 0								AS PHAS_TM_6
						, 0								AS PHAS_TM_7
						, 0								AS PHAS_TM_8
						, 0								AS PHAS_TM_START_1
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR		AS PHAS_TM_START_2
						, 0								AS PHAS_TM_START_3
						, 0								AS PHAS_TM_START_4
						, 0								AS PHAS_TM_START_5
						, 0								AS PHAS_TM_START_6
						, 0								AS PHAS_TM_START_7
						, 0								AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.LNKG_GRUP_ID = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_2_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 3 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, A.A_RING_3_PHAS_HR 							AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, A.CLCT_UNIX_TM - A.CYCL_HR
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR	AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.LNKG_GRUP_ID = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_3_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 4 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, A.A_RING_4_PHAS_HR 							AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR
							+ A.A_RING_3_PHAS_HR						AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.LNKG_GRUP_ID = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_4_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 5 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, A.A_RING_5_PHAS_HR 							AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR	AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.LNKG_GRUP_ID = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_5_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 6 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, A.A_RING_6_PHAS_HR 							AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR
							+ A.A_RING_5_PHAS_HR						AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.LNKG_GRUP_ID = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_6_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 7 											AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, A.A_RING_7_PHAS_HR 							AS PHAS_TM_7
						, 0												AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR 
							+ A.A_RING_5_PHAS_HR + A.A_RING_6_PHAS_HR	AS PHAS_TM_START_7
						, 0												AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.LNKG_GRUP_ID = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_7_PHAS_HR > 0
				UNION
					SELECT A.SPOT_INTS_ID, A.CLCT_UNIX_TM, A.TM_PLAN_NO, A.PHAS_OPER_PLAN_NO, A.BGNG_HH, A.BGNG_MI, A.CYCL_HR, A.OFST_HR
						, 8												AS PHAS_NO
						, 0												AS PHAS_TM_1
						, 0												AS PHAS_TM_2
						, 0												AS PHAS_TM_3
						, 0												AS PHAS_TM_4
						, 0												AS PHAS_TM_5
						, 0												AS PHAS_TM_6
						, 0												AS PHAS_TM_7
						, A.A_RING_8_PHAS_HR 							AS PHAS_TM_8
						, 0												AS PHAS_TM_START_1
						, 0												AS PHAS_TM_START_2
						, 0												AS PHAS_TM_START_3
						, 0												AS PHAS_TM_START_4
						, 0												AS PHAS_TM_START_5
						, 0												AS PHAS_TM_START_6
						, 0												AS PHAS_TM_START_7
						, A.CLCT_UNIX_TM - A.CYCL_HR 
							+ A.A_RING_1_PHAS_HR + A.A_RING_2_PHAS_HR 
							+ A.A_RING_3_PHAS_HR + A.A_RING_4_PHAS_HR 
							+ A.A_RING_5_PHAS_HR + A.A_RING_6_PHAS_HR
							+ A.A_RING_7_PHAS_HR						AS PHAS_TM_START_8
					FROM ICSIGNAL.SOITDTODCNFG A
						, (SELECT M.SPOT_INTS_ID
								, (CASE WHEN ◆v_in_anly_wk = 0 THEN M.SUN_TM_PLAN_NO WHEN ◆v_in_anly_wk = 1 THEN M.MON_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 2 THEN M.TUE_TM_PLAN_NO WHEN ◆v_in_anly_wk = 3 THEN M.WED_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 4 THEN M.THU_TM_PLAN_NO WHEN ◆v_in_anly_wk = 5 THEN M.FRI_TM_PLAN_NO
										WHEN ◆v_in_anly_wk = 6 THEN M.SAT_TM_PLAN_NO ELSE 0 END) AS TM_PLAN_NO
							FROM ICSIGNAL.SOITDWKLYPLAN M
							WHERE (M.SPOT_INTS_ID, M.CLCT_UNIX_TM)
								IN (SELECT A.SPOT_INTS_ID, MAX(A.CLCT_UNIX_TM)
									FROM ICSIGNAL.SOITDWKLYPLAN A
										, ICSIGNAL.SOITSNODE B
									WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID 
									AND A.SPOT_INTS_ID IS NOT NULL 
									AND B.LNKG_GRUP_ID = ◆v_in_anly_detail
									GROUP BY A.SPOT_INTS_ID)
									) B
					WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID AND A.TM_PLAN_NO = B.TM_PLAN_NO AND A.A_RING_8_PHAS_HR > 0
				) A
				, ICSIGNAL.SOITSPHASINFO B
			WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
			AND A.PHAS_NO = B.PHAS_NO
		) A
	;

"""	




traffic_anly_cd_1_turn = """
	SELECT A.TM_DIV		    			-- 분석 단위시간
		, B.ENTR_EDGE_ID        		-- 진입엣지ID
		, B.EXIT_EDGE_ID        		-- 진출엣지ID
		, A.SPOT_CAMR_ID				-- 현장카메라ID
		, A.KNCR_CD           			-- 차종코드
		, A.TURN_TYPE_CD      			-- 회전유형코드
		, A.CNT							-- 차량대수
	FROM
		(
			SELECT A.TM_DIV
				, A.SPOT_INTS_ID		-- 현장교차로ID
				, A.SPOT_CAMR_ID		-- 현장카메라ID
				, A.KNCR_CD           	-- 차종코드
				, A.TURN_TYPE_CD_CONV  	-- 회전유형코드
				, COUNT(1) AS CNT
			FROM
				(
					SELECT FLOOR((A.STLN_PASG_UNIX_TM - ◆v_in_anly_tm) / 900) AS TM_DIV
						, C.SPOT_INTS_ID		-- 현장교차로ID
						, A.SPOT_CAMR_ID		-- 현장카메라ID
						, A.KNCR_CD           	-- 차종코드
						, (CASE WHEN A.TURN_TYPE_CD = '11' THEN 's'
								WHEN A.TURN_TYPE_CD = '21' THEN 'L'
								WHEN A.TURN_TYPE_CD = '22' THEN 'l'
								WHEN A.TURN_TYPE_CD = '31' THEN 'R'
								WHEN A.TURN_TYPE_CD = '32' THEN 'r'
								WHEN A.TURN_TYPE_CD = '41' THEN 'u'
							END) AS TURN_TYPE_CD_CONV
					FROM ICSIGNAL.SOITGRTMDTINFO A
						, ICSIGNAL.SOITGCAMRINFO B
						, ICSIGNAL.SOITSNODE C
					WHERE A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
					AND B.SPOT_INTS_ID = C.SPOT_INTS_ID
					AND C.SPOT_INTS_ID IS NOT NULL 
					AND C.PBADMS_DSTT_CD = ◆v_in_anly_detail
					AND A.STLN_PASG_UNIX_TM >= ◆v_in_anly_tm 
					AND A.STLN_PASG_UNIX_TM  < ◆v_in_anly_tm + 86400
				) A
			GROUP BY A.SPOT_INTS_ID
					, A.SPOT_CAMR_ID
					, A.KNCR_CD
					, A.TURN_TYPE_CD
					, A.TM_DIV
		) A
		, (
			SELECT A.SPOT_INTS_ID		-- 현장교차로ID
				, A.ENTR_EDGE_ID      	-- 진입엣지ID
				, A.EXIT_EDGE_ID      	-- 진출엣지ID
				, A.TURN_CLSF_CD      	-- 회전분류코드
				, A.SPOT_CAMR_ID      	-- 현장카메라ID
			FROM ICSIGNAL.SOITSCNCTLANESGNLINFO A
				, ICSIGNAL.SOITSNODE C
			WHERE A.SPOT_INTS_ID = C.SPOT_INTS_ID
			AND C.SPOT_INTS_ID IS NOT NULL 
			AND C.PBADMS_DSTT_CD = ◆v_in_anly_detail
			GROUP BY A.SPOT_INTS_ID
					, A.ENTR_EDGE_ID
					, A.EXIT_EDGE_ID
					, A.TURN_CLSF_CD
					, A.SPOT_CAMR_ID
		) B
	WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
	AND A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
	AND A.TURN_TYPE_CD_CONV = B.TURN_CLSF_CD
	;

"""	

traffic_anly_cd_1_edge = """

	SELECT A.TM_DIV		    	-- 분석 단위시간
		, B.ENTR_EDGE_ID        -- 진입엣지ID
		, A.SPOT_CAMR_ID		-- 현장카메라ID
		, A.KNCR_CD           	-- 차종코드
		, A.CNT					-- 차량대수
	FROM
		(
			SELECT A.SPOT_INTS_ID
				, A.TM_DIV
				, A.SPOT_CAMR_ID		-- 현장카메라ID
				, A.KNCR_CD           	-- 차종코드
				, COUNT(1) AS CNT
			FROM
				(
					SELECT C.SPOT_INTS_ID
						, FLOOR((A.STLN_PASG_UNIX_TM - ◆v_in_anly_tm) / 900) AS TM_DIV
						, A.SPOT_CAMR_ID		-- 현장카메라ID
						, A.KNCR_CD           	-- 차종코드
					FROM ICSIGNAL.SOITGRTMDTINFO A
						, ICSIGNAL.SOITGCAMRINFO B
						, ICSIGNAL.SOITSNODE C
					WHERE A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
					AND B.SPOT_INTS_ID = C.SPOT_INTS_ID
					AND C.SPOT_INTS_ID IS NOT NULL 
					AND C.PBADMS_DSTT_CD = ◆v_in_anly_detail
					AND A.STLN_PASG_UNIX_TM >= ◆v_in_anly_tm 
					AND A.STLN_PASG_UNIX_TM  < ◆v_in_anly_tm + 86400
				) A
			GROUP BY A.SPOT_INTS_ID
					, A.SPOT_CAMR_ID
					, A.KNCR_CD
					, A.TM_DIV
		) A
		, (
			SELECT A.SPOT_INTS_ID		-- 현장교차로ID
				, A.ENTR_EDGE_ID      	-- 진입엣지ID
				, A.SPOT_CAMR_ID      	-- 현장카메라ID
			FROM ICSIGNAL.SOITSCNCTLANESGNLINFO A
				, ICSIGNAL.SOITSNODE C
			WHERE A.SPOT_INTS_ID = C.SPOT_INTS_ID
			AND C.SPOT_INTS_ID IS NOT NULL 
			AND C.PBADMS_DSTT_CD = ◆v_in_anly_detail
			GROUP BY A.SPOT_INTS_ID
					, A.ENTR_EDGE_ID
					, A.SPOT_CAMR_ID
		) B
	WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
	AND A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
	;


"""	


traffic_anly_cd_2_turn = """
		
	SELECT A.TM_DIV		    			-- 분석 단위시간
		, B.ENTR_EDGE_ID        		-- 진입엣지ID
		, B.EXIT_EDGE_ID        		-- 진출엣지ID
		, A.SPOT_CAMR_ID				-- 현장카메라ID
		, A.KNCR_CD           			-- 차종코드
		, A.TURN_TYPE_CD      			-- 회전유형코드
		, A.CNT							-- 차량대수
	FROM
		(
			SELECT A.TM_DIV
				, A.SPOT_INTS_ID		-- 현장교차로ID
				, A.SPOT_CAMR_ID		-- 현장카메라ID
				, A.KNCR_CD           	-- 차종코드
				, A.TURN_TYPE_CD_CONV  	-- 회전유형코드
				, COUNT(1) AS CNT
			FROM
				(
					SELECT FLOOR((A.STLN_PASG_UNIX_TM - ◆v_in_anly_tm) / 900) AS TM_DIV
						, C.SPOT_INTS_ID		-- 현장교차로ID
						, A.SPOT_CAMR_ID		-- 현장카메라ID
						, A.KNCR_CD           	-- 차종코드
						, (CASE WHEN A.TURN_TYPE_CD = '11' THEN 's'
								WHEN A.TURN_TYPE_CD = '21' THEN 'L'
								WHEN A.TURN_TYPE_CD = '22' THEN 'l'
								WHEN A.TURN_TYPE_CD = '31' THEN 'R'
								WHEN A.TURN_TYPE_CD = '32' THEN 'r'
								WHEN A.TURN_TYPE_CD = '41' THEN 'u'
							END) AS TURN_TYPE_CD_CONV
					FROM ICSIGNAL.SOITGRTMDTINFO A
						, ICSIGNAL.SOITGCAMRINFO B
						, (
								SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
								WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
							UNION
								SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
								WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
							) C
					WHERE A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
					AND B.SPOT_INTS_ID = C.SPOT_INTS_ID
					AND C.SPOT_INTS_ID IS NOT NULL
					AND A.STLN_PASG_UNIX_TM >= ◆v_in_anly_tm 
					AND A.STLN_PASG_UNIX_TM  < ◆v_in_anly_tm + 86400
				) A
			GROUP BY A.SPOT_INTS_ID
					, A.SPOT_CAMR_ID
					, A.KNCR_CD
					, A.TURN_TYPE_CD
					, A.TM_DIV
		) A
		, (
			SELECT A.SPOT_INTS_ID		-- 현장교차로ID
				, A.ENTR_EDGE_ID      	-- 진입엣지ID
				, A.EXIT_EDGE_ID      	-- 진출엣지ID
				, A.TURN_CLSF_CD      	-- 회전분류코드
				, A.SPOT_CAMR_ID      	-- 현장카메라ID
			FROM ICSIGNAL.SOITSCNCTLANESGNLINFO A
				, (
						SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
						WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
					UNION
						SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
						WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
					) C
			WHERE A.SPOT_INTS_ID = C.SPOT_INTS_ID
			AND C.SPOT_INTS_ID IS NOT NULL
			GROUP BY A.SPOT_INTS_ID
					, A.ENTR_EDGE_ID
					, A.EXIT_EDGE_ID
					, A.TURN_CLSF_CD
					, A.SPOT_CAMR_ID
		) B
	WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
	AND A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
	AND A.TURN_TYPE_CD_CONV = B.TURN_CLSF_CD
	;

"""	

traffic_anly_cd_2_edge = """
	
	SELECT A.TM_DIV		    	-- 분석 단위시간
		, B.ENTR_EDGE_ID        -- 진입엣지ID
		, A.SPOT_CAMR_ID		-- 현장카메라ID
		, A.KNCR_CD           	-- 차종코드
		, A.CNT					-- 차량대수
	FROM
		(
			SELECT A.SPOT_INTS_ID
				, A.TM_DIV
				, A.SPOT_CAMR_ID		-- 현장카메라ID
				, A.KNCR_CD           	-- 차종코드
				, COUNT(1) AS CNT
			FROM
				(
					SELECT C.SPOT_INTS_ID
						, FLOOR((A.STLN_PASG_UNIX_TM - ◆v_in_anly_tm) / 900) AS TM_DIV
						, A.SPOT_CAMR_ID		-- 현장카메라ID
						, A.KNCR_CD           	-- 차종코드
					FROM ICSIGNAL.SOITGRTMDTINFO A
						, ICSIGNAL.SOITGCAMRINFO B
						, (
								SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
								WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
							UNION
								SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
								WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
							) C
					WHERE A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
					AND B.SPOT_INTS_ID = C.SPOT_INTS_ID
					AND C.SPOT_INTS_ID IS NOT NULL
					AND A.STLN_PASG_UNIX_TM >= ◆v_in_anly_tm 
					AND A.STLN_PASG_UNIX_TM  < ◆v_in_anly_tm + 86400
				) A
			GROUP BY A.SPOT_INTS_ID
					, A.SPOT_CAMR_ID
					, A.KNCR_CD
					, A.TM_DIV
		) A
		, (
			SELECT A.SPOT_INTS_ID		-- 현장교차로ID
				, A.ENTR_EDGE_ID      	-- 진입엣지ID
				, A.SPOT_CAMR_ID      	-- 현장카메라ID
			FROM ICSIGNAL.SOITSCNCTLANESGNLINFO A
				, (
						SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
						WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.BGNG_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
					UNION
						SELECT B.SPOT_INTS_ID FROM ICSIGNAL.SOITSEDGEGRUP A, ICSIGNAL.SOITSNODE B, ICSIGNAL.SOITSEDGEGRUPCNFG C, ICSIGNAL.SOITSEDGE D
						WHERE A.EDGE_GRUP_ID = C.EDGE_GRUP_ID AND C.EDGE_ID = D.EDGE_ID AND D.ROAD_NM = ◆v_in_anly_detail AND A.END_NODE_ID = B.NODE_ID AND B.SPOT_INTS_ID IS NOT NULL
					) C
			WHERE A.SPOT_INTS_ID = C.SPOT_INTS_ID
			AND C.SPOT_INTS_ID IS NOT NULL
			GROUP BY A.SPOT_INTS_ID
					, A.ENTR_EDGE_ID
					, A.SPOT_CAMR_ID
		) B
	WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
	AND A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
	;


"""	



traffic_anly_cd_3_turn = """
	SELECT A.TM_DIV		    			-- 분석 단위시간
		, B.ENTR_EDGE_ID        		-- 진입엣지ID
		, B.EXIT_EDGE_ID        		-- 진출엣지ID
		, A.SPOT_CAMR_ID				-- 현장카메라ID
		, A.KNCR_CD           			-- 차종코드
		, A.TURN_TYPE_CD      			-- 회전유형코드
		, A.CNT							-- 차량대수
	FROM
		(
			SELECT A.TM_DIV
				, A.SPOT_INTS_ID		-- 현장교차로ID
				, A.SPOT_CAMR_ID		-- 현장카메라ID
				, A.KNCR_CD           	-- 차종코드
				, A.TURN_TYPE_CD_CONV  	-- 회전유형코드
				, COUNT(1) AS CNT
			FROM
				(
					SELECT FLOOR((A.STLN_PASG_UNIX_TM - ◆v_in_anly_tm) / 900) AS TM_DIV
						, C.SPOT_INTS_ID		-- 현장교차로ID
						, A.SPOT_CAMR_ID		-- 현장카메라ID
						, A.KNCR_CD           	-- 차종코드
						, (CASE WHEN A.TURN_TYPE_CD = '11' THEN 's'
								WHEN A.TURN_TYPE_CD = '21' THEN 'L'
								WHEN A.TURN_TYPE_CD = '22' THEN 'l'
								WHEN A.TURN_TYPE_CD = '31' THEN 'R'
								WHEN A.TURN_TYPE_CD = '32' THEN 'r'
								WHEN A.TURN_TYPE_CD = '41' THEN 'u'
							END) AS TURN_TYPE_CD_CONV
					FROM ICSIGNAL.SOITGRTMDTINFO A
						, ICSIGNAL.SOITGCAMRINFO B
						, ICSIGNAL.SOITSNODE C
					WHERE A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
					AND B.SPOT_INTS_ID = C.SPOT_INTS_ID
					AND C.SPOT_INTS_ID IS NOT NULL
					AND C.LNKG_GRUP_ID = ◆v_in_anly_detail
					AND A.STLN_PASG_UNIX_TM >= ◆v_in_anly_tm 
					AND A.STLN_PASG_UNIX_TM  < ◆v_in_anly_tm + 86400
				) A
			GROUP BY A.SPOT_INTS_ID
					, A.SPOT_CAMR_ID
					, A.KNCR_CD
					, A.TURN_TYPE_CD
					, A.TM_DIV
		) A
		, (
			SELECT A.SPOT_INTS_ID		-- 현장교차로ID
				, A.ENTR_EDGE_ID      -- 진입엣지ID
				, A.EXIT_EDGE_ID      -- 진출엣지ID
				, A.TURN_CLSF_CD      -- 회전분류코드
				, A.SPOT_CAMR_ID      -- 현장카메라ID
			FROM ICSIGNAL.SOITSCNCTLANESGNLINFO A
				, ICSIGNAL.SOITSNODE C
			WHERE A.SPOT_INTS_ID = C.SPOT_INTS_ID
			AND C.SPOT_INTS_ID IS NOT NULL
			AND C.LNKG_GRUP_ID = ◆v_in_anly_detail
			GROUP BY A.SPOT_INTS_ID
					, A.ENTR_EDGE_ID
					, A.EXIT_EDGE_ID
					, A.TURN_CLSF_CD
					, A.SPOT_CAMR_ID
		) B
	WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
	AND A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
	AND A.TURN_TYPE_CD_CONV = B.TURN_CLSF_CD
	;	

"""	

traffic_anly_cd_3_edge = """
	SELECT A.TM_DIV		    	-- 분석 단위시간
		, B.ENTR_EDGE_ID        -- 진입엣지ID
		, A.SPOT_CAMR_ID		-- 현장카메라ID
		, A.KNCR_CD           	-- 차종코드
		, A.CNT					-- 차량대수
	FROM
		(
			SELECT A.SPOT_INTS_ID
				, A.TM_DIV
				, A.SPOT_CAMR_ID		-- 현장카메라ID
				, A.KNCR_CD           	-- 차종코드
				, COUNT(1) AS CNT
			FROM
				(
					SELECT C.SPOT_INTS_ID
						, FLOOR((A.STLN_PASG_UNIX_TM - ◆v_in_anly_tm) / 900) AS TM_DIV
						, A.SPOT_CAMR_ID		-- 현장카메라ID
						, A.KNCR_CD           	-- 차종코드
					FROM ICSIGNAL.SOITGRTMDTINFO A
						, ICSIGNAL.SOITGCAMRINFO B
						, ICSIGNAL.SOITSNODE C
					WHERE A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
					AND B.SPOT_INTS_ID = C.SPOT_INTS_ID
					AND C.SPOT_INTS_ID IS NOT NULL
					AND C.LNKG_GRUP_ID = ◆v_in_anly_detail
					AND A.STLN_PASG_UNIX_TM >= ◆v_in_anly_tm 
					AND A.STLN_PASG_UNIX_TM  < ◆v_in_anly_tm + 86400
				) A
			GROUP BY A.SPOT_INTS_ID
					, A.SPOT_CAMR_ID
					, A.KNCR_CD
					, A.TM_DIV
		) A
		, (
			SELECT A.SPOT_INTS_ID		-- 현장교차로ID
				, A.ENTR_EDGE_ID      	-- 진입엣지ID
				, A.SPOT_CAMR_ID      	-- 현장카메라ID
			FROM ICSIGNAL.SOITSCNCTLANESGNLINFO A
				, ICSIGNAL.SOITSNODE C
			WHERE A.SPOT_INTS_ID = C.SPOT_INTS_ID
			AND C.SPOT_INTS_ID IS NOT NULL
			AND C.LNKG_GRUP_ID = ◆v_in_anly_detail
			GROUP BY A.SPOT_INTS_ID
					, A.ENTR_EDGE_ID
					, A.SPOT_CAMR_ID
		) B
	WHERE A.SPOT_INTS_ID = B.SPOT_INTS_ID
	AND A.SPOT_CAMR_ID = B.SPOT_CAMR_ID
	;

"""	


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
query_dic = {
	'signal':{
		  'anly_cd_1': signal_anly_cd_1
		, 'anly_cd_2': signal_anly_cd_2
		, 'anly_cd_3': signal_anly_cd_3
	},
	'traffic':{
		  'anly_cd_1': {'turn': traffic_anly_cd_1_turn
		  				, 'edge' : traffic_anly_cd_1_edge
		  				}
		, 'anly_cd_2': {'turn': traffic_anly_cd_2_turn
		  				, 'edge' : traffic_anly_cd_2_edge
		  				}
		, 'anly_cd_3': {'turn': traffic_anly_cd_3_turn
		  				, 'edge' : traffic_anly_cd_3_edge
		  				}
	}
}














