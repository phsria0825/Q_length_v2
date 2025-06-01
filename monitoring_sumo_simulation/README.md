< monitering code >


1. 경로로 이동
d:
cd D:/anyang_code/02_python/monitoring

1-1. _99_Contents의 net_file 이름 4개 교차로일 경우 400011n.net.xml, 9개일 경우 900011n.net.xml 변경 필요

2. python실행코드( 예시 : python __all_simulation.py -e 20220202000000 -d 300 )__
python __all_simulation.py -e 종료시간(YYYYMMDDHH24MISS) -d 300
python __all_simulation.py -b 시작시간(YYYYMMDDHH24MISS) -e 종료시간(YYYYMMDDHH24MISS) -d 지속시간(초) -i 특정 교차로 i 값(n)

-b, -i는 선택사항
-e, -d는 필수사항

python __all_simulation.py -b 20220202000000 -e 20220202001500 -d 900 -i 14
