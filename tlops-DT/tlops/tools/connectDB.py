import pyodbc

'''
https://ponyozzang.tistory.com/628

# 티베로 관련
https://jinisbonusbook.tistory.com/65
https://itcenter.yju.ac.kr/xe_board_tech_python/8425
https://blog.naver.com/debolm74/221893306011
'''

class connectDB:
    def __init__(self):
        self.dsn = 'Tibero'
        self.user = 'signal'
        self.pwd = 'signal'

        # False면 실행되지 않음
        self.enable = False
        # self.enable = True


    def _connect(self):
        '''DB 연결'''
        # self.conn = pyodbc.connect(DSN = self.dsn, uid = self.user, pwd = self.pwd)
        self.conn = pyodbc.connect(f'DSN={self.dsn};UID={self.user};PWD={self.pwd}')
        self.cursor = self.conn.cursor()


    def _disconnect(self):
        self.conn.close()
        self.cursor.close()


    def insert_reward(self, scenario_id, episode, reward):
        '''Insert reward value for each iteration'''
        try:
            if self.enable:
                self._connect()
                query = f"INSERT INTO soitsanlsrcrnhstr (scnr_id, rcrn_notm, otmz_valu) values ('{scenario_id}', {episode}, {reward})"
                self.cursor.execute(query)
                self.conn.commit()
                self._disconnect()
        except:
            error_message = 'Error: inserting reward to DB'
            print(error_message)
            raise Exception(error_message)


    def update_status(self, status, scenario_id):
        '''Update status code'''
        try:
            if self.enable:
                self._connect()
                query = f"UPDATE soitsanlsprcsinfo SET anls_prgrs_stts_cd='{status}' WHERE scnr_id='{scenario_id}'"
                self.cursor.execute(query)
                self.conn.commit()
                self._disconnect()
        except:
            error_message = 'Error: updating status to DB'
            print(error_message)
            raise Exception(error_message)


    def update_error(self, error, scenario_id):
        '''Update error code'''
        try:
            if self.enable:
                self._connect()
                query = f"UPDATE soitsanlsprcsinfo SET err_cd='{error}' WHERE scnr_id='{scenario_id}'"
                self.cursor.execute(query)
                self.conn.commit()
                self._disconnect()
        except:
            error_message = 'Error: updating error to DB'
            print(error_message)
            raise Exception(error_message)
