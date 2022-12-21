import json
import cx_Oracle

class CXOracle:

    def __init__(self,username,password,connstr):
        self.connect = cx_Oracle.connect(username + "/" + password + "@" + connstr)
        self.cursor = self.connect.cursor()

    def execute(self,sql):
        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception as e:
            print(e)
        finally:
            self.disconnect()

    def disconnect(self):
        self.cursor.close()
        self.connect.close()

    def select(self,sql):
        list = []
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        # print(result)
        col_name = self.cursor.description
        for row in result:
            vo = {}
            for col in range(len(col_name)):
                key = col_name[col][0]
                value = row[col]
                vo[key] = value
            list.append(dict(vo))
        return list

    def update(self,sql,list_param):
        try:
            self.cursor.executemany(sql, list_param)
            self.connect.commit()
        except Exception as e:
            print(e)
        finally:
            self.disconnect()



