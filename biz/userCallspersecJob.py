from datetime import datetime

from biz.wbxjob import WbxJob
from common.config import Config
from dao.wbxauditdbdao import wbxauditdbdao


class UserCallspersecJob(WbxJob):
    def start(self):
        config = Config()
        self._client = config.getInfluxDBclient()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.main()

    def main(self):
        sql = '''
                 select max(db_awr_metric_value) from wbxdb_monitor_odm where time >= now()-1d
                 --and db_name = 'RACAAWEB' 
                 and db_awr_metric_name= 'User_Calls_Per_Sec'
                 GROUP BY "db_name", "db_inst_name" fill(null)
                   '''
        today = datetime.utcnow().strftime("%Y-%m-%d")
        results = self._client.query(sql)
        for result in results.raw['series']:
            item = dict(result)
            db_name = item['tags']['db_name']
            db_inst_name = str(item['tags']['db_inst_name'])
            # print(item)
            values = item['values']
            max_value = 0
            max_value_time = ""
            for value in values:
                if value[1] != None and float(value[1]) > max_value:
                    max_value = value[1]
                    max_value_time = value[0]
            metricdate = max_value_time
            if str(max_value_time).split("T")[0] != today:
                json_body = [
                    {
                        "measurement": "db_metric_monitor_statistics",
                        "tags": {
                            "metricdate": metricdate,
                            "db_name":db_name,
                            "db_inst_name": db_inst_name,
                            "metric_name": "User_Calls_Per_Sec",

                        },
                        "fields": {
                            "max_value": max_value,
                            "max_value_time": max_value_time
                        }
                    }
                ]
                try:
                    print(json_body)
                    self._client.write_points(json_body)
                except Exception as e:
                    print("write to influxDB error, json_body={0}, e={1}".format(json_body, e))

if __name__ == '__main__':
    job = UserCallspersecJob()
    job.initialize()
    job.start()