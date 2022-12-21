import datetime
import statistics

import pandas as pd
import numpy as np

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from dao.wbxauditdbdao import wbxauditdbdao


class GetUserCallData(WbxJob):

    def start(self):
        config = Config()
        self.depotdbDao = wbxauditdbdao(self.depot_connectionurl)
        self.influxdbip,self.influx_database,self._client = config.getInfluxDB_SJC_client()
        connectionurl = "%s:%s@%s" % ("system", "sysnotallow", "(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.9.189)(PORT = 1701)) (ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.9.190)(PORT = 1701)) (ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.9.185)(PORT = 1701)) (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = racmvwebha.webex.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))")
        self.racmvwebDao = wbxauditdbdao(connectionurl)
        # self.start_time = '2021-06-08'
        # self.end_time = '2021-06-09'
        self.start_time = '2021-06-04'
        self.end_time = '2021-06-05'
        self.result_list = []
        self.instance_list = {}
        self.dbdomain_list = {}
        # self.main()
        self.main_2()

    def main_2(self):
        domaininfos = []
        try:
            self.racmvwebDao.connect()
            self.racmvwebDao.startTransaction()
            domaininfos = self.racmvwebDao.get_db_domaininfo()
            self.racmvwebDao.commit()
        except Exception as e:
            self.racmvwebDao.rollback()
            raise wbxexception("Error occurred: %s" % (e))

        for item in domaininfos:
            db_name = item['db_name']
            if db_name not in self.dbdomain_list:
                self.dbdomain_list[db_name] = item
        print("dbdomain_list:%s" %(len(self.dbdomain_list)) )

        db_list = []
        try:
            self.depotdbDao.connect()
            self.depotdbDao.startTransaction()
            db_list = self.depotdbDao.all_db_list()
            self.depotdbDao.commit()
        except Exception as e:
            self.depotdbDao.rollback()
            raise wbxexception("Error occurred: %s" % (e))

        res_list = []
        for db in db_list:
            db_name = db['db_name']
            domain_flag = False
            if db_name in self.dbdomain_list:
                dbdomain = self.dbdomain_list[db_name]
                # print(dbdomain)
                domain_flag = True
                starthour = dbdomain['starthour']
                if starthour < 10:
                    starthour = '0' + str(starthour)
                startminute = dbdomain['startminute']
                if startminute < 10:
                    startminute = '0' + str(startminute)
                endhour = dbdomain['endhour']
                if endhour < 10:
                    endhour = '0' + str(endhour)
                endminute = dbdomain['endminute']
                if endminute < 10:
                    endminute = '0' + str(endminute)
            else:
                starthour = "00"
                startminute = "00"
                endhour = "23"
                endminute = "59"

            now = wbxutil().getcurrenttime()
            start_date = now + datetime.timedelta(days=-7)
            date = str(start_date).split(" ")[0]
            # start = date + " " + str(starthour) + ":" + str(startminute) + ":00"
            # end = str(now).split(" ")[0] + " " + str(endhour) + ":" + str(endminute) + ":59"
            start = date
            end = str(now).split(" ")[0]
            try:
                # print("get_total_session ,db_name=%s, start:%s, end=%s" % (db_name, start, end))
                ress = self.get_total_session(db_name,date,start,end)
                for res in ress:
                    if res and res['max_total_session_count'] > 0:
                        print(res)
                        res_list.append(res)
            except Exception as e:
                raise wbxexception("Error occurred: db_name=%s, e=%s" % (db_name,e))
            # for i in range(7):
            #     start_date = now + datetime.timedelta(days=-(i+1))
            #     date = str(start_date).split(" ")[0]
            #     start = date + " " + str(starthour) + ":" + str(startminute) + ":00"
            #     end = date + " " + str(endhour) + ":" + str(endminute) + ":59"
            #     try:
            #         # print("get_total_session ,db_name=%s, start:%s, end=%s" % (db_name, start, end))
            #         res = self.get_total_session(db_name,date,start,end)
            #         if res:
            #             print(res)
            #             res_list.append(res)
            #     except Exception as e:
            #         raise wbxexception("Error occurred: db_name=%s, e=%s" % (db_name,e))

        to_excel_list = []
        for item in res_list:
            to_excel_list.append([item['db_name'], item['db_inst_name'],item['start_time'], item['end_time'],
                                  item['max_total_session_count'], item['max_time']])
        df = pd.DataFrame(to_excel_list,
                          columns=['db_name', 'db_inst_name','start_time', 'end_time', 'max_total_session_count','max_time'])
        file_name = 'result_total_session_count.xlsx'
        df.to_excel(file_name, sheet_name='data', index=False)


    def main(self):
        db_list = []
        try:
            self.depotdbDao.connect()
            self.depotdbDao.startTransaction()
            db_list = self.depotdbDao.all_db_list()
            instance_list = self.depotdbDao.all_instance_list()
        except Exception as e:
            self.depotdbDao.rollback()
            raise wbxexception("Error occurred: %s" % (e))

        for item in instance_list:
            db_name = item['db_name']
            if db_name not in self.instance_list:
                self.instance_list[db_name] = item

        for db in db_list:
            db_name = db['db_name']
            print(db_name)
            try:
                res = self.get_User_Calls_Per_Sec(db_name)
                if res:
                    res['trim_host'] = self.instance_list[db_name]['trim_host']
                    res['host_name'] = self.instance_list[db_name]['host_name']
                    time = res['time']
                    dt = time.split("T")[0] + " " + time.split("T")[1].split("Z")[0]
                    date_time = wbxutil().convertStringtoDateTime(dt)
                    start = date_time + datetime.timedelta(seconds=-30)
                    end = date_time + datetime.timedelta(seconds=30)
                    res2 = self.get_db_cpu_time(db_name, start, end)
                    res['mean_CPU_Usage_Per_Sec'] = res2
                    res3 = self.get_Undo(db_name, start, end)
                    res['mean_Redo_Generated_Per_Sec'] = res3
                    print(res)
                    self.result_list.append(res)
            except Exception as e:
                raise wbxexception("Error occurred: db_name=%s, e=%s" % (db_name,e))

        print(len(self.result_list))
        self.write_file()
        self.write_excel()

    def write_excel(self):
        to_excel_list = []
        for item in self.result_list:
            to_excel_list.append([item['db_name'],item['trim_host'],item['host_name'],item['time'],item['max_User_Calls_Per_Sec'],item['mean_CPU_Usage_Per_Sec'],item['mean_Redo_Generated_Per_Sec']])
        df = pd.DataFrame(to_excel_list,columns=['db_name','trim_host', 'host_name','time', 'max_User_Calls_Per_Sec', 'mean_CPU_Usage_Per_Sec','mean_Redo_Generated_Per_Sec'])
        file_name = 'result_%s.xlsx' % (self.start_time)
        df.to_excel(file_name, sheet_name='data',index=False)

    def write_file(self):
        import csv
        csv_rowlist = []
        csv_rowlist.append(["DB Name","trim_host","host_name","Time","max_User_Calls_Per_Sec","mean_CPU_Usage_Per_Sec","mean_Redo_Generated_Per_Sec"])
        for line in self.result_list:
            print(line)
            csv_rowlist.append([line['db_name'],line['trim_host'],line['host_name'],line['time'],line['max_User_Calls_Per_Sec'],line['mean_CPU_Usage_Per_Sec'],line['mean_Redo_Generated_Per_Sec'] ])
        file_name = 'result_%s.csv' %(self.start_time)
        with open(file_name, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(csv_rowlist)

    def get_total_session(self,db_name,date,start,end):
        sql = '''select db_name,db_inst_name,db_awr_metric_value from wbxdb_monitor_odm 
                                        where  "db_awr_metric_name" = 'Session_Count' 
                                        and db_name = '%s'
                                        and time > '%s' and time < '%s'
                                        ''' % (db_name, start, end)
        results = self._client.query(sql)
        points = results.get_points()
        map = {}
        for data in points:
            # print(data)
            key = data['db_name']+"_"+data['db_inst_name']
            if key not in map:
                map[key] = []
                map[key].append(data)
            else:
                new = map[key]
                new.append(data)
                map[key] = new
        # print(len(map))
        res_list = []
        for key in map:
            max_value = 0
            max_value_time = None
            db_list = map[key]
            for db_info in db_list:
                if db_info['db_awr_metric_value'] and db_info['db_awr_metric_value'] > max_value:
                    max_value = db_info['db_awr_metric_value']
                    max_value_time = db_info['time']
            res = {}
            res['db_name'] = db_name
            res['db_inst_name'] = str(key).split("_")[1]
            res['start_time'] = start
            res['end_time'] = end
            res['max_total_session_count'] = max_value
            res['max_time'] = max_value_time
            res_list.append(res)
        return res_list

    def get_Undo(self,db_name,start,end):
        # print("get_Undo ,db_name=%s, start:%s, end=%s" % (db_name, start, end))
        sql = '''select db_awr_metric_value from wbxdb_monitor_odm 
                                where  "db_awr_metric_name" = 'Redo_Generated_Per_Sec' 
                                and db_name = '%s'
                                and time > '%s' and time < '%s'
                                ''' % (db_name, start, end)
        results = self._client.query(sql)
        points = results.get_points()
        list = []
        for data in points:
            # print(data)
            list.append(data['db_awr_metric_value'])
        mean = statistics.mean(list)
        return round(mean,2)

    def get_db_cpu_time(self,db_name,start,end):
        # print("get_db_cpu_time ,db_name=%s, start:%s, end=%s" %(db_name,start,end))
        sql = '''select db_awr_metric_value from wbxdb_monitor_odm 
                                where  "db_awr_metric_name" = 'CPU_Usage_Per_Sec' 
                                and db_name = '%s'
                                and time > '%s' and time < '%s'
                                ''' % (db_name, start, end)
        results = self._client.query(sql)
        if len(results)>0:
            points = results.get_points()
            list = []
            for data in points:
                # print(data)
                list.append(data['db_awr_metric_value'])
            mean = statistics.mean(list)
            return round(mean,2)
        return None

    # def get_User_Calls_Per_Sec(self,db_name):
    #     User_Call = {}
    #     sql = '''
    #             select max(db_awr_metric_value) from wbxdb_monitor_odm
    #             where  "db_awr_metric_name" = 'User_Calls_Per_Sec'
    #             and db_name = '%s'
    #             and time > '%s' and time < '%s'
    #             GROUP BY time(1d),"db_name" fill(null)
    #             ''' %(db_name,self.start_time,self.end_time)
    #
    #     results = self._client.query(sql)
    #     for result in results.raw['series']:
    #         item = dict(result)
    #         db_name = item['tags']['db_name']
    #         values = item['values']
    #         for value in values:
    #             data = {}
    #             time = value[0]
    #             data['time'] = time
    #             if time and value[1]:
    #                 data['db_name'] = db_name
    #                 data['User_Calls_Per_Sec'] = round(value[1],2)
    #                 key = db_name + "_" + time
    #                 if key not in User_Call:
    #                     User_Call[key] = data
    #                 else:
    #                     new_item = User_Call[key]
    #                     new_item['User_Calls_Per_Sec'] = round(value[1],2)
    #                     User_Call[key] = new_item
    #     self.result = User_Call

    def get_User_Calls_Per_Sec(self,db_name):
        # print("get_User_Calls_Per_Sec")
        sql = '''
                   select db_awr_metric_value from wbxdb_monitor_odm 
                   where  "db_awr_metric_name" = 'User_Calls_Per_Sec' 
                   and db_name = '%s' 
                   and time > '%s' and time < '%s'
                   '''%(db_name,self.start_time,self.end_time)

        results = self._client.query(sql)
        if len(results)>0:
            max_value = 0
            max_value_time = None
            points = results.get_points()
            for data in points:
                if data['db_awr_metric_value'] > max_value :
                    max_value = data['db_awr_metric_value']
                    max_value_time = data['time']
            res = {}
            res['db_name'] = db_name
            res['time'] = max_value_time
            res['max_User_Calls_Per_Sec'] = max_value
            return res
        return None




if __name__ == '__main__':
    job = GetUserCallData()
    job.initialize()
    job.start()


