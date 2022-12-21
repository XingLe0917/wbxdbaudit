
import datetime
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import cx_Oracle
import logging

from common.config import Config

PASSWORD_EXTRA_DB=['RACAFWEB','TTA35','TTA136','RACAM1MMP']

EXCLUDE_SRC=[]
EXCLUDE_TGT=[]


from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")

#shareplex monitor adbmon
class ShareplexAdbmonTest(WbxJob):
    def start(self):
        self.src_dbs=""
        self.tgt_dbs =""
        self.type =self.args[0]
        if len(self.args) > 1:
            self.src_dbs=self.args[1]
        if len(self.args) > 2:
            self.tgt_dbs=self.args[2]
        logger.info(datetime.datetime.now())
        logger.info("input src_db:{0}".format(self.src_dbs))
        logger.info("input tgt_db:{0}".format(self.tgt_dbs))
        logger.info("input type:{0}".format(self.type))
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)

        config = Config()
        self._client = config.getInfluxDBclientNew()
        self.splexinfo_src_dict={}
        self.splexinfo_tgt_dict={}
        self.wbxadbmon={}
        self.all_splexinfo={}
        self.depotdb_url=config.getDepotConnectionurl()
        self.extra_db_pwd= {}
        self.extra_src_job=[]
        self.extra_tgt_job=[]
        self.main()

    def main(self):
        # logging.info("start TnsnameJob")
        rows = []
        shareplexinfos = []
        wbxadbmons = []
        pwds=[]
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            rows = self.auditdao.getDBTnsInfo_2()
            shareplexinfos = self.auditdao.getShareplexinfos_test(self.src_dbs,self.tgt_dbs)
            wbxadbmons = self.auditdao.getWbxadbmons()
            pwds=self.auditdao.getDBPwdInfo()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

        # Special DB password
        extra_db_pwd={}
        for db_pwds in pwds:
            item = dict(db_pwds)
            extra_db_pwd[item['db_name'] + "/" + item['schema']] = item
        self.extra_db_pwd = extra_db_pwd

        tns_vo = {}
        for vo in rows:
            item = dict(vo)
            db_name = item['db_name']
            trim_host = item['trim_host']
            listener_port = item['listener_port']
            service_name = "%s.webex.com" % item['service_name']
            key = str(db_name+"_"+trim_host)
            value = '(DESCRIPTION ='
            if item['scan_ip1']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' %(value,item['scan_ip1'],listener_port)
            if item['scan_ip2']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))'%(value, item['scan_ip2'],listener_port)
            if item['scan_ip3']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' %(value,item['scan_ip3'],listener_port)
            value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' %(value,service_name)
            tns_vo[key] = value

        logger.info("tns_vo:{0}" .format(len(tns_vo)))
        # print("TSJ8_sjdbth47:{0}". format(tns_vo['TSJ8_sjdbth47']))
        logging.debug("now:{0}".format(datetime.datetime.now()))
        logging.debug("tns_vo:{0}" .format(len(tns_vo)))

        # self.depotdb_url= tns_vo['AUDITDB_sjdbormt06']

        splexinfo_src_dict = {}
        splexinfo_tgt_dict = {}
        wbxadbmon_vo= {}
        for wbxadbmon in wbxadbmons:
            vo = dict(wbxadbmon)
            splex_key = str(vo['src_db']+"_"+vo['tgt_db']+"_"+str(vo['port'])+"_"+vo['replication_to'])
            wbxadbmon_vo[splex_key] = vo
        self.wbxadbmon= wbxadbmon_vo

        all_splexinfos = {} # use to decide insert into wbxadbmon or update wbxadbmon later
        for shareplexinfo in shareplexinfos:
            item = dict(shareplexinfo)
            key_src = item['src_db']+"_"+str(item['port'])+"_"+item['src_trim_host']
            key_tgt = item['tgt_db'] + "_" + str(item['port']) + "_" + item['src_trim_host']+ "_" +item['tgt_trim_host']
            key = item['src_db'] + "_" + item['tgt_db'] + "_" + str(item['port']) + "_" + item['replication_to']
            if key_src in splexinfo_src_dict:
                ls = splexinfo_src_dict[key_src]
                ls.append(item)
                splexinfo_src_dict[key_src] = ls
            else:
                splexinfo_src_dict[key_src] = []
                splexinfo_src_dict[key_src].append(item)
            if key_tgt in splexinfo_tgt_dict:
                tgt_ls = splexinfo_tgt_dict[key_tgt]
                tgt_ls.append(item)
            else:
                splexinfo_tgt_dict[key_tgt] = []
                splexinfo_tgt_dict[key_tgt].append(item)
                all_splexinfos[key] = item
        self.splexinfo_src_dict = splexinfo_src_dict
        self.splexinfo_tgt_dict = splexinfo_tgt_dict
        self.all_splexinfo = all_splexinfos

        src_db_tns_sql = {}
        extra_src_job=[]
        for (k, ls) in self.splexinfo_src_dict.items():
            src_db = k.split("_")[0]
            if src_db not in EXCLUDE_SRC:
                schema = "splex"+k.split("_")[1]
                src_trim_host = k.split("_")[2]

                q_name_ls = {}
                for index, v in enumerate(ls):
                    qname = v['qname']
                    if qname not in q_name_ls:
                        q_name_ls[qname] = []
                        q_name_ls[qname].append(v)
                    else:
                        ls = q_name_ls[qname]
                        ls.append(v)
                        q_name_ls[qname] = ls

                for qName in q_name_ls:
                    table = "splex_monitor_adb"
                    if qName :
                        table +="_"+qName
                    sql = "update %s.%s SET logtime=sysdate where src_db='%s' and src_host='%s' and direction in (" % (
                        schema, table,src_db, src_trim_host)

                    for index, v in enumerate(q_name_ls[qName]):
                        replication_to = v['replication_to']
                        sql += "'" + replication_to + "'"
                        if index != len(q_name_ls[qName]) - 1:
                            sql += ","
                    sql += ")"
                    if src_db not in PASSWORD_EXTRA_DB:
                        if src_db not in src_db_tns_sql :
                            src_db_tns_sql[src_db] = {}
                            src_db_tns_sql[src_db]["tns"] = tns_vo[src_db+"_"+src_trim_host]
                            src_db_tns_sql[src_db]["sql"] = []
                            src_db_tns_sql[src_db]["sql"].append(sql)
                        else:
                            src_db_tns_sql[src_db]["tns"] = tns_vo[src_db+"_"+src_trim_host]
                            sqls = src_db_tns_sql[src_db]["sql"]
                            sqls.append(sql)
                            src_db_tns_sql[src_db]["sql"]= sqls
                    else:
                        a = self.extra_db_pwd[src_db + "/" + schema]
                        src_extra = {}
                        src_extra['db'] = src_db
                        src_extra['schema'] = a['schema']
                        src_extra['pwd'] = a['pwd']
                        src_extra['tns'] = tns_vo[src_db+"_"+src_trim_host]
                        src_extra['sql'] = sql
                        extra_src_job.append(src_extra)

        logger.info("src_db: {0}".format(len(src_db_tns_sql)))

        extra_tgt_job = []
        tgt_db_tns_sql = {}
        for (k, ls) in self.splexinfo_tgt_dict.items():
            tgt_db = k.split("_")[0]
            if tgt_db not in EXCLUDE_TGT:
                port = k.split("_")[1]
                schema = "splex" + port
                src_trim_host = k.split("_")[2]
                tgt_trim_host = k.split("_")[3]

                q_name_ls = {}
                for index, v in enumerate(ls):
                    qname = v['qname']
                    if qname not in q_name_ls:
                        q_name_ls[qname] = []
                        q_name_ls[qname].append(v)
                    else:
                        ls = q_name_ls[qname]
                        ls.append(v)
                        q_name_ls[qname] = ls

                # u = "system"
                # p = "sysnotallow"
                # if tgt_db + "/" + schema in self.extra_db_pwd and tgt_db!="CONFIGDB":
                #     a = self.extra_db_pwd[tgt_db + "/" + schema]
                #     u = a['schema']
                #     p = a['pwd']

                for qName in q_name_ls:
                    table = "splex_monitor_adb"
                    if qName:
                        table +="_"+qName
                    sql = "select direction,src_host,src_db,logtime,port_number from %s.%s where src_host='%s' and port_number = %s and direction in (" % (
                        schema, table,src_trim_host, port)
                    for index, v in enumerate(q_name_ls[qName]):
                        replication_to = v['replication_to']
                        sql += "'" + replication_to + "'"
                        if index != len(q_name_ls[qName]) - 1:
                            sql += ","
                    sql += ")"
                    # sql = u + "/" + p + "@" + sql
                    if tgt_db not in PASSWORD_EXTRA_DB:
                        if tgt_db not in tgt_db_tns_sql:
                            tgt_db_tns_sql[tgt_db] = {}
                            tgt_db_tns_sql[tgt_db]["tns"] = tns_vo[tgt_db+"_"+tgt_trim_host]
                            # tgt_db_tns_sql[tgt_db]["shemapwd"] = u + "/" + p
                            tgt_db_tns_sql[tgt_db]["sql"] = []
                            tgt_db_tns_sql[tgt_db]["sql"].append(sql)
                        else:
                            tgt_db_tns_sql[tgt_db]["tns"] = tns_vo[tgt_db+"_"+tgt_trim_host]
                            # tgt_db_tns_sql[tgt_db]["shemapwd"] = u + "/" + p
                            sqls = tgt_db_tns_sql[tgt_db]["sql"]
                            sqls.append(sql)
                            tgt_db_tns_sql[tgt_db]["sql"]= sqls
                    else:
                        a = self.extra_db_pwd[tgt_db + "/" + schema]
                        tgt_extra = {}
                        tgt_extra['db'] = tgt_db
                        tgt_extra['schema'] = a['schema']
                        tgt_extra['pwd'] = a['pwd']
                        tgt_extra['tns'] = tns_vo[tgt_db + "_" + tgt_trim_host]
                        tgt_extra['sql'] = sql
                        extra_tgt_job.append(tgt_extra)

        self.extra_src_job = extra_src_job
        self.extra_tgt_job = extra_tgt_job

        logger.info("tgt_db:{0}".format(len(tgt_db_tns_sql)))

        # all data prepared
        with ThreadPoolExecutor(max_workers=40) as t:
            all_task = []
            for (db, v) in src_db_tns_sql.items():
                v['db'] = db
                obj = t.submit(self.update_src_job, v)
                all_task.append(obj)

            # wait(all_task, return_when=FIRST_COMPLETED)
            # print('first finished')
            # print(wait(all_task, timeout=5))

            # for i in all_task:
            #     try:
            #         print('sigle thread result:{}'.format(i.result(timeout=10)))
            #     except concurrent.futures._base.TimeoutError as e:
            #         print("a TimeoutError")
            #         continue

            num =0
            update_wbxadbmon = []
            for future in as_completed(all_task):
                num += 1
                if (num == len(src_db_tns_sql)):
                    logger.info("update all src finish!")
                    # query tgt logtime
                    result_1 = self.query_tgt(tgt_db_tns_sql)
                    logger.info("result_1 (according to DB):{0}".format(len(result_1)))
                    result_2 = self.extra_job()
                    logger.info("result_2 (according to port):{0}".format(len(result_2)))
                    logger.info("query tgt logtime finish!")
                    logging.debug("query tgt logtime finish!")
                    for ls in result_1:
                        if ls:
                            for wbxadbmon_vo in ls:
                                update_wbxadbmon.append(wbxadbmon_vo)
                    for vo in result_2:
                        update_wbxadbmon.append(vo)
            self.update_wbxadbmon(update_wbxadbmon)

    def update_wbxadbmon(self,update_wbxadbmon_list):
        logger.info("update_wbxadbmon_list:{0}" .format(len(update_wbxadbmon_list)))
        dataToInsertHistory = []
        dataToUpdate = []
        dataToInsert=[]
        NoOpt_key=[]
        for value in update_wbxadbmon_list:
            wbxadbmon_key = value['SRC_DB']+"_"+value['TRT_DB']+"_"+str(value['PORT_NUMBER'])+"_" + value['DIRECTION']
            is_exist_splexinfo="1"
            if wbxadbmon_key not in self.wbxadbmon:
                is_exist_wbxadbmon="1"
                if wbxadbmon_key in self.all_splexinfo:
                    vo = self.all_splexinfo[wbxadbmon_key]
                    dataToInsert.append(
                        (vo['src_host'], vo['src_db'], int(vo['port']), vo['replication_to'], vo['tgt_host'], vo['tgt_db']))
                else:
                    is_exist_splexinfo="0"
                    NoOpt_key.append(wbxadbmon_key)
                logger.info("wbxadbmon_key: [{0}] not in wbxadbmon , exist in all_splexinfo:{1}".format(wbxadbmon_key,is_exist_splexinfo))
                logging.debug("wbxadbmon_key: [{0}] not in wbxadbmon, exist in all_splexinfo:{1}".format(wbxadbmon_key,is_exist_splexinfo))
            else:
                vo = self.wbxadbmon[wbxadbmon_key]

                if value['LOGTIME']:
                    vo['lastreptime'] = value['LOGTIME']
                else:
                    value['LOGTIME'] = datetime.datetime.strptime("1970-01-01", '%Y-%m-%d')
                    vo['lastreptime'] = value['LOGTIME']
                vo['lastreptime']= value['LOGTIME']

                dataToUpdate.append((value['LOGTIME'], vo['src_host'], vo['src_db'], int(vo['port']),
                                     vo['replication_to'], vo['tgt_host'], vo['tgt_db']))
                dataToInsertHistory.append((vo['src_host'],vo['src_db'],int(vo['port']),vo['replication_to'],vo['tgt_host'],vo['tgt_db'],value['LOGTIME']))

        try:
            constr="%s/%s@%s" %(self.depotdb_url[0],self.depotdb_url[1],self.depotdb_url[2])
            srcconn = cx_Oracle.connect(constr)
            srccsr = srcconn.cursor()
            log = ""
            if self.type=="update":
                log="update/insert wbxadbmon"
                if len(dataToUpdate)>0:
                    sql = "update wbxadbmon set lastreptime=:1,montime=sysdate where src_host=:2 and src_db=:3 and port=:4 and replication_to=:5 and tgt_host=:6 and tgt_db=:7"
                    srccsr.executemany(sql,dataToUpdate)
                    logger.info("{0} success! records :{1}".format("update wbxadbmon", len(dataToUpdate)))
                    self.save_to_influxdb(dataToUpdate)

                if len(dataToInsert)>0:
                    sql = "insert into wbxadbmon(src_host,src_db,port,replication_to,tgt_host,tgt_db,lastreptime,montime) values(:1,:2,:3,:4,:5,:6,sysdate,sysdate) "
                    srccsr.executemany(sql, dataToInsert)
                    logger.info("{0} success! records :{1}".format("insert wbxadbmon", len(dataToInsert)))
                    self.save_to_influxdb(dataToInsert)
                srcconn.commit()

            else:
                log = "insert wbxadbmon_history"
                srccsr.executemany(
                    "insert into wbxadbmon_history(src_host,src_db,port,replication_to,tgt_host,tgt_db,lastreptime,montime) values(:1,:2,:3,:4,:5,:6,:7,sysdate)",
                    dataToInsertHistory)
                srcconn.commit()
                logger.info("{0} success! records :{1}" .format(log,len(dataToInsertHistory)))
            logger.info("{0} success! dataToUpdate :{1}, dataToInsert:{2}, NoOpt_key:{3}" .format(log,len(dataToUpdate),len(dataToInsert),len(NoOpt_key)))
            logging.debug("{0} success! dataToUpdate :{1}, dataToInsert:{2}, NoOpt_key:{3}" .format(log,len(dataToUpdate),len(dataToInsert),len(NoOpt_key)))

        except Exception as e:
            logger.error(str(e))
            logger.error("insert wbxadbmon_history or update wbxadbmon fail")
        finally:
            srccsr.close()
            srcconn.close()

    def save_to_influxdb(self, datalist):
        logger.info("save_to_influxdb, datalist={0}".format(len(datalist)))
        with ThreadPoolExecutor(max_workers=40) as t:
            all_task = []
            for item in datalist:
                try:
                    t.submit(self.insert_to_influxdb, item)
                    all_task.append(item)
                except Exception as e:
                    logger.error("save_to_influxdb error, e={0}," .format(str(e)))

    def insert_to_influxdb(self,data):
        GMT_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
        logtime = data[0].strftime(GMT_FORMAT)
        src_host = data[1]
        src_db = data[2]
        port = data[3]
        replication_to = data[4]
        tgt_host = data[5]
        tgt_db = data[6]
        json_body = [
            {
                "measurement": "wbxadbmon_history",
                "tags": {
                    "src_host": src_host,
                    "src_db": src_db,
                    "port": port,
                    "replication_to": replication_to,
                    "tgt_host": tgt_host,
                    "tgt_db": tgt_db
                },
                "fields": {
                    "logtime": logtime
                }
            }
        ]
        try:
            # logger.info(json_body)
            self._client.write_points(json_body)
        except Exception as e:
            logger.error("write to influxDB error, data={0}, e={1}".format(data, e))


    # query tgt logtime
    def query_tgt(self,tgt_db_tns_sql):
        result = []
        with ThreadPoolExecutor(max_workers=40) as t:
            all_task = []
            for (db, v) in tgt_db_tns_sql.items():
                v['db'] = db
                obj = t.submit(self.query_tgt_logintime_job, v)
                all_task.append(obj)

            # for i in all_task:
            #     try:
            #         print('sigle thread result:{}'.format(i.result(timeout=10)))
            #     except concurrent.futures._base.TimeoutError as e:
            #         print("TimeoutError")
            #         continue

            for future in as_completed(all_task):
                datas = future.result()
                result.append(datas)
        return result

    # update src logtime job
    def update_src_job(self, item):
        # shemapwd = item['shemapwd']
        tns = "system/sysnotallow" + "@" + item['tns']
        sql = "begin " + (';'.join(item['sql'])) + ";end;"
        try:
            conn = cx_Oracle.connect(tns)
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                conn.commit()
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            logger.error("-------------------- update src fail db:{0}\n tns:{1}\n sql_num:{2}\n sql:{3}\n e:{4}".format(
                item['db'], tns, len(item['sql']), sql, str(e)))

    # query tgt logintime job
    def query_tgt_logintime_job(self, item):
        # shemapwd = item['shemapwd']
        tns = "system/sysnotallow" + "@" + item['tns']
        sql_ls = item['sql']
        sql = ""
        for index, v in enumerate(sql_ls):
            sql += v
            if index != len(sql_ls) - 1:
                sql += " union all "
        try:
            conn = cx_Oracle.connect(tns)
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchall()
                list = []
                col_name = cursor.description
                for row in result:
                    dict = {}
                    dict['TRT_DB'] = item['db']
                    for col in range(len(col_name)):
                        key = col_name[col][0]
                        value = row[col]
                        dict[key] = value
                    list.append(dict)
                return list
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            logger.error(
                "-------------------- query tgt logintime fail: db:{0}\n tns:{1}\n sql_num:{2}\n sql:{3}\n e:{4}".format(
                    item['db'], tns, len(item['sql']), sql, str(e)))

    def extra_job(self):
        logger.info("extra_job, extra_src_job:{0}, extra_tgt_job:{1}".format(len(self.extra_src_job),len(self.extra_tgt_job)))
        for src in self.extra_src_job:
            try:
                tns = src['schema'] + "/" + src['pwd'] + "@" + src['tns']
                sql = src['sql']
                conn = cx_Oracle.connect(tns)
                try:
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    conn.commit()
                finally:
                    cursor.close()
                    conn.close()
            except Exception as e:
                logger.info("-------------------- update src fail db:{0}\n tns:{1}\n sql_num:{2}\n sql:{3}\n e:{4}".format(
                    src['db'], tns, len(src['sql']), sql, str(e)))

        list = []
        for tgt in self.extra_tgt_job:
            try:
                tns = tgt['schema'] + "/" + tgt['pwd'] + "@" + tgt['tns']
                sql = tgt['sql']
                conn = cx_Oracle.connect(tns)
                try:
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    result = cursor.fetchall()

                    col_name = cursor.description
                    for row in result:
                        dict = {}
                        dict['TRT_DB'] = tgt['db']
                        for col in range(len(col_name)):
                            key = col_name[col][0]
                            value = row[col]
                            dict[key] = value
                        list.append(dict)
                finally:
                    cursor.close()
                    conn.close()

            except Exception as e:
                logger.error(
                    "-------------------- query tgt logintime fail: db:{0}\n tns:{1}\n sql_num:{2}\n sql:{3}\n e:{4}".format(
                        tgt['db'], tns, len(tgt['sql']), sql, str(e)))
        return list

    def test(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        # self.src_dbs = "RACAAWEB_RACABWEB_RACWWEB_RACASWEB"
        # self.tgt_dbs = "RACAVWEB_RACVVWEB_RACAUWEB_RACSYWEB"
        self.src_dbs = "RACAMRPT"
        self.tgt_dbs = ""
        self.type = "update"
        config = Config()
        self.depotdb_url = config.getDepotConnectionurl()
        self.main()

if __name__ == '__main__':
    job = ShareplexAdbmonTest()
    job.initialize()
    job.test()