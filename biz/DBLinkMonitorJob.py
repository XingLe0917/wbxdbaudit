import logging
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from prettytable import PrettyTable
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import DatabaseError

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from dao.vo.depotdbvo import DBLinkMonitorResultVO
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")

exclude_ls = {
    "RACDIAGS":["RACAFWEB"],
    "AMDIAG":["RACBQWEB","RACSYWEB","RACISWEB","RACAFWEB","RACPYWEB","RACAZWEB"],
    "FRDIAG":["RACSYWEB","RACBQWEB","RACAZWEB","RACAFWEB","RACISWEB","RACPYWEB"]

}

class DBLinkMonitor(WbxJob):
    def start(self):
        config = Config()
        self.depotdbDao = wbxauditdbdao(self.depot_connectionurl)
        self.check_db_name = ""
        self.check_by = ""
        if self.args[0] != "ALL":
            self.check_db_name=self.args[0]
        if len(self.args) == 2:
            self.check_by = self.args[1]
        logger.info("check_db_name=%s" %(self.check_db_name))
        logger.info("check_by=%s" % (self.check_by))
        self.roomId = config.getAlertRoomId()
        # lexing
        # self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5"
        self.all_db_task = []
        self.dbcache = {}
        self.dbcachebycode = {}
        self.tns_vo = {}
        self.baselinecache = {}
        self.schemacache = {}
        self.shareplexcache = {}
        self.failoverdbcache = {}
        self.depotdb_url = None
        self.main()

    def main(self):
        try:
            self.depotdbDao.connect()
            self.depotdbDao.startTransaction()
            tns_list = self.depotdbDao.getDBTnsInfo()
            self.tns_vo,self.depotdb_url = self.get_tns(tns_list)
            self.all_db_task = self.depotdbDao.get_all_dblink_monitor_list(self.check_db_name)
            dbcache_list = self.depotdbDao.all_db_dbcache_list()
            self.dbcache, self.dbcachebycode = self.get_dbcache(dbcache_list)
            baselinvoList = self.depotdbDao.listDBLinkBaseline()
            self.baselinecache = self.get_baselinecache(baselinvoList)
            schemalist = self.depotdbDao.getSchemaBySchemaType()
            self.schemacache = self.get_schemacache(schemalist)
            shareplexchannellist = self.depotdbDao.shareplexchannellist()
            self.shareplexcache = self.get_shareplexcache(shareplexchannellist)
            failoverdblist = self.depotdbDao.getFailoverdbFromShareplex()
            self.failoverdbcache = self.get_failoverdblist(failoverdblist)
            self.depotdbDao.commit()
            self.deport_db = create_engine(
                'oracle+cx_oracle://%s:%s@%s' % ("depot", "depot", self.depotdb_url),
                poolclass=NullPool, echo=False)
        except Exception as e:
            self.depotdbDao.rollback()
            raise wbxexception("Error occurred: %s" % (e))

        with ThreadPoolExecutor(max_workers=10) as t:
            all_task = []
            for item in self.all_db_task:
                logger.info("========================  {0} =========================== ".format(item))
                obj = t.submit(self.monitor_db_task, item['db_name'],item['trim_host'])
                all_task.append(obj)
            num = 0
            for future in as_completed(all_task):
                num += 1
                res = future.result()
                if num == len(self.all_db_task):
                    logger.info("All tasks finish!")
                    alertlist = self.get_alertlist_dblink_monitor(self.check_db_name)
                    self.send_alert_dblink_monitor(alertlist, self.check_by)

    def monitor_db_task(self,task_db_name,task_trim_host):
        try:
            db = self.dbcache[task_db_name]
            logger.info("deleteDBLinkMonitorResult, trim_host={0}, db_name={1}" .format(db['trim_host'], db['db_name']))
            self.deleteDBLinkMonitorResult(db['trim_host'], db['db_name'])
            logger.info("deleteDBLinkMonitorResult Done")
            connectionurl = self.tns_vo[task_db_name]
            baselinekey = db['appln_support_code'] + "_" + db['db_type']
            baselinvoList = self.baselinecache[baselinekey]
            for baselinevo in baselinvoList:
                logger.info(" ****  baselinevo: {0}  **** " .format(baselinevo))
                apptypelist = baselinevo['application_type'].split(",")
                tgtapptypelist = baselinevo['tgt_application_type'].split(",")
                if db['application_type'] in apptypelist:
                    # Get and loop schema list under the schema type
                    schemakey = task_trim_host.lower() + "_" +task_db_name.upper()+"_"+str(baselinevo['schematype']).lower()+"_"+str(baselinevo['appln_support_code']).lower()
                    if schemakey in self.schemacache:
                        schemalist = self.schemacache[schemakey]
                        for schema in schemalist:
                            # logger.info("[[ schema ]]: {0} ".format(schema))
                            schemaname = schema['schema']
                            schemapwd = schema['password']
                            logger.info("start to monitor db link on %s under schema %s" % (task_db_name, schemaname))
                            tgtdblist = []
                            if baselinevo['schematype'] == "splex":
                                logger.info(" case1: baselinevo.schematype=splex")
                                if db['db_name'] in self.shareplexcache:
                                    for spchannel in self.shareplexcache[db['db_name']]:
                                        srcdbname = spchannel['src_db']
                                        splexuser = "splex%s" % spchannel['port']
                                        if srcdbname == task_db_name:
                                            continue
                                        if schemaname.lower() != splexuser:
                                            continue

                                        if srcdbname in self.dbcache:
                                            srcdb = self.dbcache[srcdbname]
                                            if srcdb['appln_support_code'] == baselinevo['tgt_appln_support_code'] and srcdb['application_type'] in tgtapptypelist and srcdb['db_type'] == baselinevo['tgt_db_type']:
                                                if baselinevo['dblinktarget'] is not None and baselinevo['dblinktarget'] == "failoverdb":
                                                    if srcdb['appln_support_code'] == db['appln_support_code']:
                                                        logger.info(srcdb)
                                                        tgtdblist.append(srcdb)
                                                else:
                                                    logger.info(srcdb)
                                                    tgtdblist.append(srcdb)
                                else:
                                    logger.info("{0} not in shareplexcache" .format(db['db_name']))
                            else:
                                if baselinevo['dblinktarget'] == "failoverdb":
                                    logger.info(" case2: baselinevo.dblinktarget=failoverdb")
                                    failover_db_info = self.failoverdbcache[task_db_name]
                                    if failover_db_info:
                                        failover_db_name = failover_db_info['tgt_db']
                                        if failover_db_name in self.dbcache:
                                            failoverdb = self.dbcache[failover_db_name]
                                            logger.info(failoverdb)
                                            tgtdblist.append(failoverdb)
                                    else:
                                        logger.info("Do not find failover_db on {0}".format(db['db_name']))
                                else:
                                    logger.info(" case3: find tgtlist by baselinevo.tgt_appln_support_code=%s" %(baselinevo['tgt_appln_support_code']))
                                    dblist = self.dbcachebycode[str(baselinevo['tgt_appln_support_code']).upper()]
                                    for tgtdb in dblist:
                                        if tgtdb['appln_support_code'] == baselinevo['tgt_appln_support_code'] and tgtdb[
                                            'application_type'] in tgtapptypelist and tgtdb[
                                            'db_type'] == baselinevo['tgt_db_type']:
                                            logger.info(tgtdb)
                                            tgtdblist.append(tgtdb)

                            logger.info("tgtdblist:{0}".format(len(tgtdblist)))
                            # If above conditions all pass, then it means the user should have dblink, so build connection with the user and try to verify the db link
                            if len(tgtdblist)>0:
                                splitTgtdblist = wbxchatbot().getSplitList(tgtdblist, 4)
                                for split_tgt in splitTgtdblist:
                                    connect = None
                                    try:
                                        schemastatus = "SUCCEED"
                                        if len(split_tgt) > 0 :
                                            logger.info("start monitordblink for db_name=%s, schema=%s" % (task_db_name, schemaname))
                                            engine = create_engine(
                                                'oracle+cx_oracle://%s:%s@%s' % (schemaname, schemapwd, connectionurl),
                                                poolclass=NullPool, echo=False)
                                            connect = engine.connect()
                                            engine.connect().close()

                                            for tgtdb in split_tgt:
                                                logger.info("tgtdb=%s" %(tgtdb))
                                                if task_db_name in exclude_ls and tgtdb['db_name'] in exclude_ls[task_db_name]:
                                                    logger.info("skip %s" %(tgtdb['db_name']))
                                                    break
                                                if tgtdb['application_type'] in tgtapptypelist and tgtdb[
                                                    'db_type'] == baselinevo['tgt_db_type']:
                                                    # Generate db link by db link template defined in baseline
                                                    dblinkName = self.generateDBLinkName(tgtdb, baselinevo['dblink_name'])
                                                    status = None
                                                    errormsg = None
                                                    logger.info("monitordblink(%s) under schema %s to dblink %s" % (
                                                        task_db_name, schemaname, dblinkName))

                                                    if dblinkName == 'TO_<DOMAINNAME>':
                                                        status = "FAILED"
                                                        errormsg = "Do not find dblinkname under %s,%s" %(tgtdb['db_name'],baselinevo['dblink_name'])
                                                    else:
                                                        dblinkName = str(dblinkName).upper()
                                                        cursor = None

                                                        try:
                                                            SQL = "select sysdate from dual@%s" % dblinkName
                                                            cursor = connect.execute(SQL)
                                                            row = cursor.fetchone()
                                                            curdate = row[0]
                                                            status = "SUCCESS"
                                                        except DatabaseError as e:
                                                            logger.error("Error occurred: monitordblink(%s) under schema %s to dblink %s. Recreate it. baselinevo=%s " % (
                                                                    task_db_name, schemaname, dblinkName,baselinevo))
                                                            tgtschemaname = None
                                                            tgtschema = None
                                                            tgt_schemakey = ""
                                                            if "splex" == baselinevo['tgt_schematype']:
                                                                if "splex" == baselinevo['schematype']:
                                                                    tgtschemaname = schemaname
                                                                else:
                                                                    logger.info("find spchannel in shareplexcache, tgtdb=%s" %(tgtdb))
                                                                    for spchannel in self.shareplexcache[tgtdb['db_name']]:
                                                                        if spchannel['src_db'] == db['db_name'] or spchannel['tgt_db'] == db['db_name']:
                                                                            tgtschemaname = "SPLEX%s" % spchannel['port']
                                                                            break
                                                                logger.info("tgtschemaname=%s" %(tgtschemaname))
                                                                if tgtschemaname is not None:
                                                                    tgt_schemakey = tgtdb['trim_host'].lower() + "_" + tgtdb[
                                                                        'db_name'].upper() + "_" + str(
                                                                        baselinevo['tgt_schematype']).lower() + "_" + str(
                                                                        baselinevo['tgt_appln_support_code']).lower()
                                                                    logger.info("tgt_schemakey(trim_host,db_name,schematype,appln_support_code)=%s" %(tgt_schemakey))
                                                                    tgt_schemalist = self.schemacache[tgt_schemakey]
                                                                    if tgt_schemalist is not None and len(tgt_schemalist) > 0:
                                                                        for tgt in tgt_schemalist:
                                                                            if str(tgtschemaname).upper() == str(tgt['schema']).upper():
                                                                                tgtschema = tgt
                                                                                break
                                                            else:
                                                                tgt_schemakey = tgtdb['trim_host'].lower() + "_" + tgtdb[
                                                                    'db_name'].upper() + "_" + str(
                                                                    baselinevo['tgt_schematype']).lower() + "_" + str(
                                                                    baselinevo['tgt_appln_support_code']).lower()
                                                                tgt_schemalist = self.schemacache[tgt_schemakey]
                                                                if len(tgt_schemalist) > 0:
                                                                    tgtschema = tgt_schemalist[0]

                                                            if tgtschema is not None:
                                                                try:
                                                                    dblinkSQL = "drop database link %s" % dblinkName
                                                                    connect.execute(dblinkSQL)
                                                                except Exception as e:
                                                                    pass

                                                                try:
                                                                    dblinkSQL = "CREATE database link %s connect to %s identified by %s using '%s'" % (
                                                                        dblinkName, tgtschema['schema'], tgtschema['password'],
                                                                        self.tns_vo[tgtdb['db_name']])
                                                                    logger.info(dblinkSQL)
                                                                    connect.execute(dblinkSQL)
                                                                    cursor = connect.execute(SQL)
                                                                    row = cursor.fetchone()
                                                                    curdate = row[0]
                                                                    status = "SUCCESS"

                                                                except Exception as e:
                                                                    errormsg = '''error msg: %s''' % (str(e))
                                                                    logger.error(
                                                                        "Error occurred: monitordblink(%s) under schema %s to dbline %s with %s" % (
                                                                            task_db_name, schemaname, dblinkName, errormsg))
                                                                    status = "FAILED"
                                                            else:
                                                                status = "FAILED"
                                                                errormsg = "Do not find scheme=%s to dblink=%s (tgt_schemakey=%s)" % (
                                                                    schemaname, dblinkName, tgt_schemakey)
                                                                logger.info("tgt_schemakey(trim_host,db_name,schematype,appln_support_code)=%s" %(tgt_schemakey))
                                                                logger.info(errormsg)

                                                    monitorvo = self.get_DBLinkMonitorResult(db['trim_host'], db['db_name'],schemaname, dblinkName)

                                                    if monitorvo is None:
                                                        monitorvo = DBLinkMonitorResultVO()
                                                        monitorvo.trim_host = db['trim_host']
                                                        monitorvo.db_name = db['db_name']
                                                        monitorvo.schema_name = schemaname
                                                        monitorvo.dblink_name = dblinkName
                                                        monitorvo.monitor_time = wbxutil.getcurrenttime()
                                                        monitorvo.status = status
                                                        monitorvo.errormsg = str(errormsg).strip()
                                                        self.insertDBLinkMonitorResult(monitorvo)
                                                    else:
                                                        monitorvo.monitor_time = wbxutil.getcurrenttime()
                                                        monitorvo.status = status
                                                        monitorvo.errormsg = str(errormsg).strip()
                                            logger.info("end monitordblink for dbid=%s, schema=%s" % (task_db_name, schemaname))
                                    except DatabaseError as e:
                                        errormsg = "error msg: %s" % e
                                        logger.error("Database Error occurred: monitordblink(%s) under schema %s with %s" % (
                                        task_db_name, schemaname, errormsg))
                                        schemastatus = "FAILED"
                                        schemaerrormsg = errormsg
                                    except Exception as e:
                                        errormsg = "error msg: %s" % e
                                        logger.error("Exception occurred: monitordblink(%s) under schema %s with %s" % (
                                        task_db_name, schemaname, errormsg))
                                        schemastatus = "FAILED"
                                        schemaerrormsg = errormsg
                                    finally:
                                        if connect is not None:
                                            connect.close()

                                    if schemastatus == "FAILED":
                                        monitorvo = self.get_DBLinkMonitorResult(task_trim_host,task_db_name,schemaname,baselinevo['dblink_name'])
                                        if monitorvo is None:
                                            monitorvo = DBLinkMonitorResultVO()
                                            monitorvo.trim_host = task_trim_host
                                            monitorvo.db_name = task_db_name
                                            monitorvo.schema_name = schemaname
                                            monitorvo.dblink_name = baselinevo['dblink_name']

                                            monitorvo.monitor_time = wbxutil.getcurrenttime()
                                            monitorvo.status = schemastatus
                                            monitorvo.errormsg = str(schemaerrormsg).strip()
                                            self.insertDBLinkMonitorResult(monitorvo)
                                        else:
                                            monitorvo.monitor_time = wbxutil.getcurrenttime()
                                            monitorvo.status = schemastatus
                                            monitorvo.errormsg = str(schemaerrormsg).strip()
                    else:
                        logger.info(
                            "Do not find schema list. trim_host={0}, db_name={1}, schematype={2}, appln_support_code={3}".format(
                                task_trim_host.lower(), task_db_name.upper(), str(baselinevo['schematype']).lower(),
                            str(baselinevo['appln_support_code']).lower()))
        except Exception as e:
            raise wbxexception("Error occurred in DBLinkMonitor.monitordblink(%s,%s) with error msg: %s" % (task_db_name, task_trim_host,e))

    def generateDBLinkName(self,tgtdb, nameformat):
        if nameformat.find("<domainname>") >= 0:
            if tgtdb['web_domain']:
                return nameformat.replace("<domainname>", tgtdb['web_domain'])
            else:
                return nameformat.upper()
        elif nameformat.find("<dbname>") >= 0:
            return nameformat.replace("<dbname>", tgtdb['db_name'])
        elif nameformat.find("<clustername>") >= 0:
            return nameformat.replace("<clustername>", tgtdb['wbx_cluster'].rjust(2, 'd'))
        return nameformat.upper()

    def get_alertlist_dblink_monitor(self,check_db_name):
        conn = self.deport_db.connect()
        try:
            sql = '''
                            select trim_host,db_name,schema_name,dblink_name,status,errormsg,to_char(monitor_time,'YYYY-MM-DD hh24:mi:ss') monitor_time
                            from wbxdblinkmonitordetail
                            where status !='SUCCESS'
                            and monitor_time > sysdate-1/24*2
                            '''
            if check_db_name:
                sql += " and db_name='%s' " %(check_db_name)
            sql += " order by monitor_time desc"
            cursor = conn.execute(sql)
            res = cursor.fetchall()
            return [dict(row) for row in res]
        except DatabaseError as e:
            logger.error("Error occurred(get_alert_dblink_monitor): %s" % (e))

    def send_alert_dblink_monitor(self,alertlist,cec):
        chatjob = wbxchatbot()
        roomId = self.roomId
        if cec:
            people = chatjob.get_people_by_cec(cec)
            logger.info(people)
            toPersonId = people[0]['id']
            if len(alertlist)>0:
                lists = chatjob.getSplitList(alertlist, 10)
                index = 1
                for new_list in lists:
                    alert_title = "DBLink Monitor Result (%s/%s) " % (index, len(lists))
                    content = self.getServerItemsForBot(new_list)
                    # chatjob.create_message_to_people(toPersonId,content)
                    self.sendAlertToChatBot(content, alert_title, "", toPersonId)
                    index += 1
            else:
                alert_title = "DBLink Monitor Result"
                content = " %s Success" % (self.check_db_name)
                # chatjob.create_message_to_people(toPersonId,content)
                self.sendAlertToChatBot(content, alert_title, "", toPersonId)
        else:
            if len(alertlist) > 0:
                lists = chatjob.getSplitList(alertlist, 10)
                index = 1
                for new_list in lists:
                    logger.info("Send alert to chatbot")
                    content = self.getServerItemsForBot(new_list)
                    alert_title = "DBLink Monitor Result (%s/%s) " % (index, len(lists))
                    self.sendAlertToChatBot(content, alert_title, roomId,"")
                    index += 1

    def getServerItemsForBot(self, listdata):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        title = ["#","Trim Host", "DB Name", "Schema Name", "DBlink Name", "Status", "Monitor time","Errormsg"]
        index = 1
        for data in listdata:
            line_content = [index,data["trim_host"],data['db_name'],data['schema_name'],data['dblink_name'],data['status'],data['monitor_time'],data['errormsg']]
            x.add_row(line_content)
            index += 1
        x.field_names = title
        return str(x)

    def sendAlertToChatBot(self, rows_list,alert_title,roomId,toPersonId):
        msg = "### %s \n" %(alert_title)
        msg += "```\n {} \n```".format(rows_list)
        logger.info(msg)
        if roomId:
            wbxchatbot().alert_msg_to_dbabot_by_roomId(msg,roomId)
        if toPersonId:
            wbxchatbot().create_message_to_people(toPersonId,msg)

    def deleteDBLinkMonitorResult(self,trim_host, db_name):
        conn = self.deport_db.connect()
        try:
            sql = "DELETE FROM wbxdblinkmonitordetail WHERE trim_host='%s' and db_name='%s'" % (trim_host, db_name)
            conn.execute(sql)
        except DatabaseError as e:
            logger.error("Error occurred: deleteDBLinkMonitorResult")

    def insertDBLinkMonitorResult(self,monitorvo):
        conn = self.deport_db.connect()
        try:
            sql = '''
            insert into wbxdblinkmonitordetail(trim_host,db_name,schema_name,dblink_name,status,errormsg,monitor_time) values('%s','%s','%s','%s','%s','%s',sysdate)
            ''' %(monitorvo.trim_host, monitorvo.db_name, monitorvo.schema_name,monitorvo.dblink_name,monitorvo.status,monitorvo.errormsg)
            conn.execute(sql)
        except DatabaseError as e:
            logger.error("Error occurred(insertDBLinkMonitorResult): %s" % (e))

    def get_DBLinkMonitorResult(self,trim_host, db_name,schemaname, dblinkName):
        logger.info("get_DBLinkMonitorResult, trim_host={0}, db_name={1}, schemaname={2}, dblinkName={3}" .format(trim_host, db_name,schemaname, dblinkName))
        conn = self.deport_db.connect()
        try:
            sql = '''
            select trim_host,db_name,schema_name,dblink_name,status
            from wbxdblinkmonitordetail
            where trim_host= '%s' and db_name= '%s' and schema_name= '%s' and dblink_name ='%s'
            ''' % (trim_host, db_name,schemaname, dblinkName)
            cursor = conn.execute(sql)
            vo = cursor.fetchone()
            if vo:
                item = dict(vo)
                monitorvo = DBLinkMonitorResultVO()
                monitorvo.schema_name = item['schema_name']
                monitorvo.db_name = item['db_name']
                monitorvo.status = item['status']
                monitorvo.trim_host = item['trim_host']
                monitorvo.dblink_name = item['dblink_name']
                return monitorvo
            else:
                return vo
        except DatabaseError as e:
            logger.error("Error occurred(get_DBLinkMonitorResult): %s" % (e))

    def get_baselinecache(self,baselinvoList):
        baselinecache = {}
        for vo in baselinvoList:
            key = str(vo.appln_support_code) + "_" + str(vo.db_type)
            item = {}
            item['appln_support_code'] = vo.appln_support_code
            item['db_type'] = vo.db_type
            item['schematype'] = vo.schematype
            item['tgt_db_type'] = vo.tgt_db_type
            item['application_type'] = vo.application_type
            item['tgt_application_type'] = vo.tgt_application_type
            item['tgt_appln_support_code'] = vo.tgt_appln_support_code
            item['tgt_schematype'] = vo.tgt_schematype
            item['dblink_name'] = vo.dblink_name
            item['dblinktarget'] = vo.dblinktarget
            # baselinecache[key] = item
            if key not in baselinecache:
                baselinels = []
                baselinels.append(item)
            else:
                baselinels = baselinecache[key]
                baselinels.append(item)
            baselinecache[key] = baselinels
        return baselinecache

    def get_schemacache(self,schemalist):
        schemacache = {}
        for vo in schemalist:
            item = dict(vo)
            key = str(item['trim_host']).lower() + "_" +str(item['db_name']).upper()+"_"+str(item['schematype']).lower()+"_"+str(item['appln_support_code']).lower()
            if key not in schemacache:
                ls = []
                ls.append(item)
            else:
                ls = schemacache[key]
                ls.append(item)
            schemacache[key] = ls
        return schemacache

    def get_shareplexcache(self,shareplexchannellist):
        shareplexcache = {}
        for vo in shareplexchannellist:
            item = dict(vo)
            src_db = item['src_db']
            tgt_db = item['tgt_db']
            if src_db not in shareplexcache:
                src_ls = []
                src_ls.append(item)
            else:
                src_ls = shareplexcache[src_db]
                src_ls.append(item)
            shareplexcache[src_db] = src_ls
            if tgt_db not in shareplexcache:
                tgt_ls = []
                tgt_ls.append(item)
            else:
                tgt_ls = shareplexcache[tgt_db]
                tgt_ls.append(item)
            shareplexcache[tgt_db] = tgt_ls
        return shareplexcache

    def get_failoverdblist(self,failoverdblist):
        failoverdbcache = {}
        for vo in failoverdblist:
            item = dict(vo)
            src_db = item['src_db']
            failoverdbcache[src_db] = item
        return failoverdbcache

    def get_dbcache(self,dbcache_list):
        dbcache = {}
        dbcachebycode = {}
        for vo in dbcache_list:
            item = dict(vo)
            db_name = item['db_name']
            dbcache[db_name] = item
            appln_support_code = str(item['appln_support_code']).upper()
            if appln_support_code not in dbcachebycode:
                ls = []
                ls.append(item)
            else:
                ls = dbcachebycode[appln_support_code]
                ls.append(item)
            dbcachebycode[appln_support_code] = ls
        return dbcache,dbcachebycode

    def get_tns(self,tns_list):
        tns_vo = {}
        for vo in tns_list:
            item  = dict(vo)
            db_name = item['db_name']
            trim_host = item['trim_host']
            listener_port = item['listener_port']
            service_name = "%s.webex.com" % item['service_name']
            # key = str(db_name + "_" + trim_host)
            key = str(db_name)
            value = '(DESCRIPTION ='
            if item['scan_ip1']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip1'], listener_port)
            if item['scan_ip2']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip2'], listener_port)
            if item['scan_ip3']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip3'], listener_port)
            value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
                value, service_name)
            tns_vo[key] = value
        depotdb_url = tns_vo['AUDITDB']
        return tns_vo,depotdb_url

    def test(self):
        self.depotdbDao = wbxauditdbdao(self.depot_connectionurl)
        self.check_db_name = "AMDIAG"
        self.check_by = "lexing"
        self.main()


if __name__ == '__main__':
    job = DBLinkMonitor()
    job.initialize()
    job.test()