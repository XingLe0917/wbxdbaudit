import cx_Oracle
import logging

from common.config import Config

from biz.wbxjob import WbxJob
from common.cxOracle import CXOracle
from common.wbxexception import wbxexception
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")

class shareplexRealDelayJob(WbxJob):
    def start(self):
        config = Config()
        self.src_dbs = ""
        self.tgt_dbs = ""
        logger.info(self.args)
        if len(self.args) > 0:
            self.src_dbs = self.args[0]
        if len(self.args) > 1:
            self.tgt_dbs = self.args[1]
        logger.info("input src_db:{0}".format(self.src_dbs))
        logger.info("input tgt_db:{0}".format(self.tgt_dbs))
        self.depotdb_url = config.getDepotConnectionurl()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.source_db_info_ls = []
        self.target_db_info_ls = []
        self.FEDERAMP_DB = []
        self.source_db_infos = {}
        self.target_db_infos = {}
        self.tns_vo = {}
        self.db_user_pwd = {}
        self.main()

    def main(self):
        all_db_tns = []
        shareplexinfos = []
        pwds = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            all_db_tns = self.auditdao.getDBTnsInfo()
            logger.info("all_db_tns:%s" % (len(all_db_tns)))
            pwds = self.auditdao.getDBPWDBySchema("wbxdba")
            self.FEDERAMP_DB = self.auditdao.getFEDERAMP_DB()
            shareplexinfos = self.auditdao.getShareplexinfos(self.src_dbs, self.tgt_dbs)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

        for vo in pwds:
            item = dict(vo)
            self.db_user_pwd[item['db_name']] = item['pwd']

        logger.info("db_user_pwd:{0}" .format(len(self.db_user_pwd)))

        for vo in all_db_tns:
            item = dict(vo)
            key = str(item['db_name'] + "_" + item['trim_host'])
            value = self.db_connect_str(item['listener_port'], item['service_name'],item['scan_ip1'], item['scan_ip2'],
                                        item['scan_ip3'])
            self.tns_vo[key] = value
        self.process_shareplexinfos(shareplexinfos)
        logger.info("source_db_info_ls={0}".format(len(self.source_db_info_ls)))
        logger.info("target_db_info_ls={0}".format(len(self.target_db_info_ls)))
        self.operations_src()
        # self.operations_tgt()

    def test_src(self):
        logger.info("--------- start test src db ----------")
        index = 1
        for dbinfo in self.source_db_info_ls:
            db_name = str(dbinfo['key']).split("_")[0]
            logger.info("(%s/%s)key=%s" % (index, len(self.source_db_info_ls), dbinfo['key']))
            if db_name not in self.FEDERAMP_DB:
                cx_Oracle = CXOracle("wbxdba","china2000",dbinfo['tns'])
                sql = '''
                select t.status,to_char(actual_start_date,'yyyy-mm-dd hh24:mi:ss') actual_start_date,
                to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') sysdate1 from dba_scheduler_job_run_details t  where job_name = 'SPLEX_MONITOR_REAL_TIME' 
                and to_char(actual_start_date,'yyyy-mm-dd hh24:mi:ss') > '2022-04-28 14:20:00' order by t.actual_start_date desc
                '''
                res = cx_Oracle.select(sql)
                if res[0]['STATUS'] !="SUCCEEDED":
                    logger.error("ERROR --------  %s" %(db_name))
                index += 1
            else:
                logger.info("skip FEDERAMP DB %s" % (db_name))

    """
    operations on src DB:
    1.create procedure sp_shareplex_monitor 
        sql like: update SPLEX2000.splex_monitor_adb SET logtime=sysdate WHERE direction='%s' and src_db='%s' and src_host='%s' and PORT_NUMBER = %s ...
    2.grant_sql
        sql like : GRANT CREATE JOB TO wbxdba
    3.create oracle job SPLEX_MONITOR_REAL_TIME to exec procedure sp_shareplex_monitor 
        INTERVAL=15 
    """
    def operations_src(self):
        logger.info("--------- start operations src db ----------")
        index = 1
        for dbinfo in self.source_db_info_ls:
            db_name = str(dbinfo['key']).split("_")[0]
            logger.info("(%s/%s)key=%s" %(index,len(self.source_db_info_ls),dbinfo['key']))
            if db_name not in self.FEDERAMP_DB:
                connect = cx_Oracle.connect("wbxdba/china2000@" + dbinfo['tns'])
                cursor = connect.cursor()
                try:
                    monitor_adb_delay_sql = dbinfo['procedure_sql']
                    logger.info("\t1.exec procedure_sql")
                    logger.info(monitor_adb_delay_sql)
                    # cursor.execute(monitor_adb_delay_sql)
                    connect.commit()
                except Exception as e:
                    raise wbxexception(e)

                connect_sys = cx_Oracle.connect("sys/sysnotallow@" + dbinfo['tns'], mode=cx_Oracle.SYSDBA)
                cursor_sys = connect_sys.cursor()
                grant_sql = "GRANT CREATE JOB TO wbxdba"
                try:
                    logger.info("\t2.grant_sql")
                    logger.info(grant_sql)
                    # cursor_sys.execute(grant_sql)
                    connect_sys.commit()
                except Exception as e:
                    raise wbxexception(e)
                finally:
                    cursor_sys.close()
                    connect_sys.close()

                try:
                    monitor_adb_delay_sql = dbinfo['schedure_sql']
                    logger.info("\t3.exec schedure_sql")
                    logger.info(monitor_adb_delay_sql)
                    # cursor.execute(monitor_adb_delay_sql)
                    connect.commit()
                except Exception as e:
                    raise wbxexception(e)

                index+=1
            else:
                logger.info("skip FEDERAMP DB %s" %(db_name))

    """
    operations on target oracle
    1.create procedure sp_output_splexmonitor_data
    2.create trigger trg_splex_delay_<schema>_<qname> after update on <schema>.splex_monitor_adb_<qname> and insert into splex_monitor_adb_real_delay
    3.create procedure sp_output_splexmonitor_data, take the last 2 minutes of data from splex_monitor_adb_real_delay and save to file <db_name>_splex_monitor_delay_influx.log 
    4.exec grant sql
    5.reate oracle job OUTPUT_SPLEX_MONITOR_REAL_TIME to exec sp_output_splexmonitor_data,INTERVAL=1 MINUTELY
    """
    def operations_tgt(self):
        logger.info("--------- start operations tgt db ---------")
        index = 1
        for dbinfo in self.target_db_info_ls:
            db_name = str(dbinfo['key']).split("_")[0]
            logger.info("(%s/%s)key=%s" % (index, len(self.target_db_info_ls), dbinfo['key']))
            if db_name not in self.FEDERAMP_DB:
                connect = cx_Oracle.connect("wbxdba/china2000@" + dbinfo['tns'])
                cursor = connect.cursor()
                try:
                    monitor_adb_delay_sql = dbinfo['monitor_adb_delay_sql']
                    logger.info("\t1.exec monitor_adb_delay_sql")
                    logger.info(monitor_adb_delay_sql)
                    # cursor.execute(monitor_adb_delay_sql)
                    connect.commit()
                except Exception as e:
                    raise wbxexception(e)

                drop_trigger_sql_list = dbinfo['drop_trigger_sql_list']
                try:
                    for sql in drop_trigger_sql_list:
                        logger.info(sql)
                        # cursor.execute(sql)
                        connect.commit()
                except Exception as e:
                    logger.info("Error, drop trigger, ignore it, e={0}" .format(str(e)))

                trigger_sql_list = dbinfo['trigger_sql_list']
                logger.info("\t2.trigger_sql_list:{0}" .format(len(trigger_sql_list)))
                try:
                    for sql in trigger_sql_list:
                        logger.info(sql)
                        # cursor.execute(sql)
                        connect.commit()
                except Exception as e:
                    raise wbxexception(e)

                logger.info("\t3.exec sp_output_procedure_sql")
                sp_output_procedure_sql = dbinfo['sp_output_procedure_sql']
                try:
                    logger.info(sp_output_procedure_sql)
                    # cursor.execute(sp_output_procedure_sql)
                    connect.commit()
                except Exception as e:
                    raise wbxexception(e)

                connect_sys = cx_Oracle.connect("sys/sysnotallow@" + dbinfo['tns'], mode=cx_Oracle.SYSDBA)
                cursor_sys = connect_sys.cursor()
                grant_sql_list = dbinfo['grant_sql_list']
                logger.info("\t4.grant_sql_list:{0}" .format(len(grant_sql_list)))
                try:
                    for grant_sql in grant_sql_list:
                        logger.info(grant_sql)
                        # cursor_sys.execute(grant_sql)
                        connect_sys.commit()
                except Exception as e:
                    raise wbxexception(e)
                finally:
                    cursor_sys.close()
                    connect_sys.close()

                sp_output_scheduler_sql = dbinfo['sp_output_scheduler_sql']
                try:
                    logger.info("\t5.exec sp_output_scheduler_sql")
                    logger.info(sp_output_scheduler_sql)
                    # cursor.execute(sp_output_scheduler_sql)
                    connect.commit()
                except Exception as e:
                    raise wbxexception(e)

                # to_wbxadbmon_procedure_sql = dbinfo['to_wbxadbmon_procedure_sql']
                # try:
                #     logger.info("\t6.exec to_wbxadbmon_procedure_sql")
                #     # print(to_wbxadbmon_procedure_sql)
                #     cursor.execute(to_wbxadbmon_procedure_sql)
                #     connect.commit()
                # except Exception as e:
                #     raise wbxexception(e)
                #
                # to_wbxadbmon_scheduler_sql = dbinfo['to_wbxadbmon_scheduler_sql']
                # try:
                #     logger.info("\t7.exec to_wbxadbmon_scheduler_sql")
                #     # print(to_wbxadbmon_scheduler_sql)
                #     cursor.execute(to_wbxadbmon_scheduler_sql)
                #     connect.commit()
                # except Exception as e:
                #     raise wbxexception(e)
                index += 1
            else:
                logger.info("skip FEDERAMP DB %s" % (db_name))

    def db_connect_str(self, listener_port, service_name, scan_ip1, scan_ip2, scan_ip3):
        service_name = "%s.webex.com" % service_name
        value = '(DESCRIPTION ='
        if scan_ip1:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, scan_ip1, listener_port)
        if scan_ip2:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, scan_ip2, listener_port)
        if scan_ip3:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, scan_ip3, listener_port)
        value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
            value, service_name)
        return value

    def test(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        config = Config()
        self.source_db_info_ls = []
        self.target_db_info_ls = []
        self.depotdb_url = config.getDepotConnectionurl()
        self.src_dbs = "RACACWEB"
        self.tgt_dbs = ""
        self.tns_vo = {}
        self.db_user_pwd = {}
        self.main()

    def process_shareplexinfos(self,shareplexinfos):
        source_db_infos = {}
        target_db_infos = {}
        for channel_data in shareplexinfos:
            replication_to_list = []
            source_db_key = str(channel_data["src_db"]) +"_"+str(channel_data["src_trim_host"])
            source_db_value = dict(channel_data)
            port = str(channel_data["port"])
            schema = "SPLEX"+str(channel_data["port"])
            # schema = "wbxdba"
            src_table_name = "splex_monitor_adb"
            src_qName = source_db_value["qname"]
            if src_qName:
                src_table_name += "_" + src_qName
            src_sql_update = "EXECUTE IMMEDIATE '"
            src_sql_update += "update %s.%s SET logtime=sysdate WHERE direction=''%s'' and src_db=''%s'' and src_host=''%s'' and PORT_NUMBER = %s " % (
            schema, src_table_name, channel_data["replication_to"], channel_data["src_db"],channel_data["src_trim_host"],port)
            src_sql_update +="';"
            # print(src_sql_update)
            if channel_data["replication_to"] not in replication_to_list:
                replication_to_list.append(channel_data["replication_to"])
            source_db_value['username'] = schema
            source_db_value['connstr'] = self.tns_vo[source_db_key]
            source_db_value['table_name'] = src_table_name
            source_db_value['port'] = port
            source_db_value['src_db'] = channel_data["src_db"]
            source_db_value['src_host'] = channel_data["src_trim_host"]
            source_db_value['replication_to'] = channel_data["replication_to"]
            source_db_value['src_sql_update'] = src_sql_update
            if source_db_infos.get(source_db_key, None) is None:
                source_db_infos[source_db_key] = []
            source_db_infos[source_db_key].append(source_db_value)

            target_db_key = str(channel_data["tgt_db"])+"_"+channel_data["tgt_trim_host"]
            target_db_value = dict(channel_data)
            tgt_table_name = "splex_monitor_adb"
            tgt_qName = target_db_value["qname"]
            if tgt_qName:
                tgt_table_name += "_" + tgt_qName
            target_db_value['username'] = schema
            target_db_value['table_name'] = tgt_table_name
            target_db_value['connstr'] = self.tns_vo[target_db_key]
            # target_db_value['channel'] = channel_data
            # target_db_value['sql'] = tgt_sql
            if target_db_infos.get(target_db_key, None) is None:
                target_db_infos[target_db_key] = []
            target_db_infos[target_db_key].append(target_db_value)

        # merge src db operations
        for key in source_db_infos.items():
            sql_list = []
            for src in key[1]:
                sql_list.append(src['src_sql_update'])
            sql = "\n".join(str(i) for i in sql_list)

            procedure_sql = '''
            create or replace procedure sp_shareplex_monitor 
            AUTHID CURRENT_USER 
            IS
            BEGIN
                %s
            END sp_shareplex_monitor;
            ''' %(sql)

            schedure_sql = '''
            DECLARE
            v_count NUMBER;
                  v_job_name VARCHAR2(64) := 'SPLEX_MONITOR_REAL_TIME';
                BEGIN
                  SELECT count(*) INTO v_count FROM dba_scheduler_jobs WHERE job_name = v_job_name;
                  IF v_count = 1 THEN
                     dbms_scheduler.drop_job(job_name=>v_job_name);
                     dbms_scheduler.CREATE_JOB(job_name=>v_job_name,job_type=>'PLSQL_BLOCK',job_action=>'BEGIN sp_shareplex_monitor; END;',start_date=>SYSDATE,repeat_interval=>'FREQ=SECONDLY;INTERVAL=15',enabled => TRUE,auto_drop=>FALSE);
                  ELSE
                     dbms_scheduler.CREATE_JOB(job_name=>v_job_name,job_type=>'PLSQL_BLOCK',job_action=>'BEGIN sp_shareplex_monitor; END;',start_date=>SYSDATE,repeat_interval=>'FREQ=SECONDLY;INTERVAL=15',enabled => TRUE,auto_drop=>FALSE);
                  END IF;
                END;
            '''
            source_db_infos = {}
            source_db_infos['key'] = str(key[0])
            source_db_infos['data'] = key[1]
            source_db_infos['procedure_sql'] = procedure_sql
            source_db_infos['schedure_sql'] = schedure_sql
            source_db_infos['tns'] = self.tns_vo[key[0]]
            self.source_db_info_ls.append(source_db_infos)

        # merge tgt db operations
        monitor_adb_delay_sql = '''
        DECLARE
        nCount NUMBER;
        v_sql VARCHAR2(4000);
        BEGIN
            SELECT count(*) into nCount FROM dba_tables where table_name = 'SPLEX_MONITOR_ADB_REAL_DELAY';
        IF(nCount <= 0)
            THEN
            v_sql:='create table splex_monitor_adb_real_delay(DIRECTION VARCHAR2(30),SRC_HOST VARCHAR2(30),SRC_DB VARCHAR2(30),LOGTIME DATE,PORT_NUMBER NUMBER,accepttime DATE DEFAULT SYSDATE)';
            EXECUTE IMMEDIATE v_sql;
            END IF;
        END;
        '''
        sp_output_procedure_sql = '''
        create or replace procedure sp_output_splexmonitor_data
        AUTHID CURRENT_USER 
        AS
            vdbname         VARCHAR2(30);
            vLogFile        UTL_FILE.FILE_TYPE; 
            vLogRecord      VARCHAR2(32767);
        BEGIN
            SELECT name INTO vdbname FROM v$database;
            vLogFile := UTL_FILE.FOPEN('MONITOR_LOG_DIR', vdbname||'_splex_monitor_delay_influx.log', 'A', 32767); 
            FOR rec in (
            SELECT direction,src_db,PORT_NUMBER,to_char(logtime,'YYYY-MM-DD/hh24:mi:ss') logtime,to_char(accepttime,'YYYY-MM-DD/hh24:mi:ss') accepttime,src_host src_trim_host,
            trunc((accepttime-LOGTIME)*24*60*60) as delayinsecond,TRIM(TRUNC(ceil((LOGTIME - DATE '1970-01-01' ) * 60 * 60 * 24) * 1000000000)) sample_time 
            FROM splex_monitor_adb_real_delay WHERE logtime >= sysdate -2/(24*60) 
            )
            LOOP
                vLogRecord := 'wbxdb_monitor_sp_delay,src_db_name='||rec.src_db||',tgt_db_name='||vdbname||',splex_port='||rec.port_number||',direction='||rec.direction||',src_trim_host='||rec.src_trim_host||' delayinsecond='||rec.delayinsecond||' '||rec.sample_time;
                UTL_FILE.PUT_LINE(vLogFile, vLogRecord, FALSE);
            END LOOP;
            UTL_FILE.FFLUSH(vLogFile);
            UTL_FILE.FCLOSE(vLogFile); 
            DELETE FROM splex_monitor_adb_real_delay WHERE logtime < sysdate - 1;
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                IF UTL_FILE.IS_OPEN(vLogFile) THEN
                    UTL_FILE.FCLOSE(vLogFile); 
                END IF;
                UTL_FILE.FCLOSE_ALL;
                dbms_output.put_line(SQLERRM);
        END sp_output_splexmonitor_data;
        '''

        grant_sql_list = []
        grant_sql_list.append("grant execute on utl_file to wbxdba")
        grant_sql_list.append("create or replace directory MONITOR_LOG_DIR as '/var/log/dbmonitor/data'")
        grant_sql_list.append("grant read on directory MONITOR_LOG_DIR to wbxdba")
        grant_sql_list.append("grant write on directory MONITOR_LOG_DIR to wbxdba")
        sp_output_scheduler_sql = '''
        DECLARE
          v_count NUMBER;
          v_job_name VARCHAR2(64) := 'OUTPUT_SPLEX_MONITOR_REAL_TIME';
        BEGIN
          SELECT count(*) INTO v_count FROM dba_scheduler_jobs WHERE job_name = v_job_name;
          IF v_count = 1 THEN
            dbms_scheduler.drop_job(job_name=>v_job_name);
            DBMS_SCHEDULER.CREATE_JOB(job_name=>v_job_name,job_type=>'PLSQL_BLOCK',job_action=>'BEGIN sp_output_splexmonitor_data; END;',start_date=>SYSDATE,repeat_interval=>'FREQ=MINUTELY;INTERVAL=1',enabled => TRUE,auto_drop=>FALSE);
          ELSE
             DBMS_SCHEDULER.CREATE_JOB(job_name=>v_job_name,job_type=>'PLSQL_BLOCK',job_action=>'BEGIN sp_output_splexmonitor_data; END;',start_date=>SYSDATE,repeat_interval=>'FREQ=MINUTELY;INTERVAL=1',enabled => TRUE,auto_drop=>FALSE);
          END IF;
        END;
        '''
        to_wbxadbmon_procedure_sql ='''
        create or replace procedure splex_delay_to_wbxadbmon
        AS
            vSQL          VARCHAR2(4000);
            vDBName       v$database.name%TYPE;
        BEGIN
            SELECT name into vDBName FROM v$database;
            FOR rec in (
            select direction,src_host,src_db,port_number,max(accepttime) accepttime
            from wbxdba.splex_monitor_adb_real_delay
            where logtime>sysdate -3/24 
            group by direction,src_host,src_db,port_number
            )
            LOOP
            update depot.wbxadbmon1@DEPOT set lastreptime=rec.accepttime,montime=sysdate where src_db= ''||rec.src_db||'' and tgt_db=''||vDBName||'' and port=rec.port_number and replication_to=''||rec.direction||'' and src_host like ''||rec.src_host||'%' ;
            END LOOP;
            COMMIT;
        END splex_delay_to_wbxadbmon;
        '''

        to_wbxadbmon_scheduler_sql = '''
        DECLARE
            v_count NUMBER;
                  v_job_name VARCHAR2(64) := 'OUTPUT_TO_WBXADBMON';
                BEGIN
                  SELECT count(*) INTO v_count FROM dba_scheduler_jobs WHERE job_name = v_job_name;
                  IF v_count = 1 THEN
                    dbms_scheduler.drop_job(job_name=>v_job_name);
                    dbms_scheduler.CREATE_JOB(job_name=>v_job_name,job_type=>'PLSQL_BLOCK',job_action=>'BEGIN splex_delay_to_wbxadbmon; END;',start_date=>SYSDATE,repeat_interval=>'FREQ=MINUTELY;INTERVAL=1',enabled => TRUE,auto_drop=>FALSE);
                  ELSE
                    dbms_scheduler.CREATE_JOB(job_name=>v_job_name,job_type=>'PLSQL_BLOCK',job_action=>'BEGIN splex_delay_to_wbxadbmon; END;',start_date=>SYSDATE,repeat_interval=>'FREQ=MINUTELY;INTERVAL=1',enabled => TRUE,auto_drop=>FALSE);
                  END IF;
                END;
        '''
        for key in target_db_infos.items():
            target_db_infos = {}
            target_db_infos['key'] = str(key[0])
            trigger_table_name_list = []
            for db in key[1]:
                table_name_key = "%s.%s" % (db['username'], db['table_name'])
                if table_name_key not in trigger_table_name_list:
                    trigger_table_name_list.append(table_name_key)

            trigger_sql_list = []
            drop_trigger_sql_list = []
            trigger_sql_key_map=[]
            for trigger_table in trigger_table_name_list:
                schema = trigger_table.split(".")[0]
                table = trigger_table.split(".")[1]
                trigger_name = "trg_splex_delay_%s" % (schema)
                if len(trigger_name)>30:
                    trigger_name = "trg_sp_delay_%s" % (schema)
                qname = ""
                if "splex_monitor_adb_" in table:
                    qname = table.split("splex_monitor_adb_")[1]
                    trigger_name ="%s_%s" %(trigger_name,qname)
                trigger_sql_key= "%s.%s" %(schema,table)
                drop_trigger_sql = '''
                drop trigger %s''' %(trigger_name)
                trigger_sql = '''               
                create or replace trigger %s after update on %s.%s for each row 
                WHEN (USER LIKE 'SPLEX%s')
                BEGIN 
                IF :NEW.logtime is not null THEN
                    INSERT INTO splex_monitor_adb_real_delay(direction,SRC_HOST,SRC_DB,LOGTIME,PORT_NUMBER) 
                    VALUES(:NEW.direction,:NEW.SRC_HOST,:NEW.SRC_DB,:NEW.LOGTIME,:NEW.PORT_NUMBER); 
                END IF;
                END;''' %(trigger_name,schema,table,"%")
                if trigger_sql_key not in trigger_sql_key_map:
                    trigger_sql_list.append(trigger_sql)
                    trigger_sql_key_map.append(trigger_sql_key)
                    drop_trigger_sql_list.append(drop_trigger_sql)
            target_db_infos['monitor_adb_delay_sql'] = monitor_adb_delay_sql
            target_db_infos['drop_trigger_sql_list'] = drop_trigger_sql_list
            target_db_infos['trigger_sql_list'] = trigger_sql_list
            target_db_infos['sp_output_procedure_sql'] = sp_output_procedure_sql
            target_db_infos['grant_sql_list'] = grant_sql_list
            target_db_infos['sp_output_scheduler_sql'] = sp_output_scheduler_sql
            target_db_infos['to_wbxadbmon_procedure_sql'] = to_wbxadbmon_procedure_sql
            target_db_infos['to_wbxadbmon_scheduler_sql'] = to_wbxadbmon_scheduler_sql
            target_db_infos['tns'] = self.tns_vo[key[0]]
            self.target_db_info_ls.append(target_db_infos)


if __name__ == '__main__':
    job = shareplexRealDelayJob()
    job.initialize()
    job.test()