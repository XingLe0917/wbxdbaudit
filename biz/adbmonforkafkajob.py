import logging
import queue

from biz.wbxjob import WbxJob
import os, sys, re, time, cx_Oracle, base64
import xml.etree.ElementTree as ET
from kafka import KafkaConsumer
from concurrent.futures.thread import ThreadPoolExecutor
from dao.wbxauditdbdao import wbxauditdbdao
from common.wbxmail import wbxemailmessage
from common.wbxmail import sendemail_for_adbmon
from prettytable import PrettyTable
from common.wbxchatbot import wbxchatbot

logger = logging.getLogger("dbaudit")

class ADBMonForKafkaJob(WbxJob):
    def __init__(self):
        super(ADBMonForKafkaJob,self).__init__()
        super(ADBMonForKafkaJob, self).initialize()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self._kafkaMapping = []
        kafkasplexlist = self._getKafkaSplex()
        for item in kafkasplexlist:
            qname_list = item['qname_list'].split(',')
            monitor_table = list("splex_monitor_adb_" + i for i in qname_list)
            monitor_table = [m.replace('_0', '') for m in monitor_table]
            schema = "splex" + item['port']
            monitor_table_schema = []
            channel_list = self._getChannels(schema)
            if len(channel_list) == 0:
                continue
            str1 = item['tgt_db'].split("-")
            dbtype=channel_list[0]['appln_support_code']
            connectinfo=channel_list[0]['connectinfo']
            topic = ""
            if dbtype == "TEO":
                monitor_table_schema = list("" + schema + "." + i for i in monitor_table)
                dbtype1 = "teodb"
                topic = list(str1[0]+"_"+dbtype1+"_"+""+schema.upper()+"_"+ i.upper() + "_pda_hbase" for i in monitor_table)
            elif dbtype == "MEDIATE":
                monitor_table_schema = list("" + schema + "." + i for i in monitor_table)
                dbtype1 = "mediation"
                topic = list(str1[0]+"_"+dbtype1+"_"+""+schema.upper()+"_"+ i.upper() + "_CiscoIT_sbp" for i in monitor_table)
            self._kafkaChannel = {
                "src_connectioninfo": connectinfo,
                "monitor_table": monitor_table_schema,
                "src_host": item['src_host'],
                "src_db": item['src_db'],
                "port": item['port'],
                "tgt_host": item['tgt_host'],
                "direction": item['direction'],
                "broker": item['tgt_db']+".webex.com:9092",
                "topic": topic
            }
            # print(self._kafkaChannel)
            self._kafkaMapping.append(self._kafkaChannel)
        self._threadpool = ThreadPoolExecutor(len(self._kafkaMapping) * 3)
        self._dataqueue = queue.Queue()
        self._scan_times = 0

    def _getKafkaSplex(self):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            splexinfolist = self.auditdao.getKafkaSplexInfo()
            self.auditdao.commit()
            return splexinfolist
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    def _getChannels(self,schema):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dblist = self.auditdao.getKafkaDBList(schema)
            self.auditdao.commit()
            return dblist
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    def _startConsumer(self, broker, topic, direction):
        logger.info("start consumer for %s on broker %s" % (topic, broker))
        consumer = KafkaConsumer(topic, group_id=direction, bootstrap_servers=broker)
        try:
            for message in consumer:
                (tableName, col_logtime, col_direction, col_src_host, col_src_db, dml) = self.dealxml(message.value)
                if tableName == "" or tableName == None:
                    continue
                else:
                    if dml != 'del':
                        logtime = re.sub('T', ' ', col_logtime)
                        port = tableName.split(".")[0].replace("SPLEX","")
                        self._dataqueue.put({"lastreptime": logtime, "src_host": col_src_host,"src_db":col_src_db,"port":port, "direction": col_direction})
                    else:
                        continue
        except KeyboardInterrupt as e:
            logger.error("ADBMONForKafkaJob._startConsumer with broker=%s,topic=%s error occurred: %s" % (broker,topic, e))

    def _updateSplexADBMonData(self):
        for channel in self._kafkaMapping:
            conn = None
            csr = None
            try:
                connect_str = channel["src_connectioninfo"]
                tableList = channel["monitor_table"]
                conn = cx_Oracle.connect(connect_str)
                csr = conn.cursor()
                for table_name in tableList:
                    direction = channel["direction"] + table_name.split("splex_monitor_adb")[-1]
                    upsql = "update %s set logtime=SYSDATE where direction='%s'" %(table_name, direction)
                    # print(upsql)
                    csr.execute(upsql)
                conn.commit()
            except Exception as e:
                logger.error("ADBMONForKafkaJob._updateSplexADBMonData error occurred: %s" % e)
            finally:
                try:
                    if csr is not None:
                        csr.close()
                    if conn is not None:
                        conn.close()
                except Exception as e:
                    print(e)

    def _updateADBMON(self):
        if self._dataqueue.qsize() > 0:
            try:
                self._auditdao.connect()
                self._auditdao.startTransaction()
                while not self._dataqueue.empty():
                    resData = self._dataqueue.get()
                    self._auditdao.updatewbxadbmonforkafka(resData["src_db"], resData["src_host"], resData["direction"], resData["lastreptime"], resData["port"])
                    logger.info(resData)
                self._auditdao.commit()
            except Exception as e:
                self._auditdao.rollback()
                logger.error("ADBMONForKafkaJob._updateADBMON error occurred: %s" % e)
            finally:
                self._auditdao.close()

    def _sendAlert(self):
        if self._scan_times == 9:
            try:
                self._auditdao.connect()
                self._auditdao.startTransaction()
                rows = self._auditdao.getADBMonDataForKafka()
                self._auditdao.commit()
                if rows is not None and len(rows) > 0:
                    self.sendAlertMsg("Email", rows)
                self._scan_times = 0

            except Exception as e:
                self._auditdao.rollback()
                logger.error("ADBMONForKafkaJob._updateADBMON error occurred: %s" % e)
            finally:
                self._auditdao.close()

    #send message to chatbot/email
    def sendAlertMsg(self,Target,server_list):
        msg = ' Send Alert Message'
        print(msg)
        hasError = False

        bot_server_list = server_list[:21]
        mail_server_list = server_list[:31]
        if Target == "All":
            s_bot = self.getServerItemsForBot(bot_server_list)
            self.sendAlertToChatBot(s_bot)
            self.sendAlertEmail(mail_server_list)
        elif Target == "ChatBot":
            s_bot = self.getServerItemsForBot(bot_server_list)
            self.sendAlertToChatBot(s_bot)
        elif Target == "Email":
            self.sendAlertEmail(mail_server_list)
        else:
            pass
        return hasError

    #send problem server list alert to chat bot
    def sendAlertToChatBot(self, rows_list):
        msg = "Send Alert Message To ChatBot"
        print(msg)

        hasError = False
        msg = "### PROD : Oracle to Kafka Shareplex Replication Monitoring Alert! \n"
        msg += "```\n {} \n```".format(rows_list)
        # wbxchatbot().alert_msg_to_dbabot(msg)
        # wbxchatbot().alert_msg_to_dbateam(msg)
        wbxchatbot().alert_msg_to_dbabot_by_roomId(msg,"Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")
        return hasError

    #use PrettyTable function to display chatbot alert
    def getServerItemsForBot(self,listdata):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        # title = ["Check Time","Host Name","Port Number","Check Status","User Name","UserID DB","UserID SP","Comments"]
        title = ["SRC_HOST", "SRC_DB", "PORT", "REPLICATION_TO", "TGT_DB", "TGT_HOST", "LASTREP_TIME",
                 "MONITOR_TIME", "LAG_BY(min)"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title
        print(x)
        return str(x)

    def sendAlertEmail(self, rows_list):
        msg = "Send Alert Message To Mail"
        print(msg)
        msg = wbxemailmessage(emailtopic="PROD : Oracle to Kafka Shareplex Replication Monitoring Alert!", receiver="cwopsdba@cisco.com",
                               emailcontent=rows_list)
        html_template = """
            <html>
        <head>
        <style> 
         table, th, td {{ border: 1px solid black; border-collapse: collapse; }}
          th, td {{ padding: 5px; }}
        </style>
        </head>
        <body><p>Hello, Team, This data is from depotdb.</p>
        <p>Here is alert detail data:</p>
        {table}
        <p>Regards,</p>
        <p>PCCP Team</p>
        </body></html>

            """
        title = ["SRC_HOST", "SRC_DB", "PORT", "REPLICATION_TO", "TGT_DB", "TGT_HOST", "LASTREP_TIME",
                 "MONITOR_TIME", "LAG_BY(min)"]

        sendemail_for_adbmon(msg,rows_list,title,html_template)


    def start(self):
        self._auditdao = wbxauditdbdao(self.depot_connectionurl)
        for channel in self._kafkaMapping:
            broker = channel["broker"]
            topics = channel["topic"]
            direction = channel["direction"]
            for topic in topics:
                self._threadpool.submit(fn=self._startConsumer, broker=broker, topic=topic, direction=direction)
        while True:
            self._updateADBMON()
            self._sendAlert()
            self._updateSplexADBMonData()
            time.sleep(60)
            self._scan_times += 1

    def dealxml(self, xml):
        tableName = ""
        dml = ""
        col_logtime = ""
        col_direction = ""
        col_src_host = ""
        col_src_db = ""
        try:
            root = ET.fromstring(xml)
            tbl = root.find('tbl')
            tableName = tbl.get('name')
            opr = tbl.find('cmd')
            dml = opr.get('ops')
            row = opr.find('row')
            for column in row.findall('col'):
                if column.get('name') == 'LOGTIME':
                    col_logtime = column.text
                elif column.get('name') == 'DIRECTION':
                    col_direction = column.text
                elif column.get('name') == 'SRC_HOST':
                    col_src_host = column.text
                elif column.get('name') == 'SRC_DB':
                    col_src_db = column.text.upper()
                else:
                    pass
            if dml == 'upd':
                dmltab = row.find('lkup')
                for column in dmltab.findall('col'):
                    if column.get('name') == 'DIRECTION':
                        col_direction = column.text
                    elif column.get('name') == 'SRC_HOST':
                        col_src_host = column.text
                    elif column.get('name') == 'SRC_DB':
                        col_src_db = column.text.upper()
                    else:
                        pass
        except UnboundLocalError as e:
            logger.error("ADBMONForKafkaJob.dealxml with %s " % xml,exc_info = e)
        finally:
            return tableName, col_logtime, col_direction, col_src_host, col_src_db, dml

if __name__ == '__main__':
    job = ADBMonForKafkaJob()
    job.initialize()
    job.start()
