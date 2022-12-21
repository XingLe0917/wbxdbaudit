from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from concurrent.futures.thread import ThreadPoolExecutor
from common.wbxssh import wbxssh
from common.wbxchatbot import wbxchatbot
import cx_Oracle
import logging

'''
Desc:   Purge WBX Meeting log 
'''

logger = logging.getLogger("dbaudit")
class chkhostinfo(WbxJob):

    def start(self):
        hasError = False
        try:
            auditdao = wbxauditdbdao(self.depot_connectionurl)

            logger.info('Get All Server HostInfo Start')

            auditdao.connect()
            auditdao.startTransaction()
            hostvolist = auditdao.getAllhostserver()

            errmsg=[]
            executor = ThreadPoolExecutor(max_workers=5)
            for res in executor.map(self.runJobOnHost, hostvolist):
                if res["status"]=="FAILED":
                    errmsg.append([res["Data"]["host_name"],res["Data"]["msg"]])
                    # self.upddbpack(**kargs)
            if len(errmsg)>0:
                self.sendAlertToChatBot(*errmsg)

            auditdao.commit()

        except Exception as e:
            logger.error(e)
            auditdao.rollback()
            hasError = True
        finally:
            auditdao.close()
        return hasError

    def runJobOnHost(self, hostvo):
        host_name = hostvo.getHostName()
        pwd = hostvo.getpwd()
        params=hostvo.getParams().split('|+|')
        ssh_port = int(params[0])
        physical_cpu = int(params[1])
        cores = int(params[2])

        try:
            commandstr = ''' cat /proc/cpuinfo | grep -E "physical id|cpu cores" | sort -u |uniq|sed -r 's/[ \t]+//g' '''
            server = wbxssh(host_name, ssh_port, "oracle", pwd)
            res={"status":"SUCCEED","Data":""}
            data = {"host_name": host_name}
            cpunum=0
            rows, status = server.exec_command(commandstr)
            for row in rows.splitlines():
                datalist=row.split(":")
                if datalist[0]=="cpucores":
                    # corenum=datalist[1]
                    corenum=int(datalist[1])
                else:
                    cpunum+=1
            data["CPU"]=cpunum
            data["CORS"]=corenum

            logger.info("host:%s in depot cpu:%s, cores:%s; on server:cpu:%s, cores:%s" %(host_name,str(physical_cpu),str(cores),str(cpunum),str(corenum)))
            if cpunum != physical_cpu or corenum != cores:
                res["status"] = "FAILED"
                data["msg"] = "host:%s in depot cpu:%s, cores:%s; on server:cpu:%s, cores:%s" %(host_name,str(physical_cpu),str(cores),str(cpunum),str(corenum))
        except Exception as e:
            logger.error("host:%s end with error: %s" %(host_name,str(e)))
            res["status"] = "FAILED"
            data["msg"]="host:%s failed with err:%s"  %(str(e))
        finally:
            server.close()
        res["Data"] = data
        return res

    def sendAlertToChatBot(self, *args):
        logger.info("Send Alert Message To ChatBot")
        hasError = False
        titleList = ['hostname', 'errormsg']
        dataList = []
        msg = "### Testing The host CPU and core in the depot are different from server to server \n"
        for num, item in enumerate(args):
            dataList.append([item[0], item[1] ])
        msg += wbxchatbot().address_alert_list(titleList, dataList)
        wbxchatbot().alert_msg_to_dbateam(msg)
        # wbxchatbot().alert_msg_to_dbabot_and_call_person(msg,"wentazha")
        return hasError

    def upddbpack(self,**kargs):
        connectionurl = self.depot_connectionurl.replace(':', '/', 2)
        try:
            conn = cx_Oracle.connect('%s' % connectionurl)
            cursor = conn.cursor()
            vSQL = '''
            update host_info set physical_cpu='%s',cores='%s' where host_name='%s'
            ''' % (kargs["CPU"],kargs["CORS"],kargs["host_name"])
            # cursor.execute(vSQL)
            conn.commit()
        except Exception as e:
            logger.error(str(e))
        finally:
            cursor.close()
            conn.close()

if __name__ == '__main__':
    job = chkhostinfo()
    job.initialize()
    job.start()
