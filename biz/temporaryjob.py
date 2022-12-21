from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.logmnrDao import logmnrdao
from common.wbxssh import wbxssh

# This job is used for temporary tool
class TempJob(WbxJob):

    def start(self):
        # self.checkCRSStatus()
        self.modifyNTPD()

    def modifyNTPD(self):
        todo_serverlist = ['sjdbormt013',
                      'sjdbormt017',
                      'sjdbormt018',
                      'sjdbormt019',
                      'sjdbormt020',
                      'sjdbormt021',
                      'sjdbormt022',
                      'sjdbormt024',
                      'sjdbormt025',
                      'sjdbormt026',
                      'sjdbormt027',
                      'sjdbormt028',
                      'sjdbormt036',
                      'sjdbormt037',
                      'sjdbormt046',
                      'sjdbormt047',
                      'sjdbormt048',
                      'sjdbormt049',
                      'sjdbormt051',
                      'sjdbormt053',
                      'sjdbormt054',
                      'sjdbormt055',
                      'sjdbormt056',
                      'sjdbormt057',
                      'sjdbormt060',
                      'sjdbormt061',
                      'sjdbormt062',
                      'sjdbormt063',
                      'sjdbormt064',
                      'sjdbormt065',
                      'sjdbormt066',
                      'sjdbormt067',
                      'sjdbormt068',
                      'sjdbormt069',
                      'sjdbormt070',
                      'sjdbormt076',
                      'sjdbormt077',
                      'sjdbormt078',
                      'sjdbormt079',
                      'sjdbormt080',
                      'sjdbormt081',
                      'sjdbormt082',
                      'sjdbormt083']

        auditdao = wbxauditdbdao(self.depot_connectionurl)
        serverList = []
        try:
            auditdao.connect()
            auditdao.startTransaction()
            list1 = auditdao.listOracleServer(dc_name="SJC02", justFirstNode=False)
            list2 = auditdao.listOracleServer(dc_name="SJC03", justFirstNode=False)
            serverList.extend(list1)
            serverList.extend(list2)
            auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
        finally:
            auditdao.close()
        totalsize = len(todo_serverlist)
        cnt = 0
        for i, serverdict in enumerate(serverList):
            host_name = serverdict[0]
            pwd = serverdict[1]
            if host_name not in todo_serverlist:
                continue
            cnt += 1
            print("------------%s/%s on server %s-------------------" % (cnt, totalsize, host_name))
            server = wbxssh(host_name, 22, "oracle", pwd)
            try:
                server.connect()
                res, status = server.exec_command("sh /staging/gates/modifyntpd.sh")
                print(res)
            except Exception as e:
                print(e)
            finally:
                server.close()

    def checkCRSStatus(self):
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        serverList = []
        try:
            auditdao.connect()
            auditdao.startTransaction()
            list1 = auditdao.listOracleServer(dc_name="SJC02", justFirstNode=False)
            list2 = auditdao.listOracleServer(dc_name="SJC03", justFirstNode=False)
            serverList.extend(list1)
            serverList.extend(list2)
            auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
        finally:
            auditdao.close()

        totalsize = len(serverList)
        for i, serverdict in enumerate(serverList):
            host_name = serverdict[0]
            pwd = serverdict[1]
            print("------------%s/%s on server %s-------------------" % (i, totalsize, host_name))
            server = wbxssh(host_name, 22, "oracle", pwd)
            try:
                server.connect()
                res, status = server.exec_command("sudo service crond status")
                print(res)
                res, status = server.exec_command("crsstat")
                hasOFFLINE = False
                for line in res.splitlines():
                    if line.startswith("ora.") or line.startswith("shareplex"):
                        if line.find("OFFLINE") > 0:
                            print(line)
                            hasOFFLINE = True
                if not hasOFFLINE:
                    print("All resouce are ONLINE")
            except Exception as e:
                print(e)
            finally:
                server.close()