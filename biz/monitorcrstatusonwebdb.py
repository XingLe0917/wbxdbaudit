import logging
from biz.wbxjob import WbxJob
import time
from common.wbxssh import wbxssh
from dao.wbxauditdbdao import wbxauditdbdao
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait

LINUX_LINE_SEPARATOR="\n"

'''This job is used to enable/disable shareplex CR;
   This job will be configured to run at 55 min or 20 min '''

logger = logging.getLogger("dbaudit")

class MonitorCRStatusOnWebDBJob(WbxJob):
    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        if len(self.args) > 0:
            portlist = self.args[0].split(",")
        else:
            portlist = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            rows = self.auditdao.listWEBDBShareplexPort()
            self.auditdao.commit()
            m = datetime.now().minute
            h = datetime.now().hour
            with ThreadPoolExecutor(max_workers=20) as t:
                all_task = []
                for i, row in enumerate(rows):
                    if len(portlist) > 0 and str(row[2]) not in portlist:
                        continue
                    runjob = False
                    if row[0][0:3] in ("SJC", "IAD", "YYZ", "DFW") and h in (13, 16):
                        runjob = True
                    elif row[0][0:3] in ("AMS", "LNH") and h in (9, 12):
                        runjob = True
                    elif row[0][0:3] in ("NRT", "NRT") and h in (23,2):
                        runjob = True
                    elif row[0][0:3] in ("SYD") and h in (21,0):
                        runjob = True
                    if runjob:
                        # at 13:20, enableCR also executed; at 16:55, disableCR also executed;
                        if m >= 55:
                            all_task.append(t.submit(self.disableCR, row[2], row[1], "oracle", row[4], row[3]))
                        elif m >= 20:
                            all_task.append(t.submit(self.enableCR, row[2], row[1], "oracle", row[4], row[3]))
                            # self.enableCR(row[2], row[1], "oracle", row[4], row[3])
                wait(all_task, timeout = 180)
                # time.sleep(120)

        except Exception as e:
            logger.error("auditdao.listWEBDBShareplexPort() has error in MonitorCRStatusOnWebDBJob", exc_info = e)
            self.auditdao.rollback()
        finally:
            self.auditdao.close()



    def disableCR(self, port, host_name, login_user, login_pwd, splex_sid):
        self.changeCRStatus(port, host_name, login_user, login_pwd, False, splex_sid)

    def enableCR(self, port, host_name, login_user, login_pwd, splex_sid):
        self.changeCRStatus(port, host_name, login_user, login_pwd, True, splex_sid)

    def changeCRStatus(self, port, host_name, login_user, login_pwd, crstatus, splex_sid):
        logger.info("%s shareplex port %s CR on server %s" % ("Start" if crstatus else "Stop", port, host_name))
        server = wbxssh(host_name, 22, login_user, login_pwd)
        try:
            var_dir = ""
            prod_dir = ""
            server.connect()
            # cmd = '''ps aux | grep  sp_cop | grep %s | sed "s/-u//g" | awk '{print $((NF-1))}' | awk -F/ '{print "/"$2"/"$3"/bin"}' ''' % port
            cmd = '''ps aux | grep  sp_cop | grep %s | sed "s/-u//g" | awk '{print $((NF-1))}' | awk -F/ '{print "/"$2"/"$3"/bin/.profile_%s"}' | xargs cat | grep -E 'SP_SYS_VARDIR|SP_SYS_PRODDIR' | awk -F\; '{print $1}' ''' % (port, port)
            res, status = server.exec_command(cmd)
            if res is not None:
                for line in res.split(LINUX_LINE_SEPARATOR):
                    if line.find("SP_SYS_VARDIR") >= 0:
                        var_dir = line.split("=")[1]
                    elif line.find("SP_SYS_PRODDIR") >= 0:
                        prod_dir = line.split("=")[1]

                crfile_stop = "%s/data/conflict_resolution.%s.stopcr" % (var_dir, splex_sid)
                crfile = "%s/data/conflict_resolution.%s" % (var_dir, splex_sid)

                profile_name="%s/bin/.profile_u%s" % (prod_dir, port)
                restartpost = False
                if not crstatus:
                    if server.isFile(crfile) and not server.isFile(crfile_stop):
                        logger.info("port=%s on host=%s with result %s" % (port, host_name, res))
                        cmd = "source %s; cd $SP_SYS_VARDIR/data; mv conflict_resolution.%s conflict_resolution.%s.stopcr" % (profile_name,splex_sid,splex_sid)
                        server.exec_command(cmd)
                        restartpost = True
                else:
                    if not server.isFile(crfile) and server.isFile(crfile_stop):
                        cmd = "source %s; cd $SP_SYS_VARDIR/data; mv conflict_resolution.%s.stopcr conflict_resolution.%s" % (profile_name, splex_sid, splex_sid)
                        server.exec_command(cmd)
                        restartpost = True
                if restartpost:
                    cmd = '''
cd %s/bin;
source %s;
./sp_ctrl << EOF
show
stop post
start post
show
EOF''' %(prod_dir, profile_name)
                    res, status = server.exec_command(cmd)
                    logger.info("port=%s on host=%s with result %s" %(port, host_name, res))
        except Exception as e:
            self.auditdao.rollback()
            logger.info("%s shareplex port %s CR on server %s failed" % ("Start" if crstatus else "Stop", port, host_name), exc_info = e)
        finally:
            server.close()
            self.auditdao.close()
