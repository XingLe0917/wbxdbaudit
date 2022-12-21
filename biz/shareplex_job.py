import subprocess
import re
import os
import logging
import socket
from datetime import datetime
from logging.handlers import RotatingFileHandler
from multiprocessing.pool import ThreadPool
import _strptime

formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
# logging.basicConfig(format=formatter,level=logging.INFO)
handler = RotatingFileHandler(filename="/var/log/telegraf/shareplex.log", mode='a', maxBytes=10*1024*1024, backupCount=5)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logging.getLogger('').addHandler(handler)

pool = ThreadPool(10)
hostname = socket.gethostname().split(".")[0]
hostname_vip="%s-vip" % hostname
queuedict = {}
def executeCommand(cmd, timeout = 60):
    try:
        # subprocess.run return CompletedProcess which has stdout attribute
        # in python 2.6 Popen() no timeout parameter
        FNULL = open(os.devnull, 'w')
        pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=FNULL).stdout
        # Notice, pipe.readlines(), each line is end with \n
        return 0, [line[0:-1] for line in pipe.readlines()]
    except Exception as e:
        logging.error("Execute %s meet error:%s" % (cmd, str(e)))
        return -1, []

def parseShareplexMetricFile(lines):
    vcount = 0
    processDict = {}
    db_name = ""
    queuename = ""
    operation = 0
    backlog = 0
    for i in range(0, len(lines)):
        line = lines[i]
        if line.startswith("SharePlex Version"):
            vcount += 1
        if vcount == 1:
            items = line.split()
            processType = items[0]
            if processType not in ("Capture","Read","Export","Import","Post"):
                continue
            if processType not in processDict:
                processDict[processType] = []
            subprocesslist = processDict[processType]
            if processType == "Capture" or processType == "Read":
                subprocesslist.append({"process_type":processType.lower(),"db_name":items[1].replace("o.",""),"src_db":items[1].replace("o.",""),"src_host": hostname_vip,
                                       "status":1 if items[2] == "Running" else 0,"operation":0,"delaytime":0})
            elif processType == "Export":
                # Error case: Export     sjdborbt5-vip                                               Idle
                # Expected case:
                if len(items) < 4:
                    continue
                # If a queue is not monitored or a queue is not used now, then ignore it
                queuename = items[1]
                if queuename not in queuedict:
                    continue
                if queuedict[queuename]["tgt_sid"] is None or queuedict[queuename]["tgt_sid"]=="":
                    continue
                subprocesslist.append({"process_type":processType.lower(),"queuename": queuename,"src_host": hostname_vip,
                                       "status": 1 if items[3] == "Running" else 0,"operation":0, "backlog":0,
                                       "replication_to":queuedict[queuename]["replication_to"],
                                       "tgt_queuename":queuedict[queuename]["tgt_queuename"],
                                       "tgt_host":items[2],"tgt_sid":queuedict[queuename]["tgt_sid"]})
            elif processType == "Import":
                # iasj1cco012-rac_4N_lkpP  Idle                        0  01-Jan-70 00:00:00 --- no -vip-
                subitems = items[1].split("-vip-")
                if len(subitems) > 1:
                    src_host = "%s-vip" % subitems[0]
                    queuename = subitems[1]
                else:
                    continue
                subprocesslist.append({"process_type":processType.lower(),"queuename": queuename, "src_host": src_host,
                                       "tgt_host": items[2], "status": 1 if items[3] == "Running" else 0,"operation":0})
            elif processType == "Post":
                idx = items[1].find("-")
                src_db = items[1][0:idx].replace("o.","")
                queuename = items[1][idx + 1:]
                tgt_db = items[2].replace("o.","")
                subprocesslist.append({"process_type":processType.lower(),"queuename": queuename, "src_db": src_db,
                                       "tgt_db": tgt_db,"status": 1 if items[3] == "Running" else 0,
                                       "operation":0, "backlog":0,"delaytime":0})
        elif vcount == 2:
            if line.startswith("o."):
                items = line.split()
                db_name = items[0].replace("o.","")
                operation = int(items[-3])
                subprocesslist = processDict["Capture"]
                for subprocess in subprocesslist:
                    if subprocess["db_name"].find(db_name) >= 0:
                        subprocess["operation"] = operation
            elif line.startswith("Operation on"):
                items = line.split()
                capturetime = " ".join(items[-2:])
                timedelta = datetime.now() - datetime.strptime(capturetime,"%m/%d/%y %H:%M:%S")
                delaytime = (timedelta.seconds + timedelta.days * 24 * 60 * 60)/60
                subprocesslist = processDict["Capture"]
                for subprocess in subprocesslist:
                    if subprocess["db_name"].find(db_name) >= 0:
                        subprocess["delaytime"] = delaytime
        elif vcount == 3:
            if line.startswith("o."):
                items = line.split()
                db_name = items[0].replace("o.", "")
                operation = int(items[-5])
                backlog = int(items[-1])
                subprocesslist = processDict["Read"]
                for subprocess in subprocesslist:
                    # Because "show read" command maybe can not show full db_name
                    if subprocess["db_name"].find(db_name) == 0:
                        subprocess["backlog"] = backlog
                        subprocess["operation"] = operation
        elif vcount == 4:
            if line.startswith("Queue"):
                queuename = line.split()[-1]
            elif line.startswith("Target"):
                dline = lines[i + 2]
                items = dline.split()
                tgt_host = items[0]
                backlog = int(items[-1])
                operation = int(items[-5])
                subprocesslist = processDict["Export"]
                for subprocess in subprocesslist:
                    if subprocess["queuename"] == queuename and subprocess["tgt_host"].find(tgt_host) == 0:
                        subprocess["backlog"] = backlog
                        subprocess["operation"] = operation
        elif vcount == 5:
            if line.startswith("Source"):
                dline = lines[i+2]
                if dline.startswith("SharePlex Version"):
                    break
            elif line.find("-vip-") > 0:
                # This means no import process and no post process
                items = line.split()
                operation = items[-3]
                subprocesslist = processDict["Import"]
                for subprocess in subprocesslist:
                    direction = "%s-%s" % (subprocess["src_host"], subprocess["queuename"])
                    if direction.startswith(items[0]):
                        subprocess["operation"] = operation
        elif vcount == 6:
            if line.startswith("Source"):
                queuename = line.split()[-1]
            elif line.startswith("o."):
                items = line.split()
                operation = items[-5]
                backlog = items[-1]
            elif line.startswith("Activation Id"):
                items = line.split()
                actid = int(items[-1])
                subprocesslist = processDict["Post"]
                if actid == 0:
                    for subprocess in subprocesslist:
                        if subprocess["queuename"] == queuename:
                            subprocess["status"] = 0
            elif line.startswith("SCN:"):
                items = line.split()
                timedelta = datetime.now() - datetime.strptime(" ".join(items[-2:]), "%m/%d/%y %H:%M:%S")
                delaytime = (timedelta.seconds + timedelta.days * 24 * 60)/60
                subprocesslist = processDict["Post"]
                for subprocess in subprocesslist:
                    if subprocess["queuename"] == queuename:
                        subprocess["operation"] = operation
                        subprocess["backlog"] = backlog
                        subprocess["delaytime"] = delaytime
    return processDict

def parseShareplexRouting(line):
    # Please do not change below pattern except you have strong reason
    items = re.split(r"[\s*@]\s*", line)
    pattern = re.compile("SPLEX\d{4,5}\.SPLEX_MONITOR_ADB_*", re.I)
    replication_to = pattern.sub("", items[0])
    tgtitems = items[-2].split(":")
    tgt_sid = items[-1].split(".")[-1]
    if tgt_sid is None:
        tgt_sid="NONE"
    tgt_host = items[-2].split(":")[0]
    src_host = hostname_vip
    srcqueuename = None
    if len(items) == 5:
        srcitems = items[2].split(":")
        src_host = srcitems[0]
        if len(srcitems) == 2:
            srcqueuename = srcitems[1]
    if srcqueuename is None:
        srcqueuename = src_host

    if len(tgtitems) == 2:
        tgtqueuename = tgtitems[1]
    else:
        tgtqueuename = srcqueuename
    queuedict[tgtqueuename]= {"src_host":hostname_vip,"src_queuename":srcqueuename, "tgt_queuename":tgtqueuename,
                              "replication_to": "NONE" if replication_to=="" else replication_to,"tgt_host": tgt_host,"tgt_sid":tgt_sid}


def jobHandler(spportinfo):
    logging.info("start to collect shareplex metric for %s" % spportinfo)
    # threadname = threading.current_thread().getName()
    sp_sys_dir = spportinfo.split()[0]
    port = int(spportinfo.split()[1])
    # if port != 21001:
    #     return []
    pointlist = []
    try:
        if port in [20001, 20002]:
            return []
        profile = "%s/bin/.profile_u%s" % (sp_sys_dir, port)
        if not os.path.isfile(profile):
            logging.error("The profile %s is not a file" % profile)
            return
        cmd = '''source %s; cat $SP_SYS_VARDIR/data/statusdb | grep -i "active from" | awk '{print $4}' | awk -F\\" '{print $2}' '''% profile
        status, linelist = executeCommand(cmd)
        if len(linelist) > 0:
            for config_file in linelist:
                cmd = ''' source %s; cat $SP_SYS_VARDIR/config/%s | grep -i splex_monitor_adb  | grep -v ^# | grep -i splex_monitor_adb | uniq''' % (profile, config_file)
                status, routinglinelist = executeCommand(cmd)
                for routingline in routinglinelist:
                    parseShareplexRouting(routingline)
        else:
            logging.info("No active config file under the port %s" % port)

        log_file = "/tmp/telegraf_sp_%s.log" % port
        if os.path.isfile(log_file):
            os.remove(log_file)
        cmd = '''
        cd %s/bin
        source %s
        ./sp_ctrl << EOF | grep -v "^*"  | grep -v "^$" | sed "s/^[[:space:]]*//g" 
        version
        show
        version
        show capture detail
        version
        show read
        version
        show export
        version
        show import
        version
        show post detail
        
        exit
        EOF
        ''' % (sp_sys_dir, profile)
        status, linelist = executeCommand(cmd)
        processDict = parseShareplexMetricFile(linelist)
        for process_type, subprocessList in processDict.items():
            for subprocess in subprocessList:
                if process_type == "Capture":
                    pointlist.append("shareplex_process,process_type=%s,port=%s,src_host=%s,src_db=%s status=%s,operation=%s,delaytime=%s" %
                        (subprocess["process_type"], port, subprocess["src_host"],
                         subprocess["src_db"],subprocess["status"],subprocess["operation"],subprocess["delaytime"]))
                elif process_type == "Read":
                    pointlist.append(
                        "shareplex_process,process_type=%s,port=%s,src_host=%s,src_db=%s status=%s,operation=%s,backlog=%s" %
                        (process_type.lower(), port, subprocess["src_host"],
                         subprocess["src_db"],subprocess["status"],subprocess["operation"], subprocess["backlog"]))
                elif process_type == "Export":
                    # if int(port) == 21001 and process_type.lower() == "export":
                    #     print(subprocess)

                    pointlist.append("shareplex_process,process_type=%s,tgt_host=%s,port=%s,"
                                     "src_queuename=%s,tgt_queuename=%s,replication_to=%s,tgt_sid=%s,src_host=%s status=%s,operation=%s,backlog=%s" %
                        (process_type.lower(), subprocess["tgt_host"], port, subprocess["queuename"],
                         subprocess["tgt_queuename"],subprocess["replication_to"],subprocess["tgt_sid"],subprocess["src_host"],
                        subprocess["status"], subprocess["operation"], subprocess["backlog"]))
                elif process_type == "Import":
                    pointlist.append("shareplex_process,process_type=%s,src_host=%s,tgt_host=%s,port=%s,queuename=%s status=%s,operation=%s" %
                        (process_type.lower(), subprocess["src_host"], subprocess["tgt_host"], port,
                        subprocess["queuename"], subprocess["status"], subprocess["operation"]))
                elif process_type == "Post":
                    pointlist.append("shareplex_process,process_type=%s,port=%s,src_db=%s,tgt_db=%s,queuename=%s status=%s,operation=%s,backlog=%s,delaytime=%s" %
                            (process_type.lower(), port, subprocess["src_db"], subprocess["tgt_db"],
                            subprocess["queuename"],subprocess["status"], subprocess["operation"], subprocess["backlog"], subprocess["delaytime"]))
    except Exception as e:
        logging.error("Error occured when handle %s with error %s" % (spportinfo, str(e)), exc_info = e)
    return pointlist

def getShareplexPortMetric():
    cmd = '''crsstat | grep ^shareplex | grep %s | awk '{print $1":"$3}' | sed "s/shareplex//g" ''' % hostname
    status, crsportList = executeCommand(cmd)
    for csrport in crsportList:
        items = csrport.split(":")
        print("shareplex_process,process_type=%s,port=%s status=%s" % ("portstatus", items[0],1 if items[1] == "ONLINE" else 0))
    cmd=''' ps aux | grep sp_cop  | grep -v grep | sed 's/-u//g' | awk '{print $((NF-1))"/"$NF}' |  awk -F/ '{print "/"$2"/"$3" "$NF}' '''
    status, portList = executeCommand(cmd)
    futurelist = pool.map(jobHandler, portList)
    pool.close()
    pool.join()
    for f in futurelist:
        for point in f:
            print(point)

    # pointlist = []
    # for spport in portList:
    #     print(spport)
    #     pointlist = jobHandler(spport)
    #     for point in pointlist:
    #         print(point)

if __name__ == "__main__":
    getShareplexPortMetric()
