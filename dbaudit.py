import importlib
import logging.config
import os
import sys

from biz.jobmonitor import JobStatusPatch

JobMapping = {
    "DBDataMonitorJob":"biz.dbdatamonitorjob.DBDataMonitorJob",
	"DBMetricAgentMonitorJob":"biz.dbmetricagentmonitorjob.DBMetricAgentMonitorJob",
	"SPReplicationMonitorJob":"biz.spreplicationmonitorjob.SPReplicationMonitorJob",
	"LOGMNR_JOB":"biz.logmnrjob.LogmnrJob",
	"DEPOTDBMONITOR_JOB":"biz.depotdbmonitorJob.DepotDBMonitorJob",
	"DBPATHDEPLOYMENTMONITOR_JOB":"biz.dbpatchdeploymointorjob.DBpatchDeployMonitorJob",
	"TEMPORARY_JOB":"biz.temporaryjob.TempJob",
	"CHECKCRONSTATUS_JOB":"biz.checkCronStatusJob.CheckCronStatusJob",
	"WRITEUNDOTABLESPACE_JOB":"biz.writeUndoTablespaceJob.UndoTablespaceJob",
	"READUNDOTABLESPACE_JOB":"biz.readUndoTablespaceJob.ReadUndoTablespaceJob",
	"SHAREPLEXADBMON_JOB":"biz.shareplexadbmonJob.ShareplexAdbmon",
    "SHAREPLEXADBMON_TEST_JOB":"biz.shareplexadbmonJob_test.ShareplexAdbmonTest",
	"SHAREPLEXADBMONBTS_JOB":"biz.shareplexadbmonJob_bts.ShareplexAdbmon_bts",
    "ShareplexAdbmonExp_JOB":"biz.shareplexadbmonJobForExp.ShareplexAdbmonExp",
	"EDRMONITOR_JOB":"biz.edrmonitorjob.EDRMonitorJob",
    "EDRERRORMONITOR_JOB":"biz.edrerrormonitorjob.EDRErrorMonitorJob",
	"TELEGRAFMONITOR_JOB":"biz.telegrafmonitor.telegrafMonitorJob",
	"MONITORCRINWEBDB_JOB":"biz.monitorcrstatusonwebdb.MonitorCRStatusOnWebDBJob",
	"CHKDBWAITEVENT_JOB":"biz.checkdbwaiteventjob.CheckDBWaitEventJob",
	"CHKDBEXECUTETIME_JOB":"biz.checkdbexcutetimejob.CheckDBExcuteTimeJob",
    "WbxCronjobMonitor":"biz.wbxcronjobmonitor.WbxCronjobMonitor",
	"WBXDELMEETINGLOG_JOB":"biz.purgewbxmeetinglog.WbxMeetingLogPurge",
    "DBConfigMonitor": "biz.dbconfigmonitor.DBConfigMonitor",
    "OSConfigMonitor": "biz.osconfigmonitor.OSConfigMonitor",
    "UserCallspersec_Job":"biz.userCallspersecJob.UserCallspersecJob",
    "wbxdbMonitorJob":"biz.wbxdbMonitorJob.wbxdbMonitorJob",
    "OpdbMetricMonitorJob":"biz.opdbMetricMonitorJob.OpdbMetricMonitorJob",
    "OpdbMetricMonitorSJC02Job":"biz.opdbMetricMonitorSJC02Job.OpdbMetricMonitorSJC02Job",
    "OpdbMetricMonitorDFW02Job":"biz.opdbMetricMonitorDFW02Job.OpdbMetricMonitorDFW02Job",
    "CPUUtilizationMonitorDFW02Job":  "biz.cpuUtilizationMonitorDFW02Job.CPUUtilizationMonitorDFW02Job",
    "DropPartitionMonitorJob": "biz.droppartitionmonitor.droppartitionMonitorJob",
    "WbxProcLogDataMonitor": "biz.wbxProcLogDataMonitor.WbxProcLogDataMonitor",
    "GetOracleDBSGAandPGAJob": "biz.getoracledbsgaandpgaJob.GetOracleDBSGAandPGAJob",
    "AlertManagementJob": "biz.alertManagementJob.AlertManagementJob",
    "SHPLEXDENYMONITOR_JOB": "biz.shplexdenymonitorjob.ShplexDenyMonitorJob",
    "CHECKCONFIGDBTOWEBDBSHPLEX_JOB": "biz.configdbtowebdbshplexJob.CheckConfigdbToWebdbShplexJob",
    "CHECKWEBDBTOWEBDBSHPLEX_JOB": "biz.checkdbsplexJob.CheckWebdbToWebdbShplexJob",
    "CHECKALLDBSHPLEX_JOB": "biz.checkdbsplexJob.CheckAlldbShplexJob",
    "CHECKTEMPDBSHPLEX_JOB": "biz.checkdbsplexJob.CheckTempShplexJob",
    "SHPLEXPARAMDETAIL_JOB": "biz.splexparamdetailjob.ShplexParamDetailJob",
    "SHPLEXDISABLEOBJECTMONITOR_JOB": "biz.shplexdisableobjectmonitorjob.ShplexDisableObjectMonitorJob",
    "DBLinkMonitor":"biz.DBLinkMonitorJob.DBLinkMonitor",
    "SHAREPLEXADBMONCHECK_JOB":"biz.shareplexadbmoncheck.ShareplexAdbmonCheck",
    "SHAREPLEXCRMONITOR_JOB":"biz.checkCRonWebdb.CheckCRonWebDBJob",
    "SHAREPLEXCRLOGMONITOR_JOB":"biz.monitorCRLogonWebdb.MonitorCRLogOnWebDBJob",
    "WBXGATHERDBBACKUPINFO_JOB":"biz.wbxgatherdbbackup.wbxgatherdbbackup",
    "GATHERSTATSMONITOR_JOB":"biz.monitorGatherStatsjob.MonitorGatherStatsJob",
    "ADBMONFORKAFKA_JOB":"biz.adbmonforkafkajob.ADBMonForKafkaJob",
    "WBXDBINDICATOR_JOB": "biz.dbindicatorjob.WbxDBIndicatorJob",
    "WBXCHECKHOSTINFO_JOB": "biz.chkhostinfo.chkhostinfo",
    "SQLEXECUTIONPLANMONITOR_JOB": "biz.monitorSqlExecutionPlan.MonitorSqlExecutionPlanJob",
    "UPDATEONCALLCHANGETOCHANGEBOT_JOB": "biz.updateoncallchangetochatbot.UpdateOncallChangeToChatbotJob",
    "ListenerlogWbxdbmonitorJob_JOB": "biz.listenerlogWbxdbmonitorJob.ListenerlogWbxdbmonitorJob",
    "shareplexRealDelayJob":"biz.shareplexRealDelayJob.shareplexRealDelayJob",
    "shareplexDelayToDepotJob":"biz.shareplexDelayToDepotJob.shareplexDelayToDepotJob",
    "APPUSERMONITOR_JOB":"biz.appusermonitorjob.AppuserMonitorJob",
    "pgShareplexDelay2DepotJob":"biz.pgShareplexDelay2DepotJob.pgShareplexDelay2DepotJob",
    "pgShareplexDelayAlert":"biz.pgShareplexDelayAlert.pgShareplexDelayAlert",
    "MonitorDelayforDcaas":"biz.monitordelayfordcaas.MonitorDelayforDcaas",
    "AlertForLongTimeSQL":"biz.alertforlongtimesql.AlertForLongTimeSQL",
    "pgSendAlert":"biz.pgsendalert.pgSendAlert"
}

def getRunJob(JobName,*args):
    Job =None
    hasError,msg = verifyArgv(*args)

    if JobName in JobMapping.keys() and not hasError:
        try:
            modulePathList = JobMapping[JobName].split('.')
            moduleSrc = ".".join(modulePathList[:-1])
            className= modulePathList[-1]
            #Load Module
            module = importlib.import_module(moduleSrc)
            classObj = getattr(module, className)
            Job = classObj()
        except Exception as e:
            msg = "The job %s failed with error msg %s" % (jobname, e)
    else:
        msg = "Input invalid job name %s" % JobName if not hasError else msg
    return Job,msg

@JobStatusPatch
def run(JobName,Job,*args):
    hasError = False
    try:
        # print("Run Job: %s " %JobName)
        Job.initialize(*args)
        hasError = Job.start()
    except Exception as e:
        print( e)
        hasError = True
    return hasError

def verifyArgv(*args):
    hasError = False
    msg=""
    if jobname == "SHAREPLEXADBMON_JOB" or jobname == "ShareplexAdbmonExp":
        if len(args) < 1:
            msg = "You must input type as 2st parameter,eg:insert/update"
            hasError = True
    elif jobname == "OSConfigMonitor":
        if len(args) != 1:
            msg = "You must input appln_support_code as 2st parameter,eg:web/WEB"
            hasError = True
    elif jobname == "DBLinkMonitor":
        if len(args) < 1:
            msg = "You must input db_name as 2st parameter,eg:ALL/RACAWWEB"
            hasError = True
    return hasError,msg

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("You must input job name as 1st parameter")
        sys.exit(-1)

    jobname = sys.argv[1]
    args = sys.argv[2:] if len(sys.argv) > 2 else []
    Job = None

    current_path = os.path.dirname(os.path.abspath(__file__))
    log_config_file = current_path + "/conf/logger.conf"
    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger("dbaudit")
    logger.info("start job %s" % jobname)

    Job,msg = getRunJob(jobname,*args)

    if Job:
        hasError = run(jobname,Job,*args)
        msg = "End of job %s with status=%s" % (jobname, hasError)
        logger.info(msg)
    else:
        logger.error(msg)

