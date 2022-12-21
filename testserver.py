import datetime
import os
import logging.config

from biz.shareplexadbmonJob import ShareplexAdbmon
from common.wbxssh import wbxssh

logger = logging.getLogger("dbaudit")

def init():
    logging.config.fileConfig("conf/logger.conf")
    logger = logging.getLogger("dbaudit")
    logger.info("start")

def testDBpatchDeploymentMonitor():
    from biz.dbpatchdeploymointorjob import DBpatchDeployMonitorJob
    job = DBpatchDeployMonitorJob()
    job.initialize()
    job.test(15995)

def testShareplexAdbmon():
    starttime = datetime.datetime.now()
    job = ShareplexAdbmon()
    job.initialize()
    job.test()
    endtime = datetime.datetime.now()
    time = (endtime - starttime).seconds
    logger.info("\n times:{0}".format(time))

if __name__=='__main__':
    init()
    testShareplexAdbmon()
