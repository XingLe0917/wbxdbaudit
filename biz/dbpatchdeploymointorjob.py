import sys
import os
import logging
import json
import xml.etree.ElementTree as ET

from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbpatchdeploymonitordao import DBpatchDeploymentMonitorDao
from biz.dbpatchinstallation import loaddbpatchreleasexml
from common.wbxutil import wbxutil
from common.wbxexception import wbxexception
from dao.vo.dbpatchdeploymonitorvo import DBPatchDeploymentVO, DBPatchReleaseVO, ShareplexBaselineVO
from common.const import const

logger = logging.getLogger("dbaudit")

'''
This job is used to monitor dbpatch deployment. The solution is:
If we find a dbpatch is deployed on a db, then we suppose the dbpatch should be deployed to all dbs with the same appln_support_code
But in fact there is always exception case, exception cases can only be fixed manually
Exception case 1:
the dbpatch is only deployed to Prod OPDB, but not deployed on BTS OPDB
Exception case 2:
the dbpatch is onlnyed deployed to Primary Webdb, but not deployed to GSB WebDB
Exception Case 3:
the dbpatch has been deployed on 10 of 50 webdbs, cancel abruptly.
Exception Case 4:
deploying 2 dbpatch at the same time range. and no deployment order/dependency

We only monitor CONFIGDB, WebDB, TahoeDB, TEODB, OPDB, GLOOKUPDB, MediateDB
'''
class DBpatchDeployMonitorJob(WbxJob):

    def start(self):
        self.db_type_map = {"WEBDB": "WEB", "TAHOEDB": "TEL", "BILLINGDB": "OPDB", "CONFIGDB": "CONFIG",
                            "GLOOKUPDB": "LOOKUP", "TEODB": "TEO", "MEDIATEDB": "MEDIATE", "STREAMDB": "CSP",
                            "DIAGNSDB":"DIAGNS"}
        self.dbtypemap = {}
        self.release_number = None
        self.release_name = None
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.initializeReleaseFromChanges()

        hasError = False
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.dbvolist = self.auditdao.getDBConnectionList("""PROD','BTS_PROD""", """CONFIG','WEB','TEL','TEO','OPDB','LOOKUP','MEDIATE""", "ALL", "ALL", "SYSTEM")
            self.schemadict = self.auditdao.getDBSchemaList()
            self.auditdao.commit()

            for dbvo in self.dbvolist:
                dbid = dbvo.getdbid()
                if dbid not in self.schemadict:
                    continue

                schemastr = self.schemadict[dbid]
                for schema_info in schemastr.split(","):
                    schema_name = schema_info.split('_')[0]
                    schema_appln_support_code = schema_info.split('_')[1]
                    logger.info("Get data from db %s_%s" % (dbvo.getTrimHost(), dbvo.getDBName()))
                    dao = None
                    try:
                        connectionurl = dbvo.getConnectionurl()
                        dao = DBpatchDeploymentMonitorDao(connectionurl)
                        dao.connect()
                        dao.startTransaction()
                        dbpatchList = dao.getdbpatchdeploymentList(schema_name, 210)
                        dao.commit()
                        self.processDeploymentLog(dbvo, dbpatchList, schema_name, schema_appln_support_code)
                    except Exception as e:
                        logger.error(e)
                        if dao is not None:
                            dao.rollback()
                        hasError = True
                    finally:
                        dao.close()

        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()
        return hasError

    def initializeReleaseFromChanges(self):
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        dbpatchdao = None
        try:
            connectinfo = None
            auditdao.connect()
            auditdao.startTransaction()
            dbvolist = auditdao.getDBConnectionList("BTS_PROD", "MON_GRID", "ALL", "ALL", "SYSTEM")
            for dbvo in dbvolist:
                if dbvo.getDBName() == "STAPDB":
                    connectinfo = dbvo.getConnectionurl()
            auditdao.commit()

            if connectinfo is not None:
                dbpatchdao = DBpatchDeploymentMonitorDao(connectinfo)
                dbpatchdao.connect()
                dbpatchdao.startTransaction()
                releaseNumberList = dbpatchdao.getScheduledReleaseNumber()
                dbpatchdao.commit()

                self.auditdao.connect()
                self.auditdao.startTransaction()
                for release_number in releaseNumberList:
                    isExist = self.auditdao.isDBPatchReleaseExist(release_number)
                    if not isExist:
                        # installDBPatch() take 3 things, get them from function comments
                        self.installDBPatch(release_number)
                        isExist = self.auditdao.isDBPatchReleaseExist(release_number)
                        if not isExist:
                            logger.error("The dbpatch installation failed or missed record for release_number=%s" % release_number)
                self.auditdao.commit()
            else:
                logger.error("Not get Stapdb connection info")
            # auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
            hasError = True
        finally:
            auditdao.close()
            if dbpatchdao is not None:
                dbpatchdao.rollback()
                dbpatchdao.close()

    def processDeploymentLog(self, dbvo, deploymentLogList, schema_name, schema_appln_support_code):
        try:
            # self.auditdao.startTransaction()
            for deploylog in deploymentLogList:
                try:
                    self.auditdao.startTransaction()
                    release_number = deploylog["releasenumber"]
                    dbdeploytime = deploylog["dbcreatetime"]
                    spdeploytime = deploylog["spcreatetime"]
                    db_appln_support_code = dbvo.getApplnSupportCode()
                    # in some db, there are multiple schemas for different apps;
                    # E.g. create wbxstream schema in webdb. so we get appln_pool_info.appln_support_code as db type
                    if schema_appln_support_code != db_appln_support_code:
                        appln_support_code = schema_appln_support_code.upper()
                    else:
                        appln_support_code = db_appln_support_code.upper()
                    isExist = self.auditdao.isDBPatchReleaseExist(release_number)
                    if not isExist:
                        # installDBPatch() take 3 things, get them from function comments
                        self.installDBPatch(release_number)
                        isExist = self.auditdao.isDBPatchReleaseExist(release_number)
                        if not isExist:
                            print("WBXERROR: The dbpatch installation failed or missed record. Please check")

                    if isExist:
                        deploymentvo = self.auditdao.getDBPatchDeploymentvo(release_number, dbvo.getTrimHost(),dbvo.getDBName(), schema_name)
                        if deploymentvo is None or deploymentvo.deploystatus == "DEPLOYED" or deploymentvo.spdeploystatus == "DEPLOYED":
                            continue
                        else:
                            deploymentvo.deploytime = dbdeploytime
                            if dbdeploytime is not None:
                                deploymentvo.deploystatus = "DEPLOYED"
                            deploymentvo.spdeploytime = spdeploytime
                            if spdeploytime is not None:
                                deploymentvo.spdeploystatus = "DEPLOYED"
                except Exception as e:
                    logger.error(e)
                    self.auditdao.rollback()
                finally:
                    self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)

    # 1. Install dbpatch by Linux yum command
    # 2. parse release.xml in dbpatch, and get shareplex replication table list, insert into auditdb wbxshareplexbaseline table
    # 3. Insert data into wbxdbpatchrelease and wbxdbpatchdeployment tables
    def installDBPatch(self, releasenumber):
        logger.info("Start to install dbpatch release %s" % (releasenumber))
        wbxutil.installdbpatch(releasenumber)
        release_dir = os.path.join("/tmp", str(releasenumber))
        if not os.path.isdir(release_dir):
            raise wbxexception("%s is not a dir. please check whether the dbpatch is installed successfully" % release_dir)
        self.moveDBPatchForBaseline(releasenumber)
        release_xml_file = os.path.join(release_dir, "release.xml")
        if not os.path.isfile(release_xml_file):
            raise wbxexception("%s is not a file. Please check with DB Engineer" % release_xml_file)
        # baseline_xml="/Users/zhiwliu/Documents/office/oracle/Shareplex/Baseline/release_15886.xml"
        self.parseXML(release_xml_file)
        self.saveBaselineToDB()

    def moveDBPatchForBaseline(self, release_number):
        os.system("sudo cp -R /tmp/%s /home/smsops/webex-db-schema-patch/" % release_number)
        os.system("sudo chown -R smsops:smsops /home/smsops/webex-db-schema-patch/%s" % release_number)
        os.system("sudo chmod -R 755 /home/smsops/webex-db-schema-patch/%s" % release_number)

    def parseXML(self, release_xml_file):
        if not os.path.isfile(release_xml_file):
            raise wbxexception("the %s does not exist" % release_xml_file)
        try:
            self.src_appln_support_code = None
            if len(self.dbtypemap) > 0:
                self.dbtypemap.clear()
            root = ET.parse(release_xml_file).getroot()
            self.parseNode(root)
            logger.info(json.dumps(self.dbtypemap, sort_keys=True, indent=4, separators=(', ', ': ')))
        except Exception as e:
            raise wbxexception("Error occurred in parsexml with error msg: %s" % e)

    def saveBaselineToDB(self):
        logger.info("start savetodb()")

        try:
            self.dbvolist = self.auditdao.getDBConnectionList("""PROD','BTS_PROD""","""CONFIG','WEB','TEL','TEO','OPDB','LOOKUP','MEDIATE""","ALL", "ALL", "SYSTEM")
            self.schemadict = self.auditdao.getDBSchemaList()

            for xml_db_type, schemalist in self.dbtypemap.items():
                # DB Engineer and DB team provide different db_type for each db. so we need a mapping at here
                if xml_db_type in self.db_type_map:
                    rel_appln_support_code = self.db_type_map[xml_db_type]
                    for schematype, schema in schemalist.items():
                        if schematype not in [const.SCHEMATYPE_TEST, const.SCHEMATYPE_APP, const.SCHEMATYPE_GLOOKUP, const.SCHEMATYPE_XXRPTH, const.SCHEMATYPE_COLLABMED]:
                            continue

                        major_number = schema["release_major_num"]
                        minor_number = schema["release_minor_num"]
                        isversionchanged = schema["release_version_change"]

                        prevreleasenumber = self.auditdao.getpreviousRelease(rel_appln_support_code, schematype)

                        # dbpatchreleasevo = auditdao.getDBPatchRelease(self.release_number, appln_support_code, schematype)

                        dbpatchreleasevo = DBPatchReleaseVO(releasenumber=self.release_number,
                                                            appln_support_code=rel_appln_support_code,
                                                            schematype=schematype,
                                                            major_number=major_number,
                                                            minor_number=minor_number,
                                                            description=self.release_name,
                                                            isversionchanged=isversionchanged)
                        self.auditdao.addDBPatchRelease(dbpatchreleasevo)
                        print("save dbpatch release %s" % dbpatchreleasevo)

                        for dbvo in self.dbvolist:
                            dbid = dbvo.getdbid()

                            if dbid not in self.schemadict:
                                continue

                            schemastr = self.schemadict[dbid]
                            for schema_info in schemastr.split(","):
                                schema_name = schema_info.split('_')[0]
                                schema_appln_support_code = schema_info.split('_')[1]
                                user_schematype = schema_info.split('_')[2]
                                clustername = schema_info.split('_')[3]

                                #  Compare appln_pool_info.appln_support_code with appln_support_code in dbpatch
                                #  Because one db maybe has multiple types of schemas
                                if schema_appln_support_code != rel_appln_support_code:
                                    continue
                                if schematype != user_schematype:
                                    continue

                                if schema_appln_support_code == const.APPLN_SUPPORT_CODE_WEBDB:
                                    clustername = "%swd" % clustername.rjust(2,"d")
                                elif schema_appln_support_code == const.APPLN_SUPPORT_CODE_TAHOEDB:
                                    # for tahoedb tahoe schema, the cluster name is appln_mapping_info.mapping_name
                                    # but for test schema, the cluster name is appln_mapping_info.db_name
                                    if schematype != const.SCHEMATYPE_APP:
                                        clustername = dbvo.getDBName()
                                else:
                                    clustername = dbvo.getDBName()

                                dbpatchdeployvo = DBPatchDeploymentVO(releasenumber=self.release_number,
                                                                      appln_support_code=schema_appln_support_code,
                                                                      db_type=dbvo.getDBType(),
                                                                      trim_host=dbvo.getTrimHost(),
                                                                      db_name=dbvo.getDBName(),
                                                                      schemaname=schema_name,
                                                                      schematype=schematype,
                                                                      cluster_name=clustername,
                                                                      deploytime=None,
                                                                      deploystatus="NOTDEPLOYED",
                                                                      spdeploytime=None,
                                                                      spdeploystatus="NOTDEPLOYED",
                                                                      major_number=major_number,
                                                                      minor_number=minor_number
                                                                      )
                                try:
                                    self.auditdao.addDBPatchDeployment(dbpatchdeployvo)
                                except Exception as e:
                                    print("WBXERROR: insert dbpatchploymentvo %s meet error: %s " % (dbpatchdeployvo, e))

                        splexdict = schema["splex"]
                        for tgtdbtype, tgtschema in splexdict.items():
                            if tgtdbtype not in self.db_type_map:
                                continue

                            tgt_appln_support_code = self.db_type_map[tgtdbtype]
                            # if not isincremental:
                            #                         #     depotdbDao.deleteShareplexBaseline(self.release_number, appln_support_code,tgt_appln_support_code)
                            for tgt_schematype, tabdict in tgtschema.items():
                                for tabstatus, tablelist in tabdict.items():
                                    for tabstr in tablelist:
                                        specifiedkeys = None
                                        columnfilter = None
                                        specifiedcolumn = None
                                        if wbxutil.isNoneString(tabstr):
                                            continue

                                        tabarr = tabstr.split(';')
                                        if tabstatus in ("add_table", "remove_table"):
                                            if not (len(tabarr) == 2 or len(tabarr) == 4):
                                                logger.error("WBXERROR: the table %s in add_table/remove_table segment, but the length is not 2" % tabstr)
                                                continue

                                            if len(tabarr) == 2:
                                                src_tab_name = tabarr[0]
                                                tgt_tab_name = tabarr[1]
                                            elif len(tabarr) == 4:
                                                src_tab_name = tabarr[0]
                                                tgt_tab_name = tabarr[2]

                                        elif tabstatus == "add_tab_with_keyword":
                                            if len(tabarr) != 3:
                                                logger.error("WBXERROR: the table %s in add_tab_with_keyword segment, but the length is not 3" % tabstr)
                                                continue
                                            src_tab_name = tabarr[0]
                                            tgt_tab_name = tabarr[2]
                                            specifiedkeys = tabarr[1]
                                        elif tabstatus == "add_tab_with_partition":
                                            if len(tabarr) != 4:
                                                logger.error("WBXERROR: the table %s in add_tab_with_partition segment, but the length is not 4" % tabstr)
                                                continue
                                            src_tab_name = tabarr[0]
                                            tgt_tab_name = tabarr[1]
                                        else:
                                            continue

                                        spvo = ShareplexBaselineVO(
                                            releasenumber=self.release_number,
                                            src_appln_support_code=rel_appln_support_code,
                                            src_schematype=schematype,
                                            src_tablename=src_tab_name.upper(),
                                            tgt_appln_support_code=tgt_appln_support_code,
                                            tgt_application_type="PRI,GSB",
                                            tgt_schematype=tgt_schematype,
                                            tgt_tablename=tgt_tab_name.upper(),
                                            tablestatus=tabstatus,
                                            specifiedkey=specifiedkeys,
                                            columnfilter=columnfilter,
                                            specifiedcolumn=specifiedcolumn,
                                            changerelease=self.release_number
                                        )
                                        try:
                                            self.auditdao.addDBPatchSPChange(spvo)
                                            logger.info("insert shareplexbaselinevo %s " % spvo)
                                        except Exception as e:
                                            logger.error(e)
                                        logger.info("insert process, insert vo  %s " % spvo)

                        # if isincremental:
                        print("-------- mergeShareplexBaseline ---------")
                        print("%s %s %s %s " %(prevreleasenumber, self.release_number, rel_appln_support_code,schematype))
                        self.auditdao.flush()
                        self.auditdao.mergeShareplexBaseline(prevreleasenumber, self.release_number, rel_appln_support_code,schematype)
                else:
                    logger.info("%s not in %s ,skip it" %(xml_db_type,self.db_type_map))

        except Exception as e:
            # self.auditdao.rollback()
            logger.error(e)
            raise e

    # getAllScheduledDBChange(dbid)
    #
    # def getAllScheduledDBChange(trim_host, db_name):
    #     daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    #     daomanager = daomanagerfactory.getDefaultDaoManager()
    #     depotDao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    #     db = daomanagerfactory.getDatabaseByDBID(dbid)
    #     starttime = wbxutil.getcurrenttime(31 * 24 * 60 * 60)
    #     try:
    #         daomanager.startTransaction()
    #         deploylist = auditdao.listScheduledChange(trim_host, db_name, starttime)
    #         for deploymentvo in deploylist:
    #             (scheduled_start_date, completed_date, summary, change_imp,
    #              infrastructure_change_id) = getDBpatchDeploymentChange(deploymentvo.releasenumber, db, deploymentvo.schemaname)
    #             if completed_date is not None or deploymentvo.change_id is None:
    #                 deploymentvo.change_id = infrastructure_change_id
    #                 deploymentvo.change_sch_start_date = scheduled_start_date
    #                 deploymentvo.change_completed_date = completed_date
    #                 deploymentvo.change_imp = change_imp
    #         daomanager.commit()
    #     except Exception as e:
    #         daomanager.rollback()
    #         logging.error("Error occurred when executing getAllScheduledDBChange(%s) with %s" % (dbid, e))

    #  parse result structure
    # {"OPDB":{
    #          "APP":{
    #                 "release_major_num":1,
    #                 "release_minor_num":1,
    #                 "splex":{
    #                         "TEODB":[table_list],
    #                         "SYSTOOL":[table_list]}
    #                 },
    #          "XXRPTH":{
    #                   "release_major_num":1,
    #                    "release_minor_num":1,
    #                    "splex":{
    #                             "TEODB":[table_list]
    #                            }
    #                   }
    #         }
    # "WEBDB": {
    #           "wbxmaint":{
    #
    #                      }
    #       }
    # }
    def parseNode(self, node):
        for child in node:
            nodetag = child.tag
            if nodetag == "release_number":
                self.release_number=child.text
            elif nodetag == "release_name":
                self.release_name=child.text
            elif nodetag == "db":
                self.cur_db_type = child.attrib["dbtype"]
                if self.cur_db_type not in self.dbtypemap:
                    self.dbtypemap[self.cur_db_type]={}  #schemaList
            elif nodetag == "schema":
                self.schematype = child.attrib["type"]
                db = self.dbtypemap[self.cur_db_type]
                if self.schematype not in db:
                    self.dbtypemap[self.cur_db_type][self.schematype] = {}
                schema = self.dbtypemap[self.cur_db_type][self.schematype]
                schema["splex"] = {}
                schema["release_major_num"] = None
                schema["release_minor_num"] = None
                schema["release_version_change"] = 1
            elif nodetag == "release_major_num":
                release_major_num = child.text
                schema = self.dbtypemap[self.cur_db_type][self.schematype]
                schema["release_major_num"] = release_major_num
            elif nodetag == "release_minor_num":
                release_minor_num = child.text
                schema = self.dbtypemap[self.cur_db_type][self.schematype]
                schema["release_minor_num"] = release_minor_num
            elif nodetag == "release_version_change":
                isreleasechanged = child.text
                schema = self.dbtypemap[self.cur_db_type][self.schematype]
                schema["release_version_change"] = 1 if isreleasechanged.strip() == "TRUE" else 0
            elif nodetag == "deployment":
                deploysteptype = child.attrib["type"]
                if deploysteptype not in ("schema_change", "splex_change_on_source"):
                    continue
            elif nodetag == "splex":
                # We only process splex with type="splex_change_on_source", so it has targetdb attribute
                self.targetdbtype = child.attrib["targetdb"]
                if self.targetdbtype  is None:
                    raise wbxexception("the targetdb should not be None dbtype=%s, schematype=%s" % (self.cur_db_type, self.schematype))
                db = self.dbtypemap[self.cur_db_type]
                schema = db[self.schematype]
                splexmap = schema["splex"]
                if self.targetdbtype not in splexmap:
                    splexmap[self.targetdbtype] = {}
            elif nodetag == "target_schema":
                targetschematype = child.attrib["type"]
                db = self.dbtypemap[self.cur_db_type]
                schema = db[self.schematype]
                splexmap = schema["splex"]
                targetdbmap = splexmap[self.targetdbtype]
                if targetschematype not in targetdbmap:
                    targetdbmap[targetschematype] = {}
                self.targetschema = targetdbmap[targetschematype]
            elif nodetag == "replication":
                self.tablestatus = child.attrib["type"]
                if self.tablestatus not in ("add_table", "remove_table"):
                    continue
                self.targetschema[self.tablestatus] = []
            elif nodetag == "table":
                nodevalue = child.text
                self.targetschema[self.tablestatus].append(nodevalue)
            self.parseNode(child)

    def test(self, releasenumber):
        self.db_type_map = {"WEBDB": "WEB", "TAHOEDB": "TEL", "BILLINGDB": "OPDB", "CONFIGDB": "CONFIG",
                            "GLOOKUPDB": "LOOKUP", "TEODB": "TEO", "MEDIATEDB": "MEDIATE", "STREAMDB": "CSP",
                            "DIAGNSDB":"DIAGNS"}
        self.dbtypemap = {}
        self.release_number = None
        self.release_name = None
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.parseXML("C:\\Users\\zhiwliu\\Documents\\office\\dbpatch_zip\\16065\\release.xml")
            self.saveBaselineToDB()
            logger.info("already saved")
            self.auditdao.commit()
        except Exception as e:
            logger.error("Error occurred when install dbpatch %s with errormsg %s" % (releasenumber, e), exc_info = e)
            self.auditdao.rollback()
        finally:
            self.auditdao.close()

if __name__ == '__main__':
    job = DBpatchDeployMonitorJob()
    job.initialize()
    job.test(16065)