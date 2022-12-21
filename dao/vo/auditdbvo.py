class wbxdatabase:
    def __init__(self,trim_host, db_name, appln_support_code, db_type, application_type, wbx_cluster, connectionurl):
        self._trim_host = trim_host
        self._db_name = db_name
        self._appln_support_code = appln_support_code
        self._db_type = db_type
        self._application_type = application_type
        self._wbx_cluster = wbx_cluster
        self._connectionurl = connectionurl

    def getConnectionurl(self):
        return self._connectionurl

    def getClusterName(self):
        return self._wbx_cluster

    def getTrimHost(self):
        return self._trim_host

    def getDBName(self):
        return self._db_name

    def getApplnSupportCode(self):
        return self._appln_support_code

    def getDBType(self):
        return self._db_type

    def getClusterName(self):
        return self._wbx_cluster

    def getdbid(self):
        return "%s_%s" % (self._trim_host, self._db_name)

