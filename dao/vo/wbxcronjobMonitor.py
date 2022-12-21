class wbxcronjobmonitor:
    def __init__(self, host_name,pwd,commandstr,params):
        self._host_name = host_name
        self._pwd = pwd
        self._commandstr = commandstr
        self._params = params

    def getHostName(self):
        return self._host_name

    def getCommandstr(self):
        return self._commandstr

    def getParams(self):
        return self._params

    def getpwd(self):
        return self._pwd
