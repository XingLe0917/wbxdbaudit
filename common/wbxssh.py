import logging
import paramiko
from paramiko import SSHClient
import socket
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
from common.config import Config

import time

LINUX_LINE_SEPARATOR="\n"
LINUX_PATH_SEPARATOR="/"

logger = logging.getLogger("dbaudit")

class wbxssh:
    def __init__(self, host_name, ssh_port, login_user, login_pwd):
        self.host_name = host_name
        self.ssh_port = ssh_port
        self.login_user = login_user
        self.login_pwd = login_pwd
        self.channel = None
        self.isconnected = False
        config = Config()
        self.domain_name = config.domain_name

    def connect(self):
        self.client = SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.client.connect("%s.%s" % (self.host_name,self.domain_name), port=self.ssh_port, username=self.login_user, password=self.login_pwd)
            self.isconnected = True
        except (paramiko.BadHostKeyException, paramiko.BadAuthenticationType, paramiko.SSHException, socket.error) as e:
            raise wbxexception(e)

    def verifyConnection(self):
        res = self.exec_command("pwd")
        if res == "":
            print("Can not login to server %s" % self.host_name)
        else:
            print("Login succeed")

    def send(self, cmd):
        if self.channel is None:
            self.channel = self.client.invoke_shell()
        newline = '\r'
        line_buffer = ''
        self.channel.send(cmd + line_buffer + newline)

    def recv(self):
        res = ""
        if self.channel is not None:
            while True:
                buffer = self.channel.recv(1024).decode('utf-8')
                if len(buffer) == 0:
                    break
                res = "%s%s" % (res,buffer)
        return buffer

    def invoke_shell(self,cmd):
        logger.info(cmd)
        try:
            if not self.isconnected:
                self.connect()
            channel=self.client.invoke_shell()
            channel.send(cmd + "\n")
        except paramiko.SSHException as e:
            raise wbxexception("Error ocurred when executing command %s with error msg %s" % (cmd, e))

    def exec_command(self, cmd, timeout = 0, *args):
        # logger.info(cmd)
        try:
            if not self.isconnected:
                self.connect()
            stdin, stdout, stderr = self.client.exec_command(cmd, bufsize=100000)
            if timeout > 0:
                stdout.channel.settimeout(timeout)

            out, err = stdout.read().decode(), stderr.read().decode()
            res_code = stdout.channel.recv_exit_status()  # exiting code for executing
            res = ""
            if out != "":
                res = out
            if err != "":
                res = "%s\n%s" %(res, err)
            return res.strip(LINUX_LINE_SEPARATOR), True if res_code == 0 else False
        except paramiko.SSHException as e:
            raise wbxexception("Error ocurred when executing command %s with error msg %s" % (cmd, e))

    def close(self):
        if self.client is not None:
            self.client.close()

    def stopService(self, servicename):
        logger.info("stopService servicename=%s" % servicename)
        self.exec_command("sudo service %s stop" % servicename, timeout=60)

    def startService(self, servicename):
        logger.info("startService servicename=%s" % servicename)
        self.exec_command("sudo service %s start" % servicename, timeout=60)

    def startBlackout(self, serverlist, hours):
        cmd = "ps aux | grep emagent | grep perl | grep -v grep | awk '{print $11}' | cut -d '/' -f 1,2,3,4,5,6,7,8"
        emctl_base = self.exec_command(cmd)
        prog_emctl = "%s/bin/emctl" % emctl_base
        if self.isFile(prog_emctl):
            for host_name in serverlist:
                cmd = "ssh %s %s start blackout forAutomationTool -nodeLevel  -d %s:00" % (host_name, prog_emctl, hours)
                logger.info("startBlackout cmd=%s" % cmd)
                self.exec_command(cmd)
        else:
            raise wbxexception("Does not find emctl command when start blackout: %s" % prog_emctl)

    def stopBlackout(self, serverlist):
        cmd = "ps aux | grep emagent | grep perl | grep -v grep | awk '{print $11}' | cut -d '/' -f 1,2,3,4,5,6,7,8"
        emctl_base = self.exec_command(cmd)
        prog_emctl = "%s/bin/emctl" % emctl_base
        if self.isFile(prog_emctl):
            for host_name in serverlist:
                cmd = "ssh %s %s stop blackout forAutomationTool" % (host_name, prog_emctl)
                logger.info("stopBlackout on server %s cmd=%s" % (host_name, cmd))
                self.exec_command(cmd)
        else:
            raise wbxexception("Does not find emctl command when stop blackout: %s" % prog_emctl)

    def isFile(self, filepath):
        cmd = "if [ -f %s ]; then echo 'Y'; else echo 'N'; fi" % filepath
        res, status = self.exec_command(cmd)
        if res == "Y":
            return True
        else:
            return False

    def isDirectory(self, pathname):
        cmd = "if [ -d %s ]; then echo 'Y'; else echo 'N'; fi" % pathname
        res = self.exec_command(cmd)
        if res == "Y":
            return True
        else:
            return False

    def removeFile(self, file_path):
        cmd = "rm -f %s" % file_path
        self.exec_command(cmd)

    def installDBpatch(self, releaseNumber, rpmfilename):
        tgtdir = "/home/oracle"
        rpmfile = "%s/%s" % (tgtdir, rpmfilename)
        if not self.isFile(rpmfile):
            rpmurl = "http://repo.webex.com/prod/DBpatch/noarch/%s" % rpmfilename
            cmd = "sudo wget -P %s %s" % (tgtdir, rpmurl)
            self.exec_command(cmd)
            if not self.isFile(rpmfile):
                raise wbxexception("Can not download rpm file %s" % rpmurl)

        res = self.exec_command("sudo rpm -qa | grep %s | grep -v grep" % releaseNumber)
        if not wbxutil.isNoneString(res):
            self.exec_command("sudo rpm -e %s" % res)
        self.exec_command("sudo rpm -ivh %s" % rpmfile)
        installdir = "/tmp/%s" % releaseNumber
        if not self.isDirectory(installdir):
            raise wbxexception("After installation, can not get the installation directory %s" % installdir)

