import logging
import cx_Oracle
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from common.wbxinfluxdb import wbxinfluxdb
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed

logger = logging.getLogger("dbaudit")


class OSConfigMonitor(WbxJob):

    def start(self):
        self.appln_support_code = self.args[0].upper()
        self.HostFactory = []
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.influxdb_table_name = "osconfig"
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.HostFactory = self.auditdao.getHostListbyAPPCode(self.appln_support_code)
            self.auditdao.commit()
        except Exception as e:
            logger.error("OSConfigMonitor met error with %s", str(e))
            self.auditdao.rollback()
        finally:
            self.auditdao.close()
        flist = []
        executor = ThreadPoolExecutor(max_workers=2)
        for hostitem in self.HostFactory:
            flist.append(executor.submit(self.jobHandler, hostitem))
        executor.shutdown(wait=True)
        # self.jobHandler(self.HostFactory[1])

    def jobHandler(self, hostitem):
        logger.info("Start to collect info from %s" % hostitem.host_name)
        try:
            hostitem.connect()
            ############## network interface ##############
            self.get_rp_filter(hostitem)
            self.get_netstat(hostitem)
            ############ MEMORY #################
            self.get_proc_meminfo(hostitem)
            self.get_redhat_transparent_hugepage_enabled(hostitem)
            ########### OS limitation ##############
            self.get_sysctl_config(hostitem)
            self.get_limits_conf(hostitem)
            self.get_selinux(hostitem)
            ########### service ##############
            self.get_service_iptables_and_multipathd_status(hostitem, "iptables")
            self.get_service_iptables_and_multipathd_status(hostitem, "multipathd")
            self.get_chkconfig_multi_and_iptables(hostitem, "multi")
            self.get_chkconfig_multi_and_iptables(hostitem, "iptables")
            ########### disk ##############
            self.get_scheduler_and_multipath(hostitem)
            ############ Process ##############
            self.get_lms_and_vktm_ps(hostitem, "lms")
            self.get_lms_and_vktm_ps(hostitem, "vktm")
            ############ File ##############
            self.get_oradism(hostitem)

        except Exception as e:
            logger.error("Execution on host %s failed with error %s" % (hostitem.host_name, str(e)))
        finally:
            hostitem.close()

    def get_rp_filter(self, server):
        cmd = "tail /proc/sys/net/ipv4/conf/*/rp_filter"
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        for item in rst_list:
            if "==> " in item:
                file_name = item.split("==> ")[-1].split(" <==")[0]
                interface = file_name.split("/")[-2]
                value_list.append({
                    "host": server.host_name + ".webex.com",
                    "datacenter": server.site_code,
                    "datastore": "ORACLE",
                    "db_env": server.db_type,
                    "metric_type": "INTERFACE_RP_FILETER",
                    "metric_name": interface,
                    "metric_value": None
                })
            else:
                value_list[-1]["metric_value"] = int(item)
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_proc_meminfo(self, server):
        cmd = "cat /proc/meminfo |grep -i ^HugePage"
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        for item in rst_list:
            item_list = item.split()
            if "HugePages_Surp" in item_list[0]:
                continue
            value_list.append({
                "host": server.host_name + ".webex.com",
                "datacenter": server.site_code,
                "datastore": "ORACLE",
                "db_env": server.db_type,
                "metric_type": "MEMORY",
                "metric_name": item_list[0].replace(":", ""),
                "metric_value": int(item_list[1])
            })
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_redhat_transparent_hugepage_enabled(self, server):
        cmd = "cat /sys/kernel/mm/redhat_transparent_hugepage/enabled"
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        enabled_value = rst.split("\n")[0]
        selected_enable_value = enabled_value.split("[")[-1].split("]")[0]
        metric_value_map = {
            "always": 0,
            "madvise": 1,
            "never": 2
        }
        value_list = [{
            "host": server.host_name + ".webex.com",
            "datacenter": server.site_code,
            "datastore": "ORACLE",
            "db_env": server.db_type,
            "metric_type": "MEMORY",
            "metric_name": "transparent_hugepage",
            "metric_value": metric_value_map[selected_enable_value]
        }]
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_netstat(self, server):
        cmd = "netstat -nai|grep -E 'eth1|eth3|eth5'"
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        for item in rst_list:
            item_list = item.split()
            value_list.append({
                "host": server.host_name + ".webex.com",
                "datacenter": server.site_code,
                "datastore": "ORACLE",
                "db_env": server.db_type,
                "metric_type": "INTERFACE_MTU",
                "metric_name": item_list[0],
                "metric_value": int(item_list[1])
            })
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_asmdisks(self, server):
        cmd = "ls -l /dev/asmdisks/*"
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        for item in rst_list:
            item_list = item.split()
            value_list.append({
                "host": server.host_name + ".webex.com",
                "datacenter": server.site_code,
                "datastore": "ORACLE",
                "db_env": server.db_type,
                "metric_type": "ASMDISK",
                "metric_name": "diskname",
                "metric_value": item_list[-1]
            })
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_service_iptables_and_multipathd_status(self, server, service_type):
        if service_type not in ["iptables", "multipathd"]:
            raise Exception("The service %s status not support" % service_type)
        cmd = "sudo service %s status" % service_type
        rst, rst_bool = server.exec_command(cmd)
        service_status = rst.split("\n")[0]
        status = None
        status_map = {
            "NOTEXIST": 0,
            "RUNNING": 1,
            "STOPPED": 2
        }
        if "unrecognized service" in service_status:
            status = "NOTEXIST"
        elif "is running" in service_status:
            status = "RUNNING"
        elif "stop" in service_status or "not running" in service_status:
            status = "STOPPED"
        value_list = [{
            "host": server.host_name + ".webex.com",
            "datacenter": server.site_code,
            "datastore": "ORACLE",
            "db_env": server.db_type,
            "metric_type": "SERVICE_STATUS",
            "metric_name": service_type,
            "status": status
        }]
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_chkconfig_multi_and_iptables(self, server, service_type):
        if service_type not in ["multi", "iptables"]:
            raise Exception("The service %s chkconfig not support" % service_type)
        cmd = "source ~/.bash_profile; chkconfig | grep %s" % service_type
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        service_autostart_map = {
            "on": "1",
            "off": "0"
        }
        for item in rst_list:
            item_list = item.split()
            value_list.append({
                "host": server.host_name + ".webex.com",
                "datacenter": server.site_code,
                "datastore": "ORACLE",
                "db_env": server.db_type,
                "metric_type": "SERVICE_AUTOSTART",
                "metric_name": item_list[0],
                "metric_value": int("".join([service_autostart_map.get(item.split(":")[-1]) for item in item_list[1:]]), 2)
            })
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_sysctl_config(self, server):
        cmd = """
        cat /etc/sysctl.conf|grep -E "shmmax|shmall|shmmni|sem|msgmni|msgmax|msgmnb|sysrq|file-max|aio-max-nr|rmem|wmem|nr_hugepages|ip_local_port_range|panic_on_oops|tcp_keepalive_time|pid_max|rp_filter"  | grep -v port_range
        """
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        for item in rst_list:
            if "#" in item:
                continue
            item_list = item.split("=")
            value_list.append({
                "host": server.host_name + ".webex.com",
                "datacenter": server.site_code,
                "datastore": "ORACLE",
                "db_env": server.db_type,
                "metric_type": "SYSCTL_CONFIG",
                "metric_name": "".join(item_list[0].split()).replace(".", "_"),
                "metric_value": int("".join(item_list[1].split()))
            })
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_limits_conf(self, server):
        cmd = "grep -v \# /etc/security/limits.conf | grep -v \*"
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        for item in rst_list:
            item_list = item.split()
            value_list.append({
                "host": server.host_name + ".webex.com",
                "datacenter": server.site_code,
                "datastore": "ORACLE",
                "db_env": server.db_type,
                "metric_type": "LIMITS_CONF",
                "metric_name": "_".join(item_list[: -1]),
                "metric_value": int(item_list[-1]) if item_list[-1] != "unlimited" else -1
            })
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_selinux(self, server):
        cmd = "cat /etc/selinux/config|grep -v \# |grep SELINUX"
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        SELINUX_map = {
            "enforcing": 0,
            "permissive": 1,
            "disabled": 2
        }
        SELINUXTYPE_map = {
            "targeted": 0,
            "strict": 1
        }
        for item in rst_list:
            item_list = item.split("=")
            matric_name = "".join(item_list[0].split())
            value_list.append({
                "host": server.host_name + ".webex.com",
                "datacenter": server.site_code,
                "datastore": "ORACLE",
                "db_env": server.db_type,
                "metric_type": "SELINUX",
                "metric_name": matric_name,
                "metric_value": SELINUX_map["".join(item_list[1].split())] if matric_name == "SELINUX" else SELINUXTYPE_map["".join(item_list[1].split())]
            })
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_scheduler_and_multipath(self, server):
        cmd = "for i in `find /sys/block/dm-*/queue/scheduler` ; do echo $i; cat $i;done | cat"
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        metric_value_map = {
            "noop": 0,
            "anticipatory": 1,
            "deadline": 2,
            "cfq": 3
        }
        for item in rst_list:
            if "/sys/block" in item:
                disk_name = item.split("/sys/block/")[-1].split("/queue/scheduler")[0]
            else:
                value_list.append({
                    "host": server.host_name + ".webex.com",
                    "datacenter": server.site_code,
                    "datastore": "ORACLE",
                    "db_env": server.db_type,
                    "metric_type": "ASMDISK_SCHEDULER",
                    "metric_name": disk_name,
                    "metric_value": metric_value_map[item.split("[")[-1].split("]")[0]]
                })
                disk_name = None
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_lms_and_vktm_ps(self, server, process_name):
        if process_name not in ["lms", "vktm"]:
            raise Exception("The process type %s ps not support" % process_name)
        cmd = "ps -Acfl|head -1;ps -Acfl|grep %s | grep -v grep" % process_name
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        for item in rst_list:
            if "CMD" in item:
                continue
            item_list = item.split()
            value_list.append({
                "host": server.host_name + ".webex.com",
                "datacenter": server.site_code,
                "datastore": "ORACLE",
                "db_env": server.db_type,
                "metric_type": "PROCESS_PRIORITY",
                "metric_name": item_list[-1],
                "metric_value": int(item_list[6])
            })
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb(self.influxdb_table_name, value_list)

    def get_oradism(self, server):
        cmd = "ls -l /u00/app/oracle/product/11.2.0/db/bin/oradism"
        rst, rst_bool = server.exec_command(cmd)
        if not rst_bool:
            raise Exception("Execute %s on server %s with error %s" % (cmd, server.host_name, str(rst)))
        rst_list = list(filter(None, rst.split("\n")))
        value_list = []
        for item in rst_list:
            item_list = item.split()
            value_list.append({
                "host": server.host_name + ".webex.com",
                "datacenter": server.site_code,
                "datastore": "ORACLE",
                "db_env": server.db_type,
                "metric_type": "FILEINFO",
                "metric_name": item_list[-1],
                "mode": item_list[0],
                "fileowner": item_list[2],
                "filegroup": item_list[3]
            })
        print(cmd, "\n", value_list[0])
        wbxinfluxdb().insert_data_to_influxdb("osconfig_file", value_list)
