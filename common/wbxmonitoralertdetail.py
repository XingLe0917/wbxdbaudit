import uuid

from common.wbxexception import wbxexception
from dao.vo.wbxmonitoralertdetailvo import WbxmonitoralertdetailVo

def geWbxmonitoralertdetailVo(**kwargs):
    if "task_type" not in kwargs:
        raise wbxexception("Do not find task_type")
    task_type = kwargs['task_type']
    db_name = ""
    host_name = ""
    instance_name = ""
    splex_port = ""
    if "db_name" in kwargs:
        db_name = kwargs['db_name']
    if "host_name" in kwargs:
        host_name = kwargs['host_name']
        if '.webex.com' in host_name:
            host_name = str(host_name).split(".")[0]
    if "instance_name" in kwargs:
        instance_name = kwargs['instance_name']
    if "splex_port" in kwargs:
        splex_port = kwargs['splex_port']
    if not db_name and not host_name and not instance_name and not splex_port:
        raise wbxexception("Do not find db_name,host_name,instance_name and splex_port")
    alert_title = task_type
    if db_name:
        alert_title += "_" + db_name
    if host_name:
        alert_title += "_" + host_name
    if instance_name:
        alert_title += "_" + instance_name
    if splex_port:
        alert_title += "_" + splex_port
    parameter = dict(kwargs)
    parameter_str = ''' {'''
    index = 1
    for key in parameter:
        parameter_str += ''' "%s": "%s" ''' % (key, parameter[key])
        index += 1
        if index <= len(parameter):
            parameter_str += ","
    parameter_str += ''' }'''
    jobvo = WbxmonitoralertdetailVo(alertdetailid=uuid.uuid4().hex,alerttitle=alert_title, db_name=db_name, host_name=host_name,
                                    instance_name=instance_name, splex_port=splex_port, parameter=parameter_str,alert_type=task_type,
                                    job_name="")
    return jobvo



