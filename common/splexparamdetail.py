import uuid

from common.wbxexception import wbxexception
from dao.vo.splexparamdetailvo import SplexparamdetailVo
from datetime import datetime


def to_date(param):
    date_time_obj = datetime.strptime(param,'%Y-%m-%d %H:%M:%S')
    return date_time_obj


def splexdetailVo(**kwargs):
    if "host_name" not in kwargs:
        raise wbxexception("Do not find host_name")
    host_name = kwargs['host_name']
    port_number = ""
    param_category = ""
    param_name = ""
    queue_name = ""
    actual_value = ""
    default_value = ""
    collect_time = ""
    ismodified = ""
    if "port_number" in kwargs:
        port_number = kwargs['port_number']
    if "param_category" in kwargs:
        param_category = kwargs['param_category']
    if "param_name" in kwargs:
        param_name = kwargs['param_name']
    if "queue_name" in kwargs:
        queue_name = kwargs['queue_name']
    if "actual_value" in kwargs:
        actual_value = kwargs['actual_value']
    if "default_value" in kwargs:
        default_value = kwargs['default_value']
    if "collect_time" in kwargs:
        collect_time = to_date(kwargs['collect_time'])
    if "ismodified" in kwargs:
        ismodified = kwargs['ismodified']
    if not collect_time and not port_number and not param_category and not param_name and not actual_value:
        raise wbxexception("Do not find collect_time,port_number,param_category and param_name and actual_value")

    paramvo = SplexparamdetailVo(host_name=host_name,port_number=port_number, param_category=param_category, param_name=param_name,
                                    queue_name=queue_name, actual_value=actual_value, default_value=default_value, collect_time=collect_time,ismodified=ismodified)
    return paramvo



