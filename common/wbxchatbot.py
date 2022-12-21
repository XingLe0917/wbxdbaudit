import json

import requests
import logging

logger = logging.getLogger("DBAMONITOR")


room_id_dict = {
    "ccp dba team": "Y2lzY29zcGFyazovL3VzL1JPT00vZjk1MmVkMjAtOWIyOC0xMWVhLTliMDQtODVlZDBhY2M0ZTNi",
    "ccp project": "Y2lzY29zcGFyazovL3VzL1JPT00vNzFjMGYwZDAtMDVkZS0xMWVhLTg2NmMtZDM2MDY0NjdjZDI2",
    "dbabot": "Y2lzY29zcGFyazovL3VzL1JPT00vZDVlZDExYTAtY2IwNS0xMWVhLThiMWEtYjdhM2Q0NWRjODBl",
    "webdb Metrics build internal group": "Y2lzY29zcGFyazovL3VzL1JPT00vZDIyMzk5NjAtYmFhNS0xMWVhLWFjYTctNWQ3NjJiMjQ3ZThl",
    "ccp dba alert": "Y2lzY29zcGFyazovL3VzL1JPT00vZDA0NzQxYjAtMDMwNi0xMWViLTk2NTktNjNhNGQ0MDY3ODFh",
    "yejfeng": "Y2lzY29zcGFyazovL3VzL1JPT00vNDE0MDJhMDAtODJmMy0xMWViLWI4YTAtNzVhZmQzZDExMjlh",
    "DBA Deployment Notifications": "Y2lzY29zcGFyazovL3VzL1JPT00vMTNiNTA3MDAtYTVjNi0xMWVhLThmNzQtMWY5NDA5MDlhOThk"
}

room_group = {
    "PCCP_TEST":[{"CCP DBA team":"Y2lzY29zcGFyazovL3VzL1JPT00vZjk1MmVkMjAtOWIyOC0xMWVhLTliMDQtODVlZDBhY2M0ZTNi"}],
    "PCCP_DBA":[{"ccp project": "Y2lzY29zcGFyazovL3VzL1JPT00vNzFjMGYwZDAtMDVkZS0xMWVhLTg2NmMtZDM2MDY0NjdjZDI2"}]
}


def get_oncall_cec_from_pagerduty():
    from pdpyras import APISession
    api_token = "u+EKr6hAANd3NQFExEYg"
    oncall_team_name = "CEO-cwopsdba-Primary"
    session = APISession(api_token)
    oncall_user_list = []
    for item in session.iter_all('oncalls'):
        if item["schedule"] and item["schedule"]["summary"] == oncall_team_name:
            if item["user"] and item["user"]["summary"]:
                oncall_user_list.append(item["user"]["summary"])
    oncall_user_list = list(set(oncall_user_list))
    if len(oncall_user_list) != 1:
        wbxchatbot().alert_msg_to_person("the oncall list from pagerduty is %s which is abnormal!!" % oncall_user_list, "yejfeng@cisco.com")
    oncall_user_name = oncall_user_list[0]
    oncall_user_email = session.find('users', oncall_user_name, attribute="name").get("email", None)
    logger.info("oncall : %s" % oncall_user_email)
    return oncall_user_email


class wbxchatbot:
    def __init__(self):
        self.room_id = room_id_dict["dbabot"]
        self.chatbot_url = "http://sjgrcabt102.webex.com:9000/api/sentAlertToBot"
        self.chatbot_rooms = "https://webexapis.com/v1/rooms"
        self.chatbot_people = "https://webexapis.com/v1/people"
        self.chatbot_message_url = "https://webexapis.com/v1/messages"
        self.chatbot_hearders = {
            "Authorization": "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"}

    def alert_msg(self, msg):
        response = requests.post(url=self.chatbot_url, json={"roomId": self.room_id,"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_dbateam(self, msg):
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["ccp dba team"],"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_dbabot(self, msg):
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["dbabot"],"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_dbabot_and_call_person(self, msg, cec):
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["dbabot"],"content": msg + "\n<@personEmail:%s@cisco.com>" % cec}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_dbabot_and_call_oncall(self, msg):
        oncall_email = get_oncall_cec_from_pagerduty()
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["dbabot"],"content": msg + "\n<@personEmail:%s>" % oncall_email}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_DBA_Deployment_Notifications_and_call_oncall(self, msg):
        oncall_email = get_oncall_cec_from_pagerduty()
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["DBA Deployment Notifications"],"content": msg + "\n<@personEmail:%s>" % oncall_email}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_web_metric(self, msg):
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["webdb Metrics build internal group"],"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_dbabot_by_roomId(self, msg,roomId):
        response = requests.post(url=self.chatbot_url, json={"roomId": roomId,"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})

    def alert_msg_to_person(self, msg, email):
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["yejfeng"],"content": msg + "\n<@personEmail:%s>" % email}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})

    def alert_msg_to_dbabot_by_group(self,group_name,msg):
        roomIds=room_group[group_name]
        for name in roomIds:
            for (name, roomId) in dict(name).items():
                self.alert_msg_to_dbabot_by_roomId(msg, roomId)

    def address_alert_list(self, title_list, info_list):
        list_len = len(title_list)
        info_len = len(info_list)
        colume_list = []
        for i in range(0, list_len):
            colume_list.append([title_list[i]])
        for row in info_list:
            for i in range(0, list_len):
                colume_list[i].append(row[i])
        new_colume_list = []
        for i in range(0, list_len):
            row_len = 0
            for item in colume_list[i]:
                row_len = len(str(item)) if len(str(item)) > row_len else row_len
                # row_len = row_len // 4 * 4 + 4
            new_colume_item = []
            for item in colume_list[i]:
                # item_info = str(item) + "\t" * ((row_len - len(str(item))) // 4 + 1)
                item_info = str(item) + " " * (row_len - len(str(item))) + "\t\t"
                new_colume_item.append(item_info)
            new_colume_list.append(new_colume_item)
        rst_list = []
        for i in range(0, info_len + 1):
            rst_list.append([])
        for row in new_colume_list:
            for i in range(info_len + 1):
                rst_list[i].append(row[i])
        msg = ""
        for row in rst_list:
            msg += "\t" + "".join(row) + "\n"
        return msg

    def getSplitList(self, ori_list,split_num):
        if len(ori_list) > split_num:
            new_list = []
            last = []
            while len(ori_list) > split_num:
                first = ori_list[0:split_num]
                last = ori_list[split_num:]
                new_list.append(first)
                ori_list = last
            if len(last) > 0:
                new_list.append(last)
            return new_list
        else:
            return [ori_list]

    def sendAlertToChatBot(self, rows_list,alert_title,roomId,toPersonId):
        msg = "### %s \n" %(alert_title)
        msg += "```\n {} \n```".format(rows_list)
        if roomId:
            wbxchatbot().alert_msg_to_dbabot_by_roomId(msg,roomId)
        if toPersonId:
            wbxchatbot().create_message_to_people(toPersonId,msg)

    def get_dbabot_rooms(self,type):
        url = self.chatbot_rooms
        if "direct" == type or "group" == type:
            url = self.chatbot_rooms + "?type=%s" %(type)
        response = requests.get(url=url, headers={
            "Authorization": "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"})
        if response.status_code == 200:
            items = json.loads(response.text)['items']
            return items
        else:
            return "Error!"

    def get_people_by_id(self,id):
        url = self.chatbot_people +"?id=%s"%(id)
        response = requests.get(url=url, headers={
            "Authorization": "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"})
        if response.status_code == 200:
            items = json.loads(response.text)['items']
            return items
        else:
            return "Error!"

    def get_people_by_cec(self,cec):
        url = self.chatbot_people +"?email=%s@cisco.com"%(cec)
        response = requests.get(url=url, headers={
            "Authorization": "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"})
        if response.status_code == 200:
            items = json.loads(response.text)['items']
            return items
        else:
            return "Error!"

    # def get_people_roomid_by_cec(self,cec):
    #     people = self.get_people_by_cec(cec)
    #     displayName = ""
    #     if people:
    #         displayName = people[0]["displayName"]
    #     if displayName:
    #         directs = self.get_dbabot_rooms("direct")
    #         for item in directs:
    #             if item['title'] == displayName:
    #                 return item['id']
    #     return None

    def create_message_to_people(self,toPersonId,text):
        response = requests.post(url=self.chatbot_message_url, headers={"Content-Type": "application/json",
                                                   "Authorization": "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"},
                                 json={"toPersonId": toPersonId, "markdown": text})
        if response.status_code == 200:
            res = json.loads(response.text)
            return res
        else:
            return "Error!"

if __name__ == '__main__':

    job = wbxchatbot()
    # job.alert_msg("hello room")
    # job.alert_msg_to_person("ddd", "yejfeng@cisco.com")
    # get_oncall_cec_from_pagerduty()
    # res = job.get_dbabot_rooms("group")

    people = job.get_people_by_cec("lexing")
    print(people)
    # hatty = job.get_people_cec("hatty")
    # print(hatty)
    peopleid = ""
    if len(people) > 0:
        peopleid = people[0]['id']
    print(peopleid)
    res = job.create_message_to_people(peopleid,
                                       "### Hi, I am testing. If you receive this message, please tell me. Thank you. \n ~ From Le Xing")
