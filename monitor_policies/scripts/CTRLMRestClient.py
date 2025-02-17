'''

Created on Feb 11, 2019



@author: JFOURNET

'''

import requests
import logging
import urllib3
import json

class CTRLMRestClient(object):

    '''

    classdocs

    '''

    LOGIN_URL = "/automation-api/session/login"
    LOGOUT_URL = "/automation-api/session/logout"
    GET_JOB_STATUSES = "/automation-api/run/jobs/status"
    GET_SERVERS = "/automation-api/config/servers"
    GET_AUTOMATION_API_SERVER_STATUS = "/automation-api/status"
    GET_HOST_GROUPS = "/automation-api/config/server/{SERVER}/hostgroups"
    GET_SLA_SERVICES = "/automation-api/run/services/sla"
    GET_WORKFLOW_INSIGHTS_STATUS = "/automation-api/config/workflowinsights/status"
    GET_AGENTS = "/automation-api/config/server/{SERVER}/agents"
    GET_EVENTS = "/automation-api/run/events"
    OK_STATUS = 200

    def __init__(self, controlm_host=None, controlm_port=None, controlm_api_token=None, logger=None):

        '''

        Constructor

        '''

        self.__controlm_host = controlm_host

        self.__controlm_port = controlm_port
        
        self.__controlm_api_token = controlm_api_token

        self.__logger = logger

        self.__logger = logging.getLogger(__name__)
        

    
    def logout(self):
    
        urllib3.disable_warnings()
        command = "https://" + self.__controlm_host + ":" + self.__controlm_port + self.LOGOUT_URL
        headers = {'content-type':'application/json', 'Authorization': 'Bearer ' + self.__controlm_api_token}
        result = requests.post(command, headers=headers, verify=False, timeout=60)
        return result
    
    def get_controlm_servers(self, logger):
        urllib3.disable_warnings()
        command = "https://" + self.__controlm_host + ":" + self.__controlm_port + self.GET_SERVERS
        headers = {'content-type':'application/json', 'x-api-key': self.__controlm_api_token}
        logger.debug("headers :" + str(headers))
        logger.debug("command :" + str(command))
        result = requests.get(command, headers=headers, verify=False, timeout=60)
        return result
    
    def get_sla_services(self, logger):
        urllib3.disable_warnings()
        command = "https://" + self.__controlm_host + ":" + self.__controlm_port + self.GET_SLA_SERVICES
        headers = {'content-type':'application/json', 'x-api-key': self.__controlm_api_token}
        logger.debug("headers :" + str(headers))
        logger.debug("command :" + str(command))
        result = requests.get(command, headers=headers, verify=False, timeout=60)
        return result
    
    def get_automation_api_server_status(self, logger):
        urllib3.disable_warnings()
        command = "https://" + self.__controlm_host + ":" + self.__controlm_port + self.GET_AUTOMATION_API_SERVER_STATUS
        headers = {'content-type':'application/json', 'x-api-key': self.__controlm_api_token}
        logger.debug("headers :" + str(headers))
        logger.debug("command :" + str(command))
        result = requests.get(command, headers=headers, verify=False, timeout=60)
        return result
    
    def get_workflow_insights_status(self, logger):
        urllib3.disable_warnings()
        command = "https://" + self.__controlm_host + ":" + self.__controlm_port + self.GET_WORKFLOW_INSIGHTS_STATUS
        headers = {'content-type':'application/json', 'x-api-key': self.__controlm_api_token}
        logger.debug("headers :" + str(headers))
        logger.debug("command :" + str(command))
        result = requests.get(command, headers=headers, verify=False, timeout=60)
        return result
    
    def get_controlm_host_groups(self, controlm_server, logger):
        urllib3.disable_warnings()
        command = "https://" + self.__controlm_host + ":" + self.__controlm_port + self.GET_HOST_GROUPS
        command = command.replace("{SERVER}", controlm_server)
        headers = {'content-type':'application/json', 'x-api-key': self.__controlm_api_token}
        logger.debug("headers :" + str(headers))
        logger.debug("command :" + str(command))
        result = requests.get(command, headers=headers, verify=False, timeout=60)
        return result
    
    def get_controlm_server_agents(self, controlm_server, logger):
        urllib3.disable_warnings()
        command = "https://" + self.__controlm_host + ":" + self.__controlm_port + self.GET_AGENTS
        command = command.replace("{SERVER}", controlm_server)
        headers = {'content-type':'application/json', 'x-api-key': self.__controlm_api_token}
        logger.debug("headers :" + str(headers))
        logger.debug("command :" + str(command))
        result = requests.get(command, headers=headers, verify=False, timeout=60)
        return result
    
    def get_controlm_job_statuses(self, start_time, end_time, logger):
        urllib3.disable_warnings()
        command = "https://" + self.__controlm_host + ":" + self.__controlm_port + self.GET_JOB_STATUSES + "?limit=1000&fromTime=" + start_time + "&toTime=" + end_time
        headers = {'content-type':'application/json', 'Authorization': 'Bearer ' + self.__controlm_api_token}
        logger.debug("headers :" + str(headers))
        logger.debug("command :" + str(command))
        result = requests.get(command, headers=headers, verify=False, timeout=60)
        return result
    
    def get_controlm_events(self, date):
        date = "20240516"
        urllib3.disable_warnings()
        command = "https://" + self.__controlm_host + ":" + self.__controlm_port + self.GET_EVENTS + "?limit=1000&date=" + date
        headers = {'content-type':'application/json', 'Authorization': 'Bearer ' + self.__controlm_api_token}
        result = requests.get(command, headers=headers, verify=False, timeout=60)
        return result