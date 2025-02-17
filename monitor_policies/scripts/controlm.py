import json
import os
from pathlib import Path
import argparse
import configparser
import time
from datetime import datetime, timezone, timedelta
import logging.config
from BHOMRestClient import BHOMRestClient
from CTRLMRestClient import CTRLMRestClient
'''
Created on May 22, 2024

@author: jfournet
'''

if __name__ == '__main__':
    pass

def usage():
    usage_msg = "control-m [-h --help] -d [debug]"
    return usage_msg

def convert_server_state(state_string):
    if (state_string == "Down"):
        return 1
    elif (state_string == "Up"):
        return 0
    else:
        return -1

def convert_status(status_string):
    if (status_string == "Ended Not OK"):
        return 1
    elif (status_string == "Ended OK"):
        return 0
    else:
        return -1
    #endif

def convert_wfi_server_status(status):
    if (status == "GREEN"):
        return 0
    else:
        return 1
    
def convert_agent_status(status_string):
    if (status_string == "Available"):
        return 0
    elif (status_string == "Unavailable"):
        return 1
    elif (status_string == "Disabled"):
        return 2
    elif (status_string == "Disable"):
        return 3
    elif (status_string == "Discovering"):
        return 4
    #endif
    
def convert_sla_service_status(status_string):
    if (status_string == "Ok"):
        return 0
    elif (status_string == "Completed Ok"):
        return 1
    elif (status_string == "Completed late"):
        return 2
    elif (status_string == "Not Ok"):
        return 3
    #endif

def calculate_execution_time(startTime, endTime):
    if startTime != "" and endTime != "":
        startTimeEpoch = convert_date_string_to_epoch(startTime)
        endTimeEpoch = convert_date_string_to_epoch(endTime)
        executionTime = endTimeEpoch - startTimeEpoch
        return executionTime
    else:
        return 0
 
def add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, customlabels):  
    bhommetric = {}
    labels = {}
    samples = []
    bhommetric["labels"] = labels
    bhommetric["samples"] = samples
    labels["metricName"] = metricname
    labels["hostname"] = hostname
    labels["source"] = source
    labels["entityId"] = source + ":" + hostname + ":" + entitytypeid + ":" + entityname
    labels["entityTypeId"] = entitytypeid
    labels["instanceName"] = instancename
    labels["entityName"] = entityname
    labels["hostType"] = "Server"
    labels["isKpi"] = iskpi
    labels["isDeviceMappingEnabled"] = True
    labels["unit"] = units
    for label in customlabels:
        labels[label] = customlabels[label]
    #endfor
    sample = {}
    sample['value'] = metricvalue
    sample['timestamp'] = timestamp
    samples.append(sample)
    bhommetrics.append(bhommetric)
    
    if (len(bhommetrics) > chunksize):
        send_to_bhom(bhom_server_host, bhom_server_port, bhommetrics)
        bhommetrics = []
    return bhommetrics

# def add_metric(job, metricName, metricValue, units):
#     bhommetric = {}
#     labels = {}
#     samples = []
#     entityName = job['name']
#     if job['host'] != "":
#         hostName = job['host']
#     else:
#         hostName = job['ctm']
#     endTime = job["endTime"]
#     startTime = job["startTime"]
#     if endTime != "":
#         executionTimeEpoch = convert_date_string_to_epoch(endTime)
#     elif startTime != "":
#         executionTimeEpoch = convert_date_string_to_epoch(startTime)
#     else:
#         return None
#     bhommetric["labels"] = labels
#     bhommetric["samples"] = samples
#     labels["metricName"] = metricName
#     labels["hostname"] = hostName
#     labels["source"] = source
#     labels["entityId"] = source + ":" + hostName + ":" + entityTypeId + ":" + entityName
#     labels["entityTypeId"] = entityTypeId
#     labels["instanceName"] = entityName
#     labels["entityName"] = entityName
#     labels["hostType"] = "Server"
#     if (metricName == "Status"):
#         labels["isKpi"] = False
#     else:
#         labels["isKpi"] = True
#     labels["isDeviceMappingEnabled"] = True
#     labels["unit"] = units
#     if "jobId" in job:
#         labels["jobId"] = job["jobId"]
#     else:
#         labels["jobId"] = ""
#     if "folderId" in job:
#         labels["folderId"] = job["folderId"]
#     else:
#         labels["folderId"] = ""
#     if "numberOfRuns" in job:
#         labels["numberOfRuns"] = job["numberOfRuns"]
#     else:
#         labels["numberOfRuns"] = ""
#     if "name" in job:
#         labels["name"] = job["name"]
#     else:
#         labels["name"] = ""
#     if "folder" in job:
#         labels["folder"] = job["folder"]
#     else:
#         labels["folder"] = ""
#     if "type" in job:
#         labels["type"] = job["type"]
#     else:
#         labels["type"] = ""
#     if "status" in job:
#         labels["status"] = job["status"]
#     else:
#         labels["status"] = ""
#     if "held" in job:
#         labels["held"] = job["held"]
#     else:
#         labels["held"] = ""
#     if "deleted" in job:
#         labels["deleted"] = job["deleted"]
#     else:
#         labels["deleted"] = ""
#     if "cyclic" in job:
#         labels["cyclic"] = job["cyclic"]
#     else:
#         labels["cyclic"] = ""
#     if "startTime" in job:
#         labels["startTime"] = job["startTime"]
#         labels["startTimeEpoch"] = convert_date_string_to_epoch(startTime)
#     else:
#         labels["startTime"] = ""
#         labels["startTimeEpoch"] = 0
#     if "endTime" in job:
#         labels["endTime"] = job["endTime"]
#         labels["endTimeEpoch"] = convert_date_string_to_epoch(endTime)
#     else:
#         labels["endTime"] = ""
#         labels["endTimeEpoch"] = 0
#     if "estimatedStartTime" in job:
#         labels["estimatedStartTime"] = job["estimatedStartTime"]
#     else:
#         labels["estimatedStartTime"] = "[]"
#     if "estimatedEndTime" in job:
#         labels["estimatedEndTime"] = job["estimatedEndTime"]
#     else:
#         labels["estimatedEndTime"] = ""
#     if "orderDate" in job:
#         labels["orderDate"] = job["orderDate"]
#     else:
#         labels["orderDate"] = ""
#     if "ctm" in job:
#         labels["ctm"] = job["ctm"]
#     else:
#         labels["ctm"] = ""
#     if "description" in job:
#         labels["description"] = job["description"]
#     else:
#         labels["description"] = ""
#     if "host" in job:
#         labels["host"] = job["host"]
#     else:
#         labels["host"] = ""
#     if "application" in job:
#         labels["application"] = job["application"]
#     else:
#         labels["application"] = ""
#     if "subApplication" in job:
#         labels["subApplication"] = job["subApplication"]
#     else:
#         labels["subApplication"] = ""
#     if "outputURI" in job:
#         labels["outputURI"] = job["outputURI"]
#     else:
#         labels["outputURI"] = ""
#     if "logURI" in job:
#         labels["logURI"] = job["logURI"]
#     else:
#         labels["logURI"] = ""
#     sample = {}
#     sample['value'] = metricValue
#     sample['timestamp'] = executionTimeEpoch * 1000
#     samples.append(sample)
#     return bhommetric

def convert_date_string_to_epoch(datetimestring):
    date_format = "%Y%m%d%H%M%S"
    try: # Convert the date string to a datetime object
        date_time_obj = datetime.strptime(datetimestring, date_format)
        # Convert the datetime object to a timestamp
        #epoch_time = int(time.mktime(datetime_obj.timetuple()))
        epoch_time = date_time_obj.replace(tzinfo=timezone.utc).timestamp()
        #print("datetimestring = " + str(datetimestring))
        #print("epoch_time = " + str(epoch_time))
    except:
        return 0
    return int(epoch_time)

def parse_commandline_args():
    
    #
    # Parse command line arguments
    parser = argparse.ArgumentParser(prog="workflowinsights", usage=usage())
    parser.add_argument("-d", "--debug", action='store_true', required=False, help='Debug')
    results = parser.parse_args()
    return results

def send_to_bhom(ii_server_host, ii_server_port, bhommetrics):
    try:
        if (debug == True):
            logger.debug("Inserting metrics: " + str(bhommetrics))
        #endif
        response = bhomRestClient.insert_metric_data(bhommetrics)
        if response.status_code == 200 or response.status_code == 202:
            if (debug == True):
                logger.debug("Successfully injected metrics for host : " + ii_server_host)
            #endif
        else:
            logger.error("Error injecting metrics for host : " + ii_server_host + " with status_code = " + str(response.status_code))
            logger.error("StatusMsg = " + str(response.content).replace(",", " "))
            print ("Control-M=server-metrics,Executed=5")
            exit(5)
        #endif
        
    except Exception as e:
        logger.error("Error injecting metrics for host : " + ii_server_host + ":" + ii_server_port)
        logger.error("Exception: " + str(e))
        print ("Control-M=server-metrics,Executed=5")
        exit(5)

if __name__ == '__main__':

    pass

# Parse the command line arguments
results = parse_commandline_args()
debug = results.debug

response = {}
path = Path(__file__)
ROOT_DIR = path.parent.absolute()
try:
    config = configparser.ConfigParser()
    ROOT_DIR = path.parent.absolute()
    config_path = os.path.join(ROOT_DIR, "settings.ini")
    config.read(config_path)
    controlm_host = config.get('Settings', 'controlmhost')
    controlm_port = config.get('Settings', 'controlmport')
    controlm_api_token = config.get('Settings', 'controlmapitoken')
    bhom_server_host = config.get('Settings', 'bhomhost')
    bhom_server_port = config.get('Settings', 'bhomport')
    bhom_access_key = config.get('Settings', 'bhomaccesskey')
    bhom_secret_key = config.get('Settings', 'bhomsecretkey')
    bhom_server_tenant_id = config.get('Settings', 'bhomtenantid')
    # collection_interval = config.get('Settings', 'collectioninterval')
    chunksize = int(config.get('Settings', 'chunksize'))
    # start_time = datetime.now(timezone.utc) - timedelta(minutes = int(collection_interval))
    # start_time = start_time.strftime("%Y%m%d%H%M%S")
    # end_time = datetime.now(timezone.utc)
    # end_time = end_time.strftime("%Y%m%d%H%M%S")
except Exception as e:
    print ("Control-M=server-metrics,Executed=1")
    exit(1)

logger_conf_file = os.path.join(ROOT_DIR, "logger.conf")
if (not os.path.exists(logger_conf_file)):
    print ("Control-M=server-metrics,Executed=2")
    exit(2)
#
# Configure Logging
logging.config.fileConfig(logger_conf_file)
logger = logging.getLogger(__name__)
if (debug == True):
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
#endif

bhomRestClient = BHOMRestClient(bhom_server_host, bhom_server_port, bhom_access_key, bhom_secret_key, bhom_server_tenant_id, None)
try:
    if (debug == True):
        logger.debug("Logging into BHOM server: " + bhom_server_host + ":" + bhom_server_port)
    #endif
    response = bhomRestClient.login(None)
   
except Exception as e:
    logger.error("Error occurred connecting to BHOM server: " + bhom_server_host + ":" + bhom_server_port)
    logger.error("Exception: " + str(e))
    print ("Control-M=server-metrics,Executed=3")
    exit(3)

controlmRestClient = CTRLMRestClient(controlm_host, controlm_port, controlm_api_token, logger)

try:
    if (debug == True):
        logger.debug("Getting Automation API Server Status  from Control-M server: "+ controlm_host  + ":" + controlm_port)
    #endif
    response = controlmRestClient.get_automation_api_server_status(logger)
    if response.status_code == controlmRestClient.OK_STATUS:
        if (debug == True):
            logger.debug("Automation API server status returned by Control-M: " + response.text)
        automationapi_server_status = response.text
    else:
        logger.error("Error getting automation server status from Control-M server: " + controlm_host + ":" + controlm_port)
        logger.error("Exception: " + str(e))
        print ("Control-M=server-metrics,Executed=4")
        exit(4)
except Exception as e:
    logger.error("Error getting automation server status from Control-M server: " + controlm_host + ":" + controlm_port)
    logger.error("Exception: " + str(e))
    print ("Control-M=server-metrics,Executed=4")
    exit(4)

try:
    if (debug == True):
        logger.debug("Getting Server list  from Control-M server: "+ controlm_host  + ":" + controlm_port)
    #endif
    response = controlmRestClient.get_controlm_servers(logger)
    if response.status_code == controlmRestClient.OK_STATUS:
        if (debug == True):
            logger.debug("Server List Returned by Control-M: " + response.text)
        controlm_servers = json.loads(response.text)
        if (len(controlm_servers) < 1):
            logger.debug("Control-M=server-metrics no servers returned, completed successfully.")
            print ("Control-M=server-metrics,Executed=5")
            exit(5)
        #endif
    else:
        logger.error("Error getting Server list from Control-M server: " + controlm_host + ":" + controlm_port)
        logger.error("Exception: " + str(e))
        print ("Control-M=server-metrics,Executed=5")
        exit(5)
except Exception as e:
    logger.error("Error getting Server list from Control-M server: " + controlm_host + ":" + controlm_port)
    logger.error("Exception: " + str(e))
    print ("Control-M=server-metrics,Executed=5")
    exit(5)

try:
    hostgroupdict = {}
    for controlm_server in controlm_servers:
        hostgrouphost = controlm_server["name"]
        if (debug == True):
            logger.debug("Getting host groups from Control-M server server " + hostgrouphost)
        #endif
        response = controlmRestClient.get_controlm_host_groups(hostgrouphost, logger)
        if response.status_code == controlmRestClient.OK_STATUS:
            if (debug == True):
                logger.debug("Host group list returned by Control-M: " + response.text)
            hostgroups = json.loads(response.text)
            if (len(hostgroups) < 1):
                logger.debug("Control-M=server-metrics no host groups returned for server: " + hostgrouphost + ", completed successfully.")
            #endif
        else:
            logger.error("Error getting host group list from Control-M server: " + controlm_host + ":" + controlm_port)
            logger.error("Exception: " + str(e))
            print ("Control-M=server-metrics,Executed=6")
            exit(6)
        hostgroupdict[controlm_server["name"]] = hostgroups
    #endfor
except Exception as e:
    logger.error("Error getting host group list from Control-M server: " + controlm_host + ":" + controlm_port)
    logger.error("Exception: " + str(e))
    print ("Control-M=server-metrics,Executed=6")
    exit(6)

try:
    controlmagentdict = {}
    for controlm_server in controlm_servers:
        agentshost = controlm_server["name"]
        if (debug == True):
            logger.debug("Getting agent info from Control-M server server " + hostgrouphost)
        #endif
        response = controlmRestClient.get_controlm_server_agents(agentshost,logger)
        if response.status_code == controlmRestClient.OK_STATUS:
            if (debug == True):
                logger.debug("Agent list returned by Control-M: " + response.text)
            agentdict = json.loads(response.text)
            if (len(agentdict) < 1):
                logger.debug("Control-M=server-metrics 0 agents returned for server.")
            #endif
        else:
            logger.error("Error getting agent list from Control-M server: " + controlm_host + ":" + controlm_port)
            logger.error("Exception: " + str(e))
            print ("Control-M=server-metrics,Executed=7")
            exit(7)
        controlmagentdict[controlm_server["name"]] = agentdict
    #endfor
except Exception as e:
    logger.error("Error getting agents list from Control-M server: " + controlm_host + ":" + controlm_port)
    logger.error("Exception: " + str(e))
    print ("Control-M=server-metrics,Executed=7")
    exit(7)

try:
    if (debug == True):
        logger.debug("Getting workflow insights status from Control-M server: "+ controlm_host  + ":" + controlm_port)
    #endif
    response = controlmRestClient.get_workflow_insights_status(logger)
    if response.status_code == controlmRestClient.OK_STATUS:
        if (debug == True):
            logger.debug("Workflow insights status returned by Control-M: " + response.text)
        workflow_insights_topology = json.loads(response.text)
    else:
        logger.error("Error getting workflow insights status from Control-M server: " + controlm_host + ":" + controlm_port)
        logger.error("Exception: " + str(e))
        print ("Control-M=server-metrics,Executed=8")
        exit(8)
except Exception as e:
    logger.error("Error getting workflow insights status from Control-M server: " + controlm_host + ":" + controlm_port)
    logger.error("Exception: " + str(e))
    print ("Control-M=server-metrics,Executed=8")
    exit(8)
    
try:
    if (debug == True):
        logger.debug("Getting SLA services from Control-M server: "+ controlm_host  + ":" + controlm_port)
    #endif
    response = controlmRestClient.get_sla_services(logger)
    if response.status_code == controlmRestClient.OK_STATUS:
        if (debug == True):
            logger.debug("SLA services returned by Control-M: " + response.text)
        sla_services = json.loads(response.text)
        if (len(sla_services) < 1):
            logger.debug("Control-M=server-metrics no sla services returned, completed successfully.")
            print ("Control-M=server-metrics,Executed=9")
            exit(9)
        #endif
    else:
        logger.error("Error getting SLA services from Control-M server: " + controlm_host + ":" + controlm_port)
        logger.error("Exception: " + str(e))
        print ("Control-M=server-metrics,Executed=9")
        exit(9)
except Exception as e:
    logger.error("Error getting SLA services from Control-M server: " + controlm_host + ":" + controlm_port)
    logger.error("Exception: " + str(e))
    print ("Control-M=server-metrics,Executed=9")
    exit(9)

# try:
#     if (debug == True):
#         logger.debug("Getting Job Statuses from Control-M server from start time: + " + start_time + " to: " + end_time + " " + controlm_host  + ":" + controlm_port)
#     #endif
#     response = controlmRestClient.get_controlm_job_statuses(start_time, end_time, logger)
#     if response.status_code == controlmRestClient.OK_STATUS:
#         if (debug == True):
#             logger.debug("Job List Returned by Control-M: " + response.text)
#         job_statuses_json = json.loads(response.text)
#         if (job_statuses_json["returned"] == 0):
#             logger.debug("Control-M=workflow-insights-metrics injection no job data matched the criteria, completed successfully.")
#             print ("Control-M=workflow-insights-metrics,Executed=0")
#             exit(0)
#         else:
#             job_statuses = job_statuses_json['statuses']
#     else:
#         logger.error("Error getting Job Statuses from Control-M server: " + controlm_host + ":" + controlm_port)
#         logger.error("Exception: " + str(e))
#         print ("Control-M=workflow-insights-metrics,Executed=4")
#         exit(4)
# except Exception as e:
#     logger.error("Error getting Job Statuses from Control-M server: " + controlm_host + ":" + controlm_port)
#     logger.error("Exception: " + str(e))
#     print ("Control-M=workflow-insights-metrics,Executed=4")
#     exit(4)
    
timestamp = round(time.time()*1000)
bhommetrics = []
source = "Control-M"
entitytypeid = "CTRLM_SERVER"
for controlm_server in controlm_servers:
    logger.debug("Processing Control-M Server Info : " + controlm_server["name"] + ".")
    metricname = "State"
    entityname = controlm_server['name']
    instancename = controlm_server['name']
    hostname = controlm_server['host']
    metricvalue = convert_server_state(controlm_server["state"])
    units = "0 = Up, 1 = Down"
    iskpi = False
    bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    metricvalue = len(hostgroupdict[controlm_server["name"]])
    metricname = "NumHostGroups"
    units = "#"
    bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    
    entitytypeid = "CTRLM_AGENT"
    agentlist = controlmagentdict[controlm_server["name"]]
    agentsavailable = 0
    agentsunavailable = 0
    agentsdisabled = 0
    agentsdisable = 0
    agentsdiscovering = 0
    for agent in agentlist["agents"]:
        agentlabels = {}
        metricname = "Status"
        entityname = controlm_server["name"] + "_Agent"
        instancename = controlm_server["name"] + "_Agent"
        hostname = agent['nodeid']
        metricvalue = convert_agent_status(agent["status"])
        if metricvalue == 0:
            agentsavailable = agentsavailable + 1
        elif metricvalue == 1:
            agentsunavailable = agentsunavailable + 1
        elif metricvalue == 2:
            agentsdisabled = agentsdisabled + 1
        elif metricvalue == 3:
            agentsdisable = agentsdisable + 1
        elif metricvalue == 4:
            agentsdiscovering = agentsdiscovering + 1
        #endif
        if "operatingSystem" in agent:
            agentlabels["operatingSystem"] = agent["operatingSystem"]
        if "hostGroups" in agent:
            agentlabels["hostGroups"] = agent["hostGroups"]
        if "version" in agent:
            agentlabels["version"] = agent["version"]
        if "tag" in agent:
            agentlabels["tag"] = agent["tag"]
        if "type" in agent:
            agentlabels["type"] = agent["type"]
        units = "0 = Available, 1 = Unavailable, 2 = Disabled, 3 = Disable, 4 = Discovering"
        iskpi = False
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, agentlabels)
    #endfor
    
    source = "Control-M"
    entitytypeid = "CTRLM_SERVER"
    metricname = "AgentsAvailable"
    entityname = controlm_server['name']
    instancename = controlm_server['name']
    hostname = controlm_server['host']
    metricvalue = agentsavailable
    units = "#"
    iskpi = True
    bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    
    metricname = "AgentsUnavailable"
    entityname = controlm_server['name']
    instancename = controlm_server['name']
    hostname = controlm_server['host']
    metricvalue = agentsunavailable
    units = "#"
    iskpi = True
    bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    
    metricname = "AgentsDisabled"
    entityname = controlm_server['name']
    instancename = controlm_server['name']
    hostname = controlm_server['host']
    metricvalue = agentsdisabled
    units = "#"
    iskpi = True
    bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    
    metricname = "AgentsDisable"
    entityname = controlm_server['name']
    instancename = controlm_server['name']
    hostname = controlm_server['host']
    metricvalue = agentsdisable
    units = "#"
    iskpi = True
    bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    
    metricname = "AgentsDiscovering"
    entityname = controlm_server['name']
    instancename = controlm_server['name']
    hostname = controlm_server['host']
    metricvalue = agentsdiscovering
    units = "#"
    iskpi = True
    bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    
    activeservicescount = len(sla_services["activeServices"])
    activeservicelist = sla_services["activeServices"]
    metricname = "ActiveServices"
    entityname = controlm_server['name']
    instancename = controlm_server['name']
    hostname = controlm_server['host']
    metricvalue = activeservicescount
    units = "#"
    iskpi = True
    bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    
    for service in activeservicelist:
        entitytypeid = "CTRLM_SLA_SERVICE"
        metricname = "Status"
        entityname = service['serviceControlM'] + "_" +service['serviceName']
        instancename = service['serviceControlM'] + "_" +service['serviceName']
        hostname = controlm_server['host']
        metricvalue = convert_sla_service_status(service['status'])
        units = units = "0 = OK, 1 = Completed OK, 2 = Completed Late, 3 = Not OK"
        iskpi = False
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
        
        metricname = "CompletionPercentage"
        metricvalue = int(service['completionPercentage'])
        units = "%"
        iskpi = True
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
        
        metricname = "JobStatusExecuted"
        metricvalue = int(service['statusByJobs']['executed'])
        units = "#"
        iskpi = True
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
        
        metricname = "JobStatusWait"
        metricvalue = int(service['statusByJobs']['waitCondition'])
        units = "#"
        iskpi = True
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
        
        metricname = "JobStatusWaitResource"
        metricvalue = int(service['statusByJobs']['waitResource'])
        units = "#"
        iskpi = True
        bhommetric = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
        
        metricname = "JobStatusWaitHost"
        metricvalue = int(service['statusByJobs']['waitHost'])
        units = "#"
        iskpi = True
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
        
        metricname = "JobStatusWaitWorkLoad"
        metricvalue = int(service['statusByJobs']['waitWorkload'])
        units = "#"
        iskpi = True
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
        
        metricname = "JobStatusCompleted"
        metricvalue = int(service['statusByJobs']['completed'])
        units = "#"
        iskpi = True
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
        
        metricname = "JobStatusError"
        metricvalue = int(service['statusByJobs']['error'])
        units = "#"
        iskpi = True
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
#endfor

workflow_insights_server = workflow_insights_topology["topology"][0]["clusters"][0]
nodes = workflow_insights_server["nodes"]
performance_list = workflow_insights_server["performance"]
entitytypeid = "CTRLM_WFI_SERVER"
entityname = "WFI_Server"
instancename = "WFI_Server"
for performance_metric in performance_list:
    metricname = performance_metric["name"]
    metricname = metricname.replace(" ", "")
    metricvalue = performance_metric["value"]
    hostname = controlm_server['host']
    units = "#"
    iskpi = True
    bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
#endfor
status = workflow_insights_server["availability"][0]["status"]
metricname = "Status"
metricvalue = convert_wfi_server_status(status)
hostname = controlm_server['host']
units = "0 = OK, 1 = Unavailable"
iskpi = False
bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])

entitytypeid = "CTRLM_WFI_NODE"
for node in nodes:
    entityname = "WFI_Node"
    instancename = "WFI_Node"
    metricname = "NodeStatus"
    hostname = node["name"]
    metricvalue = convert_wfi_server_status(node["availability"][0]["status"])
    units = "0 = OK, 1 = Unavailable"
    iskpi = False
    bhommetrics = add_metric(bhommetric, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    performance_list = node["performance"]
    for performance_metric in performance_list:
        metricname = performance_metric["name"]
        metricname = metricname.replace(" ", "")
        metricvalue = performance_metric["value"]
        units = performance_metric["unit"]
        iskpi = True
        bhommetrics = add_metric(bhommetrics, entitytypeid, metricname, hostname, entityname, instancename, source, units, timestamp, metricvalue, iskpi, [])
    #endfor
#endfor
# for job in job_statuses:
#     logger.debug("Processing Job : " + job["name"] + " " + job["type"] + " " + job["status"] + " " + job["host"] + ".")
#     if job["type"] != "Job" and job["type"] != "Command":
#         logger.debug("The Job : " + job["name"] + " " + job["type"] + " is not a job, skipping.")
#         continue
#     #endif
#     status_string = job["status"]
#     status = convert_status(status_string)
#     startTime = job["startTime"]
#     endTime = str(job["endTime"])
#
#     if (status < 0):
#
#         logger.debug("The Job : " + job['name'] + " is still executing, skipping.")
#         continue
#
#     # if (endTime == "" ):
#     #     logger.debug("The Job : " + job['name'] + " has no end time, skipping.")
#     #     continue
#     if (startTime == ""):
#         logger.debug("The Job : " + job['name'] + " has no start time, skipping.")
#         continue
#
#     executionTime = calculate_execution_time(startTime, endTime)
#     bhommetric = add_metric(job, "ExecutionTime", executionTime, "seconds")
#     bhommetrics.append(bhommetric)
#     bhommetric = add_metric(job, "Status", status, "0 = Ended OK, 1 = Failed")
#     bhommetrics.append(bhommetric)
#     logger.debug("Adding Job : " + job['name'])

if (len(bhommetrics)  > 0):
    send_to_bhom(bhom_server_host, bhom_server_port, bhommetrics)
#endif

logger.debug("Control-M=workflow-insights-metrics injection completed successfully.")
print ("Control-M=workflow-insights-metrics,Executed=0")
exit(0)
