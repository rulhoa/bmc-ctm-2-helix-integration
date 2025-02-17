import json
import os
from pathlib import Path
import argparse
import configparser
from datetime import datetime, timezone, timedelta
import logging.config
from BHOMRestClient import BHOMRestClient
'''
Created on May 22, 2024

@author: jfournet
'''

if __name__ == '__main__':
    pass

def usage():
    usage_msg = "workflowinsights [-h --help] -d [debug]"
    return usage_msg

def convert_status(status_string):
    if (status_string == "EndedNotOk"):
        return 1
    elif (status_string == "EndedOK"):
        return 0
    else:
        return -1
    
def calculate_execution_time(startTime, endTime):
    if startTime != "" and endTime != "":
        executionTime = endTime - startTime
        return executionTime/1000
    else:
        return 0
    
def add_metric(entityTypeId, source, job, metricName, metricValue, units):
    bhommetric = {}
    labels = {}
    samples = []
    logger.debug("Adding Metric: " + metricName)
    entityName = job['JobName']
    if job['Host'] != "":
        hostName = job['Host']
    else:
        hostName = job['Server']
    #endif    
    bhommetric["labels"] = labels
    bhommetric["samples"] = samples
    labels["metricName"] = metricName
    labels["hostname"] = hostName
    labels["source"] = source
    labels["entityId"] = source + ":" + hostName + ":" + entityTypeId + ":" + entityName
    labels["entityTypeId"] = entityTypeId
    labels["instanceName"] = entityName
    labels["entityName"] = entityName
    labels["hostType"] = "Server"
    if (metricName == "EndedStatus"):
        labels["isKpi"] = False
    else:
        labels["isKpi"] = True
    labels["isDeviceMappingEnabled"] = True
    labels["unit"] = units
    if "Id" in job:
        labels["jobId"] = job["Id"]
    else:
        labels["jobId"] = ""
    if "RunCount" in job:
        labels["numberOfRuns"] = job["RunCount"]
    else:
        labels["numberOfRuns"] = ""
    if "JobName" in job:
        labels["name"] = job["JobName"]
    else:
        labels["name"] = ""
    if "Folder" in job:
        labels["folder"] = job["Folder"]
    else:
        labels["folder"] = ""
    if "JobType" in job:
        labels["type"] = job["JobType"]
    else:
        labels["type"] = ""
    if "EndedStatus" in job:
        labels["status"] = job["EndedStatus"]
    else:
        labels["status"] = ""
    if "IsCyclic" in job:
        labels["cyclic"] = job["IsCyclic"]
    else:
        labels["cyclic"] = ""
    if "StartTime" in job:
        labels["startTime"] = job["StartTime"]
    else:
        labels["startTime"] = 0
    if "EndTime" in job:
        labels["endTime"] = job["EndTime"]
    else:
        labels["endTime"] = ""
    if "Server" in job:
        labels["ctm"] = job["Server"]
    else:
        labels["ctm"] = ""
    if "Host" in job:
        if job["Host"] == "":
            labels["host"] = job["Server"]
        else:
            labels["host"] = job["Host"]
        #endif
    else:
        labels["host"] = job["Server"]
    #endif
    if "Application" in job:
        labels["application"] = job["Application"]
    else:
        labels["application"] = ""
    if "SubApplication" in job:
        labels["subApplication"] = job["SubApplication"]
    else:
        labels["subApplication"] = ""
    if "indexedTime" in job:
        labels["indexedTime"] = job["indexedTime"]
    else:
        labels["indexedTime"] = ""
    if "TaskType" in job:
        labels["taskType"] = job["TaskType"]
    else:
        labels["taskType"] = ""
    if "RunId" in job:
        labels["runId"] = job["RunId"]
    else:
        labels["runId"] = ""
    if "ExecutionEndHour" in job:
        labels["executionEndHour"] = job["ExecutionEndHour"]
    else:
        labels["executionEndHour"] = ""
    if "RunDay" in job:
        labels["RunDay"] = job["RunDay"]
    else:
        labels["RunDay"] = ""
    if "ExecutedDay" in job:
        labels["executedDay"] = job["ExecutedDay"]
    else:
        labels["executedDay"] = ""
    if "ExecutionStartHour" in job:
        labels["executionStartHour"] = job["ExecutionStartHour"]
    else:
        labels["executionStartHour"] = ""
    if "RunDate" in job:
        labels["runDate"] = job["RunDate"]
    else:
        labels["runDate"] = ""
    if "Services" in job:
        labels["Services"] = job["Services"]
    else:
        labels["Services"] = ""
    if "Services" in job:
        labels["Services"] = job["Services"]
    else:
        labels["Services"] = ""
    sample = {}
    sample['value'] = metricValue
    sample['timestamp'] = endTime
    samples.append(sample)
    return bhommetric

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
            print ("Control-M=workflow-insights-metrics,Executed=5")
            exit(5)
        #endif
        
    except Exception as e:
        logger.error("Error retrieving injecting metrics for host : " + ii_server_host + ":" + ii_server_port)
        logger.error("Exception: " + str(e))
        print ("Control-M=workflow-insights-metrics,Executed=5")
        exit(5)
        
        
def get_files_modified_within_time_range(directory, time_threshold, file_prefix, logger):
    files_in_time_range = []
    logger.debug("Searching directory: " + directory + " for files with the prefix: " + file_prefix)
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            logger.debug("file_modified_time = " + str(file_modified_time.timestamp()) + "  time_threshold = " + str(time_threshold.timestamp()))
            if file_modified_time.timestamp() > time_threshold.timestamp() and file.startswith(file_prefix):
                logger.debug("Adding workflow insights file: " + file_path + " to list for processing.")
                files_in_time_range.append(file_path)
            else:
                logger.debug("Workflow insights file: " + file_path + " has not been modified since the last discovery cycle, skipping.")
            #endif
    #endfor
    return files_in_time_range

def read_workflow_metrics_from_files(files, time_threshold, logger):
    metric_list = []
    for file_path in files:
        with open(file_path, 'r') as file:
            for line in file:
                try:
                    metric_object = json.loads(line.strip())
                    logger.debug("Job End Time = " + str(metric_object["EndTime"]) + "  time_threshold = " + str(time_threshold.timestamp() * 1000))
                    if metric_object["EndTime"] > time_threshold.timestamp() * 1000:
                        logger.debug("Adding metrics for Job: " + metric_object["JobName"] + " to list for processing.")
                        metric_list.append(metric_object)
                    else:
                        logger.debug("Job: " + metric_object["JobName"] + " did not end in this discovery cycle, skipping.")
                    #endif
                except json.JSONDecodeError:
                    logger.debug("Skipping invalid JSON string in file: {file_path}")

    return metric_list

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
    workflowinsightsfiledir = config.get('Settings', 'workflowinsightsfiledir')
    bhom_server_host = config.get('Settings', 'bhomhost')
    bhom_server_port = config.get('Settings', 'bhomport')
    bhom_access_key = config.get('Settings', 'bhomaccesskey')
    bhom_secret_key = config.get('Settings', 'bhomsecretkey')
    bhom_server_tenant_id = config.get('Settings', 'bhomtenantid')
    collection_interval = int(config.get('Settings', 'workflowinsightscollectioninterval'))
    chunksize = int(config.get('Settings', 'chunksize'))
    start_time = datetime.now(timezone.utc) - timedelta(minutes = int(collection_interval))
    start_time = start_time.strftime("%Y%m%d%H%M%S")
    end_time = datetime.now(timezone.utc)
    end_time = end_time.strftime("%Y%m%d%H%M%S")
except Exception as e:
    print ("Control-M=workflow-insights-metrics,Executed=1")
    exit(1)

logger_conf_file = os.path.join(ROOT_DIR, "logger.conf")
if (not os.path.exists(logger_conf_file)):
    print ("Control-M=workflow-insights-metrics,Executed=2")
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
    print ("Control-M=workflow-insights-metrics,Executed=3")
    exit(3)
  
try:
    # Get the list of files modified within the time range
    if (debug == True):
        logger.debug("Getting files from Control-M server from start time: + " + start_time + " to: " + end_time)
    current_time = datetime.now()
    time_threshold = current_time - timedelta(minutes=collection_interval)
    workflowinsightfiles = get_files_modified_within_time_range(workflowinsightsfiledir, time_threshold, "ctm_job_execution", logger)

    # Read JSON strings from these files and store them in a list
    job_metric_list = read_workflow_metrics_from_files(workflowinsightfiles, time_threshold, logger)
except Exception as e:
    logger.error("Error getting Job metrics from Control-M server")
    logger.error("Exception: " + str(e))
    print ("Control-M=workflow-insights-metrics,Executed=4")
    exit(4)
    
bhommetrics = []
entityTypeId = "CTRLM_JOB"
source = "Control-M"
logger.debug(str(job_metric_list))
logger.debug("Job Count: " + str(len(job_metric_list)))
for job in job_metric_list:
    logger.debug("Processing Job : " + job["JobName"] + " " + job["JobType"] + " " + job["TaskType"] + " " + job["EndedStatus"] + " " + job["Host"] + ".")
    status_string = job["EndedStatus"]
    status = convert_status(status_string)
    startTime = job["StartTime"]
    endTime = job["EndTime"]
    logger.debug("Before Check Status")
    if (status < 0):
    
        logger.debug("The Job : " + job['JobName'] + " is still executing, skipping.")
        continue
    logger.debug("after check status")
    executionTime = calculate_execution_time(startTime, endTime)
    logger.debug("After Calculate Execution Time")
    bhommetric = add_metric(entityTypeId, source, job, "ExecutionTime", executionTime, "seconds")
    bhommetrics.append(bhommetric)
    bhommetric = add_metric(entityTypeId, source, job, "Status", status, "0 = Ended OK, 1 = Failed")
    bhommetrics.append(bhommetric)
    logger.debug("Adding Job : " + job['JobName'])
    if (len(bhommetrics) > chunksize):
        send_to_bhom(bhom_server_host, bhom_server_port, bhommetrics)
        bhommetrics = []
        continue
    #endif
#endfor
if (len(bhommetrics)  > 0):
    send_to_bhom(bhom_server_host, bhom_server_port, bhommetrics)
#endif

logger.debug("Control-M=workflow-insights-metrics injection completed successfully.")
print ("Control-M=workflow-insights-metrics,Executed=0")
exit(0)
