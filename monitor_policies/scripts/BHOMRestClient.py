'''

Created on Feb 11, 2019



@author: JFOURNET

'''

import requests
import logging
import urllib3
import json

class BHOMRestClient(object):

    '''

    classdocs

    '''

    LOGIN_URL = "/ims/api/v1/access_keys/login"
    GET_JWT_TOKEN_URL = "/ims/api/v1/auth/tokens"
    INSERT_METRIC_DATA_URL = "/metrics-gateway-service/api/v1.0/insert"
    OK_STATUS = "200"
    ALARM_POLICY_OK_STATUS = "OK"

    def __init__(self, host=None, port=None, accesskey=None, secretkey=None, tenant=None, proxy=None, logger=None):

        '''

        Constructor

        '''

        self.__host = host

        self.__port = port
        
        self.__proxy = proxy

        self.__accesskey = accesskey
        
        self.__secretkey = secretkey
        
        self.__jwttoken = None

        self.__tenant = tenant

        self.__logger = logger

        self.__logger = logging.getLogger(__name__)
        
    
    def login(self, logger):
        
        urllib3.disable_warnings()
        # Build the URL for the authentication request.
        url = "https://" + self.__host + ":" + self.__port + self.LOGIN_URL
        # Define the body of the authentication request.
        auth_body = {
                        "access_key": self.__accesskey,
                        "access_secret_key": self.__secretkey,
                        "tenant_id": self.__tenant
                    }
        headers = {
                    "content-type": "application/json",
                    "cache-control": "no-cache",
                    "Connection": "keep-alive"
                   }
        try:
            # Issue the request and capture the response object.
            r = requests.post(url, json=auth_body, headers=headers, proxies=self.__proxy, verify=False)
        except Exception as e:
            # Handle connection errors.
            raise Exception("Unable to get access token from host: " + self.__host + ":" + self.__port + "Exception: " + str(e))
        # endtry
        # Capture the authentication token
        if (r.status_code == 200):
            # Convert the body of the response to a dict
            response = r.json()
            # Retrieve the token from the response sub-dict of json_data.
            key_token = response['token']
        else:
            logger.error("Authentication Failure - invalid access key or secret key.  response code: " + str(r.text))
            raise Exception("Authentication Failure - invalid access key or secret key.  response code: " + str(r.text))
        # end of get_auth_token() function

        # Get the json web token token.  
        # Build the URL for the authentication request.
        url = "https://" + self.__host + ":" + self.__port + self.GET_JWT_TOKEN_URL
        # Define the body of the authentication request.
        auth_body = {"token": key_token}
        headers = {"content-type": "application/json",
                   "cache-control": "no-cache",
                   }
        try:
            # Issue the request and capture the response object.
            r = requests.post(url, json=auth_body, headers=headers, proxies=self.__proxy, verify=False)
        except requests.exceptions.ConnectionError:
            # Handle connection errors.
            raise("Unable to get jwt token from host: " + self.__host + ":" + self.__port + "Exception: " + str(e))
        # endtry
        # Capture the authentication token
        if (r.status_code == 200):
            # Convert the body of the response to a dict
            response = r.json()
            # Retrieve the JWT token from the response sub-dict of json_data.
            jwt_token = response['json_web_token']
            self.__jwttoken = jwt_token
            return response
        else:
            raise Exception("Authentication Failure - response code: " + str(r))
        # end of get_jwt_token() function


    def insert_metric_data(self, metric_json):
    
        urllib3.disable_warnings()
        command = "https://" + self.__host + ":" + self.__port + self.INSERT_METRIC_DATA_URL
        headers = {'Authorization':'Bearer ' + self.__jwttoken, 'content-type':'application/json', "Connection": "keep-alive", "MetricType":"RAW"}
        metric_json = json.dumps(metric_json)
        result = requests.post(command, data=metric_json, headers=headers, verify=False, timeout=60)
        return result