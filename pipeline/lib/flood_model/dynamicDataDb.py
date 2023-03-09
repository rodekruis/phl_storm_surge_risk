import pandas as pd
import requests
import json
from flood_model.settings import *
try:
    from flood_model.secrets import *
except ImportError:
    print('No secrets file found.')
import os
import numpy as np
import logging
logger = logging.getLogger(__name__)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry 
 

    
class DatabaseManager:

    """ Class to upload and process data in the database """

    def __init__(self, leadTimeLabel, countryCodeISO3,admin_level):
        self.countryCodeISO3 = countryCodeISO3
        self.leadTimeLabel = leadTimeLabel
        self.triggerFolder = PIPELINE_OUTPUT + "triggers_rp_per_station/"
        self.affectedFolder = PIPELINE_OUTPUT + "calculated_affected/"
        self.EXPOSURE_DATA_SOURCES = SETTINGS[countryCodeISO3]['EXPOSURE_DATA_SOURCES']
        #self.API_SERVICE_URL = SETTINGS[countryCodeISO3]['IBF_API_URL']
        #self.ADMIN_PASSWORD = SETTINGS[countryCodeISO3]['PASSWORD']
        self.levels = SETTINGS[countryCodeISO3]['levels']
        self.admin_level = admin_level

    def upload(self):
        self.uploadTriggersPerLeadTime()
        self.uploadTriggerPerStation()
        self.uploadCalculatedAffected()
        self.uploadRasterFile()
    
    def sendNotification(self):
        leadTimes = SETTINGS[self.countryCodeISO3]['lead_times']
        max_leadTime = max(leadTimes, key=leadTimes.get)

        if SETTINGS[self.countryCodeISO3]["notify_email"] and self.leadTimeLabel == max_leadTime:
            body = {
                'countryCodeISO3': self.countryCodeISO3,
                'disasterType': self.getDisasterType()
            } 
            self.apiPostRequest('notification/send', body=body)

    
    def getDisasterType(self):
        disasterType = 'floods'
        return disasterType
    def uploadCalculatedAffected2(self):
        for indicator, values in self.EXPOSURE_DATA_SOURCES.items():
            adminlevels=self.admin_level
            with open(self.affectedFolder +
                      'affected_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + '_admin_' + str(adminlevels) + '_' + indicator + '.json') as json_file:
                body = json.load(json_file)
                body['disasterType'] = self.getDisasterType()
                #body['adminLevel'] = self.admin_level
                self.apiPostRequest('admin-area-dynamic-data/exposure', body=body)
            logger.info(f'Uploaded calculated_affected for indicator: {indicator} for admin level: ' + str(adminlevels))
            if indicator == 'population':
                with open(self.affectedFolder +
                        'affected_' + self.leadTimeLabel + '_' + self.countryCodeISO3  + '_admin_' + str(adminlevels) + '_' + 'population_affected_percentage' + '.json') as json_file:
                    body = json.load(json_file)
                    body['disasterType'] = self.getDisasterType()
                    #body['adminLevel'] = self.admin_level
                    self.apiPostRequest('admin-area-dynamic-data/exposure', body=body)
                logger.info('Uploaded calculated_affected for indicator: ' + 'population_affected_percentage for admin level: ' + str(adminlevels))

    def uploadCalculatedAffected(self):
        for adminlevels in SETTINGS[self.countryCodeISO3]['levels']:#range(1,self.admin_level+1):            
            for indicator, values in self.EXPOSURE_DATA_SOURCES.items():
                #with open(self.affectedFolder +
                #          'affected_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + '_admin_' + str(adminlevels) + '_' + indicator + '.json') as json_file:
                #    body = json.load(json_file)
                #    body['disasterType'] = self.getDisasterType()
                #    #body['adminLevel'] = self.admin_level
                #    self.apiPostRequest('admin-area-dynamic-data/exposure', body=body)
                #print('Uploaded calculated_affected for indicator: ' + indicator +'for admin level: ' + str(adminlevels))
                if indicator == 'population':
                    with open(self.affectedFolder +
                            'affected_' + self.leadTimeLabel + '_' + self.countryCodeISO3  + '_admin_' + str(adminlevels) + '_' + 'population_affected_percentage' + '.json') as json_file:
                        body = json.load(json_file)
                        body['disasterType'] = self.getDisasterType()
                        #body['adminLevel'] = self.admin_level
                        self.apiPostRequest('admin-area-dynamic-data/exposure', body=body)
                    logger.info('Uploaded calculated_affected for indicator: ' + 'population_affected_percentage for admin level: ' + str(adminlevels))
                    with open(self.affectedFolder+'affected_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + '_admin_' + str(adminlevels) + '_' + indicator + '.json') as json_file:
                        body = json.load(json_file)
                        body['disasterType'] = self.getDisasterType()
                        #body['adminLevel'] = self.admin_level
                        self.apiPostRequest('admin-area-dynamic-data/exposure', body=body)
                    logger.info(f'Uploaded calculated_affected for indicator: {indicator}' +'for admin level: ' + str(adminlevels))
                    indicator2 = 'alert_threshold'
                    with open(self.affectedFolder +
                                'affected_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + '_admin_' + str(adminlevels) + '_' + indicator2 + '.json') as json_file:
                        body = json.load(json_file)
                        body['disasterType'] = self.getDisasterType()
                        self.apiPostRequest('admin-area-dynamic-data/exposure', body=body)
                    logger.info(f'Uploaded calculated_affected for indicator: {indicator2}' +'for admin level: ' + str(adminlevels))
                else:
                    with open(self.affectedFolder +'affected_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + '_admin_' + str(adminlevels) + '_' + indicator + '.json') as json_file:
                        body = json.load(json_file)
                        body['disasterType'] = self.getDisasterType()
                        #body['adminLevel'] = self.admin_level
                        self.apiPostRequest('admin-area-dynamic-data/exposure', body=body)
                    logger.info(f'Uploaded calculated_affected for indicator: {indicator}' +'for admin level: ' + str(adminlevels))
                                    
                    

        # for indicator, values in self.EXPOSURE_DATA_SOURCES.items():
        #     with open(self.affectedFolder +
        #               'affected_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + '_' + indicator + '.json') as json_file:
        #         body = json.load(json_file)
        #         body['disasterType'] = self.getDisasterType()
        #         #body['adminLevel'] = self.admin_level
        #         self.apiPostRequest('admin-area-dynamic-data/exposure', body=body)
        #     print('Uploaded calculated_affected for indicator: ' + indicator)
        #     if indicator == 'population':
        #         with open(self.affectedFolder +
        #                 'affected_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + '_' + 'population_affected_percentage' + '.json') as json_file:
        #             body = json.load(json_file)
        #             body['disasterType'] = self.getDisasterType()
        #             #body['adminLevel'] = self.admin_level
        #             self.apiPostRequest('admin-area-dynamic-data/exposure', body=body)
        #         print('Uploaded calculated_affected for indicator: ' + 'population_affected_percentage')


    def uploadRasterFile(self):
        disasterType = self.getDisasterType()
        rasterFile = RASTER_OUTPUT + '0/flood_extents/flood_extent_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + '.tif'
        files = {'file': open(rasterFile,'rb')}
        self.apiPostRequest('admin-area-dynamic-data/raster/' + disasterType, files=files)
        logger.info(f'Uploaded raster-file: {rasterFile}')


    def uploadTriggerPerStation(self):
        df = pd.read_json(self.triggerFolder +
                          'triggers_rp_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + ".json", orient='records')
        dfStation = pd.DataFrame(index=df.index)
        dfStation['stationCode'] = df['stationCode']
        dfStation['forecastLevel'] = df['fc'].astype(np.float64,errors='ignore')
        dfStation['forecastProbability'] = df['fc_prob'].astype(np.float64,errors='ignore')
        dfStation['forecastTrigger'] = df['fc_trigger'].astype(np.int32,errors='ignore')
        dfStation['forecastReturnPeriod'] = df['fc_rp'].astype(np.int32,errors='ignore')
        dfStation['triggerLevel'] = df['triggerLevel'].astype(np.int32,errors='ignore')
        stationForecasts = json.loads(dfStation.to_json(orient='records'))
        body = {
            'countryCodeISO3': self.countryCodeISO3,
            'leadTime': self.leadTimeLabel,
            'stationForecasts': stationForecasts
        }
        self.apiPostRequest('glofas-stations/triggers', body=body)
        logger.info('Uploaded triggers per station')

    def uploadTriggersPerLeadTime(self):
        with open(self.triggerFolder +
                  'trigger_per_day_' + self.countryCodeISO3 + ".json") as json_file:
            triggers = json.load(json_file)[0]
            triggersPerLeadTime = []
            for key in triggers:
                triggersPerLeadTime.append({
                    'leadTime': str(key),
                    'triggered': triggers[key]
                })
            body = {
                'countryCodeISO3': self.countryCodeISO3,
                'triggersPerLeadTime': triggersPerLeadTime
            }
            body['disasterType'] = self.getDisasterType()
            self.apiPostRequest('event/triggers-per-leadtime', body=body)
        logger.info('Uploaded triggers per leadTime')

    def apiGetRequest(self, path, countryCodeISO3):
        TOKEN = self.apiAuthenticate()
        
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        response = session.get(
            self.API_SERVICE_URL + path + '/' + countryCodeISO3,
            headers={'Authorization': 'Bearer ' + TOKEN}
        )

        '''
        response = requests.get(
            self.API_SERVICE_URL + path + '/' + countryCodeISO3,
            headers={'Authorization': 'Bearer ' + TOKEN}
        )
        '''
        data = response.json()
        return(data)

    def apiPostRequest(self, path, body=None, files=None):
        TOKEN = self.apiAuthenticate()

        if body != None:
            headers={'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json', 'Accept': 'application/json'}
        elif files != None:
            headers={'Authorization': 'Bearer ' + TOKEN}
        '''
        r = requests.post(
            self.API_SERVICE_URL + path,
            json=body,
            files=files,
            headers=headers
        )
        '''
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
      

        r = session.post(
            self.API_SERVICE_URL + path,
            json=body,
            files=files,
            headers=headers
        )
         
        if r.status_code >= 400:
            #logger.info(r.text)
            logger.error("PIPELINE ERROR")
            raise ValueError()

    def apiAuthenticate(self):
        API_LOGIN_URL=self.API_SERVICE_URL+'user/login'
        login_response = requests.post(API_LOGIN_URL, data=[(
            'email', ADMIN_LOGIN), ('password', self.ADMIN_PASSWORD)])
        return login_response.json()['user']['token']

    def getDataFromDatalake(self, path):
        import requests
        import datetime
        import hmac
        import hashlib
        import base64

        request_time = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        file_system_name = 'ibf/' + path
        logger.info(f'Downloading from datalake: {file_system_name}')

        string_params = {
            'verb': 'GET',
            'Content-Encoding': '',
            'Content-Language': '',
            'Content-Length': '',
            'Content-MD5': '',
            'Content-Type': '',
            'Date': '',
            'If-Modified-Since': '',
            'If-Match': '',
            'If-None-Match': '',
            'If-Unmodified-Since': '',
            'Range': '',
            'CanonicalizedHeaders': 'x-ms-date:' + request_time + '\nx-ms-version:' + DATALAKE_API_VERSION,
            'CanonicalizedResource': '/' + DATALAKE_STORAGE_ACCOUNT_NAME+'/'+file_system_name
        }

        string_to_sign = (string_params['verb'] + '\n'
                          + string_params['Content-Encoding'] + '\n'
                          + string_params['Content-Language'] + '\n'
                          + string_params['Content-Length'] + '\n'
                          + string_params['Content-MD5'] + '\n'
                          + string_params['Content-Type'] + '\n'
                          + string_params['Date'] + '\n'
                          + string_params['If-Modified-Since'] + '\n'
                          + string_params['If-Match'] + '\n'
                          + string_params['If-None-Match'] + '\n'
                          + string_params['If-Unmodified-Since'] + '\n'
                          + string_params['Range'] + '\n'
                          + string_params['CanonicalizedHeaders']+'\n'
                          + string_params['CanonicalizedResource'])

        signed_string = base64.b64encode(hmac.new(base64.b64decode(
            DATALAKE_STORAGE_ACCOUNT_KEY), msg=string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()).decode()
        headers = {
            'x-ms-date': request_time,
            'x-ms-version': DATALAKE_API_VERSION,
            'Authorization': ('SharedKey ' + DATALAKE_STORAGE_ACCOUNT_NAME + ':' + signed_string)
        }
        url = ('https://' + DATALAKE_STORAGE_ACCOUNT_NAME +
               '.dfs.core.windows.net/'+file_system_name)
        r = requests.get(url, headers=headers)
        return r
    
    def postResulToDatalake(self):
        import requests
        import datetime
        import hmac
        import hashlib
        import base64
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient        
        import shutil
        import os, uuid, sys
        import time
        DATALAKE_STORAGE_ACCOUNT_NAME_IBFSYSTEM='510ibfsystem'

        try:
            service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", 
                                                                                                    DATALAKE_STORAGE_ACCOUNT_NAME_IBFSYSTEM), 
                                                credential=DATALAKE_STORAGE_ACCOUNT_KEY_IBFSYSTEM)

            container_name='ibftyphoonforecast/'
            file_system_client = service_client.get_file_system_client(file_system=container_name)
            directory_name= 'ibf_model_results' 
            filename=self.Output_folder + 'ss_model_outputs'
            dir_client = file_system_client.get_directory_client(directory_name)
            dir_client.create_directory()
            

            self.zipFilesInDir(self.affectedFolder, self.affectedFolder+'ss_model_outputs.zip', lambda name : 'json' in name)
            
            time.sleep(10) # Sleep for 10 seconds
             
    
            for ibfresultfile in [x for x in os.listdir(self.affectedFolder) if x.endswith('.zip')]:  
 
                local_file = open(self.affectedFolder + ibfresultfile,'rb')
                
                file_contents = local_file.read()
                file_client = dir_client.create_file(f"{ibfresultfile}")
                file_client.upload_data(file_contents, overwrite=True)
            return 1
        except Exception as e:
            print(e)
            return 0
            
            
    def postResulToSkype(self,skypUsername,skypPassword,channel_id):
        from skpy import Skype        
        msg='AUTOMATED MESSAGE FROM DATAPIPELINE- Model output files based on latest ECMWF forecast can be found here:- https://510ibfsystem.blob.core.windows.net/ibftyphoonforecast/ibf_model_results/model_outputs.zip '
        sk = Skype(skypUsername,skypPassword)
        channel = sk.chats.chat(channel_id) 
        channel.sendMsg(msg)
               
    def zipFilesInDir(self,dirName, zipFileName, filter):
        from zipfile import ZipFile
        import os
        from os.path import basename       
        files = [ fi for fi in os.listdir(dirName) if not fi.endswith(".json") ]
        with ZipFile(zipFileName, 'w') as zipObj: # create a ZipFile object            
            for filename in files:#os.listdir(dirName): # Iterate over all the files in directory
                #if filter(filename):
                filePath = os.path.join(dirName, filename)
                # Add file to zip
                zipObj.write(filePath, basename(filePath))