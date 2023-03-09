import netCDF4
import xarray as xr
import numpy as np
import os
from os import listdir
from os.path import isfile, join
import pandas as pd
import geopandas as gpd

from geojson import Feature, FeatureCollection, Point

from pandas import DataFrame
import sys
import json
import datetime
import urllib.request
import urllib.error
import tarfile
import time

import pysftp




#import cdsapi

from flood_model.settings import *
try:
    from flood_model.secrets import *
except ImportError:
    print('No secrets file found.')
import os
import logging

import geopandas as gpd
from shapely.geometry import shape
            
            
logger = logging.getLogger(__name__)


class GlossisData:

    def __init__(self, leadTimeLabel, leadTimeValue, countryCodeISO3, district_mapping):
        self.leadTimeLabel = leadTimeLabel
        self.leadTimeValue = leadTimeValue
        self.countryCodeISO3 = countryCodeISO3         
        self.GLOSSIS_FTP=SETTINGS[countryCodeISO3]['GLOSSIS_FTP']
        self.TRIGGER_LEVELS=SETTINGS[countryCodeISO3]['TRIGGER_LEVELS']
        self.inputPath = PIPELINE_DATA+'input/glossis/'
        self.GlossisInputPath = PIPELINE_DATA+'input/glossis/'
        self.triggerPerDay = PIPELINE_OUTPUT + \
            'triggers_rp_per_station/trigger_per_day_' + countryCodeISO3 + '.json'
        self.extractedGlossisDir = PIPELINE_OUTPUT + 'glossis_extraction'
        self.admin = gpd.read_file('./data/other/input/cod/PHL_admin_areas.shp')
        
        if not os.path.exists(self.extractedGlossisDir):
            os.makedirs(self.extractedGlossisDir)
        self.extractedGlossisPath = PIPELINE_OUTPUT + \
            'glossis_extraction/glossis_forecast_' + \
            self.leadTimeLabel + '_' + countryCodeISO3 + '.json'
        self.triggersPerStationDir = PIPELINE_OUTPUT + 'triggers_rp_per_station'
        if not os.path.exists(self.triggersPerStationDir):
            os.makedirs(self.triggersPerStationDir)
        self.triggersPerStationPath = PIPELINE_OUTPUT + \
            'triggers_rp_per_station/triggers_rp_' + \
            self.leadTimeLabel + '_' + countryCodeISO3 + '.json'
        self.DISTRICT_MAPPING = district_mapping
        self.current_date = CURRENT_DATE.strftime('%Y%m%d')

    def process(self):
        if SETTINGS[self.countryCodeISO3]['mock'] == True:
            self.extractMockData()
        else:
            self.download()
            self.postDataToDatalake()
            self.extractGlossisData()            
        #self.findTrigger()

    def removeOldGlossisData(self):
        if os.path.exists(self.GlossisInputPath):
            for f in [f for f in os.listdir(self.GlossisInputPath)]:
                os.remove(os.path.join(self.GlossisInputPath, f))
        else:
            os.makedirs(self.GlossisInputPath)

    def download(self):
        downloadDone = False

        timeToTryDownload = 43200
        timeToRetry = 600

        start = time.time()
        end = start + timeToTryDownload
        #fname='2022-09-27_T060000'
        GlossisFiles=[ CURRENT_DATE.strftime("%Y-%m-%d") +f'_T{hour}0000_Calculate_surge_gtsm4_1.25eu_H.surge.simulated_fc.nc' for hour in ['00']]#,'06','12','18']]
        #[f'{fname}_DflowFM_gtsm4_1.25eu_meteo_fc_H.simulated_fc.nc', f'{fname}_Calculate_surge_gtsm4_1.25eu_H.surge.simulated_fc.nc']:
        
         

        while downloadDone == False and time.time() < end:
            try:
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None
                with pysftp.Connection(GLOSSIS_FTP, username=GLOSSIS_USER, password=GLOSSIS_PW,cnopts=cnopts) as sftp:
                    for filename in sftp.listdir('/data'):
                        if filename in GlossisFiles: #
                            sftp.get("data/" + filename, self.GlossisInputPath+ filename)            
                downloadDone = True
            except Exception as exception:
                error = 'Download data failed. Trying again in {} minutes.\n{}'.format(timeToRetry//60, exception)
                logger.error(error)
                time.sleep(timeToRetry)
        if downloadDone == False:
            raise ValueError('GLossis download failed for ' +
                            str(timeToTryDownload/3600) + ' hours, no new dataset was found')


    def buildingsFloodzone(self,floodMap):
        
        lat1=floodMap.total_bounds[1]
        lat2=floodMap.total_bounds[3]
        lon1=floodMap.total_bounds[0]
        lon2=floodMap.total_bounds[2]
        data=pd.read_csv(PIPELINE_DATA+'input/building footprint/PHL_BuildingFootprint.csv.zip')
        features = []
        result = data[(data['latitude'] >= lat1) & (data['latitude'] <= lat2)]
        result2 = result[(result['longitude'] >= lon1) & (result['longitude'] <= lon2)]
        
        for index, row in result2.iterrows():
            latitude, longitude = map(float, (row['latitude'], row['longitude']))
            features.append(        Feature(geometry = Point((longitude, latitude)),))
                
        collection = FeatureCollection(features)

        return collection          
            
    def postDataToDatalake(self):
        import requests
        import datetime
        import hmac
        import hashlib
        import base64
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient
        import os, uuid, sys

        try:


            service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", 
                                                                                                    DATALAKE_STORAGE_ACCOUNT_NAME), 
                                                credential=DATALAKE_STORAGE_ACCOUNT_KEY)

            container_name='ibf/stormsurge/Bronze/glossis/'
            file_system_client = service_client.get_file_system_client(file_system=container_name)
            for glossisfile in os.listdir(self.GlossisInputPath):  
                directory_name= glossisfile.split('_')[0]   
                
                dir_client = file_system_client.get_directory_client(directory_name)
                dir_client.create_directory()
                local_file = open(self.GlossisInputPath + glossisfile,'rb')
                
                file_contents = local_file.read()
                file_client = dir_client.create_file(f"{glossisfile}")
                file_client.upload_data(file_contents, overwrite=True)

        except Exception as e:
            print(e)            
                          
        
    def extractGlossisData(self):
        import glob
        logger.info('\nExtracting Glossis (FTP) Data\n')        
        # specify the directory path and pattern       
        pattern = '*Calculate_surge_gtsm4*'
        # use glob to get a list of files that match the pattern    
        #'glosis/2022-09-07_T060000_Calculate_surge_gtsm4_1.25eu_H.surge.simulated_fc.nc'
        files = [os.path.basename(f) for f in glob.glob(self.GlossisInputPath + pattern) if f.endswith('.nc')]
        #files = [f for f in listdir(self.GlossisInputPath) if isfile(join(self.GlossisInputPath, f)) and f.endswith('.nc')]
        #df_thresholds = pd.read_json(json.dumps(self.GLOSSIS_STATIONS))        
        #df_thresholds = df_thresholds.set_index("stationCode", drop=False)
        df_district_mapping = pd.read_json(json.dumps(self.DISTRICT_MAPPING))
        #df_district_mapping = df_district_mapping.set_index("glossisStation", drop=False)
        df_district_mapping['threshold']=0

        
        trigger_per_day = {
            '1-day': False,
            '2-day': False,
            '3-day': False,
            '4-day': False,
            '5-day': False,
            '6-day': False,
            '7-day': False,
        }
        logger.info(f'processing file {files}\n')
        stations = []
        for i in range(0, len(files)):
            Filename = os.path.join(self.GlossisInputPath, files[i])
            logger.info(f'processing file {Filename}')
            
            # Skip old stations > need to be removed from FTP
            #if 'G5230_Na_ZambiaRedcross' in Filename or 'G5196_Uganda_Gauge' in Filename:
            #    continue 2022-09-17_T180000_DflowFM_gtsm4_1.25eu_meteo_fc_H.simulated_fc.nc
            
            
            trigger_step= files[i].split('_')[1][1:3]

            
                 
            data = xr.open_dataset(Filename)
            
            DF_WL=data.water_level_surge.to_dataframe()
            
            DF_WL.reset_index(inplace=True)  
            
            DF_WL['station_id'] = DF_WL['station_id'].str.decode('utf-8') 
            DF_WL['leadtime']=DF_WL['time']-DF_WL.time.values[0]
            DF_WL['leadtime']=DF_WL['leadtime'].dt.total_seconds().astype(int)
            
            for step in range(1, 4):
                timestep=step*60*60*24
                df_wl=DF_WL.query('leadtime==@timestep')              
                df_wl2=df_wl.groupby('station_id',as_index=False).agg({'water_level_surge':'max'})
                for index, row in df_wl2.iterrows():
                    # Do something with the row
                    discharge= row['water_level_surge']
                    station = {} 
                    station_id  = row['station_id']
                    station['code'] = station_id

                    if discharge >=5:
                        return_period = 5
                        fc_trigger=1
                    elif discharge >=4:
                        return_period = 4
                        fc_trigger=1
                    elif discharge >=3:
                        return_period = 3
                        fc_trigger=1
                    elif discharge >=0.042:
                        return_period = 2
                        fc_trigger=1
                    else:
                        return_period = None
                        fc_trigger=0

                    # Get threshold for this specific station
                    if station_id in list(set(df_district_mapping['glossisStation'].values)) and return_period:
                           
                        logger.info(f'station code: {station_id} and storm level: {return_period}')       
                        
                        #df_district_mapping.loc[df_district_mapping['glossisStation'] == station_id,'threshold'] = return_period
                        #adm3_pcode,floodextent 
                        logger.info(station_id )      
                        station['fc'] = discharge
                        station['leadTimeValue'] = step
                        station['fc_trigger'] = fc_trigger  
                        if station['fc_trigger'] == 1:
                            trigger_per_day[str(step)+'-day'] = True
                        if step == self.leadTimeValue:
                            stations.append(station)
                            df_district_mapping.loc[df_district_mapping['glossisStation'] == station_id,'threshold'] = return_period
                        #logger.info(f'leadtime: {self.leadTimeValue} and step: {step}' ) 

            data.close()
            
        self.extractedFloodMap = PIPELINE_OUTPUT + 'flood_extents/district_floodmap_' + self.leadTimeLabel + '_' + self.countryCodeISO3 + '.csv'
        
        
        df_district_mapping=df_district_mapping.query('threshold >0')
        floodedDistricts = df_district_mapping.filter(['threshold', 'floodextent']).drop_duplicates(subset=['threshold', 'floodextent'])
        
        df_district_mapping.to_csv(self.extractedFloodMap)
        
        Affected_prov={}
        
        for index, row in floodedDistricts.iterrows():
            # flood files 
            BuilHazClass1=[]
            BuilHazClass2=[]
            BuilHazClass3=[]
            BuilHazClass4=[]
            
            riskLevel=int(row['threshold'])
            floodriskMap=row['floodextent']
             
            floodMap=gpd.read_file(PIPELINE_DATA+f'input/floodmaps/StormSurgeAdvisory{riskLevel}/{floodriskMap}.zip')
            buildings_in_floodzone=self.buildingsFloodzone(floodMap)
        
            jsonfilename='temp1'
            
            with open(PIPELINE_DATA+f"input/building footprint/{jsonfilename}.geojson", "w") as f:
                f.write('%s' % buildings_in_floodzone)
               
            # Load the GeoJSON FeatureCollection
            features = gpd.read_file(PIPELINE_DATA + f'input/building footprint/{jsonfilename}.geojson')
            
     
            # Save the GeoDataFrame to a shapefile
            features.to_file(PIPELINE_DATA + f'input/building footprint/{jsonfilename}.shp', driver='ESRI Shapefile')

            # Load the shapefile
            features = gpd.read_file(PIPELINE_DATA + f'input/building footprint/{jsonfilename}.shp')
            features = features.set_crs("EPSG:4326")

            # Find the intersection between the polygon and the point shapefiles
            intersection_gdf = gpd.sjoin(features, floodMap, op='within')
            
            
         
            admin= self.admin# gpd.read_file('./data/other/input/cod/PHL_admin_areas.shp')

            intersection_gdf1 = gpd.sjoin(intersection_gdf.filter(['HAZ','geometry']), 
                                        admin.query('adminLevel==3').filter(['placeCode','placeCodeP','name','geometry']), 
                                        op='within')

            #at a provinstial level
            #intersection_gdf1.groupby(['placeCodeP']).agg(AffectedBuildings = pd.NamedAgg(column="HAZ", aggfunc="count"))

            #at a manucipality level 
            df_stats_levl=intersection_gdf1.groupby(['placeCode','HAZ']).agg(AffectedBuildings = pd.NamedAgg(column="name", aggfunc="count"))
            df_stats_levl.reset_index(inplace=True)
            df_stats_levl=df_stats_levl[['placeCode','HAZ','AffectedBuildings']].to_dict(orient='records')               

            # Print the intersection
            if riskLevel==1:
                BuilHazClass1=df_stats_levl
            elif riskLevel==2:
                BuilHazClass2=df_stats_levl
            elif riskLevel==3:
                BuilHazClass3=df_stats_levl
            elif riskLevel==4:
                BuilHazClass4=df_stats_levl
                
            Affected_prov[floodriskMap]={
            'affectedbuildinsg1':BuilHazClass1,
            'affectedbuildinsg2':BuilHazClass2,
            'affectedbuildinsg3':BuilHazClass3,
            'affectedbuildinsg4':BuilHazClass4,
            }       
   
        
        exractedbuildingfiles=PIPELINE_DATA + 'output/calculated_affected/exractedbuildingfiles.json'
        
        with open(exractedbuildingfiles, 'w') as fp:
            json.dump(Affected_prov, fp)
            logger.info('Extracted affected buildings data - File saved')   
            
                 
        with open(self.extractedGlossisPath, 'w') as fp:
            json.dump(stations, fp)
            logger.info('Extracted Glossis data - File saved')

        with open(self.triggerPerDay, 'w') as fp:
            json.dump([trigger_per_day], fp)
            logger.info('Extracted Glossis data - Trigger per day File saved')

 
    
    def extractMockData(self):
        logger.info('\nExtracting Glossis (mock) Data\n')

        # Load input data
        df_thresholds = pd.read_json(json.dumps(self.GLOFAS_STATIONS))
        df_thresholds = df_thresholds.set_index("stationCode", drop=False)
        df_district_mapping = pd.read_json(json.dumps(self.DISTRICT_MAPPING))
        df_district_mapping = df_district_mapping.set_index("glofasStation", drop=False)

        # Set up variables to fill
        stations = []
        trigger_per_day = {
            '1-day': False,
            '2-day': False,
            '3-day': False,
            '4-day': False,
            '5-day': False,
            '6-day': False,
            '7-day': False,
        }

        for index, row in df_thresholds.iterrows():
            station = {}
            station['code'] = row['stationCode']

            if station['code'] in df_district_mapping['glofasStation'] and station['code'] != 'no_station':
                logger.info(station['code'])
                threshold = df_thresholds[df_thresholds['stationCode'] ==
                                          station['code']][TRIGGER_LEVEL][0]

                for step in range(1, 8):
                    # Loop through 51 ensembles, get forecast and compare to threshold
                    ensemble_options = 51
                    count = 0
                    dis_sum = 0

                    for ensemble in range(1, ensemble_options):

                        # MOCK OVERWRITE DEPENDING ON COUNTRY SETTING
                        if SETTINGS[self.countryCodeISO3]['if_mock_trigger'] == True:
                            if step < 3: # Only dummy trigger for 3-day and above
                                discharge = 0
                            elif station['code'] == 'G5220':  # UGA dummy flood station 1
                                discharge = 600
                            elif station['code'] == 'G1067':  # ETH dummy flood station 1
                                discharge = 5000
                            elif station['code'] == 'G1904':  # ETH dummy flood station 2
                                discharge = 5500
                            elif station['code'] == 'G5305':  # KEN dummy flood station
                                discharge = 3000
                            elif station['code'] == 'G7195':  # KEN dummy flood station
                                discharge = 3000
                            elif station['code'] == 'G1361':  # ZMB dummy flood station 1
                                discharge = 8000
                            elif station['code'] == 'G1328':  # ZMB dummy flood station 2
                                discharge = 9000
                            elif station['code'] == 'G1319':  # ZMB dummy flood station 3
                                discharge = 1400
                            elif station['code'] == 'G5369':  # PHL dummy flood station 1 G1964 G1966 G1967
                                discharge = 7000
                            elif station['code'] == 'G4630':  # PHL dummy flood station 2
                                discharge = 19000
                            elif station['code'] == 'G196700':  # PHL dummy flood station 3
                                discharge = 11400
                            else:
                                discharge = 0
                        else:
                            discharge = 0

                        if discharge >= threshold:
                            count = count + 1
                        dis_sum = dis_sum + discharge

                    prob = count/ensemble_options
                    dis_avg = dis_sum/ensemble_options
                    station['fc'] = dis_avg
                    station['fc_prob'] = prob
                    station['fc_trigger'] = 1 if prob > self.TRIGGER_LEVELS['minimum'] else 0
                    #station['fc_trigger'] = 1 if prob > TRIGGER_LEVELS['minimum'] else 0

                    if station['fc_trigger'] == 1:
                        trigger_per_day[str(step)+'-day'] = True

                    if step == self.leadTimeValue:
                        stations.append(station)
                    station = {}
                    station['code'] = row['stationCode']


        # Add 'no_station'
        for station_code in ['no_station']:
            station = {}
            station['code'] = station_code
            station['fc'] = 0
            station['fc_prob'] = 0
            station['fc_trigger'] = 0
            stations.append(station)

        with open(self.extractedGlossisPath, 'w') as fp:
            json.dump(stations, fp)
            logger.info('Extracted Glossis data - File saved')

        with open(self.triggerPerDay, 'w') as fp:
            json.dump([trigger_per_day], fp)
            logger.info('Extracted Glossis data - Trigger per day File saved')

    def findTrigger(self):
        # Load (static) threshold values per station
        df_thresholds = pd.read_json(json.dumps(self.GLOFAS_STATIONS))
        df_thresholds = df_thresholds.set_index("stationCode", drop=False)
        df_thresholds.sort_index(inplace=True)
        # Load extracted Glossis discharge levels per station
        with open(self.extractedGlossisPath) as json_data:
            d = json.load(json_data)
        df_discharge = pd.DataFrame(d)
        df_discharge.index = df_discharge['code']
        df_discharge.sort_index(inplace=True)

        # Merge two datasets
        df = pd.merge(df_thresholds, df_discharge, left_index=True,
                      right_index=True)
        del df['lat']
        del df['lon']

        # Determine trigger + return period per water station
        for index, row in df.iterrows():
            fc = float(row['fc'])
            trigger = int(row['fc_trigger'])
            if trigger == 1:
                if self.countryCodeISO3 == 'ZMB':
                    if fc >= row['threshold20Year']:
                        return_period_flood_extent = 20
                    else:
                        return_period_flood_extent = 10
                else:
                    return_period_flood_extent = 25
            else:
                return_period_flood_extent = None
                
            if fc >= row['threshold20Year']:
                return_period = 20
            elif fc >= row['threshold10Year']:
                return_period = 10
            elif fc >= row['threshold5Year']:
                return_period = 5
            elif fc >= row['threshold2Year']:
                return_period = 2
            else:
                return_period = None
            
            df.at[index, 'fc_rp_flood_extent'] = return_period_flood_extent
            df.at[index, 'fc_rp'] = return_period

        out = df.to_json(orient='records')
        with open(self.triggersPerStationPath, 'w') as fp:
            fp.write(out)
            logger.info('Processed Glossis data - File saved')