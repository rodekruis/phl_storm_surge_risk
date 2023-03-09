from flood_model.glossisdata import GlossisData
from flood_model.dynamicDataDb import DatabaseManager
from flood_model.settings import *
import pandas as pd
import json
from shapely import wkb, wkt
import geopandas
import os
import logging
logger = logging.getLogger(__name__)



class Forecast:
    def __init__(self, leadTimeLabel, leadTimeValue, countryCodeISO3, admin_level):
        self.db = DatabaseManager(leadTimeLabel,countryCodeISO3, admin_level)
        self.leadTimeLabel = leadTimeLabel
        self.leadTimeValue = leadTimeValue
        self.countryCodeISO3=countryCodeISO3
        self.admin_level = admin_level
        self.DistrictMappingFolder = STATION_DISTRICT_MAPPING_FOLDER
        self.TriggersFolder = TRIGGER_DATA_FOLDER_TR
        self.levels = SETTINGS[countryCodeISO3]['levels']
        self.PIPELINE_INPUT_COD=PIPELINE_INPUT+'cod/'
        self.ADMIN_AREA_GDF_PATH= os.path.join(self.PIPELINE_INPUT_COD,f"{countryCodeISO3}_admin_areas.geojson")

        self.POPULATION_PATH= os.path.join(self.PIPELINE_INPUT_COD,f"{countryCodeISO3}_{self.admin_level}_population.json") 

        df_admin1=geopandas.read_file(self.ADMIN_AREA_GDF_PATH)
        df_admin2=df_admin1.filter(['adminLevel','placeCode','placeCodeParent'])
        
        df_admin=pd.DataFrame(df_admin1)
        df_list={}       
        max_iteration=self.admin_level+1
        for adm_level in self.levels:
            df_=df_admin2.query(f"adminLevel == {adm_level}")
            df_.rename(columns={"placeCode": f"placeCode_{adm_level}","placeCodeParent": f"placeCodeParent_{adm_level}"},inplace=True)            
            df_list[adm_level]=df_            
        
        
        df=df_list[self.admin_level]        
        ################# Create a dataframe with pcodes for each admin level         
        for adm_level in self.levels:
            j=adm_level-1
            if j >0 and len(self.levels)>1:
                df=pd.merge(df.copy(),df_list[j],  how='left',left_on=f'placeCodeParent_{j+1}' , right_on =f'placeCode_{j}')
     
        df=df[[f"placeCode_{i}" for i in self.levels]]      
        self.pcode_df=df[[f"placeCode_{i}" for i in self.levels]]           
       
        df_admin=df_admin.query(f'adminLevel == {self.admin_level}')
        

        with open(self.POPULATION_PATH) as fp:
            population_df=json.load(fp)        
        
        population_df=pd.DataFrame(population_df) 

        
        #self.admin_area_gdf2 = df_admin1
        df_admin1=df_admin2.query(f'adminLevel == {self.admin_level}')

           
        #df_admin=df_admin1.filter(['placeCode','placeCodeParent','name'])#,'geometry'])

        self.admin_area_gdf =df_admin1.copy()# geopandas.GeoDataFrame.from_features(admin_area_json)
        logger.info('finished ......................pcode name') 
        
        district_mapping_df = pd.read_csv(self.DistrictMappingFolder + f'{countryCodeISO3}_glossis_station_mapping.csv', index_col=False,dtype={'placeCode': str}) 

        
        self.district_mapping = district_mapping_df.to_dict(orient='records')
        df_admin=df_admin.filter(['placeCode','placeCodeParent'])
        
        population_df = pd.merge(population_df,df_admin,  how='left',left_on='placeCode', right_on = 'placeCode')

        
        population_df=population_df.to_dict(orient='records')
        self.population_total =population_df


 
        self.glossisData = GlossisData(self.leadTimeLabel, self.leadTimeValue, countryCodeISO3, self.district_mapping)

