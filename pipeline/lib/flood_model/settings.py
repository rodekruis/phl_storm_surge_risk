import os 
import ast
##################
## LOAD SECRETS ##
##################
# 1. Try to load secrets from env-variables (i.e. when using Github Actions)
 
try:
 
    GLOSSIS_FTP = os.environ["GLOSSIS_FTP"]
    GLOSSIS_USER = os.environ["GLOSSIS_USER"]
    GLOSSIS_PW = os.environ["GLOSSIS_PW"]
    DATALAKE_STORAGE_ACCOUNT_KEY_IBFSYSTEM = os.environ["DATALAKE_STORAGE_ACCOUNT_KEY_IBFSYSTEM"]
    
    #ADMIN_LOGIN = os.environ["ADMIN_LOGIN"]
    #ADMIN_LOGIN =os.environ.get("ADMIN_LOGIN") 
    #IBF_PASSWORD=os.environ["IBF_PASSWORD"]
    #DATALAKE_STORAGE_ACCOUNT_NAME = os.environ["DATALAKE_STORAGE_ACCOUNT_NAME"]
    #DATALAKE_STORAGE_ACCOUNT_KEY = os.environ["DATALAKE_STORAGE_ACCOUNT_KEY"]


    

except:
     print('No environment variables found.')
#.format(**os.environ))    

    

# 2. If 1 fail, then assume secrets are loaded via secrets.py file (when running locally). If neither of the 3 options apply, this script will fail.
try:
    from flood_model.secrets import *
except ImportError:
    print('No secrets file found.')



######################
## COUNTRY SETTINGS ##
######################

# Countries to include

COUNTRY_CODES = ['PHL'] # 
SETTINGS = {    
    "PHL": {
        #"IBF_API_URL": IBF_URL,
        #"PASSWORD": IBF_PASSWORD,
        "mock": False,
        "if_mock_trigger": False,
        "notify_email": True,
        'lead_times': {
            "3-day": 3
        },
        'TRIGGER_LEVELS':{"minimum": 0.6,"medium": 0.7,"maximum": 0.8},
        'admin_level': 3,
        'levels':[3,2,1],
        'GLOFAS_FTP':'data-portal.ecmwf.int/RedcrossPhilippines_glofas_point/',
        'GLOSSIS_FTP':'datafeed1.deltares.nl/data/',
        'GLOFAS_FILENAME':'glofas_pointdata_RedcrossPhilippines', 
        'EXPOSURE_DATA_SOURCES': {
            "population": {
                "source": "population/hrsl_phl_pop_resized_100",
                "rasterValue": 1
            }
        }
    }
}





# Change this date only in case of specific testing purposes
from datetime import date, timedelta
#CURRENT_DATE = date.today()
CURRENT_DATE=date.today() - timedelta(1) # to use yesterday's date





# Trigger probability 
TRIGGER_LEVELS = {
    "minimum": 0.6,
    "medium": 0.7,
    "maximum": 0.8
}



###################
## PATH SETTINGS ##
###################
RASTER_DATA = 'data/raster/'
RASTER_INPUT = RASTER_DATA + 'input/'
RASTER_OUTPUT = RASTER_DATA + 'output/'
PIPELINE_DATA = 'data/other/'
PIPELINE_INPUT = PIPELINE_DATA + 'input/'
PIPELINE_OUTPUT = PIPELINE_DATA + 'output/'
TRIGGER_DATA_FOLDER='data/trigger_data/triggers_rp_per_station/'
TRIGGER_DATA_FOLDER_TR='data/trigger_data/glofas_trigger_levels/'
STATION_DISTRICT_MAPPING_FOLDER='data/trigger_data/station_district_mapping/'




