from .modules.ingestion_utils.noaa_requests import get_all_results
from .modules.ingestion_utils.save_to_s3 import save_stations_results
import os
import json

def handler(event, context):
    print("Initializing data ingestion...")

    URL_BASE = 'https://www.ncei.noaa.gov/cdo-web/api/v2/'

    #Lambda environment variables
    API_KEY = os.environ.get("nooa_api_key")
    datatypes = json.loads(os.environ.get("datatypes"))  #'TMAX', 'TMIN', 'TAVG', 'PRCP'
    
    period = {
        'start': event['start'],
        'end': event['end']
    }
    
    print(f"Data ingestion started for {period['start']} to {period['end']}.")

    stations_results = get_all_results(URL_BASE, API_KEY, datatypes, period['start'], period['end'])

    if not stations_results:
        print(f"No data for {period['start']}. Continuing to next period...")
    save_stations_results(stations_results)

    return {
        'datatypes': datatypes,
        'period': period
    }
