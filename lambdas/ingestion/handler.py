from .modules.ingestion_utils.noaa_requests import get_all_results
from .modules.ingestion_utils.save_to_s3 import save_stations_results
import os

def handler(event, context):
    print("Initializing data ingestion...")

    datatypes = ['TMAX'] #'TMAX', 'TMIN', 'TAVG', 'PRCP'    
    URL_BASE = 'https://www.ncei.noaa.gov/cdo-web/api/v2/'
    API_KEY = os.environ.get("nooa_api_key")
    startdate = event['start']
    enddate = event['end']

    print(f"Data ingestion started for {startdate} to {enddate}.")

    stations_results = get_all_results(URL_BASE, API_KEY, datatypes, startdate, enddate)
    if not stations_results:
        print(f"No data for {startdate}. Continuing to next period...")
    save_stations_results(stations_results)


    return {
        "statusCode": 200,
        "body": f"Data ingestion finished."
    }