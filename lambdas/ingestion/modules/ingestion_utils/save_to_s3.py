from collections import defaultdict
from datetime import datetime
import boto3
import json


def save_stations_results(stations_results):
    organized_results = defaultdict(lambda: defaultdict(list))

    for result in stations_results:
        datatype = result['datatype']
        date = datetime.strptime(result['date'], '%Y-%m-%dT%H:%M:%S').date()
        year = date.year
        month = date.month
        day = date.day

        organized_results[datatype][date].append(result)

    
    s3 = boto3.client('s3')
    bucket_name = 'noaaicelake'

    for datatype, dates in organized_results.items():
        for date, results in dates.items():
            year = date.year
            month = date.month
            day = date.day
            
            s3_key = f"raw/datatype={datatype}/year={year}/month={month:02}/day={day:02}/data.json"

            content = json.dumps(results, ensure_ascii=False, indent=2)

            s3.put_object(
                Bucket=bucket_name, 
                Key=s3_key, 
                Body=content
                )
