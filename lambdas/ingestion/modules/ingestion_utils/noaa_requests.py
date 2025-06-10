import requests
import math
import time

def request_with_retry(URL, headers, params, max_retries = 3, backoff_factor = 3):
    for attempt in range(max_retries):
        try:
            response = requests.get(URL, headers=headers, params=params)
            if response.status_code == 200:
                return response
            else:
                print(f"Requisition error. Code: {response.status_code}")
                if response.status_code in [429, 500, 502, 503, 504]:
                    wait_time = backoff_factor * (2 ** attempt)
                    print(f"Waiting {wait_time} seconds before next try...")
                    time.sleep(wait_time)
                else:
                    return None
        except requests.exceptions.RequestException as e:
            print(f"Requisition error: {e}")
            wait_time = backoff_factor * (2 ** attempt)
            print(f"Waiting {wait_time} seconds before next try...")
            time.sleep(wait_time)
    print("Max number of tries reached. Leaving...")
    return None


def get_results(URL, API_KEY, datatype, startdate, enddate, limit=1000):
    
    params = {
    'datasetid': 'GHCND',
    'datatypeid': datatype,
    'units': 'metric',
    'locationid': 'FIPS:BR',
    'startdate': startdate,
    'enddate': enddate,
    'limit': limit,
    'offset': 1
    }

    all_results = []

    headers = {"token": API_KEY}

    endpoint = f"{URL}/data"
    
    response = request_with_retry(endpoint, headers, params)
    
    if response is None:
        print("Requisition fail. Leaving...")
        return all_results
    try:
        data = response.json()
    except ValueError:
        print("Invalid JSON response. Leaving ...")
        return all_results
    
    results = data.get('results', [])

    print(f"Total results in first page: {len(results)}")

    all_results.extend(results)

    time.sleep(5)

    metadata = data.get('metadata', {})
    resultset = metadata.get('resultset', {})
    total = resultset.get('count', 0)

    if total > len(all_results):

        total_pages = math.ceil(total / limit)

        for page in range (2, total_pages + 1):
            params['offset'] = (page - 1)*limit + 1
            response = request_with_retry(endpoint, headers, params)

            if response is None:
                print("Requisition fail. Leaving ...")
                return all_results
            try:
                data = response.json()
            except ValueError:
                print("Invalid JSON response. Leaving ...")
                return all_results
            
            results = data.get('results', [])
            print(f"Total of results in page {page}/{total_pages}: {len(results)}")
            all_results.extend(results)
            
            time.sleep(5)
    
    return all_results

def get_all_results(URL, API_KEY, datatypes, startdate, enddate):
    all_data = []
    for datatype in datatypes:
        print(f"Requesting data for {datatype}")
        data = get_results(URL, API_KEY, datatype, startdate, enddate)
        if data is None:
            print(f"No data for {datatype}. Continuing to next datatype...")
            continue
        all_data.extend(data)
    if not all_data:
        print("No data retrieved. Leaving...")
        return []
    print(f"Total of {len(all_data)} results retrieved.")
    return all_data

