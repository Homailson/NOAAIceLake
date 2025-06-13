import requests
import math
import time

def request_with_retry(URL, headers, params=None, max_retries = 3, backoff_factor = 3):
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


def reverse_geocode(lat, lon):
    url = 'https://nominatim.openstreetmap.org/reverse'
    params = {
        'format': 'json',
        'lat': lat,
        'lon': lon,
        'zoom': 10,
        'addressdetails': 1
    }

    headers = {
        'User-Agent': 'noaa-station-data-fetcher'
    }

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        data = response.json()
        state = data.get('address', {}).get('state')
        return state
    else:
        print(f"Erro {response.status_code}")
        return None

def get_station(BASE_URL, API_KEY, station_id):
        url = f"{BASE_URL}/stations/{station_id}"
        headers = {
            'token': API_KEY
        }

        response = request_with_retry(url, headers)
        if response is None:
            print("Requisition fail. Leaving ...")
            return None
        
        station =  response.json()

        lat = station.get("latitude")
        lon = station.get("longitude")

        state = reverse_geocode(lat, lon)

        States_code = {
            "Acre": "AC",
            "Alagoas": "AL",
            "Amapá": "AP",
            "Amazonas": "AM",
            "Bahia": "BA",
            "Ceará": "CE",
            "Distrito Federal": "DF",
            "Espírito Santo": "ES",
            "Goiás": "GO",
            "Maranhão": "MA",
            "Mato Grosso": "MT",
            "Mato Grosso do Sul": "MS",
            "Minas Gerais": "MG",
            "Pará": "PA",
            "Paraíba": "PB",
            "Paraná": "PR",
            "Pernambuco": "PE",
            "Piauí": "PI",
            "Rio de Janeiro": "RJ",
            "Rio Grande do Norte": "RN",
            "Rio Grande do Sul": "RS",
            "Rondônia": "RO",
            "Roraima": "RR",
            "Santa Catarina": "SC",
            "São Paulo": "SP",
            "Sergipe": "SE",
            "Tocantins": "TO"
        }


        state_code = States_code.get(state, None)
    
        return {
            "id": station.get("id"),
            "name": station.get("name"),
            "latitude": lat,
            "longitude": lon,
            "elevation": station.get("elevation"),
            "elevation_unit": station.get("elevationUnit"),
            "state": state_code,
            "datacoverage": station.get("datacoverage"),
            "mindate":station.get("mindate"),
            "maxdate": station.get("maxdate")
        }



def get_all_stations(URL, API_KEY, stations_ids):
    all_stations = []
    for station_id in stations_ids:
        station = get_station(URL, API_KEY, station_id)

        if station is None:
            print(f"No data for {station_id}. Continuing to next station...")
            continue
        all_stations.append(station)
    if not all_stations:
        print("No data retrieved. Leaving...")
        return []
    print(f"Total of {len(all_stations)} stations retrieved.")
    return all_stations