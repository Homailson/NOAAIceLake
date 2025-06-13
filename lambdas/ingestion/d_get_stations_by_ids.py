import json
import boto3
import os
from datetime import datetime
from .modules.ingestion_utils.noaa_requests import get_all_stations

s3 = boto3.client('s3')
BUCKET = os.environ.get('BUCKET', 'noaaicelake')
STATIONS_KEY = os.environ.get('STATIONS_KEY', 'stations_ids.json')
API_KEY = os.environ.get("API_KEY")
URL_BASE = 'https://www.ncei.noaa.gov/cdo-web/api/v2'

def handler(event, context):
    """
    Busca informações detalhadas de estações meteorológicas a partir de IDs armazenados no S3.
    
    Args:
        event: Evento Lambda (pode conter parâmetros opcionais)
        context: Contexto Lambda
        
    Returns:
        Dict com status da operação e dados das estações ou mensagem de erro
    """
    # Definir caminho base para dados de estações
    base_path = "raw/stations"
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    metadata_key = f"{base_path}/metadata/stations_metadata.json"
    
    try:
        # Obter IDs das estações do S3
        response = s3.get_object(Bucket=BUCKET, Key=STATIONS_KEY)
        stations_ids = json.loads(response['Body'].read().decode('utf-8'))
        
        if not stations_ids:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Nenhum ID de estação encontrado'})
            }
        
        # Verificar estações já processadas anteriormente
        existing_stations = {}
        try:
            # Tentar carregar metadados existentes
            metadata_response = s3.get_object(Bucket=BUCKET, Key=metadata_key)
            metadata = json.loads(metadata_response['Body'].read().decode('utf-8'))
            
            # Carregar dados de estações existentes
            for file_path in metadata.get('files', []):
                file_response = s3.get_object(Bucket=BUCKET, Key=file_path)
                stations_data = json.loads(file_response['Body'].read().decode('utf-8'))
                for station in stations_data:
                    if 'id' in station:
                        existing_stations[station['id']] = station
                        
            print(f"Encontradas {len(existing_stations)} estações já processadas anteriormente")
        except s3.exceptions.NoSuchKey:
            print("Nenhum dado de estação encontrado anteriormente")
        
        # Identificar apenas as estações novas que precisam ser processadas
        new_station_ids = [id for id in stations_ids if id not in existing_stations]
        
        if not new_station_ids:
            print("Nenhuma estação nova para processar")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Nenhuma estação nova para processar',
                    'stations_count': len(existing_stations),
                    'metadata_location': f's3://{BUCKET}/{metadata_key}'
                })
            }
            
        print(f"Processando {len(new_station_ids)} novas estações")
        
        # Buscar dados apenas das novas estações na API NOAA
        new_stations = get_all_stations(URL_BASE, API_KEY, new_station_ids)
        
        if not new_stations:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Nenhuma informação encontrada para as novas estações'})
            }
            
        # Combinar estações existentes com as novas
        all_stations = list(existing_stations.values()) + new_stations
        print(f"Total de estações após atualização: {len(all_stations)}")
        
        # Organizar estações por região para facilitar consultas
        stations_by_region = {}
        for station in all_stations:
            region = station.get('state', 'unknown')
            if region not in stations_by_region:
                stations_by_region[region] = []
            stations_by_region[region].append(station)
        
        # Salvar dados por região
        saved_files = []
        for region, stations in stations_by_region.items():
            region_key = f"{base_path}/region={region}/stations_{current_date}.json"
            s3.put_object(
                Bucket=BUCKET,
                Key=region_key,
                Body=json.dumps(stations),
                ContentType='application/json'
            )
            saved_files.append(region_key)
        
        # Salvar metadados para facilitar a descoberta
        metadata = {
            'last_updated': current_date,
            'total_stations': len(all_stations),
            'regions': list(stations_by_region.keys()),
            'files': saved_files
        }
        
        s3.put_object(
            Bucket=BUCKET,
            Key=metadata_key,
            Body=json.dumps(metadata),
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Dados atualizados com sucesso. {len(new_stations)} novas estações adicionadas.',
                'new_stations_count': len(new_stations),
                'total_stations_count': len(all_stations),
                'regions': list(stations_by_region.keys()),
                'metadata_location': f's3://{BUCKET}/{metadata_key}'
            })
        }
        
    except s3.exceptions.NoSuchKey:
        error_msg = f"Arquivo de IDs de estações não encontrado: {STATIONS_KEY}"
        print(error_msg)
        return {
            'statusCode': 404,
            'body': json.dumps({'error': error_msg})
        }
    except Exception as e:
        error_msg = f"Erro ao processar dados das estações: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }