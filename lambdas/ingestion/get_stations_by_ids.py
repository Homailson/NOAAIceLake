import json
import boto3
import os
from datetime import datetime
from .modules.ingestion_utils.results_utils import get_all_stations
from .modules.ingestion_utils.stations_utils import (
    get_existing_stations,
    organize_and_save_stations,
    update_metadata
)
import logging

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
BUCKET = os.environ.get('BUCKET', 'noaaicelake')
API_KEY = os.environ.get("API_KEY")
URL_BASE = 'https://www.ncei.noaa.gov/cdo-web/api/v2'

def handler(event, context):
    """
    Busca informações detalhadas de estações meteorológicas a partir de IDs armazenados no S3.
    
    Esta função obtém os novos IDs de estações identificados pela função anterior,
    busca informações detalhadas dessas estações na API NOAA e atualiza o arquivo
    consolidado de estações.
    
    Args:
        event: Evento de entrada contendo:
            - new_station_ids (list): Lista de novos IDs de estações para processar
            - period (dict): Período de tempo processado
            - datatypes (list): Tipos de dados processados
        context: Contexto Lambda
        
    Returns:
        Dict com status da operação e dados das estações ou mensagem de erro
    """
    base_path = "raw/stations"
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    metadata_key = f"{base_path}/metadata/stations_metadata.json"
    
    try:
        new_stations = []
        new_station_ids = set(event.get('new_station_ids', []))
        
        for s in new_station_ids:
            logger.info(f"IDs de estações novas: {s}")

        logger.info(f"Recebidos {len(new_station_ids)} novos IDs de estações para processar")
        
        if not new_station_ids:
            return {
                'statusCode': 200,
                'period': event.get('period', {}),
                'new_stations': new_stations,
                'body': json.dumps({'message': 'Nenhuma estação nova para processar'})
            }
        
        # Obter estações já processadas anteriormente
        existing_stations = get_existing_stations(s3, BUCKET, metadata_key)

            
        logger.info(f"Processando {len(new_station_ids)} novas estações")
        
        # Buscar dados apenas das novas estações na API NOAA
        new_stations = get_all_stations(URL_BASE, API_KEY, new_station_ids)
        
        if not new_stations:
            return {
                'statusCode': 404,
                'period': event.get('period', {}),
                'new_stations': new_stations,
                'body': json.dumps({'message': 'Nenhuma informação encontrada para as novas estações'})
            }
            
        # Combinar estações existentes com as novas
        all_stations = list(existing_stations.values()) + new_stations
        logger.info(f"Total de estações após atualização: {len(all_stations)}")
        
        # Organizar e salvar dados por região
        saved_files, stations_by_region = organize_and_save_stations(s3, BUCKET, all_stations, base_path, current_date)
        
        # Atualizar metadados
        metadata = update_metadata(s3, BUCKET, saved_files, stations_by_region, all_stations, metadata_key, current_date)

        return {
            'statusCode': 200,
            'period': event.get('period', {}),
            'new_stations': new_stations,
            'body': json.dumps({
                'message': f'Dados atualizados com sucesso. {len(new_stations)} novas estações adicionadas.',
                'new_stations_count': len(new_stations),
                'total_stations_count': metadata['total_stations'],
                'regions': metadata['regions'],
                'metadata_location': f's3://{BUCKET}/{metadata_key}',
                
            })
        }
        
    except Exception as e:
        error_msg = f"Erro ao processar dados das estações: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }