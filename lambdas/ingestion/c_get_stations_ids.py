import boto3
import os
import logging
from .modules.ingestion_utils.stations_utils import get_existing_stations, get_current_ids


# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Nome do bucket definido nas variáveis de ambiente da lambda
BUCKET = os.environ.get('S3_BUCKET', 'noaaicelake')

def handler(event, context):
    """
    Função Lambda para extrair e atualizar IDs de estações meteorológicas da NOAA.
    
    Esta função processa arquivos de resultados armazenados no S3, extrai IDs de estações
    meteorológicas e identifica novos IDs que ainda não foram processados.
    
    Args:
        event (dict): Evento de entrada contendo:
            - period (dict): Período de tempo para busca com chaves 'start' e 'end' no formato 'YYYY-MM-DD'
            - datatypes (list): Lista de tipos de dados meteorológicos a serem processados
        context (LambdaContext): Objeto de contexto da AWS Lambda
    
    Returns:
        dict: Resposta contendo:
            - statusCode (int): 200 para sucesso, 500 para erro
            - added (int): Número de novos IDs identificados (se sucesso)
            - new_station_ids (list): Lista de novos IDs de estações para processamento
            - period (dict): Período processado
            - datatypes (list): Tipos de dados processados
            - error (str): Mensagem de erro (se falha)
    
    Raises:
        ValueError: Se os parâmetros obrigatórios 'period' ou 'datatypes' estiverem ausentes
    """

    try:
        logger.info("Iniciando processamento de IDs de estações")

        s3 = boto3.client('s3')
        period = event.get('period', {})
        datatypes = event.get('datatypes', [])
        
        if not period or not datatypes:
            raise ValueError("Parâmetros 'period' e 'datatypes' são obrigatórios")
        
        current_ids = get_current_ids(BUCKET, s3, period, datatypes)

        # Carregamento dos IDs existentes do arquivo de metadados
        metadata_key = "raw/stations/metadata/stations_metadata.json"
        
        old_ids = get_existing_stations(s3, BUCKET, metadata_key).keys()
        
        logger.info(f"ids já existentes no arquivo de metadata: {old_ids}")
        
        # Cálculo dos novos IDs
        new_ids = current_ids - old_ids
        
        logger.info(f"Encontrados {len(new_ids)} novos IDs de estações.")

        return {
            'new_station_ids': list(new_ids),  # Incluir os novos IDs diretamente no retorno
            'period': period
        }

    except Exception as e:
        logger.error(f"Erro não tratado: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e)
        }