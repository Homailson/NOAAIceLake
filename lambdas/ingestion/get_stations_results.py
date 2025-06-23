from .modules.ingestion_utils.results_utils import get_all_results, save_stations_results
import os
import json
import logging


# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Url base da API do NOAA
URL_BASE = 'https://www.ncei.noaa.gov/cdo-web/api/v2'

# Chave da API do NOAA
API_KEY = os.environ.get("noaa_api_key")


def handler(event, context):
    """
    Função Lambda para ingerir medições de estações meteorológicas da NOAA.
    
    Esta função obtém dados meteorológicos da API NOAA para um período específico
    e tipos de dados definidos, e os salva no S3.
    
    Args:
        event (dict): Evento de entrada contendo:
            - period (dict): Período de tempo para busca com chaves 'start' e 'end'
        context (LambdaContext): Objeto de contexto da AWS Lambda
    
    Returns:
        dict: Contendo os datatypes e o período processado para uso em funções subsequentes
    
    Raises:
        ValueError: Se os parâmetros obrigatórios 'period' ou 'datatypes' estiverem ausentes
    """
    logger.info("Iniciando ingestão das medições das estações")

    try:
        # Tipos de dados a requisitar - obtém da variável de ambiente ou usa lista vazia como padrão
        datatypes_str = os.environ.get("datatypes")
        datatypes = json.loads(datatypes_str) if datatypes_str else []
        
        # Período a transformar
        period = {
            "start": event['start'],
            "end": event['end']
        }

        # Validação dos parâmetros obrigatórios
        if not period or not datatypes:
            raise ValueError("Parâmetros 'period' e 'datatypes' são obrigatórios")
        
        # Obtenção dos resultados das estações
        stations_results = get_all_results(URL_BASE, API_KEY, datatypes, period['start'], period['end'])
        
        # Salvar resultados apenas se houver dados
        if stations_results:
            logger.info(f"Salvando {len(stations_results)} resultados das estações")
            save_stations_results(stations_results)
        else:
            logger.info(f"Não há dados para o período de {period['start']} a {period['end']}.")

        # Retorna os parâmetros para uso nas funções Lambda subsequentes
        return {
            'datatypes': datatypes,
            'period': period
        }

    except ValueError as e:
        logger.error(f"Erro de validação: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Erro durante a ingestão dos dados das estações: {str(e)}")
        raise