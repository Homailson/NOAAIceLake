import json
import boto3
from datetime import datetime, timedelta
import os
import logging

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constantes configuráveis via variáveis de ambiente
BUCKET = os.environ.get('S3_BUCKET', 'noaaicelake')
STATIONS_KEY = os.environ.get('STATIONS_KEY', 'stations_ids.json')

def handler(event, context):
    """
    Extrai IDs de estações de arquivos NOAA e atualiza a lista mestra.
    
    Args:
        event: Evento Lambda contendo 'period' e 'datatypes'
        context: Contexto Lambda
    
    Returns:
        Dict com status, número de IDs adicionados e total
    """
    try:
        # Inicialização do cliente S3 dentro da função
        s3 = boto3.client('s3')
        
        period = event.get('period', {})
        datatypes = event.get('datatypes', [])
        
        if not period or not datatypes:
            raise ValueError("Parâmetros 'period' e 'datatypes' são obrigatórios")
        
        start = datetime.strptime(period['start'], "%Y-%m-%d")
        end = datetime.strptime(period['end'], "%Y-%m-%d")
        
        logger.info(f"Processando dados de {start.date()} até {end.date()} para tipos: {datatypes}")
        
        all_new_ids = set()
        
        # Loop por data
        current_day = start
        while current_day <= end:
            year = current_day.year
            month = f"{current_day.month:02d}"
            day = f"{current_day.day:02d}"
            
            # Para cada tipo de dado
            for datatype in datatypes:
                prefix = f"raw/results/datatype={datatype}/year={year}/month={month}/day={day}/"
                
                try:
                    # Paginação para lidar com muitos objetos
                    paginator = s3.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)
                    
                    for page in pages:
                        if 'Contents' not in page:
                            continue
                            
                        for obj in page['Contents']:
                            try:
                                obj_data = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
                                body = json.loads(obj_data['Body'].read())
                                
                                # Extração de IDs de estação
                                for record in body:
                                    station_id = record.get('station')
                                    if station_id:
                                        all_new_ids.add(station_id)
                            except Exception as e:
                                logger.error(f"Erro ao processar objeto {obj['Key']}: {str(e)}")
                                continue
                except Exception as e:
                    logger.error(f"Erro ao listar objetos com prefixo {prefix}: {str(e)}")
                    continue
            
            current_day += timedelta(days=1)
        
        # Carregar IDs existentes
        existing_ids = set()
        try:
            existing = s3.get_object(Bucket=BUCKET, Key=STATIONS_KEY)
            existing_ids = set(json.loads(existing['Body'].read()))
        except s3.exceptions.NoSuchKey:
            logger.info(f"Arquivo {STATIONS_KEY} não encontrado. Criando novo arquivo.")
        except Exception as e:
            logger.error(f"Erro ao carregar IDs existentes: {str(e)}")
        
        # Atualizar com os novos IDs
        new_ids = all_new_ids - existing_ids
        updated_ids = list(existing_ids.union(all_new_ids))
        
        # Salvar IDs atualizados
        try:
            s3.put_object(
                Bucket=BUCKET,
                Key=STATIONS_KEY,
                Body=json.dumps(updated_ids),
                ContentType='application/json'
            )
            logger.info(f"Adicionados {len(new_ids)} novos IDs. Total: {len(updated_ids)}")
        except Exception as e:
            logger.error(f"Erro ao salvar IDs atualizados: {str(e)}")
            raise
        
        return {
            'statusCode': 200,
            'added': len(new_ids),
            'total': len(updated_ids)
        }
    except Exception as e:
        logger.error(f"Erro não tratado: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e)
        }