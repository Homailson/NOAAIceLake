import json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()

# função que retorna os ids das estações a partir dos resultados obtidos
# em b_get_stations_results
def get_current_ids(BUCKET, s3, period, datatypes):
    """
    Extrai IDs únicos de estações a partir de objetos S3 dentro de um período específico.
    
    Args:
        BUCKET (str): Nome do bucket S3 onde os dados estão armazenados
        s3: Cliente boto3 S3 para acessar os objetos
        period (dict): Dicionário contendo as datas 'start' e 'end' no formato 'YYYY-MM-DD'
        datatypes (list): Lista de tipos de dados a serem processados
        
    Returns:
        set: Conjunto de IDs únicos de estações encontrados nos objetos S3
    """
    # Converte as strings de data para objetos datetime
    start = datetime.strptime(period['start'], "%Y-%m-%d")
    end = datetime.strptime(period['end'], "%Y-%m-%d")
    logger.info(f"Processando dados de {start.date()} até {end.date()} para tipos: {datatypes}")

    # Conjunto para armazenar IDs únicos de estações (usa set para garantir unicidade)
    current_ids = set()
    current_day = start

    # Iteração por cada dia no período especificado
    while current_day <= end:
        year = current_day.year
        month = f"{current_day.month:02d}"  # Formata mês com zero à esquerda
        day = f"{current_day.day:02d}"      # Formata dia com zero à esquerda

        # Iteração por cada tipo de dado solicitado
        for datatype in datatypes:
            # Constrói o prefixo do caminho S3 seguindo a estrutura de particionamento
            prefix = f"raw/results/datatype={datatype}/year={year}/month={month}/day={day}/"
            
            try:
                # Usa paginação para lidar com grandes volumes de objetos
                paginator = s3.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)
                
                # Processa cada página de resultados
                for page in pages:
                    # Verifica se há conteúdo na página
                    if 'Contents' not in page:
                        continue
                    
                    # Processa cada objeto S3 encontrado
                    for obj in page['Contents']:
                        try:
                            # Obtém o conteúdo do objeto S3
                            obj_data = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
                            # Converte o conteúdo JSON para estrutura Python
                            body = json.loads(obj_data['Body'].read())

                            # Extrai o ID da estação de cada registro
                            for record in body:
                                station_id = record.get('station')
                                if station_id:
                                    current_ids.add(station_id)  # Adiciona ao conjunto de IDs únicos
                        except Exception as e:
                            # Registra erro e continua com o próximo objeto
                            logger.error(f"Erro ao processar objeto {obj['Key']}: {str(e)}")
                            continue

            except Exception as e:
                # Registra erro ao listar objetos e continua com o próximo tipo de dado
                logger.error(f"Erro ao listar objetos com prefixo {prefix}: {str(e)}")
                continue

        # Avança para o próximo dia
        current_day += timedelta(days=1)
    
    # Retorna o conjunto de IDs únicos encontrados
    return current_ids

# função que retorna o ids da estações antigas, que já
# foram consolidadas no banco de dados e estão nos metadados
def get_existing_stations(s3_client, bucket, metadata_key):
    """
    Recupera informações de estações já processadas anteriormente.
    
    Args:
        s3_client: Cliente boto3 S3
        bucket: Nome do bucket S3
        metadata_key: Chave do arquivo de metadados
        
    Returns:
        dict: Dicionário com estações existentes, ou dicionário vazio se nenhuma for encontrada
    """
    existing_stations = {}
    try:
        metadata_response = s3_client.get_object(Bucket=bucket, Key=metadata_key)
        metadata = json.loads(metadata_response['Body'].read().decode('utf-8'))

        for file_path in metadata.get('files', []):
            file_response = s3_client.get_object(Bucket=bucket, Key=file_path)
            stations_data = json.loads(file_response['Body'].read().decode('utf-8'))
            for station in stations_data:
                if 'id' in station:
                    existing_stations[station['id']] = station
                    
        logger.info(f"Encontradas {len(existing_stations)} estações já processadas anteriormente")
    except s3_client.exceptions.NoSuchKey:
        logger.info("Nenhum dado de estação encontrado anteriormente")
    
    return existing_stations


# função que organiza e salva as estações no data lake
def organize_and_save_stations(s3_client, bucket, all_stations, base_path, current_date):
    """
    Organiza estações por região e salva os dados em arquivos separados.
    
    Args:
        s3_client: Cliente boto3 S3
        bucket: Nome do bucket S3
        all_stations (list): Lista de todas as estações a serem organizadas e salvas
        base_path (str): Caminho base para armazenamento dos arquivos
        current_date (str): Data atual formatada para uso nos nomes dos arquivos
        
    Returns:
        tuple: (saved_files, stations_by_region) - Lista de arquivos salvos e dicionário de estações por região
    """
    # Organizar estações por região para facilitar consultas
    stations_by_region = {}
    for station in all_stations:
        # Verificar se station é um dicionário antes de chamar get()
        if isinstance(station, dict):
            region = station.get('state', 'unknown')
        else:
            # Se não for um dicionário, usar 'unknown' como região
            region = 'unknown'
            logger.warning(f"Encontrada estação em formato inválido: {type(station)}")
        
        if region not in stations_by_region:
            stations_by_region[region] = []
        stations_by_region[region].append(station)
    
    # Salvar dados por região
    saved_files = []
    for region, stations in stations_by_region.items():
        region_key = f"{base_path}/region={region}/stations_{current_date}.json"
        new_content = json.dumps(stations)
        
        # Verificar se arquivo já existe com mesmo conteúdo
        try:
            existing_obj = s3_client.get_object(Bucket=bucket, Key=region_key)
            existing_content = existing_obj['Body'].read().decode('utf-8')
            if existing_content == new_content:
                logger.info(f"Arquivo {region_key} já existe com mesmo conteúdo, pulando gravação")
                saved_files.append(region_key)
                continue
        except s3_client.exceptions.NoSuchKey:
            pass
        
        s3_client.put_object(
            Bucket=bucket,
            Key=region_key,
            Body=new_content,
            ContentType='application/json'
        )
        saved_files.append(region_key)
    
    logger.info(f"Dados salvos em {len(saved_files)} arquivos por região")
    return saved_files, stations_by_region


# função que atualiza os metadados
def update_metadata(s3_client, bucket, saved_files, stations_by_region, all_stations, metadata_key, current_date):
    """
    Cria e salva o arquivo de metadados com informações sobre os dados das estações.
    
    Args:
        s3_client: Cliente boto3 S3
        bucket: Nome do bucket S3
        saved_files (list): Lista de caminhos dos arquivos salvos
        stations_by_region (dict): Dicionário com estações organizadas por região
        all_stations (list): Lista de todas as estações
        metadata_key (str): Chave do arquivo de metadados
        current_date (str): Data atual formatada
        
    Returns:
        dict: Metadados atualizados
    """
    # Criar metadados
    metadata = {
        'last_updated': current_date,
        'total_stations': len(all_stations),
        'regions': list(stations_by_region.keys()),
        'files': saved_files
    }
    
    # Salvar metadados
    s3_client.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(metadata),
        ContentType='application/json'
    )
    logger.info(f"Metadados atualizados. Total de estações: {len(all_stations)}")
    
    return metadata