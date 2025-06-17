from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, DateType
from pyiceberg.partitioning import PartitionSpec
import duckdb
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import logging

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    Transforma dados brutos em formato tabular e armazena em tabela Iceberg.
    
    Args:
        event (dict): Evento contendo start_date e end_date
        context (LambdaContext): Objeto de contexto da AWS Lambda
    """
    logger.info(f"Iniciando transformação de dados com evento: {event}")
    
    # -----------------------------
    # Parte 1 - Criar a tabela com PyIceberg
    # -----------------------------

    catalog = load_catalog("glue")
    
    # Definir campos diretamente no esquema
    schema = Schema(
        fields=[
            {"id": 1, "name": "station", "type": StringType(), "required": True},
            {"id": 2, "name": "date", "type": DateType(), "required": True},
            {"id": 3, "name": "tmax", "type": DoubleType(), "required": False},
            {"id": 4, "name": "tmin", "type": DoubleType(), "required": False},
            {"id": 5, "name": "tavg", "type": DoubleType(), "required": False},
            {"id": 6, "name": "prcp", "type": DoubleType(), "required": False}
        ]
    )

    # Definir especificação de particionamento
    partition_spec = PartitionSpec(
        spec=[
            {"name": "year", "transform": "year", "source_id": 2, "field_id": 1000},
            {"name": "month", "transform": "month", "source_id": 2, "field_id": 1001}
        ]
    )

    # Definir nomes do banco de dados e tabela
    database_name = "noaaicelake"
    table_name = "daily_measurements"
    table_identifier = f"{database_name}.{table_name}"
    
    # Criar banco de dados se não existir
    try:
        if database_name not in catalog.list_namespaces():
            catalog.create_namespace(database_name)
            logger.info(f"✅ Banco de dados {database_name} criado com sucesso!")
        else:
            logger.info(f"ℹ️ Banco de dados {database_name} já existe.")
    except Exception as e:
        logger.warning(f"Erro ao verificar/criar banco de dados: {str(e)}")
        # Tentar abordagem alternativa para versões antigas do PyIceberg
        try:
            glue_client = boto3.client('glue')
            try:
                glue_client.get_database(Name=database_name)
                logger.info(f"ℹ️ Banco de dados {database_name} já existe.")
            except glue_client.exceptions.EntityNotFoundException:
                glue_client.create_database(
                    DatabaseInput={'Name': database_name}
                )
                logger.info(f"✅ Banco de dados {database_name} criado com sucesso!")
        except Exception as e2:
            logger.error(f"Falha ao criar banco de dados: {str(e2)}")
            raise

    # Cria a tabela no Glue, se ainda não existir
    if not catalog.table_exists(table_identifier):
        catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec,
            location="s3://noaaicelake/transformation/noaa.db/daily_measurements"
        )
        logger.info("✅ Tabela criada com sucesso!")
    else:
        logger.info("ℹ️ Tabela já existe, pulando criação.")

    # -----------------------------
    # Parte 2 - Transformar e inserir dados com DuckDB
    # -----------------------------

    try:
        # Extrair datas do dicionário period
        period = event.get('period', {
            'start': '2010-01-01',
            'end': '2010-01-31'
        })
        start_date = datetime.strptime(period["start"], "%Y-%m-%d")
        end_date = datetime.strptime(period["end"], "%Y-%m-%d")

        logger.info(f"Processando dados de {start_date.date()} até {end_date.date()}")
        
        # Gerar caminhos S3 para cada dia no período
        s3_paths = []
        current = start_date
        while current <= end_date:
            path = (
                f"s3://noaaicelake/raw/results/datatype=*/year={current.year}/"
                f"month={current.month:02}/day={current.day:02}/data.json"
            )
            s3_paths.append(path)
            current += timedelta(days=1)

        # Conectar ao DuckDB e carregar extensões necessárias
        conexao = duckdb.connect()
        conexao.execute("INSTALL httpfs; LOAD httpfs;")
        conexao.execute("INSTALL iceberg; LOAD iceberg;")

        # Criar tabela temporária para os dados transformados com tipos explícitos
        conexao.execute("""
            CREATE OR REPLACE TABLE transformed AS 
            SELECT 
                CAST(NULL AS VARCHAR) AS station, 
                CAST(NULL AS DATE) AS date, 
                CAST(NULL AS DOUBLE) AS tmax, 
                CAST(NULL AS DOUBLE) AS tmin, 
                CAST(NULL AS DOUBLE) AS tavg, 
                CAST(NULL AS DOUBLE) AS prcp 
            WHERE FALSE;
        """)
        
        # Processar cada arquivo
        for path in s3_paths:
            try:
                logger.info(f"Processando arquivo: {path}")
                conexao.execute(f"""
                    INSERT INTO transformed
                    SELECT
                        CAST(station AS VARCHAR) AS station,
                        CAST(date AS DATE) AS date,
                        MAX(CASE WHEN datatype = 'TMAX' THEN CAST(value AS DOUBLE) END) AS tmax,
                        MAX(CASE WHEN datatype = 'TMIN' THEN CAST(value AS DOUBLE) END) AS tmin,
                        MAX(CASE WHEN datatype = 'TAVG' THEN CAST(value AS DOUBLE) END) AS tavg,
                        MAX(CASE WHEN datatype = 'PRCP' THEN CAST(value AS DOUBLE) END) AS prcp
                    FROM read_json_auto('{path}')
                    GROUP BY station, date;
                """)
            except Exception as e:
                logger.warning(f"Erro ao processar {path}: {str(e)}")
                continue

        # Verificar se há dados para inserir
        resultado = conexao.execute("SELECT COUNT(*) FROM transformed").fetchone()
        if resultado[0] > 0:
            # Configurar acesso AWS para DuckDB
            conexao.execute("SET s3_region='us-east-1'")
            
            # Exportar dados diretamente para um arquivo local
            caminho_local = "/tmp/transformed_data.parquet"
            conexao.execute(f"COPY transformed TO '{caminho_local}' (FORMAT PARQUET)")
            logger.info(f"✅ Dados exportados para arquivo temporário")
            
            # Carregar como tabela PyArrow
            arrow_table = pq.read_table(caminho_local)
            
            # Corrigir o esquema para corresponder ao esquema da tabela Iceberg
            novo_schema = pa.schema([
                pa.field('station', pa.string(), nullable=False),
                pa.field('date', pa.date32(), nullable=False),
                pa.field('tmax', pa.float64(), nullable=True),
                pa.field('tmin', pa.float64(), nullable=True),
                pa.field('tavg', pa.float64(), nullable=True),
                pa.field('prcp', pa.float64(), nullable=True)
            ])
            
            # Converter para o novo esquema
            arrow_table = arrow_table.cast(novo_schema)
            
            # Carregar a tabela Iceberg e substituir os dados
            table = catalog.load_table(table_identifier)
            table.overwrite(arrow_table)
            
            logger.info(f"✅ {resultado[0]} registros processados na tabela Iceberg (inseridos ou atualizados)")
        else:
            logger.info("Nenhum dado encontrado para inserir")
            
        logger.info(f"✅ Dados entre {start_date.date()} e {end_date.date()} processados com sucesso!")
    
    except Exception as e:
        logger.error(f"Erro durante a transformação: {str(e)}")
        raise

    return {
        "statusCode": 200,
        "body": f"Transformação concluída para o período de {start_date.date()} a {end_date.date()}.",
        "period": {
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d")
        }
    }