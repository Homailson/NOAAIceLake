from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, DateType
from pyiceberg.partitioning import PartitionSpec
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import logging
import os

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constantes
DATABASE_NAME = "noaaicelake"
TABLE_NAME = "daily_measurements"
TABLE_IDENTIFIER = f"{DATABASE_NAME}.{TABLE_NAME}"
TABLE_LOCATION = "s3://noaaicelake/transformation/noaa.db/daily_measurements"
TEMP_FILE_PATH = "/tmp/transformed_data.parquet"


def setup_iceberg_table(catalog):
    """Configura a tabela Iceberg se não existir."""
    # Definir esquema da tabela com campos identificadores (chaves primárias)
    schema = Schema(
        fields=[
            {"id": 1, "name": "station", "type": StringType(), "required": True},
            {"id": 2, "name": "date", "type": DateType(), "required": True},
            {"id": 3, "name": "tmax", "type": DoubleType(), "required": False},
            {"id": 4, "name": "tmin", "type": DoubleType(), "required": False},
            {"id": 5, "name": "tavg", "type": DoubleType(), "required": False},
            {"id": 6, "name": "prcp", "type": DoubleType(), "required": False}
        ],
        # Definir station e date como campos identificadores (chaves primárias)
        identifier_field_ids=[1, 2]
    )

    # Definir particionamento
    partition_spec = PartitionSpec(
        spec=[
            {"name": "year", "transform": "year", "source_id": 2, "field_id": 1000},
            {"name": "month", "transform": "month", "source_id": 2, "field_id": 1001}
        ]
    )

    # Criar banco de dados se não existir
    try:
        namespaces = [n[0] for n in catalog.list_namespaces()]
        if DATABASE_NAME not in namespaces:
            catalog.create_namespace(DATABASE_NAME)
            logger.info(f"Banco de dados '{DATABASE_NAME}' criado.")
        else:
            logger.info(f"Banco de dados '{DATABASE_NAME}' já existe.")
    except Exception as e:
        logger.warning(f"Erro ao verificar ou criar banco de dados: {str(e)}")

    # Criar tabela se não existir
    if not catalog.table_exists(TABLE_IDENTIFIER):
        catalog.create_table(
            identifier=TABLE_IDENTIFIER,
            schema=schema,
            partition_spec=partition_spec,
            location=TABLE_LOCATION
        )
        logger.info("✅ Tabela criada com sucesso!")
    else:
        logger.info("ℹ️ Tabela já existe, pulando criação.")


def generate_s3_paths(start_date, end_date):
    """Gera caminhos S3 para cada dia no período."""
    s3_paths = []
    current = start_date
    while current <= end_date:
        path = (
            f"s3://noaaicelake/raw/results/datatype=*/year={current.year}/"
            f"month={current.month:02}/day={current.day:02}/data.json"
        )
        s3_paths.append(path)
        current += timedelta(days=1)
    return s3_paths


def transform_data(conexao, s3_paths):
    """Transforma dados dos arquivos JSON em formato tabular."""
    # Criar tabela temporária para os dados transformados
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
    
    # Retornar contagem de registros
    return conexao.execute("SELECT COUNT(*) FROM transformed").fetchone()[0]


def write_to_iceberg(conexao, catalog, count):
    """Escreve dados transformados na tabela Iceberg."""
    if count > 0:
        # Configurar acesso AWS para DuckDB
        conexao.execute("SET s3_region='us-east-1'")
        
        # Exportar dados para arquivo temporário
        conexao.execute(f"COPY transformed TO '{TEMP_FILE_PATH}' (FORMAT PARQUET)")
        logger.info("✅ Dados exportados para arquivo temporário")
        
        # Carregar como tabela PyArrow e ajustar esquema
        arrow_table = pq.read_table(TEMP_FILE_PATH)
        
        novo_schema = pa.schema([
            pa.field('station', pa.string(), nullable=False),
            pa.field('date', pa.date32(), nullable=False),
            pa.field('tmax', pa.float64(), nullable=True),
            pa.field('tmin', pa.float64(), nullable=True),
            pa.field('tavg', pa.float64(), nullable=True),
            pa.field('prcp', pa.float64(), nullable=True)
        ])
        
        arrow_table = arrow_table.cast(novo_schema)
        
        # Carregar tabela Iceberg
        table = catalog.load_table(TABLE_IDENTIFIER)
        
        try:
            # Usar upsert para deduplicação automática baseada nos campos identificadores
            result = table.upsert(arrow_table)
            logger.info(f"✅ {result.rows_inserted} registros inseridos, {result.rows_updated} atualizados na tabela Iceberg")
        except Exception as e:
            # Fallback para append simples em caso de erro
            logger.warning(f"Erro no upsert: {str(e)}")
        
        # Limpar arquivo temporário
        if os.path.exists(TEMP_FILE_PATH):
            os.remove(TEMP_FILE_PATH)
            
        return True
    else:
        logger.info("Nenhum dado encontrado para inserir")
        return False


def handler(event, context):
    """
    Transforma dados brutos em formato tabular e armazena em tabela Iceberg.
    
    Args:
        event (dict): Evento contendo period com start_date e end_date
        context (LambdaContext): Objeto de contexto da AWS Lambda
    """
    logger.info(f"Iniciando transformação de dados com evento: {event}")
    
    try:
        # Carregar catálogo e configurar tabela
        catalog = load_catalog("glue")
        setup_iceberg_table(catalog)
        
        # Extrair datas do evento
        period = {
            "start": event['start'],
            "end": event['end']
        }
        
        start_date = datetime.strptime(period["start"], "%Y-%m-%d")
        end_date = datetime.strptime(period["end"], "%Y-%m-%d")
        
        logger.info(f"Processando dados de {start_date.date()} até {end_date.date()}")
        
        # Gerar caminhos S3
        s3_paths = generate_s3_paths(start_date, end_date)
        
        # Conectar ao DuckDB e carregar extensões
        conexao = duckdb.connect()
        conexao.execute("INSTALL httpfs; LOAD httpfs;")
        conexao.execute("INSTALL iceberg; LOAD iceberg;")
        
        # Transformar dados
        count = transform_data(conexao, s3_paths)
        
        # Escrever na tabela Iceberg
        write_to_iceberg(conexao, catalog, count)
        
        logger.info(f"✅ Dados entre {start_date.date()} e {end_date.date()} processados com sucesso!")
        
        return {
            "statusCode": 200,
            "body": f"Transformação concluída para o período de {start_date.date()} a {end_date.date()}.",
            "period": {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d")
            }
        }
    
    except Exception as e:
        logger.error(f"Erro durante a transformação: {str(e)}")
        raise