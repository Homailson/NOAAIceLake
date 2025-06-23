from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, DateType
from pyiceberg.partitioning import PartitionSpec
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import os

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constantes
DATABASE_NAME = "noaaicelake"
TABLE_NAME = "stations"
TABLE_IDENTIFIER = f"{DATABASE_NAME}.{TABLE_NAME}"
TABLE_LOCATION = "s3://noaaicelake/transformation/stations"
TEMP_FILE_PATH = "/tmp/transformed_data.parquet"


def setup_iceberg_table(catalog):
    """Configura a tabela Iceberg se não existir."""
    # Definir esquema da tabela com campos identificadores (chaves primárias)

    schema = Schema(
            fields=[
                {"id": 1, "name": "id", "type": StringType(), "required": True},
                {"id": 2, "name": "name", "type": StringType(), "required": True},
                {"id": 3, "name": "latitude", "type": DoubleType(), "required": True},
                {"id": 4, "name": "longitude", "type": DoubleType(), "required": True},
                {"id": 5, "name": "elevation", "type": DoubleType(), "required": True},
                {"id": 6, "name": "elevation_unit", "type": StringType(), "required": True},
                {"id": 7, "name": "state", "type": StringType(), "required": True},
                {"id": 8, "name": "datacoverage", "type": DoubleType(), "required": True},
                {"id": 9, "name": "mindate", "type": DateType(), "required": True},
                {"id": 10, "name": "maxdate", "type": DateType(), "required": True}
            ],
            identifier_field_ids=[1]
        )

    partition_spec = PartitionSpec(
        spec=[
            {"name": "state", "transform": "identity", "source_id": 7, "field_id": 1000}
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

# tranformação dos dados
def transform_data(conexao, new_stations):
    """Transforma dados dos arquivos JSON em formato tabular."""
    # Criar tabela temporária para os dados transformados
    conexao.execute("""
        CREATE OR REPLACE TABLE transformed AS 
        SELECT 
            CAST(NULL AS VARCHAR) AS id,
            CAST(NULL AS VARCHAR) AS name, 
            CAST(NULL AS DOUBLE) AS latitude, 
            CAST(NULL AS DOUBLE) AS longitude, 
            CAST(NULL AS DOUBLE) AS elevation, 
            CAST(NULL AS VARCHAR) AS elevation_unit, 
            CAST(NULL AS VARCHAR) AS state, 
            CAST(NULL AS DOUBLE) AS datacoverage, 
            CAST(NULL AS DATE) AS mindate, 
            CAST(NULL AS DATE) AS maxdate 
        WHERE FALSE;
    """)

    for station in new_stations:
        try:
            logger.info(f"Processando estação: {station['id']}")
            # Usar parâmetros para evitar SQL injection
            conexao.execute("""
                INSERT INTO transformed VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                station['id'],
                station['name'],
                station['latitude'],
                station['longitude'],
                station['elevation'],
                station['elevation_unit'],
                station['state'],
                station['datacoverage'],
                station['mindate'],
                station['maxdate']
            ])
        except KeyError as e:
            logger.warning(f"Campo obrigatório ausente na estação {station.get('id', 'desconhecida')}: {str(e)}")
            continue
        except Exception as e:
            logger.warning(f"Erro ao processar {station.get('id', 'desconhecida')}: {str(e)}")
            continue

    return conexao.execute("SELECT COUNT(*) FROM transformed").fetchone()[0]


def write_to_iceberg(conexao, catalog, count):
    if count > 0:
        # Configurar acesso AWS para DuckDB
        conexao.execute("SET s3_region='us-east-1'")

        # Exportar dados para arquivo temporário
        conexao.execute(f"COPY transformed TO '{TEMP_FILE_PATH}' (FORMAT PARQUET)")
        logger.info("✅ Dados exportados para arquivo temporário")
        
        # Carregar como tabela PyArrow e ajustar esquema
        arrow_table = pq.read_table(TEMP_FILE_PATH)

        novo_schema = pa.schema([
            pa.field('id', pa.string(), nullable=False),
            pa.field('name', pa.string(), nullable=False),
            pa.field('latitude', pa.float64(), nullable=False),
            pa.field('longitude', pa.float64(), nullable=False),
            pa.field('elevation', pa.float64(), nullable=False),
            pa.field('elevation_unit', pa.string(), nullable=False),
            pa.field('state', pa.string(), nullable=False),
            pa.field('datacoverage', pa.float64(), nullable=False),
            pa.field('mindate', pa.date32(), nullable=False),
            pa.field('maxdate', pa.date32(), nullable=False)
        ])

        arrow_table = arrow_table.cast(novo_schema)

        # Carregar tabela Iceberg
        table = catalog.load_table(TABLE_IDENTIFIER)
        
        # Usar upsert
        table.upsert(arrow_table)
        logger.info(f"✅ {len(arrow_table)} registros inseridos via upsert")
        
        # Limpar arquivo temporário
        if os.path.exists(TEMP_FILE_PATH):
            os.remove(TEMP_FILE_PATH)
            
        return True
    else:
        logger.info("Nenhum dado encontrado para inserir")
        return False

def handler(event, context):
    """Função principal para transformação dos dados."""
    conexao = None
    try:
        new_stations = event.get('new_stations', [])
        logger.info(f"Iniciando transformação de dados")
        logger.info(f"Processando {len(new_stations)} novas estações")
        
        if not new_stations:
            logger.info("Nenhuma estação nova para processar")
            return {
                "statusCode": 200,
                "body": "Nenhuma estação nova para processar",
                "new_stations": []
            }
        
        catalog = load_catalog("glue")

        # Conectar ao DuckDB e carregar extensões
        conexao = duckdb.connect()
        conexao.execute("INSTALL httpfs; LOAD httpfs;")
        conexao.execute("INSTALL iceberg; LOAD iceberg;")

        setup_iceberg_table(catalog)

        count = transform_data(conexao, new_stations)

        write_to_iceberg(conexao, catalog, count)

        logger.info(f"✅ Dados transformados com sucesso!")

        return {
            "statusCode": 200,
            "body": f"Transformação concluída para {len(new_stations)} novas estações.",
            "new_stations": event.get("new_stations", [])
        }

    except Exception as e:
        logger.error(f"Erro na transformação: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Erro na transformação: {str(e)}"
        }
    
    finally:
        if conexao:
            conexao.close()