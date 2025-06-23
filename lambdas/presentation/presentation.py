import duckdb
import logging
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, TimestampType, IntegerType


# Configurando o logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constantes
DATABASE_NAME = "noaaicelake"
TRANSFORMED_TABLE_NAMES = ["transformed_results", "transformed_stations"]

def get_presentation_views():
    """Retorna a configuração das views de apresentação."""
    return {
        "monthly_avg_temp": {
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "state", "type": StringType(), "required": True},
                    {"id": 2, "name": "month", "type": TimestampType(), "required": True},
                    {"id": 3, "name": "avg_temp", "type": DoubleType(), "required": True}
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field('state', pa.string(), nullable=False),
                pa.field('month', pa.timestamp('us'), nullable=False),
                pa.field('avg_temp', pa.float64(), nullable=False)
            ]),
            "query": """
                SELECT 
                    s.state,
                    DATE_TRUNC('month', r.date) AS month,
                    AVG(r.tavg) AS avg_temp
                FROM transformed_results r
                JOIN transformed_stations s ON r.station = s.id
                GROUP BY s.state, DATE_TRUNC('month', r.date)
                ORDER BY s.state, month
            """
        },
        "station_summary": {
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "station_id", "type": StringType(), "required": True},
                    {"id": 2, "name": "state", "type": StringType(), "required": True},
                    {"id": 3, "name": "total_measurements", "type": IntegerType(), "required": True},
                    {"id": 4, "name": "avg_temp", "type": DoubleType(), "required": True}
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field('station_id', pa.string(), nullable=False),
                pa.field('state', pa.string(), nullable=False),
                pa.field('total_measurements', pa.int32(), nullable=False),
                pa.field('avg_temp', pa.float64(), nullable=False)
            ]),
            "query": """
                SELECT 
                    r.station as station_id,
                    s.state,
                    COUNT(*) as total_measurements,
                    AVG(r.tavg) as avg_temp
                FROM transformed_results r
                JOIN transformed_stations s ON r.station = s.id
                GROUP BY r.station, s.state
                ORDER BY total_measurements DESC
            """
        }
    }

def create_presentation_table(catalog, view_name, view_config):
    """Cria uma tabela de apresentação se não existir."""
    table_name = f"presentation_{view_name}"
    table_identifier = f"{DATABASE_NAME}.{table_name}"
    table_location = f"s3://noaaicelake/presentation/{view_name}"
    
    if not catalog.table_exists(table_identifier):
        catalog.create_table(
            identifier=table_identifier,
            schema=view_config["schema"],
            location=table_location
        )
        logger.info(f"✅ Tabela {table_name} criada")
    
    return table_identifier

def process_presentation_view(catalog, conexao, view_name, view_config):
    """Processa uma view de apresentação específica."""
    # Criar tabela
    table_identifier = create_presentation_table(catalog, view_name, view_config)
    
    # Executar query
    resultado = conexao.execute(view_config["query"]).arrow()
    
    # Ajustar schema do Arrow usando a configuração
    if "arrow_schema" in view_config:
        resultado = resultado.cast(view_config["arrow_schema"])
    
    # Inserir dados
    presentation_table = catalog.load_table(table_identifier)
    presentation_table.append(resultado)
    
    dados = resultado.to_pylist()
    logger.info(f"✅ {view_name}: {len(dados)} registros inseridos")
    
    return len(dados)

def handler(event, context):
    try:
        # Conectar ao catálogo Iceberg (Glue)
        catalog = load_catalog("glue")
        
        # Conectar DuckDB
        conexao = duckdb.connect()
        
        # Carregar tabelas transformadas no DuckDB
        for table_name in TRANSFORMED_TABLE_NAMES:
            iceberg_table = catalog.load_table(f"{DATABASE_NAME}.{table_name}")
            arrow_table = iceberg_table.scan().to_arrow()
            conexao.register(table_name, arrow_table)
        
        # Processar todas as views de apresentação
        presentation_views = get_presentation_views()
        results = {}
        for view_name, view_config in presentation_views.items():
            count = process_presentation_view(catalog, conexao, view_name, view_config)
            results[view_name] = count
        
        logger.info(f"✅ Processamento concluído: {results}")
        
        return {
            "statusCode": 200,
            "body": f"Apresentação concluída: {results}"
        }
        
    except Exception as e:
        logger.error(f"Erro no processamento: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Erro: {str(e)}"
        }
    
    finally:
        if 'conexao' in locals():
            conexao.close()
