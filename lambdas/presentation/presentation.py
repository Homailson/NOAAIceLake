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
                -- Calcula a temperatura média mensal por estado
                -- Agrupa dados de temperatura por estado e mês
                SELECT 
                    s.state,
                    DATE_TRUNC('month', r.date) AS month,
                    COALESCE(AVG(r.tavg), 0) AS avg_temp
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
                -- Resumo por estação: total de medições e temperatura média
                -- Ordena por estações com mais dados coletados
                SELECT 
                    r.station as station_id,
                    s.state,
                    COUNT(*) as total_measurements,
                    COALESCE(AVG(r.tavg), 0) as avg_temp
                FROM transformed_results r
                JOIN transformed_stations s ON r.station = s.id
                GROUP BY r.station, s.state
                ORDER BY total_measurements DESC
            """
        },
        "total_stations_by_state":{
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "state", "type": StringType(), "required": True},
                    {"id": 2, "name": "total_stations", "type": IntegerType(), "required": True}
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field('state', pa.string(), nullable=False),
                pa.field('total_stations', pa.int32(), nullable=False)
            ]),
            "query": """
                -- Conta o número total de estações meteorológicas por estado
                -- Ordena por estados com mais estações
                SELECT
                    state,
                    COUNT(*) as total_stations
                FROM transformed_stations
                GROUP BY state
                ORDER BY total_stations DESC
            """
        },
        "total_measurements_by_station":{
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "station_id", "type": StringType(), "required": True},
                    {"id": 2, "name": "total_measurements", "type": IntegerType(), "required": True}
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field('station_id', pa.string(), nullable=False),
                pa.field('total_measurements', pa.int32(), nullable=False)
            ]),
            "query": """
                -- Total de medições coletadas por cada estação
                -- Identifica estações mais ativas em coleta de dados
                SELECT
                    station AS station_id,
                    COUNT(*) as total_measurements
                FROM transformed_results
                GROUP BY station
                ORDER BY total_measurements DESC
            """
        },
        "precipitation_by_state_by_year": {
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "state", "type": StringType(), "required": True},
                    {"id": 2, "name": "year", "type": TimestampType(), "required": True},
                    {"id": 3, "name": "total_precipitation", "type": DoubleType(), "required": True}
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field('state', pa.string(), nullable=False),
                pa.field('year', pa.timestamp('us'), nullable=False),
                pa.field('total_precipitation', pa.float64(), nullable=False)
            ]),
            "query": """
                -- Precipitação total anual por estado
                -- Soma toda a chuva registrada no ano em cada estado
                SELECT
                    s.state,
                    DATE_TRUNC('year', r.date) AS year,
                    COALESCE(SUM(r.prcp), 0) AS total_precipitation
                FROM transformed_results r
                JOIN transformed_stations s
                    ON r.station = s.id
                GROUP BY
                    s.state,
                    DATE_TRUNC('year', r.date)
                ORDER BY
                    s.state,
                    year
            """
        },
        "central_tendence_tmin_tmax_by_month":
        {
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "month", "type": TimestampType(), "required": True},
                    {"id": 2, "name": "avg_tmin", "type": DoubleType(), "required": True},
                    {"id": 3, "name": "avg_tmax", "type": DoubleType(), "required": True}
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field('month', pa.timestamp('us'), nullable=False),
                pa.field('avg_tmin', pa.float64(), nullable=False),
                pa.field('avg_tmax', pa.float64(), nullable=False)
            ]),
            "query": """
                -- Tendência central das temperaturas mínimas e máximas por mês
                -- Calcula médias mensais de temperaturas extremas
                SELECT
                    DATE_TRUNC('month', date) AS month,
                    COALESCE(AVG(tmin), 0) AS avg_tmin,
                    COALESCE(AVG(tmax), 0) AS avg_tmax
                FROM transformed_results
                GROUP BY DATE_TRUNC('month', date)
                ORDER BY month
            """
        },
        "extreme_temperature_by_state":
        {
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "state", "type": StringType(), "required": True},
                    {"id": 2, "name": "max_temp", "type": DoubleType(), "required": True},
                    {"id": 3, "name": "min_temp", "type": DoubleType(), "required": True}
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field('state', pa.string(), nullable=False),
                pa.field('max_temp', pa.float64(), nullable=False),
                pa.field('min_temp', pa.float64(), nullable=False)
            ]),
            "query": """
                -- Temperaturas extremas registradas por estado
                -- Encontra a maior temperatura máxima e menor temperatura mínima
                SELECT
                    s.state,
                    COALESCE(MAX(r.tmax), 0) AS max_temp,
                    COALESCE(MIN(r.tmin), 0) AS min_temp
                FROM transformed_results r
                JOIN transformed_stations s ON r.station = s.id
                GROUP BY s.state
                ORDER BY s.state
            """
        },
        "avg_temp_by_season":
        {
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "season", "type": StringType(), "required": True},
                    {"id": 2, "name": "avg_temp", "type": DoubleType(), "required": True}
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field('season', pa.string(), nullable=False),
                pa.field('avg_temp', pa.float64(), nullable=False)
            ]),
            "query": """
                -- Temperatura média por estação do ano (Brasil - Hemisfério Sul)
                -- Classifica meses em estações brasileiras e calcula médias sazonais
                SELECT
                    CASE
                        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Verão'
                        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'Outono'
                        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Inverno'
                        ELSE 'Primavera'
                    END AS season,
                    COALESCE(AVG(tavg), 0) AS avg_temp
                FROM transformed_results
                GROUP BY season
                ORDER BY
                    CASE season
                        WHEN 'Verão' THEN 1
                        WHEN 'Outono' THEN 2
                        WHEN 'Inverno' THEN 3
                        WHEN 'Primavera' THEN 4
                        ELSE 99
                    END;
            """
        },
        "preciptation_by_season":
        {
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "season", "type": StringType(), "required": True},
                    {"id": 2, "name": "total_precipitation", "type": DoubleType(), "required": True}
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field('season', pa.string(), nullable=False),
                pa.field('total_precipitation', pa.float64(), nullable=False)
            ]),
            "query": """
                -- Precipitação total por estação do ano (Brasil - Hemisfério Sul)
                -- Soma toda a chuva registrada em cada estação climática brasileira
                SELECT
                    CASE
                        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Verão'
                        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'Outono'
                        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Inverno'
                        ELSE 'Primavera'
                    END AS season,
                    COALESCE(SUM(prcp), 0) AS total_precipitation
                FROM transformed_results
                GROUP BY season
                ORDER BY
                    CASE season
                        WHEN 'Verão' THEN 1
                        WHEN 'Outono' THEN 2
                        WHEN 'Inverno' THEN 3
                        WHEN 'Primavera' THEN 4
                        ELSE 99
                    END;
            """
        },
        "precipitation_anomaly_by_state_by_year": {
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "state", "type": StringType(), "required": True},
                    {"id": 2, "name": "year", "type": TimestampType(), "required": True},
                    {"id": 3, "name": "total_precipitation", "type": DoubleType(), "required": True},
                    {"id": 4, "name": "historical_avg_precipitation", "type": DoubleType(), "required": True},
                    {"id": 5, "name": "anomaly_mm", "type": DoubleType(), "required": True},
                    {"id": 6, "name": "anomaly_pct", "type": DoubleType(), "required": True},
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field("state", pa.string(), nullable=False),
                pa.field("year", pa.timestamp("us"), nullable=False),
                pa.field("total_precipitation", pa.float64(), nullable=False),
                pa.field("historical_avg_precipitation", pa.float64(), nullable=False),
                pa.field("anomaly_mm", pa.float64(), nullable=False),
                pa.field("anomaly_pct", pa.float64(), nullable=False),
            ]),
            "query": """
                -- Anomalia de precipitação anual por estado (mm e %)
                WITH annual_precipitation AS (
                    SELECT
                        s.state,
                        DATE_TRUNC('year', r.date) AS year,
                        SUM(r.prcp) AS total_precipitation
                    FROM transformed_results r
                    JOIN transformed_stations s
                        ON r.station = s.id
                    GROUP BY
                        s.state,
                        DATE_TRUNC('year', r.date)
                ),
                historical_avg AS (
                    SELECT
                        state,
                        AVG(total_precipitation) AS historical_avg_precipitation
                    FROM annual_precipitation
                    GROUP BY state
                )
                SELECT
                    a.state,
                    a.year,
                    COALESCE(a.total_precipitation, 0) AS total_precipitation,
                    COALESCE(h.historical_avg_precipitation, 0) AS historical_avg_precipitation,
                    COALESCE(a.total_precipitation - h.historical_avg_precipitation, 0) AS anomaly_mm,
                    COALESCE(
                        ((a.total_precipitation - h.historical_avg_precipitation)
                        / NULLIF(h.historical_avg_precipitation, 0)) * 100,
                        0
                    ) AS anomaly_pct
                FROM annual_precipitation a
                JOIN historical_avg h
                    ON a.state = h.state
                ORDER BY
                    a.state,
                    a.year
            """
        },
        "rainy_days_by_state_by_year": {
            "schema": Schema(
                fields=[
                    {"id": 1, "name": "state", "type": StringType(), "required": True},
                    {"id": 2, "name": "year", "type": TimestampType(), "required": True},
                    {"id": 3, "name": "rainy_days", "type": IntegerType(), "required": True},
                ]
            ),
            "arrow_schema": pa.schema([
                pa.field("state", pa.string(), nullable=False),
                pa.field("year", pa.timestamp("us"), nullable=False),
                pa.field("rainy_days", pa.int32(), nullable=False),
            ]),
            "query": """
                -- Número de dias com chuva por estado por ano
                -- Conta os dias (distintos) em que houve precipitação > 0 mm
                SELECT
                    s.state,
                    DATE_TRUNC('year', r.date) AS year,
                    COUNT(DISTINCT r.date) AS rainy_days
                FROM transformed_results r
                JOIN transformed_stations s
                    ON r.station = s.id
                WHERE
                    r.prcp > 0
                GROUP BY
                    s.state,
                    DATE_TRUNC('year', r.date)
                ORDER BY
                    s.state,
                    year
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
    
    # Sobrescrever dados para evitar duplicação
    presentation_table = catalog.load_table(table_identifier)
    presentation_table.overwrite(resultado)
    
    dados = resultado.to_pylist()
    logger.info(f"✅ {view_name}: {len(dados)} registros sobrescritos")
    
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
