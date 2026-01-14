# NOAA Ice Lake 🌡️❄️

## Visão Geral
Pipeline completo de dados meteorológicos que coleta, transforma e apresenta dados da NOAA (National Oceanic and Atmospheric Administration) usando uma arquitetura serverless moderna na AWS. O sistema processa dados climáticos de estações meteorológicas brasileiras e os armazena em um data lake usando Apache Iceberg para análises avançadas.

## 🏗️ Arquitetura

![noaa_arquitetura](/noaaiceberg_arc.jpeg)
![noaa_powerbidashboard](/noaaiceberg_dash.jpeg)

## 🔄 Pipeline de Dados

### 1. **Camada de Ingestão**
- **generatePeriods**: Gera períodos de coleta (padrão: mês anterior)
- **getStationsResults**: Coleta medições meteorológicas da API NOAA
- **getStationsIds**: Extrai IDs únicos de estações dos resultados
- **getStationsByIds**: Obtém metadados detalhados das estações

### 2. **Camada de Transformação**
- **resultsTransformation**: Converte dados brutos em tabelas Iceberg otimizadas
- **stationsTransformation**: Processa metadados de estações para análise

### 3. **Camada de Apresentação**
- **presentation**: Cria views agregadas para consultas analíticas

## 📊 Estrutura de Dados

### Raw Data (S3)
```
s3://noaaicelake/
├── raw/
│   ├── results/
│   │   └── datatype={TMAX|TMIN|TAVG|PRCP}/
│   │       └── year={YYYY}/month={MM}/day={DD}/
│   │           └── data.json
│   └── stations/
│       ├── metadata/stations_metadata.json
│       └── region={STATE}/stations_{YYYY-MM-DD}.json
├── transformation/
│   ├── daily_measurements/  # Tabela Iceberg
│   └── stations/           # Tabela Iceberg
└── presentation/
    ├── monthly_avg_temp/   # Views agregadas
    └── station_summary/
```

### Tabelas Iceberg

**transformed_results**
- `id` (PK): Identificador único da medição
- `station`: ID da estação meteorológica
- `date`: Data da medição
- `tmax`, `tmin`, `tavg`: Temperaturas (°C)
- `prcp`: Precipitação (mm)
- Particionado por: `year`, `month`

**transformed_stations**
- `id` (PK): ID da estação
- `name`: Nome da estação
- `latitude`, `longitude`: Coordenadas
- `elevation`: Altitude
- `state`: Estado brasileiro
- `datacoverage`: Cobertura de dados
- Particionado por: `state`

## 🛠️ Tecnologias

- **Python 3.11**: Linguagem principal
- **AWS Lambda**: Computação serverless
- **Amazon S3**: Data lake storage
- **Apache Iceberg**: Formato de tabela ACID
- **AWS Glue Catalog**: Metastore
- **DuckDB**: Engine de processamento
- **PyArrow**: Manipulação de dados colunares
- **Docker**: Containerização
- **Amazon ECR**: Registry de containers

## ⚙️ Configuração

### Variáveis de Ambiente
```bash
# API NOAA
noaa_api_key=your_noaa_api_key
datatypes=["TMAX","TMIN","TAVG","PRCP"]

# AWS
S3_BUCKET=noaaicelake
BUCKET=noaaicelake
API_KEY=your_noaa_api_key

# Iceberg
PYICEBERG_CATALOG__GLUE__TYPE=glue
PYICEBERG_CATALOG__GLUE__URI=https://glue.us-east-1.amazonaws.com
PYICEBERG_CATALOG__GLUE__WAREHOUSE=s3://noaaicelake
```

### Dependências
```txt
pyiceberg[s3fs,glue,pyarrow]
duckdb==1.2.2
requests
```

## 🚀 Deploy

### Pré-requisitos
- AWS CLI configurado
- Docker instalado
- Chave API da NOAA CDO
- Bucket S3 criado

### Implantação Automatizada
```bash
# Executar deploy completo
./deploy.sh
```

O script automaticamente:
1. Cria repositório ECR (se necessário)
2. Constrói imagem Docker
3. Faz push para ECR
4. Atualiza todas as funções Lambda
5. Limpa imagens antigas

### Funções Lambda Criadas
- `generatePeriods`
- `getStationsResults`
- `getStationsIds`
- `getStationsByIds`
- `resultsTransformation`
- `stationsTransformation`
- `presentation`

## 📈 Uso

### Execução Manual
```python
# Gerar períodos
result = generate_periods_lambda({}, {})

# Coletar dados
result = get_stations_results_lambda({
    "start": "2024-11-01",
    "end": "2024-11-30"
}, {})
```

### Agendamento
Configure EventBridge para execução automática mensal.

## 🔍 Monitoramento

- **CloudWatch Logs**: Logs detalhados de cada Lambda
- **CloudWatch Metrics**: Métricas de execução
- **S3 Metrics**: Uso de storage
- **Glue Catalog**: Metadados das tabelas

## 📋 Tipos de Dados Coletados

- **TMAX**: Temperatura máxima diária (°C)
- **TMIN**: Temperatura mínima diária (°C)
- **TAVG**: Temperatura média diária (°C)
- **PRCP**: Precipitação diária (mm)

## 🌍 Cobertura Geográfica

Foco em estações meteorológicas brasileiras com dados históricos robustos.

## 🔧 Desenvolvimento

### Estrutura do Projeto
```
NOAA_Ice_Lake/
├── lambdas/
│   ├── ingestion/
│   │   ├── modules/ingestion_utils/
│   │   ├── generate_periods.py
│   │   ├── get_stations_results.py
│   │   ├── get_stations_ids.py
│   │   └── get_stations_by_ids.py
│   ├── transformation/
│   │   ├── results_transformation.py
│   │   └── stations_transformation.py
│   └── presentation/
│       └── presentation.py
├── app.py
├── Dockerfile
├── deploy.sh
└── requirements-no-hash.txt
```

### Ambiente Local
```bash
# Criar ambiente virtual
python -m venv env
source env/bin/activate  # Linux/Mac
# ou
env\Scripts\activate     # Windows

# Instalar dependências
pip install -r requirements-no-hash.txt
```

## 📄 Licença

Este projeto está disponível sob os termos da licença [MIT](https://opensource.org/licenses/MIT).

---

**Desenvolvido com ❤️ para análise de dados meteorológicos brasileiros**