# NOAA Ice Lake

## Visão Geral
Este projeto implementa um pipeline de dados para coletar, transformar e apresentar dados meteorológicos da NOAA (National Oceanic and Atmospheric Administration). O sistema utiliza uma arquitetura baseada em AWS Lambda para processar dados climáticos de estações meteorológicas, armazenando-os em um data lake no Amazon S3 usando o formato Apache Iceberg.

## Arquitetura

O pipeline é composto por três etapas principais:

1. **Ingestão**: Coleta dados da API NOAA CDO (Climate Data Online)
2. **Transformação**: Processa os dados brutos para análise
3. **Apresentação**: Disponibiliza os dados processados para consulta

## Componentes Lambda

### Ingestão
- **a_generate_periods**: Gera períodos de tempo (mês anterior) para coleta de dados
- **b_get_stations_results**: Obtém resultados de medições das estações meteorológicas
- **c_get_stations_ids**: Extrai e armazena IDs de estações dos resultados
- **d_get_stations_by_ids**: Obtém informações detalhadas das estações por ID

### Transformação
- Processa os dados brutos para análise (implementação em desenvolvimento)

### Apresentação
- Disponibiliza os dados processados para consulta (implementação em desenvolvimento)

## Estrutura de Dados

Os dados são organizados no S3 seguindo uma estrutura particionada:

```
raw/
  ├── results/
  │   └── datatype={TMAX|TMIN|TAVG|PRCP}/
  │       └── year={YYYY}/
  │           └── month={MM}/
  │               └── day={DD}/
  │                   └── data.json
  └── stations/
      ├── metadata/
      │   └── stations_metadata.json
      └── region={UF}/
          └── stations_{YYYY-MM-DD}.json
```

## Tecnologias Utilizadas

- **Python 3.11**: Linguagem de programação principal
- **AWS Lambda**: Execução de funções serverless
- **Amazon S3**: Armazenamento de dados
- **Apache Iceberg**: Formato de tabela para data lakes
- **PyIceberg**: Cliente Python para Apache Iceberg
- **Docker**: Containerização para implantação

## Requisitos

- Python 3.11+
- AWS CLI configurado
- Credenciais da API NOAA CDO
- Bucket S3 configurado

## Configuração

O projeto utiliza variáveis de ambiente para configuração:

- `nooa_api_key`: Chave de API para acesso à NOAA CDO
- `datatypes`: Tipos de dados meteorológicos a serem coletados (TMAX, TMIN, TAVG, PRCP)
- `S3_BUCKET`: Nome do bucket S3 para armazenamento (padrão: 'noaaicelake')

## Implantação

O projeto inclui um Dockerfile para criar uma imagem compatível com AWS Lambda:

```bash
# Construir a imagem Docker
docker build -t noaa-ice-lake .

# Executar o script de implantação
./deploy.sh
```

## Licença

Este projeto está disponível como código aberto sob os termos da licença [MIT](https://opensource.org/licenses/MIT).