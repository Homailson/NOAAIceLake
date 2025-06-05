from lambdas.ingestion.modules import stations_results, stations_metadata

def handler(event, context):
    print("Iniciando a ingestão de dados...")
    return {
        "statusCode": 200,
        "body": f"Ingestão concluída com sucesso."
    }
