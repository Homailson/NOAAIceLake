from lambdas.ingestion.modules import results, stations

def handler(event, context):
    print("Iniciando a ingestão de dados...")
    return {
        "statusCode": 200,
        "body": f"Ingestão concluída com sucesso."
    }
