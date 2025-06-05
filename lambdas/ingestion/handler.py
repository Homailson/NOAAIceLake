from lambdas.ingestion.modules import results

def handler(event, context):
    print("Iniciando a ingestão de dados...")
    results.get_results()
    return {
        "statusCode": 200,
        "body": f"Ingestão concluída com sucesso."
    }
