def handler(event, context):
    print("Iniciando a tranformação de dados...")
    return {
        "statusCode": 200,
        "body": f"tranformação concluída com sucesso."
    }