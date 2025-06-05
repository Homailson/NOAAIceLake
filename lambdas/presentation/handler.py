def handler(event, context):
    print("Iniciando a apresentação de dados...")
    return {
        "statusCode": 200,
        "body": f"Apresentação concluída com sucesso."
    }
