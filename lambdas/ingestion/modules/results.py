from lambdas.ingestion.modules.ingestion_utils import noaa_requests
def get_results():
    noaa_requests.file_requisition()
    print("Resultados obtidos com sucesso!")