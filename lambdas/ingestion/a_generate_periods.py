from datetime import datetime, timedelta

def handler(event, context):
    """
    Gera o período do mês anterior (do primeiro ao último dia).
    
    Args:
        event: Evento Lambda (não utilizado)
        context: Contexto Lambda (não utilizado)
        
    Returns:
        Lista contendo um dicionário com datas de início e fim do mês anterior
    """
    # Removido import json não utilizado
    
    # Usar utcnow() para consistência em diferentes fusos horários
    today = datetime.utcnow()
    first_day_current_month = today.replace(day=1)
    last_day_last_month = first_day_current_month - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)

    start_date = first_day_last_month.strftime("%Y-%m-%d")
    end_date = last_day_last_month.strftime("%Y-%m-%d")

    # Formatação consistente com espaços após os dois pontos
    return [{
        "start": start_date,
        "end": end_date
    }]
