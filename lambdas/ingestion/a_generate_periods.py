import json
from datetime import datetime, timedelta

def get_last_month_period():
    """Retorna o período do mês anterior completo."""
    today = datetime.utcnow()
    first_day_current = today.replace(day=1)
    last_day_prev = first_day_current - timedelta(days=1)
    first_day_prev = last_day_prev.replace(day=1)
    
    period = {
        "start": last_day_prev.strftime("%Y-%m-%d"),
        "end": last_day_prev.strftime("%Y-%m-%d")
    }

    return period

def get_next_date_calculator(frequency, event):
    """Retorna a função apropriada para calcular a próxima data."""
    calculators = {
        'daily': lambda dt: dt + timedelta(days=1),
        'monthly': lambda dt: dt.replace(
            year=dt.year + (dt.month // 12), 
            month=(dt.month % 12) + 1, 
            day=1
        ),
        'yearly': lambda dt: dt.replace(year=dt.year + 1, month=1, day=1)
    }
    
    if frequency in calculators:
        return calculators[frequency]
    
    try:
        custom_days = int(event.get('custom_days', 7))
        return lambda dt: dt + timedelta(days=custom_days)
    except ValueError:
        return None

def handler(event, context):
    """
    Gera períodos de data com base nos parâmetros fornecidos.
    """
    # Caso padrão: mês anterior
    if not event or ('start_date' not in event and 'end_date' not in event):
        return get_last_month_period()

    # Caso customizado com start_date e end_date
    try:
        start_date = datetime.strptime(event['start_date'], "%Y-%m-%d")
        end_date = datetime.strptime(event['end_date'], "%Y-%m-%d")
    except (KeyError, ValueError):
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid or missing 'start_date' or 'end_date'"})
        }

    frequency = event.get('frequency', 'monthly')
    get_next_date = get_next_date_calculator(frequency, event)
    
    if not get_next_date:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid 'custom_days' value"})
        }

    # Gerar períodos
    period = []
    current = start_date
    while current <= end_date:
        next_date = get_next_date(current)
        period_end = min(end_date, next_date - timedelta(days=1))
        
        period.append({
            "start": current.strftime("%Y-%m-%d"),
            "end": period_end.strftime("%Y-%m-%d")
        })
        current = next_date

    return period