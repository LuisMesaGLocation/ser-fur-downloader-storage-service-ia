from datetime import date, timedelta

import holidays

# Inicializamos el calendario de festivos de Colombia
co_holidays = holidays.CO()  # type: ignore


def get_next_business_day(d: date) -> date:
    """
    Recibe una fecha y devuelve el siguiente día hábil si no es hábil.
    Un día hábil no es fin de semana ni festivo en Colombia.
    """
    while d.weekday() >= 5 or d in co_holidays:  # 5: Sábado, 6: Domingo
        d += timedelta(days=1)
    return d
