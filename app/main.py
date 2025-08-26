from typing import List

from fastapi import FastAPI

from app.playwright.SerService import SerService
from app.repository.BigQueryRepository import BigQueryRepository, FuresRecord

app = FastAPI(
    title="Servicio de Descarga FURES",
    description="Una API para interactuar con los datos de FURES en BigQuery.",
    version="1.0.0",
)
repo = BigQueryRepository()
ser_service = SerService()


@app.get("/hola")
def read_root():
    return {"Hello": "World"}


@app.get(
    "/",
    response_model=List[FuresRecord],
    summary="Obtener los últimos 10 registros de FURES",
    tags=["FURES"],
)
def obtener_fures():
    """
    Endpoint para obtener los 10 registros más recientes de FURES
    desde BigQuery, procesados para obtener la última ingesta por
    operador y periodo.
    """
    # 4. Llama al método del repositorio y devuelve el resultado
    datos = repo.obtenerFurer()
    ser_service.start_session()

    # 3. Iteramos sobre los datos y usamos la sesión ya activa
    print(f"Procesando {len(datos)} registros en el SER...")
    for dato in datos:
        ser_service.buscar_data(
            str(dato.nitOperador), dato.periodo_fechaInicial, dato.periodo_fechaFinal
        )
    ser_service.close_session()
    return datos
