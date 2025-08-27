from datetime import datetime
from typing import List

from fastapi import FastAPI

from app.dto.FuresRequest import FuresRequest
from app.playwright.SerService import SerService
from app.repository.BigQueryRepository import BigQueryRepository, Expediente
from app.utils.fecha_habil_colombia import get_next_business_day

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


@app.post(
    "/",
    response_model=List[Expediente],
    summary="Obtener los últimos 10 registros de FURES",
    tags=["FURES"],
)
def obtener_fures(request: FuresRequest):
    """
    Endpoint para obtener los 10 registros más recientes de FURES
    desde BigQuery, procesados para obtener la última ingesta por
    operador y periodo.
    """

    datos_sanciones = repo.obtenerExpedientes()
    ser_service.start_session(token_ser=request.token_ser)
    year = datetime.now().year
    if request.year:
        year = request.year
    quarters = [
        (datetime(year, 1, 1).date(), datetime(year, 3, 31).date()),
        (datetime(year, 4, 1).date(), datetime(year, 6, 30).date()),
        (datetime(year, 7, 1).date(), datetime(year, 9, 30).date()),
        (datetime(year, 10, 1).date(), datetime(year, 12, 31).date()),
    ]
    # 3. Iteramos sobre los datos y usamos la sesión ya activa
    print(f"Procesando {len(datos_sanciones)} registros en el SER...")

    # 5. Bucle anidado: Itera sobre cada registro de SANCIONES
    for sancion in datos_sanciones:
        # ASUNCIÓN: El objeto 'sancion' tiene un atributo 'nitOperador'.
        # Si el nombre del campo es diferente (ej: sancion.nit), ajústalo aquí.
        nit = str(sancion.nitOperador)
        expediente = str(sancion.expediente)

        print(f"--- Iniciando procesamiento para NIT: {nit} (desde Sanciones) ---")

        for i, (start_date, end_date) in enumerate(quarters):
            fecha_inicial_ajustada = get_next_business_day(start_date)
            fecha_final_ajustada = get_next_business_day(end_date)
            trimestre_num = i + 1

            print(
                f"  -> Buscando en trimestre: {fecha_inicial_ajustada.strftime('%d/%m/%Y')} a {fecha_final_ajustada.strftime('%d/%m/%Y')}"
            )

            nit = "900014381"
            expediente = "96002150"

            ser_service.buscar_data(
                nitOperador=nit,
                expediente=expediente,
                fechaInicial=fecha_inicial_ajustada,  # type: ignore
                fechaFinal=fecha_final_ajustada,  # type: ignore
            )
            # ¡NUEVA LÍNEA! Llamamos al método de descarga después de la búsqueda
            ser_service.descargar_pdfs_de_tabla(
                nit=nit, anio=year, trimestre=trimestre_num
            )

    # 6. Cierra la sesión de Playwright DESPUÉS de terminar todos los bucles
    ser_service.close_session()

    # 7. Devuelve los datos de FURES originales, cumpliendo con el response_model
    return datos_fures_para_respuesta  # type: ignore
