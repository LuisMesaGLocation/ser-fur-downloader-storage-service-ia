import os
import shutil
from datetime import datetime
from typing import List

from fastapi import FastAPI, HTTPException, Query
from typing_extensions import Optional

from app.dto.FuresRequest import FuresRequest
from app.playwright.SerService import SerService
from app.repository.BigQueryRepository import BigQueryRepository, Expediente
from app.repository.StorageRepository import StorageRepository
from app.utils.fecha_habil_colombia import get_next_business_day

app = FastAPI(
    title="Servicio de Descarga FURES",
    description="Una API para interactuar con los datos de FURES en BigQuery.",
    version="1.0.0",
)
repo = BigQueryRepository()
ser_service = SerService()
storageRepository = StorageRepository()


@app.get("/hola")
def read_root():
    return {"Hello": "World"}


@app.post(
    "/",
    response_model=List[Expediente],
    summary="Obtener los últimos 10 registros de FURES",
    tags=["FURES"],
)
def obtener_fures(
    request: FuresRequest,
    originData: Optional[str] = Query(
        None,
        description="Origen de los datos: 'database' para BigQuery, otro valor o None para usar datos del body",
    ),
):
    """
    Endpoint para obtener los 10 registros más recientes de FURES
    desde BigQuery, procesados para obtener la última ingesta por
    operador y periodo.
    """
    download_folder = ser_service.download_path
    if os.path.exists(download_folder):
        print(f"Limpiando directorio de descargas principal: {download_folder}")
        shutil.rmtree(download_folder)

    datos_expedientes: List[Expediente]
    if originData == "database":
        print("📊 Obteniendo datos desde BigQuery...")
        datos_expedientes = repo.obtenerExpedientes()
        expedientes_a_procesar = datos_expedientes
        total_registros = len(datos_expedientes)

        # Aplicar filtros de NIT si se proporcionan
        if request.nitDesde is not None and request.nitHasta is not None:
            print(
                f"🔍 Filtrando registros por NIT en el rango: desde {request.nitDesde} hasta {request.nitHasta}"
            )

            if request.nitHasta < request.nitDesde:
                raise HTTPException(
                    status_code=400,
                    detail="Rango inválido: nitHasta no puede ser menor que nitDesde.",
                )

            expedientes_a_procesar = [
                exp
                for exp in datos_expedientes
                if request.nitDesde <= int(exp.nitOperador) <= request.nitHasta
            ]

            print(
                f"✅ Se encontraron {len(expedientes_a_procesar)} registros de un total de {total_registros} para procesar en el rango."
            )

    else:
        print("📄 Usando datos del body de la petición...")
        if not request.data:
            raise HTTPException(
                status_code=400,
                detail="Cuando no se usa 'originData=database', el campo 'data' es requerido en el body.",
            )

        # Convertir los datos del body a objetos Expediente para mantener consistencia
        expedientes_a_procesar = [
            Expediente(nitOperador=item.nitOperador, expediente=item.expediente)
            for item in request.data
        ]
        print(
            f"✅ Se procesarán {len(expedientes_a_procesar)} registros enviados en el body."
        )

    # ser_service.start_session(token_ser=request.token_ser)
    ser_service.login()
    year = datetime.now().year
    if request.year:
        year = request.year
    quarters = [
        (datetime(year, 1, 1).date(), datetime(year, 3, 31).date()),
        (datetime(year, 4, 1).date(), datetime(year, 6, 30).date()),
        (datetime(year, 7, 1).date(), datetime(year, 9, 30).date()),
        (datetime(year, 10, 1).date(), datetime(year, 12, 31).date()),
    ]
    for sancion in expedientes_a_procesar:
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
            # nit = "800139802"
            # expediente = "96001400"

            ser_service.buscar_data(
                nitOperador=nit,
                expediente=expediente,
                fechaInicial=fecha_inicial_ajustada,  # type: ignore
                fechaFinal=fecha_final_ajustada,  # type: ignore
            )
            # ¡NUEVA LÍNEA! Llamamos al método de descarga después de la búsqueda
            ser_service.descargar_pdfs_de_tabla(
                nit=nit,
                anio=year,
                trimestre=trimestre_num,
                expediente=int(expediente),
                seecion=request.seccion,
            )

    # 6. Cierra la sesión de Playwright DESPUÉS de terminar todos los bucles
    ser_service.close_session()

    download_folder = os.getenv("DOWNLOAD_PATH", "/descargas")
    storageRepository.upload_directory(download_folder)

    # 7. Devuelve los datos de FURES originales, cumpliendo con el response_model
    return expedientes_a_procesar  # type: ignore
