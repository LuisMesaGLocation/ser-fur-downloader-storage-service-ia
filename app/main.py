import os
import shutil
from datetime import date, datetime, timedelta
from typing import List

from fastapi import FastAPI, HTTPException, Query
from typing_extensions import Optional

from app.dto.FuresRequest import FuresRequest
from app.playwright.SerService import SerService
from app.repository.BigQueryRepository import BigQueryRepository, Expediente
from app.repository.StorageRepository import StorageRepository
from app.utils.fecha_habil_colombia import (
    get_next_business_day,
    get_previous_business_day,
)

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

    start_date = datetime(year, 1, 1).date()

    # --- CAMBIO EN LA LÓGICA DE LA FECHA FINAL ---
    # 1. Obtener el primer día del mes actual
    today = date.today()
    first_day_of_current_month = today.replace(day=1)

    # 2. Restar un día para obtener el último día del mes anterior
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

    # 3. Asegurarse de que esa fecha sea un día hábil (retrocediendo si es necesario)
    end_date = get_previous_business_day(last_day_of_previous_month)

    for sancion in expedientes_a_procesar:
        nit = str(sancion.nitOperador)
        expediente = str(sancion.expediente)

        print(
            f"--- Iniciando procesamiento para NIT: {nit}, Expediente: {expediente}, Año: {year} ---"
        )
        fecha_inicial_ajustada = get_next_business_day(start_date)
        fecha_final_ajustada = end_date  # La fecha final ya es el día hábil correcto

        print(
            f"  -> Buscando en rango: {fecha_inicial_ajustada.strftime('%d/%m/%Y')} a {fecha_final_ajustada.strftime('%d/%m/%Y')}"
        )

        # nit = "900014381"
        # expediente = "96002150"
        # nit = "800139802"
        # expediente = "96001400"

        ser_service.buscar_data(
            nitOperador=nit,
            expediente=expediente,
            fechaInicial=fecha_inicial_ajustada,  # type: ignore
            fechaFinal=fecha_final_ajustada,  # type: ignore
        )

        ser_service.descargar_y_clasificar_pdfs(
            nit=nit,
            anio=year,
            expediente=int(expediente),
            seccion=request.seccion,
        )
        storageRepository.upload_period_and_images_standalone(
            base_download_path=download_folder,
            seccion=request.seccion,
            anio=year,
            periodo=1,
            nit=nit,
            expediente=expediente,
        )
        """
        nit_folder_path = os.path.join(
            download_folder, request.seccion, str(year), f"{nit}-{expediente}"
        )

        storageRepository.upload_specific_folder(
            folder_to_upload=nit_folder_path, relative_to_path=download_folder
        )"""

    # 6. Cierra la sesión de Playwright DESPUÉS de terminar todos los bucles
    ser_service.close_session()

    # download_folder = os.getenv("DOWNLOAD_PATH", "/descargas")
    # storageRepository.upload_directory(download_folder)

    # 7. Devuelve los datos de FURES originales, cumpliendo con el response_model
    return expedientes_a_procesar  # type: ignore
