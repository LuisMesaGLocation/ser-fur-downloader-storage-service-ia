import os
import shutil
from datetime import date, datetime, timedelta
from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query
from typing_extensions import Any, Dict, Optional

from app.config.cors import configure_cors
from app.dto.FuresRequest import FuresRequest
from app.playwright.SerService import SerService
from app.repository.BigQueryRepository import BigQueryRepository, Expediente, Oficio
from app.repository.StorageRepository import StorageRepository
from app.security.firebase_auth import get_current_user, initialize_firebase_app
from app.utils.fecha_habil_colombia import (
    get_next_business_day,
    get_previous_business_day,
)

app = FastAPI(
    title="Servicio de Descarga FURES",
    description="Una API para interactuar con los datos de FURES en BigQuery.",
    version="1.0.0",
)

origins = [
    "http://localhost:3000",
    "https://dev.modelos.mintic.gov.co",
    "https://modelos.mintic.gov.co",
]

configure_cors(app, origins)
# Llama a la función de inicialización cuando este módulo se cargue
initialize_firebase_app()
repo = BigQueryRepository()
ser_service = SerService()
storageRepository = StorageRepository()


@app.get("/hola")
def read_root(
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    print(f"✅ Petición autenticada por el usuario: {current_user.get('email')}")
    print(f"UID del usuario: {current_user.get('uid')}")
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
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    print(f"✅ Petición autenticada por el usuario: {current_user.get('email')}")
    print(f"UID del usuario: {current_user.get('uid')}")

    download_folder = ser_service.download_path
    if os.path.exists(download_folder):
        print(f"Limpiando directorio de descargas principal: {download_folder}")
        shutil.rmtree(download_folder)

    expedientes_a_procesar: List[Oficio]
    if originData == "database":
        print("📊 Obteniendo datos desde BigQuery...")
        expedientes_a_procesar = repo.getOficios()

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
                for exp in expedientes_a_procesar
                if exp.nitOperador is not None
                and request.nitDesde <= int(exp.nitOperador) <= request.nitHasta
            ]
            print(
                f"✅ Se encontraron {len(expedientes_a_procesar)} registros para procesar en el rango."
            )
    else:
        print("📄 Usando datos del body de la petición...")
        if not request.data:
            raise HTTPException(
                status_code=400,
                detail="Cuando no se usa 'originData=database', el campo 'data' es requerido en el body.",
            )
        expedientes_a_procesar = [
            Oficio(
                radicado=item.radicado,
                year=item.year,
                nitOperador=item.nitOperador,
                expediente=item.expediente,
                trimestre=item.trimestre,
                trimestre_asignado=item.trimestre_asignado,
                year_asignado=item.year_asignado,
            )
            for item in request.data
        ]
        print(
            f"✅ Se procesarán {len(expedientes_a_procesar)} registros enviados en el body."
        )

    if not expedientes_a_procesar:
        print("No hay expedientes para procesar. Finalizando.")
        return List[Expediente]

    radicado_principal = expedientes_a_procesar[0].radicado
    seccion_final: str = request.seccion
    if radicado_principal:
        seccion_final = f"{request.seccion}-{radicado_principal}"

    ser_service.login()

    for sancion in expedientes_a_procesar:
        if not sancion.nitOperador or not sancion.expediente:
            print(f"⚠️ Omitiendo registro por falta de NIT o expediente: {sancion}")
            continue

        # --- NUEVA LÓGICA DE DECISIÓN POR EXPEDIENTE ---
        # 1. Decidir el año a utilizar para la búsqueda
        anio_busqueda = (
            sancion.year if sancion.year is not None else sancion.year_asignado
        )
        if anio_busqueda is None:
            # Fallback al año de la petición o al año actual si no hay ninguno
            anio_busqueda = (
                request.year if request.year is not None else datetime.now().year
            )

        # 2. Decidir la lista de trimestres a subir al Storage
        trimestres_a_subir = (
            sancion.trimestre if sancion.trimestre else sancion.trimestre_asignado
        )
        if trimestres_a_subir is None:
            trimestres_a_subir = []  # Si no hay trimestres especificados, no se subirá nada.

        nit = str(sancion.nitOperador)
        expediente = str(sancion.expediente)

        nit = "900551918"
        expediente = "96002072"
        print(
            f"--- Iniciando procesamiento para NIT: {nit}, Expediente: {expediente} ---"
        )
        print(f"  -> Año de búsqueda decidido: {anio_busqueda}")
        print(f"  -> Trimestres a subir al Storage: {trimestres_a_subir}")

        # --- LÓGICA DE FECHAS AHORA DENTRO DEL BUCLE ---
        start_date = datetime(anio_busqueda, 1, 1).date()
        today = date.today()
        # Si el año de búsqueda no es el actual, buscamos hasta el final de ese año.
        if anio_busqueda < today.year:
            end_date = datetime(anio_busqueda, 12, 31).date()
        else:  # Si es el año actual, buscamos hasta el mes anterior.
            first_day_of_current_month = today.replace(day=1)
            last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
            end_date = last_day_of_previous_month

        fecha_inicial_ajustada = get_next_business_day(start_date)
        fecha_final_ajustada = get_previous_business_day(end_date)
        # --- FIN LÓGICA DE FECHAS ---

        print(
            f"  -> Buscando en SER en rango: {fecha_inicial_ajustada.strftime('%d/%m/%Y')} a {fecha_final_ajustada.strftime('%d/%m/%Y')}"
        )

        ser_service.buscar_data(
            nitOperador=nit,
            expediente=expediente,
            fechaInicial=fecha_inicial_ajustada,
            fechaFinal=fecha_final_ajustada,
        )

        # Esta función ya descarga y clasifica todo lo que encuentra en la web en carpetas correctas
        ser_service.descargar_y_clasificar_pdfs(
            nit=nit,
            anio=anio_busqueda,  # Usamos el año de búsqueda como referencia
            expediente=int(expediente),
            seccion=seccion_final,
        )

        # --- NUEVA LÓGICA DE SUBIDA SELECTIVA AL STORAGE ---
        if not trimestres_a_subir:
            print(
                "  -> No hay trimestres especificados para subir al Storage. Omitiendo subida."
            )
        else:
            print(
                f"  -> Iniciando subida selectiva para los trimestres: {trimestres_a_subir}"
            )
            for trimestre in trimestres_a_subir:
                print(
                    f"    -> Subiendo datos para el período: {anio_busqueda}-T{trimestre}"
                )
                storageRepository.upload_period_and_images_standalone(
                    base_download_path=download_folder,
                    seccion=seccion_final,
                    anio=anio_busqueda,
                    periodo=trimestre,
                    nit=nit,
                    expediente=expediente,
                )

    ser_service.close_session()

    # La respuesta final sigue siendo la misma, convirtiendo a Expediente
    respuesta_final: List[Expediente] = []
    for oficio in expedientes_a_procesar:
        if oficio.nitOperador and oficio.expediente:
            try:
                respuesta_final.append(
                    Expediente(
                        nitOperador=int(oficio.nitOperador),
                        expediente=int(oficio.expediente),
                    )
                )
            except (ValueError, TypeError):
                pass

    return respuesta_final
