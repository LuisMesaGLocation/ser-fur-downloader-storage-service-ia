import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query
from typing_extensions import Any, Dict, Optional

from app.config.cors import configure_cors
from app.dto.FuresRequest import FuresRequest
from app.playwright.SerService import SerService
from app.repository.BigQueryRepository import BigQueryRepository, Oficio, RpaFursLog
from app.repository.StorageRepository import StorageRepository
from app.security.firebase_auth import get_current_user, initialize_firebase_app
from app.utils.fecha_habil_colombia import (
    get_next_business_day,
    get_previous_business_day,
)

# --- INICIO DE CAMBIOS ---

# 1. Se añaden las importaciones necesarias para la paralelización.
# (ThreadPoolExecutor, as_completed) - ya están arriba

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
initialize_firebase_app()

# 2. Se elimina la creación de instancias de servicio globales.
#    Estas se crearán dentro de cada hilo para garantizar el aislamiento.
#    Mantenemos una instancia solo para la LECTURA inicial, que es segura.
repo_lectura = BigQueryRepository()


def procesar_expediente_worker(
    sancion: Oficio,
    request: FuresRequest,
    seccion_final: str,
    request_ingestion_timestamp: str,
) -> List[RpaFursLog]:
    """
    Esta es la función de trabajo. Se ejecuta en un hilo separado para cada expediente.
    Es completamente autónoma para garantizar que no haya conflictos entre hilos.
    """
    # 3. CADA HILO CREA SUS PROPIAS INSTANCIAS DE SERVICIO.
    #    Esto es CRUCIAL para que cada hilo tenga su propio navegador y conexión.
    ser_service = SerService()
    storage_repo = StorageRepository()
    bq_repo = BigQueryRepository()
    logs_generados_hilo = []

    try:
        # 4. TODA LA LÓGICA ORIGINAL DEL BUCLE AHORA VIVE AQUÍ.
        #    Como cada dato (sancion, request) se pasa como argumento,
        #    no hay estado compartido y los cálculos son 100% seguros y aislados.
        if not sancion.nitOperador or not sancion.expediente:
            print(
                f"⚠️ WORKER: Omitiendo registro por falta de NIT o expediente: {sancion}"
            )
            return []

        anio_busqueda = (
            sancion.year if sancion.year is not None else sancion.year_asignado
        )
        if anio_busqueda is None:
            anio_busqueda = (
                request.year if request.year is not None else datetime.now().year
            )

        trimestres_a_subir = (
            sancion.trimestre if sancion.trimestre else sancion.trimestre_asignado
        ) or []

        nit = str(sancion.nitOperador)
        expediente_str = str(sancion.expediente)

        print(
            f"--- WORKER [{os.getpid()}]: Iniciando para NIT: {nit}, Expediente: {expediente_str} ---"
        )
        print(f"  -> Año de búsqueda: {anio_busqueda}")
        print(f"  -> Trimestres a subir: {trimestres_a_subir}")

        # La lógica de fechas se calcula de forma independiente por cada hilo. No hay conflictos.
        start_date = datetime(anio_busqueda, 1, 1).date()
        today = date.today()
        if anio_busqueda < today.year:
            end_date = datetime(anio_busqueda, 12, 31).date()
        else:
            first_day_of_current_month = today.replace(day=1)
            end_date = first_day_of_current_month - timedelta(days=1)
        end_date = today - timedelta(days=1)
        fecha_inicial_ajustada = get_next_business_day(start_date)
        fecha_final_ajustada = get_previous_business_day(end_date)

        # Cada hilo inicia sesión en su propio navegador.
        ser_service.login()

        ser_service.buscar_data(
            nitOperador=nit,
            expediente=expediente_str,
            fechaInicial=fecha_inicial_ajustada,
            fechaFinal=fecha_final_ajustada,
        )

        ser_service.descargar_y_clasificar_pdfs(
            nit=nit,
            anio=anio_busqueda,
            expediente=int(expediente_str),
            seccion=seccion_final,
            trimestres=trimestres_a_subir,
        )

        if not trimestres_a_subir:
            print(f"  -> WORKER [{nit}]: No hay trimestres para subir.")
        else:
            for trimestre in trimestres_a_subir:
                uploaded_urls, gsutil_paths = (
                    storage_repo.upload_period_and_images_standalone(
                        base_download_path=ser_service.download_path,
                        seccion=seccion_final,
                        anio=anio_busqueda,
                        periodo=trimestre,
                        nit=nit,
                        expediente=expediente_str,
                    )
                )
                image_urls: List[str] = []
                gs_images: List[str] = []
                doc_urls: List[str] = []
                gs_docs: List[str] = []
                logs_generados_hilo: list[RpaFursLog] = []
                image_urls, gs_images, doc_urls, gs_docs = [], [], [], []
                for url, gs_path in zip(uploaded_urls, gsutil_paths):
                    if gs_path.lower().endswith((".png", ".jpg", ".jpeg")):
                        image_urls.append(url)
                        gs_images.append(gs_path)
                    elif gs_path.lower().endswith(".pdf"):
                        doc_urls.append(url)
                        gs_docs.append(gs_path)

                log = RpaFursLog(
                    sesion=seccion_final,
                    radicado=sancion.radicado,
                    year=anio_busqueda,
                    nitOperador=nit,
                    expediente=expediente_str,
                    trimestre=trimestre,
                    cod_seven=sancion.cod_seven,
                    subido_a_storage=bool(uploaded_urls),
                    links_imagenes=image_urls,
                    gsutil_log_images=gs_images,
                    links_documentos=doc_urls,
                    gsutil_log_documents=gs_docs,
                    ingestion_timestamp=request_ingestion_timestamp,
                )
                bq_repo.insert_upload_log(log)
                logs_generados_hilo.append(log)

        return logs_generados_hilo

    except Exception as e:
        print(f"❌ ERROR CRÍTICO en worker para NIT {sancion.nitOperador}: {e}")
        return []  # Devolver lista vacía en caso de fallo
    finally:
        # Es vital cerrar la sesión del navegador de este hilo para liberar recursos.
        ser_service.close_session()


# --- FIN DE CAMBIOS EN LÓGICA DE WORKER ---


@app.get("/hola")
def read_root(current_user: Dict[str, Any] = Depends(get_current_user)):
    print(f"✅ Petición autenticada por el usuario: {current_user.get('email')}")
    print(f"UID del usuario: {current_user.get('uid')}")
    return {"Hello": "World"}


@app.post(
    "/",
    response_model=List[RpaFursLog],
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
) -> List[RpaFursLog]:
    request_ingestion_timestamp = datetime.now(timezone.utc).isoformat()
    print(f"✅ Petición de {current_user.get('email')} recibida.")

    # Se limpia el directorio principal una sola vez al inicio.
    download_folder = os.getenv("DOWNLOAD_PATH", "descargas")
    if os.path.exists(download_folder):
        print(f"Limpiando directorio de descargas principal: {download_folder}")
        shutil.rmtree(download_folder)

    # La lógica para obtener la lista de expedientes a procesar no cambia.
    if originData == "database":
        expedientes_a_procesar = repo_lectura.getOficios(radicado=request.radicado)
        # ... (lógica de filtrado si aplica)
    else:
        if not request.data:
            raise HTTPException(status_code=400, detail="El campo 'data' es requerido.")
        expedientes_a_procesar = [Oficio(**item.model_dump()) for item in request.data]

    if not expedientes_a_procesar:
        print("No hay expedientes para procesar. Finalizando.")
        return []

    radicado_principal = expedientes_a_procesar[0].radicado
    base_seccion = request.seccion or "rpa-descargas"
    seccion_final = (
        f"{base_seccion}-{radicado_principal}" if radicado_principal else base_seccion
    )

    # --- INICIO DE CAMBIOS EN EJECUCIÓN ---

    # 5. Se reemplaza el bucle 'for' por el ejecutor de hilos.
    logs_generados_total: List[RpaFursLog] = []
    # Este número debe coincidir con las CPUs asignadas en deploy.sh
    MAX_WORKERS = 4

    print(f"🚀 Iniciando procesamiento paralelo con hasta {MAX_WORKERS} workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Se crea una "tarea futura" para cada expediente.
        futuros = {
            executor.submit(
                procesar_expediente_worker,
                sancion,
                request,
                seccion_final,
                request_ingestion_timestamp,
            ): sancion
            for sancion in expedientes_a_procesar
        }

        # Se procesan los resultados a medida que las tareas finalizan.
        for futuro in as_completed(futuros):
            sancion_original = futuros[futuro]
            try:
                # El resultado de un worker es una lista de logs, que se añade al total.
                resultado_logs = futuro.result()
                if resultado_logs:
                    logs_generados_total.extend(resultado_logs)
            except Exception as exc:
                print(
                    f"❗ Excepción en futuro para expediente {sancion_original.expediente}: {exc}"
                )

    print(
        f"✅ Procesamiento paralelo completado. Se generaron {len(logs_generados_total)} logs."
    )
    return logs_generados_total


# --- FIN DE CAMBIOS ---
