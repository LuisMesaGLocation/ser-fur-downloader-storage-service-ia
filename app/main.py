import os
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.security.http import HTTPBearer
from pydantic.main import BaseModel
from typing_extensions import Any, Dict, Optional

from app.config.cors import configure_cors
from app.dto.FuresRequest import FuresRequest, PeriodicaRequest
from app.gen_pliegos.service import Service as PliegoService
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

security = HTTPBearer()


class FinalResponse(BaseModel):
    furs_logs: List[RpaFursLog]
    pliegos_results: List[Dict[str, Any]]


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
        # nit = "901410065"
        # expediente_str = "96005951"

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
                    radicado_informe=sancion.radicado_informe,
                    fecha_radicado_informe=sancion.fecha_radicado_informe,
                    servicio=sancion.servicio,
                    codigo_servicio=sancion.codigoServicio,
                    expediente_habilitado=sancion.expedienteHabilitado,
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
    summary="Procesar y registrar FURs (versión simplificada sin sesiones ni pliegos)",
    tags=["FURES"],
)
def procesar_fures_simplificado(
    request: PeriodicaRequest,
):
    """
    Versión simplificada del servicio de descarga de FURs.
    - Usa la estructura comprobada del endpoint original (/).
    - Ejecuta procesos en paralelo con ThreadPoolExecutor.
    - No usa sesiones, radicados, Firebase ni generación de pliegos.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import uuid

    ingestion_id = str(uuid.uuid4())
    print(f"🚀 Iniciando procesamiento simplificado para años {request.annos} y trimestres {request.trimestres}...")

    #  Limpieza del directorio de descargas
    download_folder = os.getenv("DOWNLOAD_PATH", "descargas")
    if os.path.exists(download_folder):
        print(f"🧹 Limpiando directorio de descargas principal: {download_folder}")
        shutil.rmtree(download_folder)

    # 🔹 Inicialización de repositorios
    bq_repo = BigQueryRepository()
    storage_repo = StorageRepository()

    # 🔹 Obtener registros desde BigQuery
    registros = bq_repo.obtenerPeriodica(request.annos, request.trimestres)
    if not registros:
        raise HTTPException(status_code=404, detail="No se encontraron registros para los periodos solicitados.")

    # 🔹 Variables globales
    logs_generados_total: List[RpaFursLog] = []
    playwright_lock = threading.Lock()  # evita condiciones de carrera al iniciar Playwright

    # ============================================================
    #  Worker: procesa un registro individual
    # ============================================================
    def procesar_item(item):
        try:
            nit = str(item["Identificacion"])
            expediente = str(item["Expediente"])
            anio = int(item["ANNO"])
            trimestre = int(item["TRIMESTRE"])

            print(f"🧩 Procesando NIT {nit} | Expediente {expediente} | {anio}-T{trimestre}")

            # Calcular fechas del trimestre
            mes_inicio = 3 * (trimestre - 1) + 1
            fecha_inicial = get_next_business_day(date(anio, mes_inicio, 1))
            mes_final = mes_inicio + 2
            if mes_final == 12:
                fecha_final = get_previous_business_day(date(anio, 12, 31))
            else:
                fecha_final = get_previous_business_day(
                    date(anio, mes_final + 1, 1) - timedelta(days=1)
                )

            # Inicializar SER
            ser_service = SerService()
            with playwright_lock:
                ser_service.start_session(request.token_ser)

            # Buscar y descargar datos
            ser_service.buscar_data(
                nitOperador=nit,
                expediente=expediente,
                fechaInicial=fecha_inicial,
                fechaFinal=fecha_final,
            )

            ser_service.descargar_y_clasificar_furs_paginado(
                nit=nit,
                anio=anio,
                expediente=int(expediente),
                seccion="ia",
                trimestres=[trimestre],
            )

            # Subir a Storage
            uploaded_urls, gsutil_paths = storage_repo.upload_period_and_images_standalone(
                base_download_path=ser_service.download_path,
                seccion="ia",
                anio=anio,
                periodo=trimestre,
                nit=nit,
                expediente=expediente,
            )

            # Insertar en BigQuery
            if uploaded_urls:
                log = {
                    "year": anio,
                    "nitOperador": nit,
                    "expediente": expediente,
                    "trimestre": trimestre,
                    "cod_seven": item.get("Cod_Servicio_Seven"),
                    "subido_a_storage": True,
                    "links_documentos": uploaded_urls,
                    "gsutil_log_documents": gsutil_paths,
                    "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                    "codigo_servicio": item.get("Cod_Servicio"),
                    "servicio": item.get("Servicio"),
                    "expediente_habilitado": "NO",
                }
                bq_repo.insert_upload_log(RpaFursLog(**log), ingestion_id=ingestion_id)
                print(f"✅ Log insertado en BigQuery para NIT {nit} | Exp {expediente}")
                return log

        except Exception as e:
            print(f"❌ Error al procesar NIT {item.get('Identificacion')}: {e}")
            return None
        finally:
            try:
                ser_service.close_session()
            except Exception:
                pass

    # ============================================================
    # Ejecución paralela (idéntico al formato del servicio original)
    # ============================================================
    MAX_WORKERS = 6
    print(f"⚙️ Iniciando procesamiento paralelo con {MAX_WORKERS} workers...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {executor.submit(procesar_item, item): item for item in registros}
        for futuro in as_completed(futuros):
            resultado = futuro.result()
            if resultado:
                logs_generados_total.append(resultado)

    print(f"🏁 Procesamiento completado. Total registros procesados: {len(logs_generados_total)}")

    # Respuesta final coherente con el estilo original
    return {
        "summary": {
            "registros_totales": len(registros),
            "registros_procesados": len(logs_generados_total),
            "registros_fallidos": len(registros) - len(logs_generados_total),
        },
        "detalle": logs_generados_total,
    }