import os
import shutil
import uuid
import requests
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

def obtener_bearer_token():
    """Autentica en Firebase y devuelve el token Bearer."""
    auth_url = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=AIzaSyDXeQOINuNrpcIO8c6aVdnuRM80QJvR8ME"
    payload = {
        "email": "wilson@glocation.com.co",
        "password": "wilson123",
        "returnSecureToken": True
    }
    try:
        response = requests.post(auth_url, json=payload, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data["idToken"]
    except Exception as e:
        print(f"❌ Error al obtener el token Bearer: {e}")
        return None

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

    ingestion_id = str(uuid.uuid4())
    ingestion_timestamp_global = datetime.now(timezone.utc).isoformat()
    print(f"🚀 Iniciando procesamiento simplificado para años {request.anno} y trimestres {request.trimestre}...")

    #  Limpieza del directorio de descargas
    download_folder = os.getenv("DOWNLOAD_PATH", "descargas")
    if os.path.exists(download_folder):
        print(f"🧹 Limpiando directorio de descargas principal: {download_folder}")
        shutil.rmtree(download_folder)

    # 🔹 Inicialización de repositorios
    bq_repo = BigQueryRepository()
    storage_repo = StorageRepository()

    # 🔹 Obtener registros desde BigQuery
    registros = bq_repo.obtenerPeriodica(request.anno, request.trimestre)
    print(registros)
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
            # mes_inicio = 3 * (trimestre - 1) + 1
            # fecha_inicial = get_next_business_day(date(anio, mes_inicio, 1))
            fecha_inicial = get_next_business_day(date(anio, 1, 1))
            # mes_final = mes_inicio + 2
            fecha_final = get_previous_business_day(date(anio, 12, 31))
            # if mes_final == 12:
            #     fecha_final = get_previous_business_day(date(anio, 12, 31))
            # else:
            #     fecha_final = get_previous_business_day(
            #         date(anio, mes_final + 1, 1) - timedelta(days=1)
            #     )

            # Inicializar SER
            ser_service = SerService()

            # Inicio de sesion con token de request
            # with playwright_lock:
            #     ser_service.start_session(request.token_ser)

            with playwright_lock:
                # Inicio de sesion con token de request
                if getattr(request, "token_ser", None):
                    print("🔐 Iniciando sesión con token_ser (localStorage)")
                    ser_service.start_session(request.token_ser)
                else:
                # Inicio de sesion manual
                    print("🔑 Iniciando sesión manual en el SER (login con usuario y contraseña)...")
                    ser_service.login()

            # nit = "10722639"
            # expediente = "96003411"

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
            print(f"🟦 Iniciando subida a Storage para NIT {nit} | {anio}-T{trimestre}...")
            uploaded_urls, gsutil_paths = storage_repo.upload_period_and_images_standalone(
                base_download_path=ser_service.download_path,
                seccion="ia",
                anio=anio,
                periodo=trimestre,
                nit=nit,
                expediente=expediente,
            )
            print(f"🟩 Finalizó subida a Storage para {nit}: {len(uploaded_urls)} archivos subidos.")

            # Clasificar archivos según tipo
            image_urls, gs_images, doc_urls, gs_docs = [], [], [], []
            for url, gs_path in zip(uploaded_urls, gsutil_paths):
                if gs_path.lower().endswith((".png", ".jpg", ".jpeg")):
                    image_urls.append(url)
                    gs_images.append(gs_path)
                elif gs_path.lower().endswith(".pdf"):
                    doc_urls.append(url)
                    gs_docs.append(gs_path)

            # Insertar en BigQuery
            log = {
                "year": anio,
                "nitOperador": nit,
                "expediente": expediente,
                "trimestre": trimestre,
                "cod_seven": item.get("Cod_Servicio_Seven"),
                "subido_a_storage": bool(uploaded_urls),
                "links_imagenes": image_urls,
                "gsutil_log_images": gs_images,
                "links_documentos": doc_urls,
                "gsutil_log_documents": gs_docs,
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "codigo_servicio": item.get("Cod_Servicio"),
                "servicio": item.get("Servicio"),
                "expediente_habilitado": "NO",
                "ingestion_timestamp_global": ingestion_timestamp_global,
            }

            bq_repo.insert_upload_log(RpaFursLog(**log), ingestion_id=ingestion_id)
            print(f"✅ Log insertado en BigQuery para NIT {nit} | Exp {expediente}")
            return log
        except Exception as e:
                print(f"⚠️ Error menor al procesar NIT {item.get('Identificacion')}: {e}")
                return None  # No detiene todo el flujo
        finally:
            try:
                ser_service.close_session()
            except Exception:
                pass


    # ============================================================
    # Ejecución paralela (idéntico al formato del servicio original)
    # ============================================================
    MAX_WORKERS = 4
    print(f"⚙️ Iniciando procesamiento paralelo con {MAX_WORKERS} workers...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {executor.submit(procesar_item, item): item for item in registros}
        for futuro in as_completed(futuros):
            resultado = futuro.result()
            if resultado:
                logs_generados_total.append(resultado)

    print(f"🏁 Procesamiento completado. Total registros procesados: {len(logs_generados_total)}")

    # ============================================================
    #  Enviar notificación a md-sanciones-gen-ia 
    # ============================================================
    # LE FALTA EL INGESTION TIME STAMP GLOBAL SI ES QUE SE QUIERE VOLVER A PONER EN ESTE CODIGO
    # try:
    #     token = obtener_bearer_token()
    #     if token:
    #         print("🔑 Token obtenido correctamente, llamando al servicio md-sanciones-gen-ia...")"registros_totales": len(registros),
    #         headers = {"Authorization": f"Bearer {token}"}
    #         notify_url = "https://md-sanciones-gen-ia-120048616777.us-central1.run.app"
    #         payload = {"ingestion_id": ingestion_id}

    #         notify_response = requests.post(notify_url, json=payload, headers=headers)
    #         print(f"📡 Respuesta del servicio IA: {notify_response.status_code} - {notify_response.text[:300]}")
    #     else:
    #         print("⚠️ No se pudo obtener token Bearer, no se notificará al servicio IA.")
    # except Exception as e:
    #     print(f"❌ Error al notificar a md-sanciones-gen-ia: {e}")

    # # Respuesta final coherente con el estilo original
    # return {
    #     "summary": {
    #         "registros_totales": len(registros),
    #         "registros_procesados": len(logs_generados_total),
    #         "registros_fallidos": len(registros) - len(logs_generados_total),
    #     },
    #     "detalle": logs_generados_total,
    # }
