import os
from datetime import date
from app.playwright.SerService import SerService
from app.repository.StorageRepository import StorageRepository
from app.utils.fecha_habil_colombia import get_next_business_day, get_previous_business_day

# 🔹 Datos del registro que quieres probar (T2)
nit = "800048212"
expediente = "96002564"
anio = 2025
trimestre = 2
seccion = "ia"

# 🔹 Inicializa los servicios
ser_service = SerService()
storage_repo = StorageRepository()

# 🔹 Calcula las fechas del trimestre 2 (abril-junio)
mes_inicio = 3 * (trimestre - 1) + 1
fecha_inicial = get_next_business_day(date(anio, mes_inicio, 1))
mes_final = mes_inicio + 2
if mes_final == 12:
    fecha_final = get_previous_business_day(date(anio, 12, 31))
else:
    fecha_final = get_previous_business_day(
        date(anio, mes_final + 1, 1) - date.resolution
    )

print(f"📅 Ejecutando prueba para {nit} | {expediente} | {anio}-T{trimestre}")
print(f"Rango de fechas: {fecha_inicial} → {fecha_final}")

# 🔹 Inicia sesión en SER (usa tu token del .env o uno real)
token = os.getenv("TOKEN_SER")
if token:
    ser_service.start_session(token)
else:
    ser_service.login()

# 🔹 Descarga datos del SER
ser_service.buscar_data(
    nitOperador=nit,
    expediente=expediente,
    fechaInicial=fecha_inicial,
    fechaFinal=fecha_final,
)

# 🔹 Descarga y clasifica los FURs
ser_service.descargar_y_clasificar_furs_paginado(
    nit=nit,
    anio=anio,
    expediente=int(expediente),
    seccion=seccion,
    trimestres=[trimestre],
)

# 🔹 Sube al Storage la carpeta del período
print(f"🟦 Iniciando subida manual al Storage para {anio}-T{trimestre}")
uploaded_urls, gsutil_paths = storage_repo.upload_period_and_images_standalone(
    base_download_path=ser_service.download_path,
    seccion=seccion,
    anio=anio,
    periodo=trimestre,
    nit=nit,
    expediente=expediente,
)
print(f"🟩 Subida completa. Archivos subidos: {len(uploaded_urls)}")

# 🔹 Cierra la sesión
ser_service.close_session()
