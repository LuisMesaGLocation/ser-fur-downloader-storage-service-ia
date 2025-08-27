# 1. Usar una imagen base oficial de Python más ligera
FROM mcr.microsoft.com/playwright/python:v1.54.0-noble


# 2. Establecer el directorio de trabajo
WORKDIR /apphome


COPY app app
COPY .env .env

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Se instala Playwright con dependencias de Chromium
RUN playwright install --with-deps chromium
RUN mkdir descargas


CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
