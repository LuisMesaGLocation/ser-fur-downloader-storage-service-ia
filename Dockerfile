# 1. Usar una imagen base oficial de Python más ligera
FROM python:3.12-slim

# 2. Establecer el directorio de trabajo
WORKDIR /apphome

COPY app app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
