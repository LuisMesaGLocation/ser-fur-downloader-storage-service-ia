#!/bin/bash

# Salir inmediatamente si cualquier comando falla
set -e

clear
gcloud config set project emuclient

# --- Configuración ---
# Define el nombre completo de la imagen y la etiqueta (tag).
IMAGE_NAME="us-central1-docker.pkg.dev/emuclient/cloud-run-source-deploy/mcp-server-bigquery-example:latest"
SERVICE_NAME="mcp-server-bigquery-example"

# --- Lógica del Script ---
echo "Verificando si la imagen '$IMAGE_NAME' ya existe..."

# 'docker image inspect' falla si la imagen no existe.
# Suprimimos la salida (stdout y stderr) para mantener el script limpio.
if docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
  echo "Imagen encontrada. Eliminando la versión anterior..."
  docker rmi "$IMAGE_NAME"
else
  echo "No se encontró una imagen existente. Se creará una nueva."
fi

echo "Construyendo la nueva imagen..."
docker build -t "$IMAGE_NAME" .

echo "Subiendo la imagen a Artifact Registry..."
docker push "$IMAGE_NAME"
echo "La imagen '$IMAGE_NAME' ha sido subida exitosamente."

# --- Despliegue en Google Cloud Run ---
echo "Desplegando el servicio '$SERVICE_NAME' en Cloud Run..."
gcloud run deploy "$SERVICE_NAME" \
  --image "$IMAGE_NAME" \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --cpu 1 \
  --memory 500Mi \
  --timeout 100

echo "¡Script completado! El servicio ha sido desplegado exitosamente."
