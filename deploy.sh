#!/bin/bash

# Salir inmediatamente si cualquier comando falla
set -e

clear
gcloud config set project mintic-models-dev

# --- Configuración ---
# Define el nombre completo de la imagen y la etiqueta (tag).
IMAGE_NAME="us-central1-docker.pkg.dev/mintic-models-dev/cloud-run-source-deploy/ser-furs-downloader-storage-service-ia:latest"
SERVICE_NAME="ser-furs-downloader-storage-service-ia"

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
  --cpu 6 \
  --memory 6200Mi \
  --timeout 3600 \
  --set-env-vars "SER_URL=https://ser.mintic.gov.co/" \
  --set-env-vars "SER_USER=servinf135121" \
  --set-env-vars "SER_PASSWORD=ServinF-20+5_" \
  --set-env-vars "SER_AUTH_COOKIE=73BCF8DF549F6B6423ED029A9002C25D6C1DD22EEE47997B70B36AD7A2E670F01DD24BDFC3818DF70FB4E6C470CABC06F811FF67F9FABC8FCB3E869C6028D3D67314CF0DE7FC1D944E677033C11E9011740B3EBE7EB13902F30F915CAF674E2C6E63D36969F62ED534651C7B4D8CF6C0F6B33905948B249B81BEEB4F6403FE1874F0EA092C7C846234F55B3FDFB882F503784BAE1D21667D6CBFBD2431B9C3E188D33FF1B9F2E76CC32B599547C1066236033305A57DAEE57F45CE9F82A8735B93895D17820B540C554EBFDD562D74B99177BD264F2902E100B8FCFB8AEABBFE487467370FDB581C65A92CA0C839E78893A9A797E1DFD5A03B9CBC773BCBC45F9457637CDFF443E27AB63CEF413DE2E8D1F4299B072B3CAD406FA6BAD11656FB698924AD" \
  --set-env-vars "SER_URL_CONSUL_FUR=https://ser.mintic.gov.co/Reportes/ObtenerFurGenerados?esUnAnalista=True" \
  --set-env-vars "DOWNLOAD_PATH=/apphome/descargas"


echo "¡Script completado! El servicio ha sido desplegado exitosamente."
