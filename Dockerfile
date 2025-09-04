# 1. Usar una imagen base oficial de Python de Microsoft para Playwright
FROM mcr.microsoft.com/playwright/python:v1.54.0-noble

# 2. Actualizar paquetes e instalar el módulo venv para Python
#    Esto soluciona las vulnerabilidades y nos permite crear entornos virtuales.
RUN apt-get update && \
    apt-get install -y python3.12-venv && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/*

# 3. Crear un entorno virtual y establecerlo como el entorno por defecto
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# 4. Establecer el directorio de trabajo
WORKDIR /apphome

# 5. Copiar los archivos de la aplicación y los requisitos
COPY app app
COPY requirements.txt .

# 6. Instalar las dependencias de Python DENTRO del entorno virtual
RUN pip install --no-cache-dir -r requirements.txt

# 7. Instalar Playwright con las dependencias del navegador Chromium
RUN playwright install --with-deps chromium
RUN mkdir descargas

# 8. Exponer el puerto
EXPOSE 8080

# 9. Comando para iniciar la aplicación (usará el uvicorn del venv)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
