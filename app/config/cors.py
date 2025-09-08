from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


# 2. Crea una función para configurar el middleware
def configure_cors(app: FastAPI, origins: list[str]):
    """
    Aplica la configuración de CORS a la instancia de la aplicación FastAPI.
    """
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],  # Permite todos los métodos (GET, POST, etc.)
        allow_headers=["*"],  # Permite todos los encabezados
    )
