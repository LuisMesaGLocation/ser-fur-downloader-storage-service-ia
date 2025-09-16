from typing import Optional

import requests


class Service:
    def __init__(self):
        self.base_url = (
            "https://us-central1-mintic-models-dev.cloudfunctions.net/md-sanciones-gen/"
        )

    def get_pliegos(self, cod_sesion: str, token: Optional[str] = None):
        """
        Llama a la API para generar pliegos para una sesión específica.

        Args:
            cod_sesion: El código de sesión para el que se generarán los pliegos.
            token: El token de autenticación Bearer.
        """
        if not token:
            print(
                "⚠️ No se proporcionó token. La llamada a la API de pliegos podría fallar."
            )
            headers = {}
        else:
            headers = {"Authorization": f"Bearer {token}"}

        params = {"cod_sesion": cod_sesion}

        try:
            print(
                f"🚀 Llamando a la API de generación de pliegos para la sesión: {cod_sesion}"
            )
            response = requests.get(self.base_url, headers=headers, params=params)
            response.raise_for_status()  # Lanza una excepción para respuestas 4xx/5xx
            print(
                f"✅ Respuesta exitosa de la API de pliegos para la sesión {cod_sesion}: {response.status_code}"
            )
            return response.json()
        except requests.exceptions.RequestException as e:
            print(
                f"❌ Error al llamar a la API de generación de pliegos para la sesión {cod_sesion}: {e}"
            )
            # Dependiendo de la necesidad, podrías querer relanzar la excepción o manejarla.
            # Por ahora, solo imprimimos el error.
            return None
