from functools import lru_cache

import firebase_admin  # type: ignore
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from firebase_admin import auth  # type: ignore
from typing_extensions import Any, Dict

# El esquema de seguridad sigue siendo el mismo
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@lru_cache()
def initialize_firebase_app():
    """
    Inicializa la aplicación de Firebase Admin SDK de forma inteligente.

    - En Cloud Run/Functions, usa las credenciales del entorno automáticamente.
    - En local, busca la variable de entorno GOOGLE_APPLICATION_CREDENTIALS
      que apunta al archivo serviceAccountKey.json.
    """
    try:
        # Si no se pasan credenciales, el SDK buscará automáticamente
        # las credenciales predeterminadas de la aplicación (ADC).
        print(
            "🚀 Intentando inicializar Firebase Admin SDK con credenciales predeterminadas..."
        )
        firebase_admin.initialize_app()  # type: ignore
        print("✅ Firebase Admin SDK inicializado correctamente.")
    except Exception as e:
        print(f"❌ Error al inicializar Firebase: {e}")
        raise RuntimeError(
            "No se pudo inicializar Firebase. "
            "En un entorno local, asegúrate de que la variable de entorno 'GOOGLE_APPLICATION_CREDENTIALS' "
            "esté configurada y apunte a tu archivo de clave de servicio."
        )


# La dependencia get_current_user no necesita cambios
async def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """
    Dependencia de FastAPI para validar el token de Firebase y obtener los datos del usuario.
    """
    try:
        decoded_token: Dict[str, Any] = auth.verify_id_token(token)  # type: ignore
        return decoded_token  # type: ignore
    except auth.ExpiredIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="El token ha expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except auth.InvalidIdTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token de ID inválido: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ocurrió un error al validar el token: {e}",
        )


def require_permission(required_permission: str):
    """
    Esta es una 'fábrica' de dependencias. Devuelve una nueva dependencia que
    verifica si un permiso específico existe en la lista 'permissions' del usuario.
    """

    def permission_checker(
        current_user: Dict[str, Any] = Depends(get_current_user),
    ) -> Dict[str, Any]:
        user_permissions = current_user.get("permissions", [])
        if required_permission not in user_permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Acceso denegado. Se requiere el permiso: '{required_permission}'.",
            )
        return current_user

    return permission_checker
