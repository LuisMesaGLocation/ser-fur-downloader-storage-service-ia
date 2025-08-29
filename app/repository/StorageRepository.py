import os

from dotenv import load_dotenv
from google.api_core import exceptions
from google.cloud import storage  # type: ignore

# Cargar las variables de entorno para encontrar las credenciales
load_dotenv()


class StorageRepository:
    """
    Gestiona la conexión y las operaciones con un bucket de Google Cloud Storage.
    """

    def __init__(self, bucket_name: str = "contraprestaciones-pro-ser"):
        """
        Inicializa el cliente de Cloud Storage y se asegura de que el bucket exista.

        Args:
            bucket_name (str): El nombre del bucket al que se subirán los archivos.
        """
        self.bucket_name = bucket_name

        try:
            # La autenticación se maneja automáticamente a través de la variable
            # de entorno GOOGLE_APPLICATION_CREDENTIALS.
            self.storage_client = storage.Client()
            self.bucket: Bucket = self.storage_client.bucket(
                self.bucket_name
            )  # <- 2. Añade la anotación de tipo

            if not self.bucket.exists():
                # En un entorno de producción, es mejor que el bucket ya esté creado.
                # Lanzar un error es más seguro que crearlo programáticamente.
                raise FileNotFoundError(
                    f"El bucket de Google Cloud Storage '{self.bucket_name}' no existe."
                )
            print(f"Conectado exitosamente al bucket: '{self.bucket_name}'")

        except exceptions:
            print("Error de autenticación con Google Cloud.")
            print(
                "Asegúrate de que la variable de entorno 'GOOGLE_APPLICATION_CREDENTIALS' está configurada correctamente."
            )
            raise
        except Exception as e:
            print(f"Error al conectar con Google Cloud Storage: {e}")
            raise

    def upload_directory(self, local_directory_path: str):
        """
        Sube el contenido de un directorio local al bucket de Google Cloud Storage,
        manteniendo la estructura de carpetas. Reemplaza cualquier archivo existente.

        Args:
            local_directory_path (str): La ruta al directorio local que se va a subir (ej: 'descargas').
        """
        if not os.path.isdir(local_directory_path):
            print(f"Error: El directorio local '{local_directory_path}' no existe.")
            return

        print(f"--- Iniciando subida del directorio '{local_directory_path}' a GCS ---")

        # os.walk() recorre el árbol de directorios de forma recursiva
        for root, _, files in os.walk(local_directory_path):
            for filename in files:
                # 1. Construir la ruta completa del archivo local
                local_file_path = os.path.join(root, filename)

                # 2. Construir el nombre del "blob" (el archivo en la nube)
                #    Esto mantiene la estructura de carpetas.
                #    Ej: '2023/900014381_96002150/1T/autoliquidacion/evidencia.png'
                destination_blob_name = os.path.relpath(
                    local_file_path, local_directory_path
                )

                # GCS usa '/' como separador, independientemente del SO
                destination_blob_name = destination_blob_name.replace("\\", "/")

                print(
                    f"  -> Subiendo '{local_file_path}' a 'gs://{self.bucket_name}/{destination_blob_name}'..."
                )

                try:
                    # 3. Crear un objeto 'blob' en el bucket
                    blob = self.bucket.blob(destination_blob_name)

                    # 4. Subir el archivo. Por defecto, esto sobrescribe si ya existe.
                    blob.upload_from_filename(local_file_path)

                except Exception as e:
                    print(f"  -> ERROR al subir el archivo {filename}: {e}")

        print("--- Subida de archivos a Google Cloud Storage completada. ---")
