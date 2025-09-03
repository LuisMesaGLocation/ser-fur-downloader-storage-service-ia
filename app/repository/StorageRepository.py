import os
from concurrent.futures import ThreadPoolExecutor

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

    def upload_specific_folder(self, folder_to_upload: str, relative_to_path: str):
        """
        Sube el contenido de un subdirectorio específico a GCS, pero construye la ruta
        en el bucket de forma relativa a una carpeta base.

        Args:
            folder_to_upload (str): La ruta específica a la carpeta cuyo contenido se subirá.
            relative_to_path (str): La ruta base para construir el nombre del blob en GCS.
        """
        if not os.path.isdir(folder_to_upload):
            print(
                f"Advertencia: El directorio local '{folder_to_upload}' no existe, no se subirá nada."
            )
            return

        print(f"--- Iniciando subida del directorio '{folder_to_upload}' a GCS ---")

        for root, _, files in os.walk(folder_to_upload):
            for filename in files:
                local_file_path = os.path.join(root, filename)
                destination_blob_name = os.path.relpath(
                    local_file_path, relative_to_path
                )
                destination_blob_name = destination_blob_name.replace("\\", "/")

                print(
                    f"  -> Subiendo '{local_file_path}' a 'gs://{self.bucket_name}/{destination_blob_name}'..."
                )

                try:
                    blob = self.bucket.blob(destination_blob_name)
                    blob.upload_from_filename(local_file_path)

                except Exception as e:
                    print(f"  -> ERROR al subir el archivo {filename}: {e}")

        print(f"--- Subida del directorio '{folder_to_upload}' completada. ---")

    def upload_period_and_images_standalone(
        self,
        base_download_path: str,
        seccion: str,
        anio: int,
        periodo: int,
        nit: str,
        expediente: str,
    ):
        """
        Sube los archivos de un período específico Y las imágenes generales para un ÚNICO NIT.
        Esta función es independiente y ahora es específica para un solo NIT.
        """
        period_folder_name = f"{periodo}T"
        # Construye la ruta directa a la carpeta del NIT que se está procesando
        nit_folder_path = os.path.join(
            base_download_path, seccion, str(anio), f"{nit}-{expediente}"
        )

        if not os.path.isdir(nit_folder_path):
            print(
                f"Advertencia: La carpeta del NIT '{nit_folder_path}' no existe. No se subirá nada."
            )
            return

        print(
            f"--- Iniciando subida para NIT {nit}, período '{period_folder_name}' ---"
        )

        upload_tasks = []

        # --- 1. SUBIR ARCHIVOS DEL PERÍODO (CON DUPLICACIÓN) ---
        period_path = os.path.join(nit_folder_path, period_folder_name)
        if os.path.isdir(period_path):
            print(f"  -> Encontrada carpeta de período '{period_folder_name}'.")
            for root, _, files in os.walk(period_path):
                for filename in files:
                    local_file_path = os.path.join(root, filename)
                    # Tarea 1: Subida a la ruta original
                    dest_blob_original = os.path.relpath(
                        local_file_path, base_download_path
                    ).replace("\\", "/")
                    upload_tasks.append((local_file_path, dest_blob_original))
                    # Tarea 2: Subida a la raíz del período
                    period_root_path = os.path.join(
                        nit_folder_path, period_folder_name, filename
                    )
                    dest_blob_copy = os.path.relpath(
                        period_root_path, base_download_path
                    ).replace("\\", "/")
                    upload_tasks.append((local_file_path, dest_blob_copy))
        else:
            print(f"  -> No se encontró carpeta del período '{period_folder_name}'.")

        # --- 2. SUBIR IMÁGENES GENERALES (.png) DE LA RAÍZ DEL NIT ---
        print("  -> Buscando imágenes generales (.png)...")
        for item in os.listdir(nit_folder_path):
            if item.lower().endswith(".png"):
                local_image_path = os.path.join(nit_folder_path, item)
                if os.path.isfile(local_image_path):
                    dest_blob_image = os.path.relpath(
                        local_image_path, base_download_path
                    ).replace("\\", "/")
                    upload_tasks.append((local_image_path, dest_blob_image))

        if not upload_tasks:
            print("  -> No se encontraron archivos para subir en este NIT y período.")
            return

        print(
            f"  -> {len(upload_tasks)} tareas de subida listas. Ejecutando en paralelo..."
        )

        def _upload_worker(task):
            local_path, destination_path = task
            try:
                blob = self.bucket.blob(destination_path)
                blob.upload_from_filename(local_path)
            except Exception as e:
                print(f"    -> ERROR al subir '{local_path}': {e}")

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(_upload_worker, upload_tasks)

        print(f"--- Subida para NIT {nit} completada. ---")
