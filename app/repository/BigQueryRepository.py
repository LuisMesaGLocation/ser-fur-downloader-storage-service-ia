from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError


@dataclass
class Sancion:
    nitOperador: int
    expediente: int


@dataclass
class Expediente:
    nitOperador: int
    expediente: int


@dataclass
class Oficio:
    radicado: Optional[str]
    year: Optional[int]
    nitOperador: Optional[str]
    expediente: Optional[str]
    cod_seven: Optional[str]
    trimestre: Optional[List[int]]
    trimestre_asignado: Optional[List[int]]
    year_asignado: Optional[int]


@dataclass
class RpaFursLog:
    sesion: Optional[str]
    radicado: Optional[str]
    year: Optional[int]
    nitOperador: Optional[str]
    expediente: Optional[str]
    trimestre: Optional[int]
    cod_seven: Optional[str]
    subido_a_storage: bool
    links_imagenes: Optional[List[str]] = field(default_factory=list)  # type: ignore
    gsutil_log_images: Optional[List[str]] = field(default_factory=list)  # type: ignore
    links_documentos: Optional[List[str]] = field(default_factory=list)  # type: ignore
    gsutil_log_documents: Optional[List[str]] = field(default_factory=list)  # type: ignore
    ingestion_timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class BigQueryRepository:
    """
    Una clase para interactuar con Google BigQuery, encargada de obtener
    y insertar datos relacionados con las obligaciones.
    """

    def __init__(self):
        """Inicializa el cliente de BigQuery."""
        self.bigquery_client = bigquery.Client()

    def obtenerSanciones(self) -> List[Sancion]:
        query_sql = """
        SELECT DISTINCT Sanciones.EXPEDIENTE     AS expediente,
                        Sanciones.Identificacion AS nitOperador
        FROM `mintic-models-dev`.contraprestaciones_pro.oficios AS Sanciones
        WHERE ESTADO = 1 LIMIT 10;
        """

        try:
            print("Ejecutando consulta para obtener datos de FURES...")
            query_job = self.bigquery_client.query(query_sql)

            results = [
                Sancion(
                    nitOperador=int(row.nitOperador),  # type: ignore
                    expediente=row.expediente,  # type: ignore
                )
                for row in query_job.result()  # type: ignore
            ]

            print(f"Consulta finalizada. Se obtuvieron {len(results)} registros.")
            return results

        except GoogleCloudError as e:
            print(f"Error al ejecutar la consulta en BigQuery: {e}")
            return []

    def obtenerExpedientes(self) -> List[Expediente]:
        query_sql = """
        SELECT DISTINCT Sanciones.EXPEDIENTE     AS expediente,
                        Sanciones.Identificacion AS nitOperador
        FROM `mintic-models-dev`.contraprestaciones_pro.oficios AS Sanciones
        WHERE ESTADO = 1
        ORDER BY nitOperador, expediente ASC;
        ---LIMIT 186 OFFSET 152;
        """

        try:
            print("Ejecutando consulta para obtener datos de FURES...")
            query_job = self.bigquery_client.query(query_sql)

            results = [
                Expediente(
                    nitOperador=int(row.nitOperador),  # type: ignore
                    expediente=row.expediente,  # type: ignore
                )
                for row in query_job.result()  # type: ignore
            ]

            print(f"Consulta finalizada. Se obtuvieron {len(results)} registros.")
            return results

        except GoogleCloudError as e:
            print(f"Error al ejecutar la consulta en BigQuery: {e}")
            return []

    def getOficios(self, radicado: Optional[str] = None) -> List[Oficio]:
        # Base de la consulta sin WHERE ni QUALIFY
        query_sql = """
        SELECT DISTINCT t.radicado,
                t.year,
                t.year_asignado,
                t.ingestion_timestamp,
                registros.nit AS nitOperador,
                registros.expediente,
                registros.cod_seve AS cod_seven,
                trimestre,
                trimestre_asignado
        FROM `mintic-models-dev`.SANCIONES_DIVIC_PRO.oficios AS t
                LEFT JOIN
            UNNEST(t.registros_excel) AS registros
        """

        # Inicializar lista de parámetros y cláusulas WHERE
        query_params = []
        where_clauses = []

        # Añadir filtro por radicado si se proporciona
        if radicado:
            where_clauses.append("t.radicado = @radicado")
            query_params.append(
                bigquery.ScalarQueryParameter("radicado", "STRING", radicado)
            )

        # Construir la cláusula WHERE si hay condiciones
        if where_clauses:
            query_sql += " WHERE " + " AND ".join(where_clauses)

        # Añadir la cláusula QUALIFY al final
        query_sql += " QUALIFY RANK() OVER (PARTITION BY t.radicado ORDER BY t.ingestion_timestamp DESC) = 1;"

        # Configurar los parámetros de la consulta
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            print("Ejecutando consulta de los oficios.")
            # Ejecutar la consulta con su configuración
            query_job = self.bigquery_client.query(query_sql, job_config=job_config)

            results = [
                Oficio(
                    radicado=row.radicado,  # type: ignore
                    year=row.year,  # type: ignore
                    year_asignado=row.year_asignado,  # type: ignore
                    nitOperador=row.nitOperador,  # type: ignore
                    expediente=row.expediente,  # type: ignore
                    trimestre=row.trimestre,  # type: ignore
                    trimestre_asignado=row.trimestre_asignado,  # type: ignore
                    cod_seven=row.cod_seven,  # type: ignore
                )
                for row in query_job.result()  # type: ignore
            ]

            print(f"Consulta finalizada. Se obtuvieron {len(results)} registros.")
            return results

        except GoogleCloudError as e:
            print(f"Error al ejecutar la consulta en BigQuery: {e}")
            return []

    def insert_upload_log(self, log_entry: RpaFursLog):
        """
        Inserta un registro de log en la tabla rpa_furs_logs de BigQuery.
        """
        table_id = "mintic-models-dev.SANCIONES_DIVIC_PRO.rpa_furs_logs_v2"

        row_to_insert = {
            "sesion": log_entry.sesion,
            "radicado": log_entry.radicado,
            "year": log_entry.year,
            "nitOperador": log_entry.nitOperador,
            "expediente": log_entry.expediente,
            "trimestre": log_entry.trimestre,
            "subido_a_storage": log_entry.subido_a_storage,
            "links_imagenes": log_entry.links_imagenes,
            "gsutil_log_images": log_entry.gsutil_log_images,
            "links_documentos": log_entry.links_documentos,
            "gsutil_log_documents": log_entry.gsutil_log_documents,
            "ingestion_timestamp": log_entry.ingestion_timestamp,
            "codigo_seven": log_entry.cod_seven,
        }

        try:
            errors = self.bigquery_client.insert_rows_json(table_id, [row_to_insert])  # type: ignore
            if not errors:
                print(
                    f"✅ Log para radicado {log_entry.radicado}, {log_entry.year}-T{log_entry.trimestre} insertado."
                )
            else:
                print(f"❌ Errores al insertar el log en BigQuery: {errors}")
        except Exception as e:
            print(f"❌ Error crítico al insertar log en BigQuery: {e}")
