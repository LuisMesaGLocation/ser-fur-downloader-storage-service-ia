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
    trimestre: Optional[List[int]]
    trimestre_asignado: Optional[List[int]]
    year_asignado: Optional[int]


@dataclass
class RpaFursLog:
    radicado: Optional[str]
    year: Optional[int]
    nitOperador: Optional[str]
    expediente: Optional[str]
    trimestre: Optional[int]
    subido_a_storage: bool
    links_imagenes: Optional[List[str]] = field(default_factory=list)  # type: ignore
    links_documentos: Optional[List[str]] = field(default_factory=list)  # type: ignore
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

    def getOficios(self) -> List[Oficio]:
        query_sql = """
        SELECT DISTINCT t.radicado,
                t.year,
                t.year_asignado,
                registros.nit AS nitOperador,
                registros.expediente,
                trimestre,
                trimestre_asignado
        FROM `mintic-models-dev`.SANCIONES_DIVIC_PRO.oficios_prueba AS t
                 LEFT JOIN
             UNNEST(t.registros_excel) AS registros;
        """

        try:
            print("Ejecutando consulta de los oficios.")
            query_job = self.bigquery_client.query(query_sql)

            results = [
                Oficio(
                    radicado=row.radicado,  # type: ignore
                    year=row.year,  # type: ignore
                    year_asignado=row.year_asignado,  # type: ignore
                    nitOperador=row.nitOperador,  # type: ignore
                    expediente=row.expediente,  # type: ignore
                    trimestre=row.trimestre,  # type: ignore
                    trimestre_asignado=row.trimestre_asignado,  # type: ignore
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
        table_id = "mintic-models-dev.SANCIONES_DIVIC_PRO.rpa_furs_logs"

        # Convertimos el dataclass a un diccionario para la inserción
        row_to_insert = {
            "radicado": log_entry.radicado,
            "year": log_entry.year,
            "nitOperador": log_entry.nitOperador,
            "expediente": log_entry.expediente,
            "trimestre": log_entry.trimestre,
            "subido_a_storage": log_entry.subido_a_storage,
            "links_imagenes": log_entry.links_imagenes,
            "links_documentos": log_entry.links_documentos,
            "ingestion_timestamp": log_entry.ingestion_timestamp,
        }

        try:
            errors = self.bigquery_client.insert_rows_json(table_id, [row_to_insert])
            if not errors:
                print(
                    f"✅ Log para radicado {log_entry.radicado}, {log_entry.year}-T{log_entry.trimestre} insertado correctamente."
                )
            else:
                print(f"❌ Ocurrieron errores al insertar el log en BigQuery: {errors}")
        except Exception as e:
            print(f"❌ Error crítico al intentar insertar log en BigQuery: {e}")
