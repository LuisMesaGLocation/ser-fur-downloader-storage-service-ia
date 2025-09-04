from dataclasses import dataclass

# PASO 1: Asegúrate de que `cast` está importado desde `typing`
from typing import List

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
