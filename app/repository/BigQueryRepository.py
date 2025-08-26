from dataclasses import dataclass
from datetime import datetime

# PASO 1: Asegúrate de que `cast` está importado desde `typing`
from typing import List, cast

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError


@dataclass
class FuresRecord:
    nitOperador: int
    periodo_fechaInicial: datetime
    periodo_fechaFinal: datetime


class BigQueryRepository:
    """
    Una clase para interactuar con Google BigQuery, encargada de obtener
    y insertar datos relacionados con las obligaciones.
    """

    def __init__(self):
        """Inicializa el cliente de BigQuery."""
        self.bigquery_client = bigquery.Client()

    def obtenerFurer(self) -> List[FuresRecord]:
        """
        Obtiene los 10 registros más recientes de FURES por operador y periodo.
        """
        query_sql = """
            SELECT
                FURES.nitOperador,
                TIMESTAMP(FURES.periodo_fechaInicial) AS periodo_fechaInicial,
                TIMESTAMP(FURES.periodo_fechaFinal) AS periodo_fechaFinal
            FROM
                `mintic-models-dev`.contraprestaciones_pro.SER_ConsultarFuresOperador AS FURES
            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY FURES.nitOperador, FURES.periodo_fechaInicial, FURES.periodo_fechaFinal
                    ORDER BY FURES.ingestion_timestamp DESC
                ) = 1
            LIMIT 1;
        """

        try:
            print("Ejecutando consulta para obtener datos de FURES...")
            query_job = self.bigquery_client.query(query_sql)

            results = [
                FuresRecord(
                    nitOperador=int(row.nitOperador),  # type: ignore
                    periodo_fechaInicial=cast(datetime, row.periodo_fechaInicial),  # type: ignore
                    periodo_fechaFinal=cast(datetime, row.periodo_fechaFinal),  # type: ignore
                )
                for row in query_job.result()  # type: ignore
            ]

            print(f"Consulta finalizada. Se obtuvieron {len(results)} registros.")
            return results

        except GoogleCloudError as e:
            print(f"Error al ejecutar la consulta en BigQuery: {e}")
            return []
