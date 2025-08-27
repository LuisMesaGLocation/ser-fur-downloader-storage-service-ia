from dataclasses import dataclass

# PASO 1: Asegúrate de que `cast` está importado desde `typing`
from typing import List

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError


@dataclass
class Sancion:
    yearVigencia: int
    nitOperador: int
    expediente: int
    trimestre: int


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
        SELECT DISTINCT Sanciones.ANO_VIGENCIA                                      AS yearVigencia,
                        Sanciones.NIT                                               AS nitOperador,
                        Sanciones.EXPEDIENTE                                        AS expediente,
                        CAST(REPLACE(LOWER(Sanciones.TRIMESTRE), 't', '') AS INT64) AS trimestre
        FROM `mintic-models-dev`.SANCIONES_DIVIC_PRO.SANCIONES_DIVIC_PRO AS Sanciones
        WHERE Sanciones.CANCELADOS = 'NO';
        """

        try:
            print("Ejecutando consulta para obtener datos de FURES...")
            query_job = self.bigquery_client.query(query_sql)

            results = [
                Sancion(
                    yearVigencia=int(row.yearVigencia),  # type: ignore
                    nitOperador=int(row.nitOperador),  # type: ignore
                    expediente=row.expediente,  # type: ignore
                    trimestre=int(row.trimestre),  # type: ignore
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
        SELECT VW_E_BDU.Identificacion AS nitOperador,
               VW_E_BDU.Expediente     AS expediente,
        FROM `mintic-models-dev`.contraprestaciones_pro.VW_EXPEDIENTES_BDU AS VW_E_BDU
                 INNER JOIN
             `mintic-models-dev`.contraprestaciones_pro.rues_nit AS RUES
             ON SAFE_CAST(VW_E_BDU.Identificacion AS INT64) = SAFE_CAST(RUES.numIdTributaria AS INT64)
                 LEFT JOIN
             UNNEST(RUES.informacionRepresentanteLegalPrincipal) AS rep_legal_principal
        WHERE TRIM(VW_E_BDU.`Tipo Identificacion`) = 'NIT'
          AND RUES.nomEstadoMatricula != 'CANCELADA'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY VW_E_BDU.Identificacion, VW_E_BDU.Expediente
            ORDER BY CAST(VW_E_BDU.FECHA_EJECUCION AS TIMESTAMP) DESC
            ) = 1 ORDER BY nitOperador ASC, expediente ASC;
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
