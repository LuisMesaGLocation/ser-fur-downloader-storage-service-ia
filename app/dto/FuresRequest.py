from pydantic import BaseModel
from typing_extensions import List, Optional


class FuresDataItem(BaseModel):
    # Campos obligatorios, alineados con el tipo de Oficio (str)
    nitOperador: str
    expediente: str
    radicado: Optional[str] = None
    year: Optional[int] = None
    trimestre: Optional[List[int]] = None
    trimestre_asignado: Optional[List[int]] = None
    year_asignado: Optional[int] = None
    cod_seven: Optional[str] = None


class FuresRequest(BaseModel):
    token_ser: Optional[str] = None
    year: Optional[int] = None
    nitDesde: Optional[int] = None
    nitHasta: Optional[int] = None
    seccion: Optional[str] = None
    radicado: Optional[str] = None
    data: Optional[List[FuresDataItem]] = None
