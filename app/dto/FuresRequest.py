from pydantic import BaseModel
from typing_extensions import List, Optional


class FuresDataItem(BaseModel):
    # Campos obligatorios, alineados con el tipo de Oficio (str)
    nitOperador: str
    expediente: str

    # Campos opcionales para que coincida con la estructura completa de Oficio
    radicado: Optional[str] = None
    year: Optional[int] = None
    trimestre: Optional[List[int]] = None
    trimestre_asignado: Optional[List[int]] = None
    year_asignado: Optional[int] = None


class FuresRequest(BaseModel):
    token_ser: Optional[str] = None
    year: Optional[int] = None
    nitDesde: Optional[int] = None
    nitHasta: Optional[int] = None
    seccion: str
    data: Optional[List[FuresDataItem]] = None
