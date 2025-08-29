from pydantic import BaseModel
from typing_extensions import List, Optional


class FuresDataItem(BaseModel):
    nitOperador: int
    expediente: int


class FuresRequest(BaseModel):
    token_ser: str
    year: Optional[int]
    nitDesde: Optional[int]
    nitHasta: Optional[int]
    seccion: str
    data: Optional[List[FuresDataItem]]
