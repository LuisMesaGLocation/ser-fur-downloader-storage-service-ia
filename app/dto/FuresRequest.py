from pydantic import BaseModel
from typing_extensions import List, Optional


class FuresDataItem(BaseModel):
    nitOperador: int
    expediente: int


class FuresRequest(BaseModel):
    token_ser: Optional[str] = None
    year: Optional[int] = None
    nitDesde: Optional[int] = None
    nitHasta: Optional[int] = None
    seccion: str
    data: Optional[List[FuresDataItem]] = None
