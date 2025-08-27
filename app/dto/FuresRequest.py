from pydantic import BaseModel
from typing_extensions import Optional


class FuresRequest(BaseModel):
    token_ser: str
    year: Optional[int]
    nitDesde: Optional[int]
    nitHasta: Optional[int]
