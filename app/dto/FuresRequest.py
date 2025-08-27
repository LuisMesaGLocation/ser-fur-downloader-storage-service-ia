from pydantic import BaseModel


class FuresRequest(BaseModel):
    token_ser: str
    year: int
