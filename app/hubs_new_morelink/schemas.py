
from typing import List
from pydantic import BaseModel


class DahuoUploadSuccessItem(BaseModel):
    shipmendID: str
    operNo: str
    boxNum: float
    Volume: float
    Weight: float
    fbaWarehouse: str
    zipCode: str
    Sono: str

class DahuoUploadResponse(BaseModel):
    code: int = 200
    message: str = "success"
    data: List[DahuoUploadSuccessItem]  # 明确 data 是 SuccessItem 的列表