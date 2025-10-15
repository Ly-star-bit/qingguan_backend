
from typing import List
from pydantic import BaseModel


class DahuoUploadSuccessItem(BaseModel):
    shipmendID: str
    A单号: str
    箱数: str
    体积: str
    实重: str
    fba仓库: str
    邮编: str
    sono: str

class DahuoUploadResponse(BaseModel):
    code: int = 200
    msg: str = "success"
    data: List[DahuoUploadSuccessItem]  # 明确 data 是 SuccessItem 的列表