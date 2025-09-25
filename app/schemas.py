from datetime import datetime
from typing import List, Optional, Union
from uuid import UUID
from fastapi import File, UploadFile
from pydantic import BaseModel

from app.dadan.models import CustomClearHistorySummaryLog


class Policy(BaseModel):
    sub: str
    obj: str
    act: str
    eft: str = None

class UpdatePolicy(BaseModel):
    old_sub: str
    old_obj: str
    old_act: str
    old_eft: str
    new_sub: str
    new_obj: str
    new_act: str
    new_eft: str

class Group(BaseModel):
    user: str
    group: str


class UserCreate(BaseModel):
    username: str
    password: str
class UserLogin(BaseModel):
    username: str
    password: str
class UserUpdate(BaseModel):
    username: Optional[str]
    password: Optional[str]
    # permissions: Optional[List[str]]

# 定义请求体数据模型
class ProductData(BaseModel):
    product_name: str
    box_num: int
    single_price: float = None
    packing: Union[int, float] = None

class ShippingRequest(BaseModel):
    export_country:str="China"
    import_country:str=""
    predict_tax_price:float=0.00
    totalyugutax: float=0.00
    port:str
    packing_type:str
    shipper_name: str
    receiver_name: str
    master_bill_no: str
    gross_weight: float
    volume: float
    execute_type:str
    currency_type:str="CAD"
    product_list: List[ProductData]
class DaleiCreate(BaseModel):
    hs_code: Optional[str]
    英文大类: Optional[str]
    中文大类: Optional[str]


class FileInfo(BaseModel):
    name: str
    time: datetime

class SummaryResponse(BaseModel):
    summaries: List[CustomClearHistorySummaryLog]
    total: Optional[int] = None
    total_pages: Optional[int] = None

class update_cumstom_clear_history_summary_remarks(BaseModel):
    remarks:str
    id:UUID




class UpdateUserMenuPermissions(BaseModel):
    user_id: str
    menu_ids: List[str]

class UpdateUserApiPermissions(BaseModel):
    user_id: str
    api_ids: List[str]

class DownloadOrderListRequest(BaseModel):
    urls: List[str]

class OutputSelectedLogRequest(BaseModel):
    id_list: List[str]
    start_time: Optional[str]
    end_time: Optional[str]



class PackingType(BaseModel):
    packing_type: str
    sender_name: Optional[str]
    receiver_name: Optional[str]
    remarks: Optional[str]=""
    check_remarks: Optional[str]=""
    country: Optional[str]="China"
    check_data: Optional[list]=[]
class ShuidanFileUpload(BaseModel):
    id: str
    file_type: str
    file: UploadFile = File(...)