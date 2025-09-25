

from typing import List
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    HTTPException
)

from pydantic import BaseModel,Field
from typing import Optional
from app.db_mongo import get_session
class IpWhiteList(BaseModel):
    id: Optional[str] = Field(default=None, primary_key=True)
    ip: str = Field(max_length=255, nullable=False)

    remarks: str = Field(default="", max_length=255, nullable=False)
ip_white_list_router = APIRouter(tags=["ip白名单"],prefix="/ip_white_list")

@ip_white_list_router.post("/", response_model=IpWhiteList, summary="添加IP白名单")
def create_ip_white_list(
    ip_white_list: IpWhiteList, session: MongoClient = Depends(get_session)
):
    db = session
    if db.ip_white_list.find_one({"ip": ip_white_list.ip}):
        raise HTTPException(status_code=400, detail="IP already exists")
    result = db.ip_white_list.insert_one(ip_white_list.dict())
    ip_white_list.id = str(result.inserted_id)
    return ip_white_list


@ip_white_list_router.get("/", response_model=List[IpWhiteList], summary="获取所有IP白名单")
def get_all_ip_white_list(session: MongoClient = Depends(get_session)):
    db = session
    ip_white_lists = list(db.ip_white_list.find())
    for item in ip_white_lists:
        item["id"] = str(item["_id"])
    return ip_white_lists


@ip_white_list_router.get("/{ip_white_list_id}", response_model=IpWhiteList, summary="获取IP白名单详情")
def get_ip_white_list(
    ip_white_list_id: str, session: MongoClient = Depends(get_session)
):
    db = session
    ip_white_list = db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})
    if not ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")
    ip_white_list["id"] = str(ip_white_list["_id"])
    return ip_white_list


@ip_white_list_router.put("/{ip_white_list_id}", response_model=IpWhiteList, summary="更新IP白名单")
def update_ip_white_list(
    ip_white_list_id: str,
    ip_white_list: IpWhiteList,
    session: MongoClient = Depends(get_session),
):
    db = session
    db_ip_white_list = db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})
    if not db_ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")

    update_data = ip_white_list.dict(exclude_unset=True)
    db.ip_white_list.update_one(
        {"_id": ObjectId(ip_white_list_id)}, {"$set": update_data}
    )
    return db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})


@ip_white_list_router.delete("/{ip_white_list_id}", response_model=IpWhiteList, summary="删除IP白名单")
def delete_ip_white_list(
    ip_white_list_id: str, session: MongoClient = Depends(get_session)
):
    db = session
    ip_white_list = db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})
    if not ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")
    db.ip_white_list.delete_one({"_id": ObjectId(ip_white_list_id)})
    return ip_white_list