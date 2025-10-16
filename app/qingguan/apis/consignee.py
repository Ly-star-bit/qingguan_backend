from typing import Optional
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    HTTPException
)

from pydantic import BaseModel,Field

from app.db_mongo import get_session
class ConsigneeData(BaseModel):
    id: Optional[str] = Field(default=None)
    中文: str = Field(max_length=255, nullable=False)
    发货人: str = Field(max_length=255, nullable=False)
    发货人详细地址: str = Field(nullable=False)
    类型: str  = Field(nullable=False)
    关税类型: str = Field(nullable=False)
    备注: str = Field(nullable=False)
    hide: str = Field(nullable=False,default='0')
consignee_router = APIRouter(tags=["收发货人"],prefix="/consignee")
# 收发货人CRUD操作
@consignee_router.post("/", response_model=ConsigneeData, summary="创建收货人")
def create_consignee(
    consignee: ConsigneeData, session: MongoClient = Depends(get_session)
):
    db = session
    consignee_dict = consignee.dict()
    consignee_dict.pop("id", None)
    result = db.consignees.insert_one(consignee_dict)
    consignee_dict["id"] = str(result.inserted_id)
    return consignee_dict


@consignee_router.get("/", summary="获取收货人列表")
def read_consignees(
    skip: int = 0,
    limit: Optional[int] = None,
    session: MongoClient = Depends(get_session),
):
    db = session
    query = {}
    total = db.consignees.count_documents(query)
    cursor = db.consignees.find(query).skip(skip)
    if limit is not None:
        cursor = cursor.limit(limit)
    consignees = list(cursor)
    for consignee in consignees:
        consignee["id"] = str(consignee["_id"])
        consignee.pop("_id", None)
    return {"items": consignees, "total": total}


@consignee_router.get("/{consignee_id}", response_model=ConsigneeData, summary="获取收货人详情")
def read_consignee(consignee_id: str, session: MongoClient = Depends(get_session)):
    db = session
    consignee = db.consignees.find_one({"_id": ObjectId(consignee_id)})
    if not consignee:
        raise HTTPException(status_code=404, detail="Consignee not found")
    consignee["id"] = str(consignee["_id"])
    consignee.pop("_id", None)
    return consignee


@consignee_router.put("/{consignee_id}", response_model=ConsigneeData, summary="更新收货人")
def update_consignee(
    consignee_id: str,
    consignee: ConsigneeData,
    session: MongoClient = Depends(get_session),
):
    db = session
    existing = db.consignees.find_one({"_id": ObjectId(consignee_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="Consignee not found")

    update_data = consignee.dict(exclude_unset=True)
    update_data.pop("id", None)
    db.consignees.update_one({"_id": ObjectId(consignee_id)}, {"$set": update_data})
    updated = db.consignees.find_one({"_id": ObjectId(consignee_id)})
    updated["id"] = str(updated["_id"])
    updated.pop("_id", None)
    return updated


@consignee_router.delete("/{consignee_id}", response_model=ConsigneeData, summary="删除收货人")
def delete_consignee(consignee_id: str, session: MongoClient = Depends(get_session)):
    db = session
    consignee = db.consignees.find_one({"_id": ObjectId(consignee_id)})
    if not consignee:
        raise HTTPException(status_code=404, detail="Consignee not found")
    db.consignees.delete_one({"_id": ObjectId(consignee_id)})
    consignee["id"] = str(consignee["_id"])
    consignee.pop("_id", None)
    return consignee