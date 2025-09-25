from typing import Optional
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    HTTPException
)
from loguru import logger
from pydantic import BaseModel


from app.db_mongo import get_session

class PackingType(BaseModel):
    packing_type: str
    sender_name: Optional[str]
    receiver_name: Optional[str]
    remarks: Optional[str]=""
    check_remarks: Optional[str]=""
    country: Optional[str]="China"
    check_data: Optional[list]=[]
packing_type_router = APIRouter(tags=["装箱"],prefix='/packing_types')
@packing_type_router.post("/", response_model=PackingType, summary="创建包装类型")
def create_packing_type(packing_type: PackingType, session: MongoClient = Depends(get_session)):
    db = session
    
    packing_type_dict = packing_type.model_dump()
    logger.info(f"packing_type: {packing_type_dict}")

    packing_type_dict.pop("id", None)
    result = db.packing_types.insert_one(packing_type_dict)
    
    # Create a new dict with just the fields we want to return
    response_dict = {
        "id": str(result.inserted_id),
        "packing_type": packing_type_dict["packing_type"],
        "sender_name": packing_type_dict.get("sender_name"),
        "receiver_name": packing_type_dict.get("receiver_name"), 
        "remarks": packing_type_dict.get("remarks", "")
    }
    
    return response_dict


@packing_type_router.get("/", summary="获取包装类型列表")
def read_packing_types(
    session: MongoClient = Depends(get_session),
    skip: int = 0,
    country: Optional[str]="",
    limit: Optional[int] = None,
):
    db = session
    if country:
        query = {"country": country}
    else:
        query = {}
    cursor = db.packing_types.find(query).skip(skip)
    if limit:
        cursor = cursor.limit(limit)
    packing_types = list(cursor)
    for packing_type in packing_types:
        packing_type["id"] = str(packing_type["_id"])
        packing_type.pop("_id", None)
    return packing_types


@packing_type_router.get("/{packing_type_id}", summary="获取包装类型详情")
def read_packing_type(packing_type_id: str, session: MongoClient = Depends(get_session)):
    db = session
    packing_type = db.packing_types.find_one({"_id": ObjectId(packing_type_id)})
    if not packing_type:
        raise HTTPException(status_code=404, detail="PackingType not found")
    packing_type["id"] = str(packing_type["_id"])
    packing_type.pop("_id", None)
    return packing_type


@packing_type_router.put("/{packing_type_id}", summary="更新包装类型")
def update_packing_type(
    packing_type_id: str, updated_packing_type: PackingType, session: MongoClient = Depends(get_session)
):
    db = session
    packing_type = db.packing_types.find_one({"_id": ObjectId(packing_type_id)})
    if not packing_type:
        raise HTTPException(status_code=404, detail="PackingType not found")

    update_data = updated_packing_type.dict(exclude_unset=True)
    logger.info(f"update_data: {update_data}")
    update_data.pop("id", None)
    db.packing_types.update_one({"_id": ObjectId(packing_type_id)}, {"$set": update_data})
    updated = db.packing_types.find_one({"_id": ObjectId(packing_type_id)})
    updated["id"] = str(updated["_id"])
    updated.pop("_id", None)
    return updated


@packing_type_router.delete("/{packing_type_id}", summary="删除包装类型")
def delete_packing_type(packing_type_id: str, session: MongoClient = Depends(get_session)):
    db = session
    packing_type = db.packing_types.find_one({"_id": ObjectId(packing_type_id)})
    if not packing_type:
        raise HTTPException(status_code=404, detail="PackingType not found")
    db.packing_types.delete_one({"_id": ObjectId(packing_type_id)})
    packing_type["id"] = str(packing_type["_id"])
    packing_type.pop("_id", None)
    return packing_type
