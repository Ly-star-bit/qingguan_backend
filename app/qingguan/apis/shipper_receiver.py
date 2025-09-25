from typing import Optional
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    HTTPException
)



from app.db_mongo import get_session
from pydantic import BaseModel,Field
class ShippersAndReceivers(BaseModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    ShipperName: Optional[str] = None
    ShipperAddress: Optional[str] = None
    ReceiverName: Optional[str] = None
    ReceiverAddress: Optional[str] = None
    Attribute: Optional[str] = None
    ChineseName: Optional[str] = None
    EnglishName: Optional[str] = None
    Address: Optional[str] = None
shipperandreceiver_router = APIRouter(tags=["发货人和收货人"],prefix="/shippersandreceivers")
@shipperandreceiver_router.get("/", response_model=dict, summary="获取发货人和收货人列表")
def read_shippers_and_receivers(
    skip: int = 0,
    limit: int = 10,
    ShipperName: Optional[str] = None,
    session: MongoClient = Depends(get_session),
):
    db = session
    query = {}
    if ShipperName:
        query["ShipperName"] = {"$regex": ShipperName}

    total = db.shippersandreceivers.count_documents(query)
    shippers_and_receivers = list(
        db.shippersandreceivers.find(query).skip(skip).limit(limit)
    )

    for item in shippers_and_receivers:
        item["id"] = str(item["_id"])
        item.pop("_id", None)

    return {"items": shippers_and_receivers, "total": total}


@shipperandreceiver_router.post("/", response_model=ShippersAndReceivers, summary="创建发货人或收货人")
def create_shipper_or_receiver(
    shipper_or_receiver: ShippersAndReceivers,
    session: MongoClient = Depends(get_session),
):
    db = session
    shipper_dict = shipper_or_receiver.dict()
    shipper_dict.pop("id", None)
    result = db.shippersandreceivers.insert_one(shipper_dict)
    shipper_dict["id"] = str(result.inserted_id)
    return shipper_dict


@shipperandreceiver_router.put("/{id}", response_model=ShippersAndReceivers, summary="更新发货人或收货人")
def update_shipper_or_receiver(
    id: str,
    shipper_or_receiver: ShippersAndReceivers,
    session: MongoClient = Depends(get_session),
):
    db = session
    existing = db.shippersandreceivers.find_one({"_id": ObjectId(id)})
    if not existing:
        raise HTTPException(status_code=404, detail="Shipper or Receiver not found")

    update_data = shipper_or_receiver.dict(exclude_unset=True)
    update_data.pop("id", None)
    db.shippersandreceivers.update_one({"_id": ObjectId(id)}, {"$set": update_data})
    updated = db.shippersandreceivers.find_one({"_id": ObjectId(id)})
    updated["id"] = str(updated["_id"])
    updated.pop("_id", None)
    return updated


@shipperandreceiver_router.delete(
    "/{id}", response_model=ShippersAndReceivers, summary="删除发货人或收货人"
)
def delete_shipper_or_receiver(id: str, session: MongoClient = Depends(get_session)):
    db = session
    shipper = db.shippersandreceivers.find_one({"_id": ObjectId(id)})
    if not shipper:
        raise HTTPException(status_code=404, detail="Shipper or Receiver not found")
    db.shippersandreceivers.delete_one({"_id": ObjectId(id)})
    shipper["id"] = str(shipper["_id"])
    shipper.pop("_id", None)
    return shipper
