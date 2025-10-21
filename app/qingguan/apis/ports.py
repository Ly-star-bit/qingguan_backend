
from typing import Optional
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    HTTPException
)
from typing import Any, Dict
from fastapi.encoders import jsonable_encoder



from app.db_mongo import get_session
ports_router = APIRouter(tags=['港口'],prefix="/ports")


@ports_router.post("/", summary="创建港口")
def create_port(port: Dict[str, Any], session: MongoClient = Depends(get_session)):
    db = session  # 假设这里已经是 Database 对象
    # 先把请求体里的潜在 ObjectId 等可序列化处理好，并去掉 id/_id
    doc = jsonable_encoder(
        port,
        custom_encoder={ObjectId: str}
    )
    doc.pop("id", None)
    doc.pop("_id", None)

    result = db.ports.insert_one(doc)

    # 返回值同样走一次 encoder，确保没有原始 ObjectId
    response = {**doc, "id": str(result.inserted_id)}
    return jsonable_encoder(response, custom_encoder={ObjectId: str})


@ports_router.get("/", summary="获取港口列表")
def read_ports(
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
    cursor = db.ports.find(query).skip(skip)
    if limit:
        cursor = cursor.limit(limit)
    ports = list(cursor)
    for port in ports:
        port["id"] = str(port["_id"])
        port.pop("_id", None)
    return ports


@ports_router.get("/{port_id}", summary="获取港口详情")
def read_port(port_id: str, session: MongoClient = Depends(get_session)):
    db = session
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    port["id"] = str(port["_id"])
    port.pop("_id", None)
    return port


@ports_router.put("/{port_id}", summary="更新港口")
def update_port(
    port_id: str, updated_port: dict, session: MongoClient = Depends(get_session)
):
    db = session
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")

    update_data = updated_port
    update_data.pop("id", None)
    db.ports.update_one({"_id": ObjectId(port_id)}, {"$set": update_data})
    updated = db.ports.find_one({"_id": ObjectId(port_id)})
    updated["id"] = str(updated["_id"])
    updated.pop("_id", None)
    return updated


@ports_router.delete("/{port_id}", summary="删除港口")
def delete_port(port_id: str, session: MongoClient = Depends(get_session)):
    db = session
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    db.ports.delete_one({"_id": ObjectId(port_id)})
    port["id"] = str(port["_id"])
    port.pop("_id", None)
    return port

