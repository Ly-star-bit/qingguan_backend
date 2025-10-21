
from typing import Optional
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    HTTPException
)

from pydantic import BaseModel, Field

from app.db_mongo import get_session
class FactoryData(BaseModel):
    id: Optional[str] = Field(default=None)
    属性: str = Field(max_length=255, nullable=False)
    中文名字: str = Field(max_length=255, nullable=False)
    英文: str = Field(nullable=False)
    地址: str = Field(nullable=False)
factory_router = APIRouter(tags=["工厂"], prefix="/factory")
# 工厂数据CRUD操作
@factory_router.post("/", response_model=FactoryData, summary="创建工厂")
def create_factory(factory: FactoryData, session: MongoClient = Depends(get_session)):
    db = session
    factory_dict = factory.dict()
    factory_dict.pop("id", None)
    result = db.factories.insert_one(factory_dict)
    factory_dict["id"] = str(result.inserted_id)
    return factory_dict


@factory_router.get("/", summary="获取工厂列表")
def read_factories(
    skip: int = 0,
    limit: Optional[int] = None,
    session: MongoClient = Depends(get_session),
):
    db = session
    query = {}
    total = db.factories.count_documents(query)
    cursor = db.factories.find(query).skip(skip)
    if limit is not None:
        cursor = cursor.limit(limit)
    factories = list(cursor)
    for factory in factories:
        factory["id"] = str(factory["_id"])
        factory.pop("_id", None)
    return {"items": factories, "total": total}


@factory_router.get("/{factory_id}", response_model=FactoryData, summary="获取工厂详情")
def read_factory(factory_id: str, session: MongoClient = Depends(get_session)):
    db = session
    factory = db.factories.find_one({"_id": ObjectId(factory_id)})
    if not factory:
        raise HTTPException(status_code=404, detail="Factory not found")
    factory["id"] = str(factory["_id"])
    factory.pop("_id", None)
    return factory


@factory_router.put("/{factory_id}", response_model=FactoryData, summary="更新工厂")
def update_factory(
    factory_id: str, factory: FactoryData, session: MongoClient = Depends(get_session)
):
    db = session
    existing = db.factories.find_one({"_id": ObjectId(factory_id)})
    if not existing:
        raise HTTPException(status_code=404, detail="Factory not found")

    update_data = factory.dict(exclude_unset=True)
    update_data.pop("id", None)
    db.factories.update_one({"_id": ObjectId(factory_id)}, {"$set": update_data})
    updated = db.factories.find_one({"_id": ObjectId(factory_id)})
    updated["id"] = str(updated["_id"])
    updated.pop("_id", None)
    return updated


@factory_router.delete("/{factory_id}", response_model=FactoryData, summary="删除工厂")
def delete_factory(factory_id: str, session: MongoClient = Depends(get_session)):
    db = session
    factory = db.factories.find_one({"_id": ObjectId(factory_id)})
    if not factory:
        raise HTTPException(status_code=404, detail="Factory not found")
    db.factories.delete_one({"_id": ObjectId(factory_id)})
    factory["id"] = str(factory["_id"])
    factory.pop("_id", None)
    return factory