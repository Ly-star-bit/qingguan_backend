from typing import Optional

from bson import ObjectId
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
)
from pymongo import MongoClient
from pydantic import BaseModel,Field

from app.db_mongo import get_session
class Dalei(BaseModel):
    id: Optional[str] = Field(default=None, primary_key=True)
    属性: Optional[str] = Field(default=None, max_length=255)

    hs_code: Optional[str] = Field(default=None, max_length=255)
    类别: Optional[str] = Field(default=None, max_length=255)

    英文大类: Optional[str] = Field(default=None, max_length=255)
    中文大类: Optional[str] = Field(default=None, max_length=255)
    客供: Optional[str] = Field(default=None, max_length=255)
    备注: Optional[str] = Field(default=None, max_length=255)
dalei_router = APIRouter(tags=["大类"], prefix="/dalei")


@dalei_router.post("/", response_model=Dalei, summary="创建大类")
def create_dalei(dalei: Dalei, session: MongoClient = Depends(get_session)):
    db = session
    dalei_dict = dalei.dict()
    dalei_dict.pop("id", None)
    result = db.dalei.insert_one(dalei_dict)
    dalei_dict["id"] = str(result.inserted_id)
    return dalei_dict


@dalei_router.get("/", summary="获取大类列表")
def read_dalei(
    skip: int = 0,
    limit: int = 10,
    名称: Optional[str] = None,
    get_all: bool = False,
    session: MongoClient = Depends(get_session),
):
    db = session
    query = {}
    if 名称:
        query["中文大类"] = {"$regex": 名称}

    total = db.dalei.count_documents(query)

    if get_all:
        dalei_list = list(db.dalei.find(query))
    else:
        dalei_list = list(db.dalei.find(query).skip(skip).limit(limit))

    for item in dalei_list:
        item["id"] = str(item["_id"])
        item.pop("_id", None)

    return {"items": dalei_list, "total": total}


@dalei_router.get("/{id}", response_model=Dalei, summary="根据ID获取大类")
def read_dalei_by_id(id: str, session: MongoClient = Depends(get_session)):
    db = session
    dalei = db.dalei.find_one({"_id": ObjectId(id)})
    if not dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")
    dalei["id"] = str(dalei["_id"])
    dalei.pop("_id", None)
    return dalei


@dalei_router.put("/{id}", response_model=Dalei, summary="更新大类")
def update_dalei(id: str, dalei: Dalei, session: MongoClient = Depends(get_session)):
    db = session
    existing_dalei = db.dalei.find_one({"_id": ObjectId(id)})
    if not existing_dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")

    update_data = dalei.dict(exclude_unset=True)
    update_data.pop("id", None)

    db.dalei.update_one({"_id": ObjectId(id)}, {"$set": update_data})
    updated_dalei = db.dalei.find_one({"_id": ObjectId(id)})
    updated_dalei["id"] = str(updated_dalei["_id"])
    updated_dalei.pop("_id", None)
    return updated_dalei


@dalei_router.delete("/{id}", response_model=Dalei, summary="删除大类")
def delete_dalei(id: str, session: MongoClient = Depends(get_session)):
    db = session
    dalei = db.dalei.find_one({"_id": ObjectId(id)})
    if not dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")
    db.dalei.delete_one({"_id": ObjectId(id)})
    dalei["id"] = str(dalei["_id"])
    dalei.pop("_id", None)
    return dalei
