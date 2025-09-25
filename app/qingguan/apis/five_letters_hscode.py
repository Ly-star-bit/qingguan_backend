from datetime import datetime
from typing import Optional
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile
)


from app.db_mongo import get_session
from .air_product import upload_huomian_file
five_letters_hscode_router = APIRouter(tags=["5位码"],prefix="/5_letters_hscode")

@five_letters_hscode_router.get("/", response_model=dict, summary="获取5位码列表")
def read_5_letters_hscode(
    skip: int = 0,
    limit: int = 10,
    chinese_goods_name: Optional[str] = None,
    goods_name: Optional[str] = None,
    get_all: bool = False,
    session: MongoClient = Depends(get_session),
):
    db = session
    query = {}
    query["chinese_goods"] = {"$exists": True}  # 确保有chinese_goods字段
    if goods_name:
        query["Goods"] = {"$regex": goods_name}
    if chinese_goods_name:
        query["chinese_goods"] = {"$regex": chinese_goods_name}
    total = db["5_letters_hscode"].count_documents(query)

    # 使用排序
    sort = [("ReferenceNumber", 1)]  # 1 表示升序

    if get_all:
        five_letters_hscode_list = list(db["5_letters_hscode"].find(query).sort(sort))
    else:
        five_letters_hscode_list = list(
            db["5_letters_hscode"].find(query).sort(sort).skip(skip).limit(limit)
        )

    for item in five_letters_hscode_list:
        item["id"] = str(item["_id"])
        item.pop("_id", None)

    return {"items": five_letters_hscode_list, "total": total}


@five_letters_hscode_router.post("/", summary="创建5位码")
def create_5_letters_hscode(
    ReferenceNumber: str = Form(...),
    Goods: str = Form(...),
    chinese_goods: str = Form(...),
    类别: str = Form(...),
    客供: str = Form(...),
    备注: str = Form(...),
    file: Optional[UploadFile] = File(None),
    session: MongoClient = Depends(get_session),
):
    db = session
    five_letters_hscode_data = {
        "ReferenceNumber": ReferenceNumber,
        "Goods": Goods,
        "chinese_goods": chinese_goods,
        "类别": 类别,
        "客供": 客供,
        "备注": 备注,
        "更新时间": datetime.utcnow(),
    }

    if file:
        file_name = upload_huomian_file(file)["file_name"]
        five_letters_hscode_data["huomian_file_name"] = file_name

    result = db["5_letters_hscode"].insert_one(five_letters_hscode_data)
    five_letters_hscode_data["id"] = str(result.inserted_id)
    five_letters_hscode_data.pop("_id", None)
    return five_letters_hscode_data


@five_letters_hscode_router.put("/{five_letters_hscode_id}", summary="更新5位码")
def update_5_letters_hscode(
    five_letters_hscode_id: str,
    ReferenceNumber: str = Form(...),
    Goods: str = Form(...),
    chinese_goods: str = Form(...),
    类别: str = Form(...),
    客供: str = Form(...),
    备注: str = Form(...),
    session: MongoClient = Depends(get_session),
):
    db = session

    existing_5_letters_hscode = db["5_letters_hscode"].find_one(
        {"_id": ObjectId(five_letters_hscode_id)}
    )
    if not existing_5_letters_hscode:
        raise HTTPException(status_code=404, detail="5_letters_hscode not found")

    update_data = {
        "ReferenceNumber": ReferenceNumber,
        "Goods": Goods,
        "chinese_goods": chinese_goods,
        "类别": 类别,
        "客供": 客供,
        "备注": 备注,
    }

    update_data = {k: v for k, v in update_data.items() if v is not None and v != ""}

    db["5_letters_hscode"].update_one(
        {"_id": ObjectId(five_letters_hscode_id)}, {"$set": update_data}
    )
    updated_5_letters_hscode = db["5_letters_hscode"].find_one(
        {"_id": ObjectId(five_letters_hscode_id)}
    )
    updated_5_letters_hscode["id"] = str(updated_5_letters_hscode["_id"])
    updated_5_letters_hscode.pop("_id", None)
    return updated_5_letters_hscode


@five_letters_hscode_router.delete("/{five_letters_hscode_id}", summary="删除5位码")
def delete_5_letters_hscode(
    five_letters_hscode_id: str, session: MongoClient = Depends(get_session)
):
    db = session
    five_letters_hscode = db["5_letters_hscode"].find_one(
        {"_id": ObjectId(five_letters_hscode_id)}
    )
    if not five_letters_hscode:
        raise HTTPException(status_code=404, detail="5_letters_hscode not found")
    db["5_letters_hscode"].delete_one({"_id": ObjectId(five_letters_hscode_id)})
    five_letters_hscode["id"] = str(five_letters_hscode["_id"])
    five_letters_hscode.pop("_id", None)
    return five_letters_hscode