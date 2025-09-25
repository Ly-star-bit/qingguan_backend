import json
import os
from pathlib import Path
from datetime import datetime
from typing import Optional
import uuid
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
from fastapi.responses import FileResponse


from app.db_mongo import get_session
from .air_product import upload_huomian_file
sea_product_router = APIRouter(tags=["海运产品"], prefix="/products_sea")
@sea_product_router.get("/", response_model=dict, summary="获取海运产品列表")
def read_products_sea(
    skip: int = 0,
    limit: int = 10,
    名称: Optional[str] = None,
    get_all: bool = False,
    country: str = "China",
    zishui: bool = None,
    is_hidden: bool = None,
    session: MongoClient = Depends(get_session),
):
    db = session
    query = {"country": country}
    if is_hidden is not None:
        if is_hidden:
            query["is_hidden"] = True
        else:
            query["$or"] = [
                {"is_hidden": False},
                {"is_hidden": {"$exists": False}}
            ]
    if 名称:
        # query["中文品名"] = {"$regex": 名称}
        query["中文品名"] = 名称
    if zishui is not None:
        if zishui:
            query["自税"] = {"$in": [1, True]}
        else:
            query["自税"] = {"$in": [0, False]}
    total = db.products_sea.count_documents(query)

    if get_all:
        products = list(db.products_sea.find(query))
    else:
        products = list(db.products_sea.find(query).skip(skip).limit(limit))

    for product in products:
        product["id"] = str(product["_id"])
        product.pop("_id", None)

    return {"items": products, "total": total}


@sea_product_router.get("/upload_huomian_file", summary="上传海运货免文件")
def upload_huomian_file_sea(file: UploadFile = File(...)):
    save_directory = Path("./file/huomian_file/")
    save_directory.mkdir(parents=True, exist_ok=True)
    file_name = f"{uuid.uuid4()}-{file.filename}"
    file_path = save_directory / file_name

    with file.file as file_content:
        with open(file_path, "wb") as buffer:
            buffer.write(file_content.read())
    return {"file_name": file_name}


@sea_product_router.post("/", summary="创建海运产品")
def create_product_sea(
    product: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    session: MongoClient = Depends(get_session),
):
    db = session
    product_data = json.loads(product)
    product_data["更新时间"] = datetime.utcnow()

    if file:
        file_name = upload_huomian_file(file)["file_name"]
        product_data["huomian_file_name"] = file_name
    if product_data.get("single_weight"):
        product_data["single_weight"] = float(product_data["single_weight"])

    result = db.products_sea.insert_one(product_data)
    product_data["id"] = str(result.inserted_id)
    product_data.pop("_id", None)
    return product_data


@sea_product_router.get("/{pic_name}", summary="下载海运货免文件")
def download_pic_sea(pic_name: str):
    file_path = os.path.join("./file/huomian_file/", pic_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        file_path, media_type="application/octet-stream", filename=pic_name
    )


@sea_product_router.put("/{product_id}", summary="更新海运产品")
def update_product_sea(
    product_id: str,
    product: str = Form(...),
    file: Optional[UploadFile] = File(None),
    session: MongoClient = Depends(get_session),
):
    db = session
    try:
        product_data = json.loads(product)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")

    existing_product = db.products_sea.find_one({"_id": ObjectId(product_id)})
    if not existing_product:
        raise HTTPException(status_code=404, detail="Product not found")

    update_data = {
        k: v for k, v in product_data.items() if k != "id" and v is not None 
    }

    if file:
        file_name = upload_huomian_file(file)["file_name"]
        update_data["huomian_file_name"] = file_name
    
    try:
        if update_data.get("single_weight"):
            update_data["single_weight"] = float(update_data["single_weight"])
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid single_weight value")

    update_data["更新时间"] = datetime.utcnow()

    try:
        db.products_sea.update_one({"_id": ObjectId(product_id)}, {"$set": update_data})
        updated_product = db.products_sea.find_one({"_id": ObjectId(product_id)})
        updated_product["id"] = str(updated_product["_id"])
        updated_product.pop("_id", None)
        return updated_product
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@sea_product_router.post("/bulk_hide", summary="批量隐藏/显示海运产品")
def bulk_hide_products(bulk_hide:str = Form(...), session: MongoClient = Depends(get_session)):
    db = session
    try:
        bulk_hide_data = json.loads(bulk_hide)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")

    product_ids = bulk_hide_data.get("product_ids", [])
    is_hidden = bulk_hide_data.get("is_hidden", False)
    
    # Convert string IDs to ObjectId
    object_ids = []
    try:
        object_ids = [ObjectId(pid) for pid in product_ids]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid product ID format")
        
    db.products_sea.update_many({"_id": {"$in": object_ids}}, {"$set": {"is_hidden": is_hidden}})
    return {"message": "Products hidden successfully"}


@sea_product_router.delete("/{product_id}", summary="删除海运产品")
def delete_product_sea(product_id: str, session: MongoClient = Depends(get_session)):
    db = session
    product = db.products_sea.find_one({"_id": ObjectId(product_id)})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    db.products_sea.delete_one({"_id": ObjectId(product_id)})
    product["id"] = str(product["_id"])
    product.pop("_id", None)
    return product

