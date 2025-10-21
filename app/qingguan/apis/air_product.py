import json
import os
from pathlib import Path
import traceback
from datetime import datetime
from typing import Optional
import uuid
import pandas as pd
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
from fastapi.responses import FileResponse, JSONResponse
from loguru import logger


from app.db_mongo import get_session


air_product_router = APIRouter(prefix="/products", tags=["空运产品"])
@air_product_router.get("/", response_model=dict, summary="获取产品列表")
def read_products(
    skip: int = 0,
    limit: int = 10,
    名称: Optional[str] = None,
    get_all: bool = False,
    startland:str = "China",
    destination: str = "America",
    zishui: bool = None,
    is_hidden:bool=None,
    session: MongoClient = Depends(get_session),
):
    db = session
    query = {"destination": destination,"startland":startland}
    if is_hidden is not None:
        if is_hidden:
            query["is_hidden"] = True
        else:
            query["$or"] = [
                {"is_hidden": False},
                {"is_hidden": {"$exists": False}}
            ]
    if zishui is not None:
        if zishui:
            query["自税"] = {"$in": [1, True]}
        else:
            query["自税"] = {"$in": [0, False]}
    if 名称:
        # query["中文品名"] = {"$regex": 名称}
        query["中文品名"] = 名称

    total = db.products.count_documents(query)

    if get_all:
        products = list(db.products.find(query))
    else:
        products = list(db.products.find(query).skip(skip).limit(limit))

    for product in products:
        product["id"] = str(product["_id"])
        product.pop("_id", None)

    return {"items": products, "total": total}
@air_product_router.get("/categories/list", response_model=dict, summary="获取所有类别列表")
def get_categories(
    startland: str = "China",
    destination: str = "America",
    session: MongoClient = Depends(get_session),
):
    """获取指定路线的所有产品类别"""
    db = session
    
    query = {"destination": destination, "startland": startland}
    
    # 使用 distinct 获取所有不同的类别值
    categories = db.products.distinct("类别", query)
    
    return {
        "categories": sorted(categories) if categories else [],
        "total": len(categories) if categories else 0
    }





@air_product_router.post("/upload_huomian_file", summary="上传货免文件")
def upload_huomian_file(file: UploadFile = File(...)):
    save_directory = Path("./file/huomian_file/")
    save_directory.mkdir(parents=True, exist_ok=True)
    file_name = f"{uuid.uuid4()}-{file.filename}"
    file_path = save_directory / file_name

    with file.file as file_content:
        with open(file_path, "wb") as buffer:
            buffer.write(file_content.read())
    return {"file_name": file_name}


@air_product_router.post("/", summary="创建产品")
def create_product(
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

    result = db.products.insert_one(product_data)
    product_data["id"] = str(result.inserted_id)
    product_data.pop("_id", None)
    return product_data


@air_product_router.get("/{pic_name}", summary="下载货免文件")
def download_pic(pic_name: str):
    file_path = os.path.join("./file/huomian_file/", pic_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        file_path, media_type="application/octet-stream", filename=pic_name
    )


@air_product_router.put("/{product_id}", summary="更新产品")
def update_product(
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

    existing_product = db.products.find_one({"_id": ObjectId(product_id)})
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
        db.products.update_one({"_id": ObjectId(product_id)}, {"$set": update_data})
        updated_product = db.products.find_one({"_id": ObjectId(product_id)})
        updated_product["id"] = str(updated_product["_id"])
        updated_product.pop("_id", None)
        return updated_product
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@air_product_router.post("/update_batch", summary="批量更新产品信息")
def update_batch(
    transport_type: str = "",
    file: UploadFile = File(...),
    session: MongoClient = Depends(get_session)
):
    db = session
    
    # 读取Excel文件
    df = pd.read_excel(file.file)
    
    # 确保id列存在
    if 'id' not in df.columns:
        raise HTTPException(status_code=400, detail="Excel文件必须包含id列")
        
    # 获取所有列名,排除id列
    update_fields = [col for col in df.columns if col != 'id']
    
    # 遍历每一行数据进行更新
    updated_count = 0
    for _, row in df.iterrows():
        try:
            # 构建更新数据
            update_data = {}
            for field in update_fields:
                value = row[field]
                if field == "件/箱":
                    field = "件箱"

                if field.startswith("加征"):
                    field = "加征." + field
                
                # 检查是否为日期类型
                if isinstance(value, pd.Timestamp):
                    update_data[field] = value.strftime("%Y-%m-%d")
                else:
                    update_data[field] = value
            logger.info(f"更新数据: {update_data}")
            logger.info(f"更新ID: {row['id']}")
            if transport_type == "空运":
                # 执行更新
                result = db.products.update_one(
                    {"_id": ObjectId(row['id'])},
                    {"$set": update_data}
                )
            elif transport_type == "海运":
                result = db.products_sea.update_one(
                    {"_id": ObjectId(row['id'])},
                    {"$set": update_data}
                )
            if result.modified_count:
                updated_count += 1
                
        except Exception:
            logger.error(f"更新ID {row['id']} 失败: {traceback.format_exc()}")
            continue
            
    return {"message": f"成功更新 {updated_count} 条记录"}


@air_product_router.delete("/{product_id}", summary="删除产品")
def delete_product(product_id: str, session: MongoClient = Depends(get_session)):
    db = session
    product = db.products.find_one({"_id": ObjectId(product_id)})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    db.products.delete_one({"_id": ObjectId(product_id)})
    product["id"] = str(product["_id"])
    product.pop("_id", None)
    return product


@air_product_router.get("/output_products", summary="导出产品Excel")
def output_products(
    session: MongoClient = Depends(get_session),
    transport_type: str = "",
    startland:str = "China",
    destination: str = "America",
):
    try:
        db = session
        if transport_type == "sea":
            products = list(db.products_sea.find({"destination": destination,"startland":startland}))
        else:
            products = list(db.products.find({"destination": destination,"startland":startland}))

        output_products = []
        all_jia_zheng_keys = set()
        for product in products:
            if "加征" in product:
                all_jia_zheng_keys.update(product["加征"].keys())

        for product in products:
            # 计算总税率
            duty = product.get("Duty", 0)
            total_tax = 0  # 初始化总税率
            try:
                duty = float(duty)
                total_tax = duty
            except ValueError:
                # duty不是数字格式时,不计算总税率
                pass
            piece_per_box = product.get("件箱")
            unit_price = product.get("单价")
            try:
                piece_per_box = int(piece_per_box) if piece_per_box else 0
                unit_price = float(unit_price) if unit_price else 0
            except (ValueError, TypeError):
                piece_per_box = 0
                unit_price = 0

            output_product = {
                "序号": product.get("序号"),
                "中文品名": product.get("中文品名"),
                "英文品名": product.get("英文品名"),
                "HS_CODE": product.get("HS_CODE"),
                "件/箱": product.get("件箱"),
                "单价": product.get("单价"),
                "Duty": product.get("Duty"),
            }

            # 添加加征字段
            jia_zheng_values = product.get("加征", {})
            for key in all_jia_zheng_keys:
                value = jia_zheng_values.get(key)
                if value is not None:
                    output_product[f"{key}"] = value
                    if isinstance(value, (int, float, str)):
                        try:
                            value = float(value)
                            total_tax += value
                        except ValueError:
                            pass  # 如果value不能转换为float，则跳过
                else:
                    output_product[f"{key}"] = (
                        None  # 确保所有产品都有相同的加征字段, 没有的设置为None
                    )

            if transport_type == "sea":
                single_tax = (
                    piece_per_box * unit_price * (total_tax + 0.003464 + 0.00125)
                )
            else:
                single_tax = piece_per_box * unit_price * (total_tax + 0.003464)
            output_product.update(
                {
                    "总税率": f"{round(total_tax, 4) * 100}%",
                    "单箱空运关税\n单箱海运关税": f"{round(single_tax, 4) * 100}%",
                    "认证": product.get("认证"),
                    "豁免代码": product.get("豁免代码"),
                    "豁免代码含义": product.get("豁免代码含义"),
                    "豁免截止日期说明": product.get("豁免截止日期说明"),
                    "豁免过期后": product.get("豁免过期后"),
                    "材质": product.get("材质"),
                    "用途": product.get("用途"),
                    "属性绑定工厂": product.get("属性绑定工厂"),
                    "类别": product.get("类别"),
                    "备注": product.get("备注"),
                    "单件重量合理范围": product.get("单件重量合理范围"),
                    "客户": product.get("客户"),
                    "报关代码": product.get("报关代码"),
                    "客人资料美金": product.get("客人资料美金"),
                    "single_weight": product.get("single_weight"),
                    "自税": product.get("自税"),
                    "类型": product.get("类型"),
                    "豁免文件名称": product.get("huomian_file_name"),
                    "id": str(product.get("_id")),
                    "更新时间": product.get("更新时间"),
                    "is_hidden": product.get("is_hidden"),
                }
            )

            output_products.append(output_product)

        # 创建Excel文件
        df = pd.DataFrame(output_products)
        if transport_type == "sea":
            excel_file = f"./file/output_products/products_output_sea_{datetime.now().strftime('%Y%m%d %H%M%S')}.xlsx"
        else:
            excel_file = f"./file/output_products/products_output_{datetime.now().strftime('%Y%m%d %H%M%S')}.xlsx"
        os.makedirs(os.path.dirname(excel_file), exist_ok=True)
        df.to_excel(excel_file, index=False)

        # 返回Excel文件
        return FileResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=excel_file,
        )
    except Exception as e:
        logger.error(f"错误为:{e}")
        return JSONResponse({"status": "False", "content": f"错误为:{e}"})
