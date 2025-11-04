from io import BytesIO
import io
import json
import os
from pathlib import Path
import random
import re
import traceback
from datetime import datetime, timedelta
from typing import List, Optional
from uuid import UUID
import uuid
import PyPDF2
import numpy as np
import pandas as pd
import bcrypt
from bson import ObjectId
import casbin
import httpx
from pymongo import MongoClient
import requests
from casbin_sqlalchemy_adapter import Adapter
from dotenv import load_dotenv

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
)
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from loguru import logger
from sqlalchemy import QueuePool, func
from sqlalchemy.exc import InvalidRequestError
from sqlmodel import Session, create_engine, or_, select
from starlette.middleware.base import BaseHTTPMiddleware
from tenacity import retry, stop_after_attempt, wait_fixed
from openpyxl import Workbook as Openpyxl_Workbook

from app.models import (
    ConsigneeData,
    CustomClearHistoryDetailLog,
    CustomClearHistorySummaryLog,
    Dalei,
    FactoryData,
    HaiYunZiShui,
    IpWhiteList,
    Port,
    Product3,
    ShipmentLog,
    ShippersAndReceivers,
    User,
)
from app.schemas import (
    DaleiCreate,
    FileInfo,
    Group,
    Policy,
    ProductData,
    ShippingRequest,
    SummaryResponse,
    UpdatePolicy,
    UserCreate,
    UserLogin,
    UserUpdate,
    update_cumstom_clear_history_summary_remarks,
)
from app.utils import (
    create_access_token,
    create_email_handler,
    create_refresh_token,
    extract_zip_codes_from_pdf,
    generate_excel_from_template_test,
    get_ups_zip_data,
    output_custom_clear_history_log,
    fedex_process_excel_with_zip_codes,
    query_usps_zip,
    shenzhen_customes_pdf_gennerate,
    ups_process_excel_with_zip_codes,
)
from morelink_api import MoreLinkClient
from rpa_tools import find_playwright_node_path
from rpa_tools.email_tools import send_email
from app.db_mongo import get_session, enforcer

# logger.level("ALERT", no=35, color="<red>")

# logger.add(create_email_handler("yu.luo@hubs-scs.com"), level="ALERT")


class IPWhitelistMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        print(client_ip)
        # 从MongoDB中获取白名单
        try:
            session = next(get_session())  # 获取实际的session对象
            db = session["qingguan"]
            ip_whitelist = [doc["ip"] for doc in db.ip_white_list.find()] + [
                "127.0.0.1",
                "localhost",
            ]
        except Exception as e:
            logger.error(f"Error fetching IP whitelist from database: {e}")
            ip_whitelist = []

        if client_ip not in ip_whitelist:
            return JSONResponse(
                status_code=403, content={"detail": f"IP {client_ip} not allowed"}
            )

        response = await call_next(request)
        return response


# @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
# def get_session():
#     try:
#         return Session(pool_engine)
#     except Exception as e:
#         logger.error(f"Database connection error: {e}")
#         raise


# 定义处理数据的函数
def process_shipping_data(
    shipper_name: str,
    receiver_name: str,
    master_bill_no: str,
    gross_weight: int,
    volume: int,
    product_list: List[ProductData],
    totalyugutax: float,
    predict_tax_price: float,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
    exchange_rate = db.exchange_rates.find_one({"version": "latest"})
    num_products = sum([i.box_num for i in product_list])
    if num_products == 0:
        raise ValueError("Product list cannot be empty")

    avg_gross_weight = gross_weight / num_products
    avg_volume = volume / num_products

    results = []
    accumulated_gross_weight = 0
    accumulated_volume = 0
    if product_list[0].single_price or product_list[0].packing:
        execute_type = "Sea"
    else:
        execute_type = "Air"
    receiver_record = db.consignees.find_one({"发货人": receiver_name.upper()})

    # 查询发货人地址
    shipper_record = db.consignees.find_one(
        {"发货人": {"$regex": f"^{shipper_name}$", "$options": "i"}}
    )
    # 查询收件人地址

    if not shipper_record:
        raise ValueError(f"Shipper '{shipper_name}' not found in database")
    if not receiver_record:
        raise ValueError(f"Receiver '{receiver_name}' not found in database")

    shipper_address = shipper_record.get("发货人详细地址")
    receiver_address = receiver_record.get("发货人详细地址")
    detail_data_log_list = []

    def safe_round(value,product_naem, default=0.0):
        if not value or value == '/':
            raise ValueError(f"Product '{product_naem}' value is empty or '/'")
        try:
            return round(float(value), 4)
        except (ValueError, TypeError):
            return default
    def safe_float(value,product_naem, default=0.0):
        if not value or value == '/':
            raise ValueError(f"Product '{product_naem}' value is empty or '/'")
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    for idx, product in enumerate(product_list):
        product_name = product.product_name
        box_num = product.box_num

        product_record = db.products.find_one({"中文品名": product_name})
        if not product_record:
            raise ValueError(f"Product '{product_name}' not found in database")

        origin_duty = product_record.get("Duty")
        origin_additional_duty = product_record.get("加征")
       
        duty = safe_round(origin_duty,product_name)
        additional_duty = safe_round(origin_additional_duty,product_name)

        detail_data_log = {
            "hs_code": product_record.get("HS_CODE"),
            "chinese_name": product_record.get("中文品名"),
            "transport_mode": execute_type,
            "master_bill_number": master_bill_no,
            "total_tax_rate": duty + additional_duty,
            "exemption_code": product_record.get("豁免代码"),
            "category": product_record.get("类别"),
            'box_nums': box_num,
        }

        if product.single_price:
            #sea海运
            single_price = product.single_price
            detail_data_log = {
            "hs_code": product_record.get("HS_CODE"),
            "chinese_name": product_record.get("中文品名"),
            "transport_mode": execute_type,
            "master_bill_number": master_bill_no,
            "total_tax_rate": duty + additional_duty,
            "exemption_code": product_record.get("豁免代码"),
            "category": product_record.get("类别"),
            'box_nums': box_num,
            "single_price":single_price,
            "packing":product.packing
        }
        else:
            #air空运
            single_price = product_record.get("单价")
            detail_data_log = {
                "hs_code": product_record.get("HS_CODE"),
                "chinese_name": product_record.get("中文品名"),
                "transport_mode": execute_type,
                "master_bill_number": master_bill_no,
                "total_tax_rate": duty + additional_duty,
                "exemption_code": product_record.get("豁免代码"),
                "category": product_record.get("类别"),
                'box_nums': box_num,
            }
        detail_data_log_list.append(detail_data_log)

        if product.packing:
            packing = product.packing
        else:
            packing = product_record.get("件箱")

        product_type = product_record.get("属性绑定工厂")
        address_name_list = list(
            db.factories.find(
                {"地址": {"$ne": None}, "属性": product_type}, {"地址": 1, "英文": 1}
            )
        )

        if not address_name_list:
            logger.warning(f"{product_type}工厂地址数据库中没有对应的属性")
            return {
                "product_attribute": f"{product_type}工厂在地址数据库中不存在"
            }, None

        random_address_name = random.choice(address_name_list)
        address = random_address_name.get("地址")
        address_name = random_address_name.get("英文")

        if idx == len(product_list) - 1:  # 处理最后一个产品
            gross_weight_for_this_product = round(
                gross_weight - accumulated_gross_weight, 2
            )
            volume_for_this_product = round(volume - accumulated_volume, 2)
        else:
            if product_record.get("single_weight"):
                gross_weight_for_this_product = (
                    product_record.get("single_weight") * box_num
                )
                gross_weight -= gross_weight_for_this_product
                num_products -= box_num
                avg_gross_weight = gross_weight / num_products
            else:
                gross_weight_for_this_product = round(avg_gross_weight * box_num, 2)
                accumulated_gross_weight += gross_weight_for_this_product
            volume_for_this_product = round(avg_volume * box_num, 2)
            accumulated_volume += volume_for_this_product

        net_weight_for_this_product = round(gross_weight_for_this_product * 0.8, 2)

        product_data = {
            "MasterBillNo": master_bill_no,
            "shipper_name": shipper_name,
            "shipper_address": shipper_address,
            "receiver_address": receiver_address,
            "receiver_name": receiver_name,
            "ProductName": product_name,
            "carton": box_num,
            "quanity": box_num * safe_float(packing,product_name),
            "danwei": "PCS" if product_record.get("HS_CODE") else "",
            "unit_price": single_price,
            "total_price": safe_float(single_price,product_name) * box_num * safe_float(packing,product_name),
            "HS_CODE": product_record.get("HS_CODE"),
            "DESCRIPTION": product_record.get("英文品名"),
            "GrossWeight": gross_weight_for_this_product,
            "net_weight": net_weight_for_this_product,
            "Volume": volume_for_this_product,
            "usage": product_record.get("用途"),
            "texture": product_record.get("材质"),
            "address_name": address_name or "",
            "address": address or "",
            "note": product_record.get("豁免代码"),
            "note_explaination": product_record.get("豁免代码含义"),
            "execute_type": execute_type,
            "huomian_file_name": product_record.get("huomian_file_name"),
            "good_type": product_record.get("类别"),
        }

        results.append(product_data)

    total_price_all = sum(i.get("total_price", 0) for i in results)
    good_type_totals = {}
    for p in results:
        good_type = p.get("good_type", "未知")
        if isinstance(good_type,list):
            good_type = good_type[0]
        good_type_totals[good_type] = good_type_totals.get(good_type, 0) + p.get("total_price", 0)
    
    # 按金额排序取前3
    sorted_types = sorted(good_type_totals.items(), key=lambda x: x[1], reverse=True)[:3]
    good_type_text = []
    for good_type, total in sorted_types:
        pct = round(total / total_price_all * 100, 2) if total_price_all else 0
        good_type_text.append(f"{good_type}-{pct}%")
    good_type_percentages = " ".join(good_type_text) if good_type_text else "未知-100%"
    summary_log_data = {
        "filename": master_bill_no,
        "generation_time": datetime.now(),
        "port": "",
        "packing_type": "",
        "shipper": shipper_name,
        "consignee": receiver_name,
        "estimated_tax_amount": totalyugutax,
        "gross_weight_kg": gross_weight,
        "volume_cbm": volume,
        "total_boxes": sum([i.box_num for i in product_list]),
        "estimated_tax_rate_cny_per_kg": predict_tax_price,
        "details": detail_data_log_list,
        'rate':exchange_rate["rate"],
        'total_price_sum':total_price_all,
        'good_type':good_type_percentages
    }
    return results, summary_log_data


web_vba_router = APIRouter()


@web_vba_router.post("/dalei/", response_model=Dalei)
def create_dalei(dalei: Dalei, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    dalei_dict = dalei.dict()
    dalei_dict.pop("id", None)
    result = db.dalei.insert_one(dalei_dict)
    dalei_dict["id"] = str(result.inserted_id)
    return dalei_dict


@web_vba_router.get("/dalei/")
def read_dalei(
    skip: int = 0,
    limit: int = 10,
    名称: Optional[str] = None,
    get_all: bool = False,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
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


@web_vba_router.get("/dalei/{id}", response_model=Dalei)
def read_dalei_by_id(id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    dalei = db.dalei.find_one({"_id": ObjectId(id)})
    if not dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")
    dalei["id"] = str(dalei["_id"])
    dalei.pop("_id", None)
    return dalei


@web_vba_router.put("/dalei/{id}", response_model=Dalei)
def update_dalei(id: str, dalei: Dalei, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
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


@web_vba_router.delete("/dalei/{id}", response_model=Dalei)
def delete_dalei(id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    dalei = db.dalei.find_one({"_id": ObjectId(id)})
    if not dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")
    db.dalei.delete_one({"_id": ObjectId(id)})
    dalei["id"] = str(dalei["_id"])
    dalei.pop("_id", None)
    return dalei


@web_vba_router.get("/products/", response_model=dict)
def read_products(
    skip: int = 0,
    limit: int = 10,
    名称: Optional[str] = None,
    get_all: bool = False,
    country: str = "China",
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
    query = {"country": country}
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


@web_vba_router.get("/products/upload_huomian_file")
def upload_huomian_file(file: UploadFile = File(...)):
    save_directory = Path("./file/huomian_file/")
    save_directory.mkdir(parents=True, exist_ok=True)
    file_name = f"{uuid.uuid4()}-{file.filename}"
    file_path = save_directory / file_name

    with file.file as file_content:
        with open(file_path, "wb") as buffer:
            buffer.write(file_content.read())
    return {"file_name": file_name}


@web_vba_router.post("/products/")
def create_product(
    product: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
    product_data = json.loads(product)
    product_data["更新时间"] = datetime.utcnow()

    if file:
        file_name = upload_huomian_file(file)["file_name"]
        product_data["huomian_file_name"] = file_name

    result = db.products.insert_one(product_data)
    product_data["id"] = str(result.inserted_id)
    product_data.pop("_id", None)
    return product_data


@web_vba_router.get("/products/{pic_name}")
def download_pic(pic_name: str):
    file_path = os.path.join("./file/huomian_file/", pic_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        file_path, media_type="application/octet-stream", filename=pic_name
    )


@web_vba_router.put("/products/{product_id}")
def update_product(
    product_id: str,
    product: str = Form(...),
    file: Optional[UploadFile] = File(None),
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
    product_data = json.loads(product)

    existing_product = db.products.find_one({"_id": ObjectId(product_id)})
    if not existing_product:
        raise HTTPException(status_code=404, detail="Product not found")

    update_data = {
        k: v for k, v in product_data.items() if k != "id" and v is not None and v != ""
    }

    if file:
        file_name = upload_huomian_file(file)["file_name"]
        update_data["huomian_file_name"] = file_name

    db.products.update_one({"_id": ObjectId(product_id)}, {"$set": update_data})
    updated_product = db.products.find_one({"_id": ObjectId(product_id)})
    updated_product["id"] = str(updated_product["_id"])
    updated_product.pop("_id", None)
    return updated_product


@web_vba_router.delete("/products/{product_id}")
def delete_product(product_id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    product = db.products.find_one({"_id": ObjectId(product_id)})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    db.products.delete_one({"_id": ObjectId(product_id)})
    product["id"] = str(product["_id"])
    product.pop("_id", None)
    return product


@web_vba_router.post("/process-shipping-data")
async def process_shipping_data_endpoint(
    request: ShippingRequest, session: MongoClient = Depends(get_session)
):
    try:
        results, summary_log = process_shipping_data(
            shipper_name=request.shipper_name,
            receiver_name=request.receiver_name,
            master_bill_no=request.master_bill_no,
            gross_weight=request.gross_weight,
            volume=request.volume,
            product_list=request.product_list,
            totalyugutax=request.totalyugutax,
            predict_tax_price=request.predict_tax_price,
            session=session,
        )

        if isinstance(results, dict) and results.get("product_attribute"):
            return JSONResponse(
                {"status": "False", "content": "产品属性在地址数据库中不存在"}
            )
        if (
            isinstance(results, dict)
            and results.get("type") == "net_weight大于gross_weight"
        ):
            return JSONResponse({"status": "False", "content": results.get("msg")})

        excel_path = generate_excel_from_template_test(results, request.totalyugutax)
        summary_log["packing_type"] = request.packing_type
        summary_log["port"] = request.port
        summary_log["filename"] = Path(excel_path).name

        await create_summary(summary_log, session)

        if results[0]["execute_type"] == "Sea":
            try:
                data = {
                    "shipper_name": request.shipper_name
                    + "\n"
                    + results[0]["shipper_address"],
                    "receiver_name": request.receiver_name
                    + "\n"
                    + results[0]["receiver_address"],
                    "master_bill_no": request.master_bill_no,
                    "gross_weight": request.gross_weight,
                    "volume": request.volume,
                    "total_boxes": summary_log["total_boxes"],
                    "all_english_name": ",".join([i["DESCRIPTION"] for i in results]),
                    "other_data": {"totalyugutax": request.totalyugutax},
                }
                await create_shipment_log(ShipmentLog(**data), session)
            except Exception as e:
                logger.error(f"错误为:{e}")

        return FileResponse(
            path=excel_path,
            filename=f"{request.master_bill_no} CI&PL.{excel_path.split('.')[-1]}",
        )
    except ValueError as e:
        logger.error(f"Value Error: {traceback.format_exc()}")
        return JSONResponse({"status": "False", "content": f"Value Error: {str(e)}"})
    except Exception as e:
        logger.error(f"Internal Server Error: {str(e)}---{traceback.format_exc()}")
        return JSONResponse(
            {"status": "False", "content": f"Internal Server Error: {str(e)}"}
        )


# ... (rest of the CRUD operations would follow similar MongoDB conversion pattern)


@web_vba_router.get("/shippersandreceivers/", response_model=dict)
def read_shippers_and_receivers(
    skip: int = 0,
    limit: int = 10,
    ShipperName: Optional[str] = None,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
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


@web_vba_router.post("/shippersandreceivers/", response_model=ShippersAndReceivers)
def create_shipper_or_receiver(
    shipper_or_receiver: ShippersAndReceivers,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
    shipper_dict = shipper_or_receiver.dict()
    shipper_dict.pop("id", None)
    result = db.shippersandreceivers.insert_one(shipper_dict)
    shipper_dict["id"] = str(result.inserted_id)
    return shipper_dict


@web_vba_router.put("/shippersandreceivers/{id}", response_model=ShippersAndReceivers)
def update_shipper_or_receiver(
    id: str,
    shipper_or_receiver: ShippersAndReceivers,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
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


@web_vba_router.delete(
    "/shippersandreceivers/{id}", response_model=ShippersAndReceivers
)
def delete_shipper_or_receiver(id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    shipper = db.shippersandreceivers.find_one({"_id": ObjectId(id)})
    if not shipper:
        raise HTTPException(status_code=404, detail="Shipper or Receiver not found")
    db.shippersandreceivers.delete_one({"_id": ObjectId(id)})
    shipper["id"] = str(shipper["_id"])
    shipper.pop("_id", None)
    return shipper


@web_vba_router.post("/ports/", response_model=Port)
def create_port(port: Port, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    port_dict = port.dict()
    port_dict.pop("id", None)
    result = db.ports.insert_one(port_dict)
    port_dict["id"] = str(result.inserted_id)
    return port_dict


@web_vba_router.get("/ports/", response_model=List[Port])
def read_ports(
    session: MongoClient = Depends(get_session),
    skip: int = 0,
    limit: Optional[int] = None,
):
    db = session["qingguan"]
    query = {}
    cursor = db.ports.find(query).skip(skip)
    if limit:
        cursor = cursor.limit(limit)
    ports = list(cursor)
    for port in ports:
        port["id"] = str(port["_id"])
        port.pop("_id", None)
    return ports


@web_vba_router.get("/ports/{port_id}", response_model=Port)
def read_port(port_id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    port["id"] = str(port["_id"])
    port.pop("_id", None)
    return port


@web_vba_router.put("/ports/{port_id}", response_model=Port)
def update_port(
    port_id: str, updated_port: Port, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")

    update_data = updated_port.dict(exclude_unset=True)
    update_data.pop("id", None)
    db.ports.update_one({"_id": ObjectId(port_id)}, {"$set": update_data})
    updated = db.ports.find_one({"_id": ObjectId(port_id)})
    updated["id"] = str(updated["_id"])
    updated.pop("_id", None)
    return updated


@web_vba_router.delete("/ports/{port_id}", response_model=Port)
def delete_port(port_id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    db.ports.delete_one({"_id": ObjectId(port_id)})
    port["id"] = str(port["_id"])
    port.pop("_id", None)
    return port


# 工厂数据CRUD操作
@web_vba_router.post("/factory/", response_model=FactoryData)
def create_factory(factory: FactoryData, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    factory_dict = factory.dict()
    factory_dict.pop("id", None)
    result = db.factories.insert_one(factory_dict)
    factory_dict["id"] = str(result.inserted_id)
    return factory_dict


@web_vba_router.get("/factory/")
def read_factories(
    skip: int = 0,
    limit: Optional[int] = None,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
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


@web_vba_router.get("/factory/{factory_id}", response_model=FactoryData)
def read_factory(factory_id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    factory = db.factories.find_one({"_id": ObjectId(factory_id)})
    if not factory:
        raise HTTPException(status_code=404, detail="Factory not found")
    factory["id"] = str(factory["_id"])
    factory.pop("_id", None)
    return factory


@web_vba_router.put("/factory/{factory_id}", response_model=FactoryData)
def update_factory(
    factory_id: str, factory: FactoryData, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
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


@web_vba_router.delete("/factory/{factory_id}", response_model=FactoryData)
def delete_factory(factory_id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    factory = db.factories.find_one({"_id": ObjectId(factory_id)})
    if not factory:
        raise HTTPException(status_code=404, detail="Factory not found")
    db.factories.delete_one({"_id": ObjectId(factory_id)})
    factory["id"] = str(factory["_id"])
    factory.pop("_id", None)
    return factory


# 收发货人CRUD操作
@web_vba_router.post("/consignee/", response_model=ConsigneeData)
def create_consignee(
    consignee: ConsigneeData, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    consignee_dict = consignee.dict()
    consignee_dict.pop("id", None)
    result = db.consignees.insert_one(consignee_dict)
    consignee_dict["id"] = str(result.inserted_id)
    return consignee_dict


@web_vba_router.get("/consignee/")
def read_consignees(
    skip: int = 0,
    limit: Optional[int] = None,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
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


@web_vba_router.get("/consignee/{consignee_id}", response_model=ConsigneeData)
def read_consignee(consignee_id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    consignee = db.consignees.find_one({"_id": ObjectId(consignee_id)})
    if not consignee:
        raise HTTPException(status_code=404, detail="Consignee not found")
    consignee["id"] = str(consignee["_id"])
    consignee.pop("_id", None)
    return consignee


@web_vba_router.put("/consignee/{consignee_id}", response_model=ConsigneeData)
def update_consignee(
    consignee_id: str,
    consignee: ConsigneeData,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
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


@web_vba_router.delete("/consignee/{consignee_id}", response_model=ConsigneeData)
def delete_consignee(consignee_id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    consignee = db.consignees.find_one({"_id": ObjectId(consignee_id)})
    if not consignee:
        raise HTTPException(status_code=404, detail="Consignee not found")
    db.consignees.delete_one({"_id": ObjectId(consignee_id)})
    consignee["id"] = str(consignee["_id"])
    consignee.pop("_id", None)
    return consignee


@web_vba_router.get("/api/exchange-rate")
def get_exchange_rate(session: MongoClient = Depends(get_session)):
    url = "https://finance.pae.baidu.com/selfselect/sug?wd=%E7%BE%8E%E5%85%83%E4%BA%BA%E6%B0%91%E5%B8%81&skip_login=1&finClientType=pc"
    db = session["qingguan"]
    exchange_rate = db.exchange_rates.find_one({"version": "latest"})
    print(exchange_rate)
    if exchange_rate:
        rate = exchange_rate["rate"]
        return {"USDCNY": rate}
    # 12月上汇率
    rate = "7.3000"
    return {"USDCNY": rate}


@web_vba_router.post("/login")
def login_for_access_token(
    user: UserLogin, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    user_db = db.users.find_one({"username": user.username})
    if not user_db or not bcrypt.checkpw(
        user.password.encode("utf-8"), user_db["password"].encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    permissions = enforcer.get_filtered_policy(0, user_db["username"])
    print(permissions)
    access_token_expires = timedelta(hours=1)
    access_token = create_access_token(
        data={"sub": user_db["username"], "permissions": permissions,'menu_ids':user_db.get('menu_ids',[])},
        expires_delta=access_token_expires,
    )

    refresh_token = create_refresh_token(data={"sub": user_db["username"]})

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }


@web_vba_router.post("/users/", response_model=User)
def create_user(user: UserCreate, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    if db.users.find_one({"username": user.username}):
        raise HTTPException(status_code=400, detail="Username already exists")

    hashed_password = bcrypt.hashpw(user.password.encode("utf-8"), bcrypt.gensalt())
    new_user = {"username": user.username, "password": hashed_password.decode("utf-8")}
    result = db.users.insert_one(new_user)
    new_user["id"] = str(result.inserted_id)

    # 添加用户权限
    # for perm in user.permissions:
    #     obj, act = perm.split(':')
    #     enforcer.add_policy(user.username, obj, act,'allow')

    # enforcer.load_policy()
    return new_user


@web_vba_router.put("/users/{user_id}/", response_model=User)
def update_user(
    user_id: str, user_update: UserUpdate, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    user_db = db.users.find_one({"_id": ObjectId(user_id)})
    if not user_db:
        raise HTTPException(status_code=404, detail="User not found")

    update_data = {}
    if user_update.username:
        update_data["username"] = user_update.username
    if user_update.password:
        hashed_password = bcrypt.hashpw(
            user_update.password.encode("utf-8"), bcrypt.gensalt()
        )
        update_data["password"] = hashed_password.decode("utf-8")

    if update_data:
        db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update_data})

    # 更新用户权限


    # if user_update.permissions is not None:
    #     current_policies = enforcer.get_filtered_policy(0, user_db["username"])
    #     current_permissions = {
    #         f"{policy[1]}:{policy[2]}"
    #         for policy in current_policies
    #         if policy[3] == "allow"
    #     }
    #     update_permissions = set(user_update.permissions)

    #     # 需要删除的权限
    #     for perm in current_permissions - update_permissions:
    #         obj, act = perm.split(":")
    #         enforcer.update_policy(
    #             [user_db["username"], obj, act, "allow"],
    #             [user_db["username"], obj, act, "deny"],
    #         )

    #     # 需要添加的权限
    #     for perm in update_permissions - current_permissions:
    #         obj, act = perm.split(":")
    #         enforcer.add_policy(user_db["username"], obj, act, "allow")

    enforcer.load_policy()
    return db.users.find_one({"_id": ObjectId(user_id)})


@web_vba_router.get("/users/", response_model=List[User])
def read_users(
    skip: int = 0, limit: int = 10, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    users = list(db.users.find().skip(skip).limit(limit))
    for user in users:
        user["id"] = str(user["_id"])
    return users


@web_vba_router.get("/users/{user_id}/", response_model=User)
def read_user(user_id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    user = db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user["id"] = str(user["_id"])
    return user


@web_vba_router.delete("/users/{user_id}/", response_model=User)
def delete_user(user_id: str, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    user = db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # 删除用户权限
    enforcer.delete_roles_for_user(user["username"])
    enforcer.delete_user(user["username"])

    db.users.delete_one({"_id": ObjectId(user_id)})

    return {"message": "User and associated permissions deleted successfully"}


@web_vba_router.post("/add_policy/")
def add_policy(policy: Policy):
    if enforcer.add_policy(policy.sub, policy.obj, policy.act, policy.eft):
        enforcer.load_policy()
        return {"message": "策略添加成功"}
    else:
        raise HTTPException(status_code=400, detail="策略已存在或无法添加")


@web_vba_router.delete("/remove_policy/")
async def remove_policy(policy: Policy):
    if enforcer.remove_policy(policy.sub, policy.obj, policy.act, policy.eft, "", ""):
        enforcer.load_policy()
        return {"message": "策略删除成功"}
    else:
        raise HTTPException(status_code=400, detail="策略不存在或无法删除")


@web_vba_router.put("/update_policy/")
async def update_policy(update_policy: UpdatePolicy):
    old_policy = [
        update_policy.old_sub,
        update_policy.old_obj,
        update_policy.old_act,
        update_policy.old_eft,
        "",
        "",
    ]
    new_policy = [
        update_policy.new_sub,
        update_policy.new_obj,
        update_policy.new_act,
        update_policy.new_eft,
        "",
        "",
    ]

    if not enforcer.has_policy(*old_policy):
        raise HTTPException(status_code=404, detail="旧策略不存在")

    result = enforcer.update_policy(old_policy, new_policy)

    if result:
        enforcer.load_policy()
        return {"message": "策略更新成功"}
    else:
        raise HTTPException(status_code=400, detail="策略更新失败或未找到旧策略")


@web_vba_router.get("/get_policies/")
async def get_policies():
    return enforcer.get_policy()


@web_vba_router.get("/get_user_policies/")
async def get_user_policies(user: str):
    user_policies = enforcer.get_filtered_policy(0, user)
    if not user_policies:
        raise HTTPException(status_code=404, detail="该用户没有策略")
    return user_policies


@web_vba_router.post("/add_group/")
async def add_group(group: Group):
    if enforcer.add_grouping_policy(group.user, group.group):
        enforcer.load_policy()
        return {"message": "组添加成功"}
    else:
        raise HTTPException(status_code=400, detail="组已存在或无法添加")


@web_vba_router.delete("/remove_group/")
async def remove_group(group: Group):
    if enforcer.remove_grouping_policy(group.user, group.group):
        enforcer.load_policy()
        return {"message": "组删除成功"}
    else:
        raise HTTPException(status_code=400, detail="组不存在或无法删除")


@web_vba_router.get("/get_groups/")
async def get_groups():
    return enforcer.get_grouping_policy()


@web_vba_router.post("/ip_white_list/", response_model=IpWhiteList)
def create_ip_white_list(
    ip_white_list: IpWhiteList, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    if db.ip_white_list.find_one({"ip": ip_white_list.ip}):
        raise HTTPException(status_code=400, detail="IP already exists")
    result = db.ip_white_list.insert_one(ip_white_list.dict())
    ip_white_list.id = str(result.inserted_id)
    return ip_white_list


@web_vba_router.get("/ip_white_list/", response_model=List[IpWhiteList])
def get_all_ip_white_list(session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    ip_white_lists = list(db.ip_white_list.find())
    for item in ip_white_lists:
        item["id"] = str(item["_id"])
    return ip_white_lists


@web_vba_router.get("/ip_white_list/{ip_white_list_id}", response_model=IpWhiteList)
def get_ip_white_list(
    ip_white_list_id: str, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    ip_white_list = db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})
    if not ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")
    ip_white_list["id"] = str(ip_white_list["_id"])
    return ip_white_list


@web_vba_router.put("/ip_white_list/{ip_white_list_id}", response_model=IpWhiteList)
def update_ip_white_list(
    ip_white_list_id: str,
    ip_white_list: IpWhiteList,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
    db_ip_white_list = db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})
    if not db_ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")

    update_data = ip_white_list.dict(exclude_unset=True)
    db.ip_white_list.update_one(
        {"_id": ObjectId(ip_white_list_id)}, {"$set": update_data}
    )
    return db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})


@web_vba_router.delete("/ip_white_list/{ip_white_list_id}", response_model=IpWhiteList)
def delete_ip_white_list(
    ip_white_list_id: str, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    ip_white_list = db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})
    if not ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")
    db.ip_white_list.delete_one({"_id": ObjectId(ip_white_list_id)})
    return ip_white_list


@web_vba_router.get("/files", response_model=List[FileInfo])
async def get_files():
    directory_path = "./pdf"
    if not os.path.exists(directory_path):
        raise HTTPException(status_code=404, detail="Directory not found")

    files = []
    for file_name in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file_name)
        if os.path.isfile(file_path):
            file_info = os.stat(file_path)
            files.append(
                FileInfo(
                    name=file_name, time=datetime.fromtimestamp(file_info.st_mtime)
                )
            )

    files.sort(key=lambda x: x.time, reverse=True)

    return files[:500]


@web_vba_router.get("/download/{file_name}")
async def download_file(file_name: str):
    file_path = os.path.join("./pdf", file_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        file_path, media_type="application/octet-stream", filename=file_name
    )


@web_vba_router.post(
    "/cumstom_clear_history_summary/",
)
async def create_summary(summary: dict, session: MongoClient = Depends(get_session)):
    db = session["qingguan"]
    summary_dict = {k: v for k, v in summary.items() if k != "id"}
    # print(summary_dict)
    result = db.custom_clear_history_summary.insert_one(summary_dict)
    summary_dict["id"] = str(result.inserted_id)
    # logger.info(f"新增清理历史汇总成功: {summary_dict}")
    if summary_dict["estimated_tax_rate_cny_per_kg"] >= 1.2:
        send_email(
            receiver_email="caitlin.fang@hubs-scs.com",
            subject=f"{'-'.join(summary_dict['filename'].split('-')[1:-1]).replace('CI&PL','').strip()}-{summary_dict['estimated_tax_amount']}-{summary_dict['estimated_tax_rate_cny_per_kg']} CNY/Kg",
            body="",
        )
    # for detail in summary_dict['details']:
    #     detail['summary_log_id'] = summary_dict['id']
    #     detail['generation_time'] = summary_dict['generation_time']
    #     db.custom_clear_history_detail.insert_one(detail)

    return summary_dict


@web_vba_router.post(
    "/update_cumstom_clear_history_summary_remarks/",
)
async def update_summary(
    request_body: dict, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    # 如果remarks为空，则将abnormal的值赋值给remarks
    update_data = {}
    if "remarks" in request_body:
        update_data["remarks"] = request_body["remarks"]
    if "abnormal" in request_body:
        update_data["abnormal"] = request_body["abnormal"]
    db.custom_clear_history_summary.update_one(
        {"_id": ObjectId(request_body["id"])},
        {"$set": update_data},
    )
    result = db.custom_clear_history_summary.find_one(
        {"_id": ObjectId(request_body["id"])}
    )
    if result:
        result["id"] = str(result.pop("_id"))
    return result


@web_vba_router.get("/cumstom_clear_history_summary/")
def read_summaries(
    enable_pagination: bool = Query(False, description="Enable pagination"),
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(10, description="Number of items per page", ge=1, le=100),
    file_name: Optional[str] = Query(None, description="File name to filter by"),
    convey_type: Optional[str] = Query(None, description="convey_type to filter by"),
    remarks: Optional[str] = Query(None, description="remarks filter by"),
    abnormal:Optional[str] = Query(None, description="abnormal filter by"),
    start_time: datetime = Query(None, description="开始时间"),
    end_time: datetime = Query(None, description="结束时间"),
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
    collection = db.custom_clear_history_summary

    query = {"$or": [{"remarks": {"$ne": "删除"}}, {"remarks": None}]}

    if file_name:
        query["filename"] = {"$regex": f".*{file_name}.*", "$options": "i"}

    if remarks:
        query["remarks"] = {"$regex": f".*{remarks}.*", "$options": "i"}
    if abnormal:
        query["abnormal"] = {"$regex": f".*{abnormal}.*", "$options": "i"}

    if convey_type:
        #如果运输方式为海运，则查询packing_type不为空的，如果为空运，则port不为空的
        if convey_type == "海运":
            query["packing_type"] = {"$ne": ""}
        elif convey_type == "空运":
            query["port"] = {"$ne": ""}
    if start_time:
        query["generation_time"] = {"$gte": start_time,"$lte": end_time}

    if enable_pagination:
        offset = (page - 1) * page_size
        summaries = list(
            collection.find(query)
            .sort("generation_time", -1)
            .skip(offset)
            .limit(page_size)
        )

        # Convert ObjectId to string
        for summary in summaries:
            summary["id"] = str(summary.pop("_id"))

        total = collection.count_documents(query)
        total_pages = (total + page_size - 1) // page_size

        return {"summaries": summaries, "total": total, "total_pages": total_pages}
    else:
        summaries = list(collection.find(query).sort("generation_time", -1))
        # Convert ObjectId to string
        for summary in summaries:
            summary["id"] = str(summary.pop("_id"))

        return {"summaries": summaries, "total": len(summaries), "total_pages": 1}


# 查询单个 Summary 记录
@web_vba_router.get(
    "/cumstom_clear_history_summary/{summary_id}",
)
async def read_summary(summary_id: str, session: MongoClient = Depends(get_session)):
    try:
        db = session["qingguan"]
        if not ObjectId.is_valid(summary_id):
            raise HTTPException(status_code=400, detail="Invalid ID format")

        summary = db.custom_clear_history_summary.find_one(
            {"_id": ObjectId(summary_id)}
        )
        if not summary:
            raise HTTPException(status_code=404, detail="Summary not found")
        summary["id"] = str(summary.pop("_id"))
        return summary
    except Exception as e:
        logger.error(f"Error reading summary: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.get("/output_cumtoms_clear_log/")
async def output_log(
    start_time: str = Query(..., description="开始时间"),
    end_time: str = Query(..., description="结束时间"),
):
    file_path = output_custom_clear_history_log(start_time, end_time)
    #将文件路径转换为文件流
    file_stream = open(file_path, "rb")
    # 返回 Excel 文件
    return StreamingResponse(
        file_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=custom_clear_history_log.xlsx"
        },
    )

 


@web_vba_router.post(
    "/haiyunzishui/",
)
async def create_haiyunzishui(
    haiyunzishui: HaiYunZiShui, session: MongoClient = Depends(get_session)
):
    try:
        db = session["qingguan"]

        haiyunzishui_dict = haiyunzishui.model_dump()
        haiyunzishui_dict.pop("id", None)

        result = db.haiyunzishui.insert_one(haiyunzishui_dict)
        if result.inserted_id:
            haiyunzishui_dict["_id"] = str(result.inserted_id)
            return haiyunzishui_dict
        raise HTTPException(status_code=500, detail="Failed to create haiyunzishui")
    except Exception as e:
        logger.error(f"Error creating haiyunzishui: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.get("/haiyunzishui/", response_model=List[HaiYunZiShui])
async def read_haiyunzishuis(
    session: MongoClient = Depends(get_session),
    skip: int = 0,
    limit: Optional[int] = None,
):
    try:
        db = session["qingguan"]
        query = db.haiyunzishui.find().skip(skip)
        if limit:
            query = query.limit(limit)
        haiyunzishuis = list(query)
        # print(haiyunzishuis)
        for item in haiyunzishuis:
            # 将_id转换为字符串并赋值给id字段
            item["id"] = str(item["_id"])
            # 删除原始的_id字段
            item.pop("_id", None)

        return haiyunzishuis
    except Exception as e:
        logger.error(f"Error reading haiyunzishuis: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.get(
    "/haiyunzishui/{haiyunzishui_id}",
)
async def read_haiyunzishui(
    haiyunzishui_id: str, session: MongoClient = Depends(get_session)
):
    try:
        db = session["qingguan"]
        if not ObjectId.is_valid(haiyunzishui_id):
            raise HTTPException(status_code=400, detail="Invalid ID format")

        haiyunzishui = db.haiyunzishui.find_one({"_id": ObjectId(haiyunzishui_id)})
        if not haiyunzishui:
            raise HTTPException(status_code=404, detail="Haiyunzishui not found")
        haiyunzishui["_id"] = str(haiyunzishui["_id"])
        return haiyunzishui
    except Exception as e:
        logger.error(f"Error reading haiyunzishui: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.put(
    "/haiyunzishui/{haiyunzishui_id}",
)
async def update_haiyunzishui(
    haiyunzishui_id: str,
    updated_haiyunzishui: HaiYunZiShui,
    session: MongoClient = Depends(get_session),
):
    try:
        db = session["qingguan"]
        if not ObjectId.is_valid(haiyunzishui_id):
            raise HTTPException(status_code=400, detail="Invalid ID format")

        existing_haiyunzishui = db.haiyunzishui.find_one(
            {"_id": ObjectId(haiyunzishui_id)}
        )
        if not existing_haiyunzishui:
            raise HTTPException(status_code=404, detail="Haiyunzishui not found")

        update_data = updated_haiyunzishui.model_dump(exclude_unset=True)
        db.haiyunzishui.update_one(
            {"_id": ObjectId(haiyunzishui_id)}, {"$set": update_data}
        )
        updated_haiyunzishui = db.haiyunzishui.find_one(
            {"_id": ObjectId(haiyunzishui_id)}
        )
        updated_haiyunzishui["_id"] = str(updated_haiyunzishui["_id"])
        return updated_haiyunzishui
    except Exception as e:
        logger.error(f"Error updating haiyunzishui: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.delete(
    "/haiyunzishui/{haiyunzishui_id}",
)
async def delete_haiyunzishui(
    haiyunzishui_id: str, session: MongoClient = Depends(get_session)
):
    try:
        db = session["qingguan"]
        if not ObjectId.is_valid(haiyunzishui_id):
            raise HTTPException(status_code=400, detail="Invalid ID format")

        haiyunzishui = db.haiyunzishui.find_one({"_id": ObjectId(haiyunzishui_id)})
        if not haiyunzishui:
            raise HTTPException(status_code=404, detail="Haiyunzishui not found")

        db.haiyunzishui.delete_one({"_id": ObjectId(haiyunzishui_id)})
        haiyunzishui["_id"] = str(haiyunzishui["_id"])
        return haiyunzishui
    except Exception as e:
        logger.error(f"Error deleting haiyunzishui: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# 增：创建 ShipmentLog
@web_vba_router.post("/shipment_logs/")
async def create_shipment_log(
    shipment_log: ShipmentLog, session: MongoClient = Depends(get_session)
):
    try:
        db = session["qingguan"]
        # 检查是否已存在相同提单号的记录
        existing_log = db.shipment_logs.find_one(
            {"master_bill_no": shipment_log.master_bill_no}
        )
        if existing_log:
            raise HTTPException(
                status_code=400,
                detail="Shipment log with this bill number already exists",
            )

        shipment_log_dict = shipment_log.model_dump()
        result = db.shipment_logs.insert_one(shipment_log_dict)

        if result.inserted_id:
            shipment_log_dict["_id"] = str(result.inserted_id)
            return shipment_log_dict
        raise HTTPException(status_code=500, detail="Failed to create shipment log")
    except Exception as e:
        logger.error(f"Error creating shipment log: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.put("/shipment_logs/{shipment_log_id}")
async def update_shipment_log(
    shipment_log_id: str,
    shipment_log: ShipmentLog,
    session: MongoClient = Depends(get_session),
):
    try:
        db = session["qingguan"]
        if not ObjectId.is_valid(shipment_log_id):
            raise HTTPException(
                status_code=400, detail="Invalid shipment log ID format"
            )

        existing_log = db.shipment_logs.find_one({"_id": ObjectId(shipment_log_id)})
        if not existing_log:
            raise HTTPException(status_code=404, detail="ShipmentLog not found")

        update_data = shipment_log.model_dump(exclude_unset=True)
        db.shipment_logs.update_one(
            {"_id": ObjectId(shipment_log_id)}, {"$set": update_data}
        )
        updated_log = db.shipment_logs.find_one({"_id": ObjectId(shipment_log_id)})
        updated_log["_id"] = str(updated_log["_id"])
        updated_log["id"] = str(updated_log["_id"])
        return updated_log
    except Exception as e:
        logger.error(f"Error updating shipment log: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.get("/shipment_logs/", response_model=dict)
async def read_shipment_logs(
    status: Optional[int] = Query(None, description="Filter by status"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Limit for pagination"),
    session: MongoClient = Depends(get_session),
):
    try:
        db = session["qingguan"]
        query = {}
        if status is not None:
            query["$or"] = [{"status": status}, {"status": -1}]

        cursor = db.shipment_logs.find(query).skip(offset).limit(limit)
        shipment_logs = []
        for log in cursor:
            log["_id"] = str(log["_id"])
            log["id"] = str(log["_id"])
            shipment_logs.append(log)

        total_count = db.shipment_logs.count_documents(query)
        total_pages = (total_count + limit - 1) // limit

        return {
            "shipment_logs": shipment_logs,
            "total": total_count,
            "total_pages": total_pages,
        }
    except Exception as e:
        logger.error(f"Error reading shipment logs: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.get("/shipment_logs/{master_bill_no}", response_model=dict)
async def read_shipment_log(
    master_bill_no: str, session: MongoClient = Depends(get_session)
):
    try:
        db = session["qingguan"]
        if not master_bill_no or len(master_bill_no.strip()) == 0:
            raise HTTPException(
                status_code=400, detail="Master bill number cannot be empty"
            )

        shipment_log = db.shipment_logs.find_one({"master_bill_no": master_bill_no})
        if not shipment_log:
            raise HTTPException(status_code=404, detail="ShipmentLog not found")

        shipment_log["_id"] = str(shipment_log["_id"])
        return {"shipment_logs": shipment_log, "total": 1, "total_pages": 1}
    except Exception as e:
        logger.error(f"Error reading shipment log: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.get("/get_tidan_pdf_again/{id}")
async def get_tidan_pdf(id: str, session: MongoClient = Depends(get_session)):
    # 获取 ShipmentLog 数据
    db = session["qingguan"]
    request_data = db.shipment_logs.find_one({"_id": ObjectId(id)})
    if not request_data:
        raise HTTPException(status_code=404, detail="ShipmentLog not found")
    node_path = find_playwright_node_path()
    morelink_client = MoreLinkClient(node_path)
    # morelink_client = MoreLinkClient()
    data = morelink_client.zongdan_api_httpx()

    filter_data = [
        row for row in data if row.get("billno") == request_data["master_bill_no"]
    ]

    if not filter_data:
        logger.log("ALERT", f"morelink提单号搜索不到：{request_data['master_bill_no']}")
        return

    pdf_file = shenzhen_customes_pdf_gennerate(request_data, filter_data[0])
    logger.info(f"已生成pdf文件->{pdf_file}")

    # 更新 ShipmentLog 的状态
    db.shipment_log.update_one({"_id": ObjectId(id)}, {"$set": {"status": 1}})

    # 读取 PDF 文件内容
    with open(pdf_file, "rb") as file:
        pdf_content = file.read()

    # 返回 PDF 文件
    response = Response(content=pdf_content, media_type="application/pdf")
    response.headers["Content-Disposition"] = f"attachment; filename={pdf_file}"
    return response


@web_vba_router.get("/5_letters_hscode/", response_model=dict)
def read_5_letters_hscode(
    skip: int = 0,
    limit: int = 10,
    chinese_goods_name: Optional[str] = None,
    goods_name: Optional[str] = None,
    get_all: bool = False,
    session: MongoClient = Depends(get_session),
):
    db = session["qingguan"]
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


@web_vba_router.post("/5_letters_hscode/")
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
    db = session["qingguan"]
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


@web_vba_router.put("/5_letters_hscode/{five_letters_hscode_id}")
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
    db = session["qingguan"]

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


@web_vba_router.delete("/5_letters_hscode/{five_letters_hscode_id}")
def delete_5_letters_hscode(
    five_letters_hscode_id: str, session: MongoClient = Depends(get_session)
):
    db = session["qingguan"]
    five_letters_hscode = db["5_letters_hscode"].find_one(
        {"_id": ObjectId(five_letters_hscode_id)}
    )
    if not five_letters_hscode:
        raise HTTPException(status_code=404, detail="5_letters_hscode not found")
    db["5_letters_hscode"].delete_one({"_id": ObjectId(five_letters_hscode_id)})
    five_letters_hscode["id"] = str(five_letters_hscode["_id"])
    five_letters_hscode.pop("_id", None)
    return five_letters_hscode


@web_vba_router.post("/process_excel_usp_data")
async def process_excel_usp_data(file: UploadFile = File(...)):
    """处理上传的Excel文件"""
    try:
        # 读取上传的文件内容
        
        contents = await file.read()
        
        # 使用pandas读取Excel文件
        xls = pd.ExcelFile(io.BytesIO(contents))
        
        # 检查必要的工作表是否存在
        required_sheets = ['数据粘贴', 'LAX分区', '燃油', '尾程25年非旺季报价单']
        missing_sheets = [sheet for sheet in required_sheets if sheet not in xls.sheet_names]
        
        if missing_sheets:
            raise ValueError(f"缺少工作表: {', '.join(missing_sheets)}")
        
        # 读取各个工作表
        sheet_data = pd.read_excel(xls, sheet_name='数据粘贴',header=1)
        if '邮编' in sheet_data.columns:
            sheet_data['邮编'] = sheet_data['邮编'].astype(str).str.zfill(5)
        sheet_lax_partition = pd.read_excel(xls, sheet_name='LAX分区',skiprows=5)
        sheet_fuel = pd.read_excel(xls, sheet_name='燃油')
        sheet_fuel = sheet_fuel.dropna(subset=[sheet_fuel.columns[0]])

        # sheet_usp_raw = pd.read_excel(xls, sheet_name='USPS报价单',skiprows=1)
        sheet_usp_25 = pd.read_excel(xls, sheet_name='尾程25年非旺季报价单',skiprows=1)

        # 检查数据有效性
        if sheet_data.empty or sheet_lax_partition.empty or sheet_fuel.empty :
            raise ValueError("一个或多个工作表为空")
        
        # 处理燃油数据
        fuel_data = []
        for _, row in sheet_fuel.iterrows():
            date_range = str(row[0]).split('~')
            start_date = date_range[0].strip()
            end_date = date_range[1].strip()
            
            # 将日-月-年转换为年-月-日
            start_parts = start_date.split('-')
            end_parts = end_date.split('-')
            
            if len(start_parts) == 3 and len(end_parts) == 3:
                try:
                    start_date = pd.to_datetime(f"{start_parts[2]}-{start_parts[1]}-{start_parts[0]}", format='%Y-%m-%d')
                    end_date = pd.to_datetime(f"{end_parts[2]}-{end_parts[1]}-{end_parts[0]}", format='%Y-%m-%d')
                except ValueError:
                    print(f"燃油数据日期转换错误: {start_date}, {end_date}")
                    continue
                
                fuel_data.append({
                    'startDate': start_date,
                    'endDate': end_date,
                    'rate': float(row[1])
                })
        
        # 处理USPS报价单数据
        def process_usp_sheet(sheet):
            usp_data = {}
            for i in range(len(sheet)):
                row_name = sheet.iloc[i, 0]  # A列作为行名
                if pd.notna(row_name):
                    usp_data[row_name] = {}
                    for j in range(1, len(sheet.columns)):
                        col_name = sheet.columns[j]
                        try:
                            usp_data[row_name][col_name] = float(sheet.iloc[i, j])
                        except ValueError:
                            usp_data[row_name][col_name] = sheet.iloc[i, j]
            return usp_data
        
        # sheet_usp = process_usp_sheet(sheet_usp_raw)
        # sheet_usp_25_data = process_usp_sheet(sheet_usp_25)
        
        # 检查日期格式
        # sheet_data['第一枪\n扫描时间时间'] = pd.to_datetime(sheet_data['第一枪\n扫描时间时间'], errors='coerce')
        sheet_data['第一枪\n扫描时间时间'] = sheet_data['第一枪\n扫描时间时间'].apply(lambda x: pd.to_datetime('1899-12-30') + pd.to_timedelta(x, unit='D') if pd.notna(x) and isinstance(x, (int, float)) else x)
        invalid_dates = sheet_data[sheet_data['第一枪\n扫描时间时间'].isna()]
        
        if not invalid_dates.empty:
            raise ValueError("日期格式不对可能为空")
        
        # 获取联邦快递的邮政编码
        fedex_pdf_path = os.path.join(os.getcwd(), 'file', 'remoteaddresscheck', 'DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf')  # 确保PDF文件名正确
        fedex_zip_codes_by_category = extract_zip_codes_from_pdf(fedex_pdf_path)

        ups_zip_data = get_ups_zip_data()
        # 处理每一行数据
        for index, row in sheet_data.iterrows():
            # 计算计费重量
            jifei_weight = np.ceil(row['重量\n(LB)'])
            if pd.isna(jifei_weight):
                jifei_weight = 0
            
            # 获取邮编前五位
            zip_code = str(row['邮编']).zfill(5)
            zip_code_prefix = zip_code[:5] if len(zip_code) >= 5 else zip_code
            
            # 查找分区
            partition = '未找到分区'
            for _, partition_row in sheet_lax_partition.iterrows():
                dest_zip = str(partition_row['Dest. ZIP']).strip()
                if '-' in dest_zip:
                    zip_range = dest_zip.split('-')
                    if len(zip_range) == 2:
                        try:
                            start_zip = int(zip_range[0])
                            end_zip = int(zip_range[1])
                            zip_prefix = int(zip_code_prefix)
                            if start_zip <= zip_prefix <= end_zip:
                                partition = partition_row['Ground']
                                break  # 找到分区后退出循环
                        except ValueError:
                            continue  # 如果转换失败，则跳过此行
                else:
                    if dest_zip.startswith(zip_code_prefix):
                        partition = partition_row['Ground']
                        break  # 找到分区后退出循环
            
            # 获取订单日期
            order_date = row['第一枪\n扫描时间时间']
            
            # 查找燃油费率
            fuel_rate = 0
            for fuel in fuel_data:
                if order_date and fuel['startDate'] <= order_date <= fuel['endDate']:
                    fuel_rate = fuel['rate']
                    break
            
            # 根据月份选择不同的报价单
            # current_sheet_usp = sheet_usp_25 #默认使用25年
            #只需要到71行的数据
            current_sheet_usp = sheet_usp_25.iloc[:50]

            # 查找价格
            money = 0
            if int(jifei_weight) in [int(i) for i in current_sheet_usp['Ibs'].values] and int(partition) in [int(i) for i in list(current_sheet_usp.columns)[2:]]:
                partition = str(int(float(partition))).zfill(3)
                money = current_sheet_usp.loc[int(jifei_weight)-1, partition]
            
            # 计算总金额
            all_money = np.ceil(money * (1 + fuel_rate) * 100) / 100
            
            # 更新数据
            sheet_data.at[index, '计费重量（美制）'] = jifei_weight
            sheet_data.at[index, '分区'] = partition
            sheet_data.at[index, '燃油'] = f"{fuel_rate * 100:.2f}%"
            sheet_data.at[index, '总金额'] = all_money
            
            # 格式化日期
            sheet_data.at[index, '第一枪\n扫描时间时间'] = order_date.strftime('%Y-%m-%d') if pd.notna(order_date) else None
            
            # 处理其他日期字段
            for date_field in ['美国出库\n时间', '送达时间']:
                if date_field in sheet_data.columns:
                    # 使用pd.to_datetime转换日期，允许无法解析的值
                    date_value = pd.to_datetime(row[date_field], errors='coerce')
                    # 格式化日期，如果无法解析则设为None
                    sheet_data.at[index, date_field] = date_value.strftime('%Y-%m-%d') if pd.notna(date_value) else None


            # 计算是否偏远，根据快递单号 列来判断是fedex还是ups(1z开头)
            if str(row['快递单号']).startswith('1Z'):
                for property_name, codes in ups_zip_data.items():
                    if row['邮编'] in codes:
                        sheet_data.at[index, '是否偏远'] = property_name
                        break
            else:
                for property_name, codes in fedex_zip_codes_by_category.items():
                    if row['邮编'] in codes:
                        sheet_data.at[index, '是否偏远'] = property_name
                        break
        

        # 创建输出文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            sheet_data.to_excel(writer, sheet_name='结果', index=False)
        
        output.seek(0)
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        filename = f"output-{timestamp}.xlsx"
        
        # 保存到本地
        output_dir = os.path.join(os.getcwd(), 'output')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        output_path = os.path.join(output_dir, filename)
        with open(output_path, 'wb') as f:
            f.write(output.getvalue())
        
        print(f"文件已保存到: {output_path}")
        
        # 返回文件流
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")

@web_vba_router.get("/get_ups_excel_template")
def get_ups_excel_template():
    excel_path = next((os.path.join(os.getcwd(), 'excel_template', f) for f in os.listdir(os.path.join(os.getcwd(), 'excel_template')) if f.startswith('LAX发出-HTT')), None)
    if not excel_path:
        raise HTTPException(status_code=404, detail="未找到LAX发出-HTT开头的Excel模板文件")
    return FileResponse(excel_path)



@web_vba_router.post("/fedex_remoteaddresscheck")
async def remoteaddresscheck(file: UploadFile = File(...)):
    """
    上传Excel文件，根据PDF中的邮政编码信息进行处理，并返回处理后的Excel文件。
    
    Args:
        file (UploadFile): 上传的Excel文件。
    
    Returns:
        StreamingResponse: 处理后的Excel文件流。
    
    Raises:
        HTTPException: 如果处理文件时出错。
    """
    try:
        
        # 检查上传的文件是否为Excel文件
        if not file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(status_code=400, detail="请上传Excel文件")
        
        # 读取上传的Excel文件
        contents = await file.read()
        excel_file = io.BytesIO(contents)
        df = pd.read_excel(excel_file)
        
        # 定义PDF文件路径
        pdf_path = os.path.join(os.getcwd(), 'file', 'remoteaddresscheck', 'DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf')  # 确保PDF文件名正确
        
        # 检查PDF文件是否存在
        if not os.path.exists(pdf_path):
            raise HTTPException(status_code=404, detail="未找到Delivery Area Surcharge.pdf文件")
        
        # 使用process_excel_with_zip_codes函数处理Excel数据
        result_df = fedex_process_excel_with_zip_codes(excel_file, pdf_path)
        
        # 创建输出文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            result_df.to_excel(writer, sheet_name='结果', index=False)
        output.seek(0)
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        filename = f"processed-{timestamp}.xlsx"
        
        # 返回文件流
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")

@web_vba_router.get("/get_fedex_remoteaddresscheck_effective_date")
def get_fedex_remoteaddresscheck_effective_date():
 
    
    pdf_path = os.path.join(os.getcwd(), 'file', 'remoteaddresscheck', 'DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf')
    effective_date = None
    try:
        with open(pdf_path, 'rb') as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            page = pdf_reader.pages[0]
            text = page.extract_text()
            # 在这里添加提取日期的逻辑，例如使用正则表达式
            # 这里只是一个示例，你需要根据PDF的具体格式来提取
            # 示例：假设日期格式为 "Effective Date: YYYY-MM-DD"
            match = re.search(r"Effective\s*([A-Za-z]+\s*\d{1,2},\s*\d{4})", text)
            if match:
                effective_date = match.group(1)
            else:
                effective_date = "日期未找到"
    except FileNotFoundError:
        effective_date = "文件未找到"
    except Exception as e:
        effective_date = f"读取文件出错: {str(e)}"
    
    return {"effective_date": effective_date}

@web_vba_router.post("/ups_remoteaddresscheck")
async def ups_remoteaddresscheck(file: UploadFile = File(...)):
    """
    上传Excel文件，根据PDF中的邮政编码信息进行处理，并返回处理后的Excel文件。
    """
    try:
        # 保存上传的Excel文件
        excel_file = io.BytesIO(await file.read())
        
        # 获取property定义Excel文件路径
        property_excel_path = os.path.join(os.getcwd(), 'file', 'remoteaddresscheck', 'area-surcharge-zips-us-en.xlsx')
        
        # 读取输入Excel
        input_df = pd.read_excel(excel_file)
        
        # 读取property定义Excel中的所有sheet
        xl = pd.ExcelFile(property_excel_path)
        
        # 存储code和property的映射关系
        code_property_map = {}
        
        # 遍历每个sheet获取code和property的对应关系
        for sheet_name in xl.sheet_names:
            df = pd.read_excel(property_excel_path, sheet_name=sheet_name)
            data = []
            # 遍历每一列
            for col in df.columns:
                for cell in df[col].dropna():
                    if isinstance(cell, str):
                        # 使用正则表达式提取数字
                        codes = re.findall(r'\b\d+\b', cell)
                        for code in codes:
                            if code == '00000':
                                continue
                            data.append(code)
            
                code_property_map[sheet_name] = data
                
        # 添加property列
        def get_property(code):
            # 检查邮编长度
            if len(str(code)) != 5:
                return '邮编错误，不足五位'
                
            for property_name, codes in code_property_map.items():
                if str(code) in codes:
                    return property_name
            return 'Unknown'
            
        input_df['property'] = input_df['code'].apply(get_property)
        
        # 创建输出文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            input_df.to_excel(writer, index=False)
        output.seek(0)
        
        # 生成文件名
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        filename = f"zip_codes_processed_{timestamp}.xlsx"
        
        # 返回文件流
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")
    

@web_vba_router.get("/get_ups_remoteaddresscheck_effective_date")
def get_ups_remoteaddresscheck_effective_date():
    pdf_path = os.path.join(os.getcwd(), 'file', 'remoteaddresscheck', 'area-surcharge-zips-us-en.xlsx')
    # 读取active sheet的B8单元格
    wb = pd.ExcelFile(pdf_path)
    active_sheet = wb.sheet_names[0]  # 获取第一个sheet作为active sheet
    df = pd.read_excel(pdf_path, sheet_name=active_sheet)
    return {"effective_date": df.iloc[6, 1].replace('Effective', '').strip()}



@web_vba_router.post("/all_remoteaddresscheck_process")
async def all_remoteaddresscheck_process(zip_code_str: str = Form(...)):
    pdf_path = os.path.join(os.getcwd(), 'file', 'remoteaddresscheck', 'DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf')  # 确保PDF文件名正确
    
    # 检查PDF文件是否存在
    if not os.path.exists(pdf_path):
            raise HTTPException(status_code=404, detail="未找到Delivery Area Surcharge.pdf文件")    # 调用ups_process_excel_with_zip_codes函数
    fedex_result = fedex_process_excel_with_zip_codes(zip_code_str,pdf_path)
    ups_result = ups_process_excel_with_zip_codes(zip_code_str)
    # 合并两个结果列表并按zip_code排序
    combined_result = sorted(fedex_result + ups_result, key=lambda x: x['zip_code'])
    usa_state_chinese = pd.read_excel(os.path.join(os.getcwd(), 'file', 'remoteaddresscheck', '美国州名.xlsx'))
    
    # 定义 property 中文映射
    property_chinese_mapping = {
        'FEDEX': {
            'Contiguous U.S.': '普通偏远',
            'Contiguous U.S.: Extended': '超偏远',
            'Contiguous U.S.: Remote': '超级偏远',
            'Alaska': '阿拉斯加偏远',
            'Hawaii': '夏威夷偏远',
            'Intra-Hawaii': '夏威夷内部偏远'
        },
        'UPS': {
            'US 48 Zip': '普通偏远',
            'US 48 Zip DAS Extended': '超偏远',
            'Remote HI Zip': '夏威夷偏远',
            'Remote AK Zip': '阿拉斯加偏远',
            'Remote US 48 Zip': '超级偏远'
        }
    }

    # 遍历结果添加USPS信息和中文 property
    for item in combined_result:
        if item['property'] != '邮编错误,不足五位' and item['property'] != 'Unknown':
            # usps_info = query_usps_zip(item['zip_code'])
            usps_info = None
            if usps_info and usps_info.get('resultStatus') == 'SUCCESS':
                item['city'] = usps_info.get('defaultCity', '')
                item['state'] = usps_info.get('defaultState', '')
                if item['state'] in usa_state_chinese['美国州名缩写'].values:
                    # 找到对应的 列 ‘中文译名’
                    item['state'] += f'\n{usa_state_chinese[usa_state_chinese["美国州名缩写"] == item["state"]]["中文译名"].values[0]}'

                # 获取避免使用的城市名称列表
                avoid_cities = [x['city'] for x in usps_info.get('nonAcceptList', [])]
                item['avoid_city'] = avoid_cities

            # 添加中文 property
            carrier_type = item['type'].upper()  # 获取承运商类型 (FEDEX 或 UPS)
            english_property = item['property']  # 获取英文 property
            
            if carrier_type in property_chinese_mapping and english_property in property_chinese_mapping[carrier_type]:
                item['property_chinese'] = property_chinese_mapping[carrier_type][english_property]
            else:
                item['property_chinese'] = '未知偏远'  # 默认值
    return combined_result

@web_vba_router.post("/get_city_by_zip")
async def get_city_by_zip(request: Request):
    """根据邮编获取城市信息"""
    data = await request.json()
    zip_code = data.get("zip_code")
    
    if not zip_code:
        raise HTTPException(status_code=400, detail="邮编不能为空")
        
    usps_info = query_usps_zip(zip_code)
    
    if not usps_info:
        raise HTTPException(status_code=404, detail="未找到城市信息")
    usa_state_chinese = pd.read_excel(os.path.join(os.getcwd(), 'file', 'remoteaddresscheck', '美国州名.xlsx'))

    item = {}
    if usps_info and usps_info.get('resultStatus') == 'SUCCESS':
                item['city'] = usps_info.get('defaultCity', '')
                item['state'] = usps_info.get('defaultState', '')
                if item['state'] in usa_state_chinese['美国州名缩写'].values:
                    # 找到对应的 列 ‘中文译名’
                    item['state'] += f'\n{usa_state_chinese[usa_state_chinese["美国州名缩写"] == item["state"]]["中文译名"].values[0]}'

                # 获取避免使用的城市名称列表
                avoid_cities = [x['city'] for x in usps_info.get('nonAcceptList', [])]
                item['avoid_city'] = avoid_cities

    return item


@web_vba_router.get("/tiles/{z}/{x}/{y}.png")
async def proxy_tiles(z: int, x: int, y: int):
    """
    瓦片地图反向代理,支持本地缓存
    """
    # 检查本地缓存目录是否存在
    cache_dir = os.path.join(os.getcwd(), "tile_cache", str(z), str(x))
    os.makedirs(cache_dir, exist_ok=True)
    
    # 构建本地缓存文件路径
    cache_file = os.path.join(cache_dir, f"{y}.png")
    
    # 如果本地缓存存在,直接返回
    if os.path.exists(cache_file):
        with open(cache_file, "rb") as f:
            return Response(
                content=f.read(),
                media_type="image/png"
            )
    
    # 构建目标URL
    target_url = f"https://fresh-deer-84.deno.dev/tiles/{z}/{x}/{y}.png"
    
    async with httpx.AsyncClient() as client:
        # 发送请求到目标服务器
        response = await client.get(target_url)
        
        # 保存到本地缓存
        if response.status_code == 200:
            with open(cache_file, "wb") as f:
                f.write(response.content)
        
        # 返回响应
        return Response(
            content=response.content,
            media_type="image/png",
            status_code=response.status_code
        )
