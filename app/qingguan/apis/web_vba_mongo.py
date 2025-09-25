from io import BytesIO
import io
import json
import os
from pathlib import Path
import math
import random
import re
import traceback
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import quote
import uuid
import PyPDF2
import numpy as np
import pandas as pd
from bson import ObjectId
import httpx
from pymongo import MongoClient
from morelink_api import MoreLinkClient

from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile
)
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

from app.dadan.models import (
    ConsigneeData,
    Dalei,
    FactoryData,
    HaiYunZiShui,
    IpWhiteList,
    ShipmentLog,
    ShippersAndReceivers,
)
from app.schemas import (
    FenDanUploadData,
    FileInfo,
    OutputSelectedLogRequest,
    PackingType,
    ProductData,
    ShippingRequest,
)
from app.utils import (
    MinioClient,
    extract_zip_codes_from_excel,
    generate_admin_shenhe_canada_template,
    # generate_admin_shenhe_template,
    # generate_excel_from_template_canada,
    # generate_excel_from_template_test,
    # generate_fencangdan_file,
    get_ups_zip_data,
    output_custom_clear_history_log,
    fedex_process_excel_with_zip_codes,
    query_usps_zip,
    # shenzhen_customes_pdf_gennerate,
    ups_process_excel_with_zip_codes,
)

from app.utils_aspose import (
    shenzhen_customes_pdf_gennerate,
    generate_excel_from_template_canada,
    generate_excel_from_template_test,
    generate_fencangdan_file,
    generate_admin_shenhe_template
)
from rpa_tools import find_playwright_node_path
from rpa_tools.email_tools import send_email
from app.db_mongo import get_session

# logger.level("ALERT", no=35, color="<red>")

# logger.add(create_email_handler("yu.luo@hubs-scs.com"), level="ALERT")


class IPWhitelistMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        print(client_ip)
        # 从MongoDB中获取白名单
        try:
            session = next(get_session())  # 获取实际的session对象
            db = session
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
    execute_type: str,
    export_country: str,
    session: MongoClient = Depends(get_session),
):
    db = session
    exchange_rate = db.exchange_rates.find_one({"version": "latest","type":"美金人民币汇率"})
    num_products = sum([i.box_num for i in product_list])
    if num_products == 0:
        raise ValueError("Product list cannot be empty")

    origin_gross_weight = gross_weight
    avg_gross_weight = gross_weight / num_products
    avg_volume = volume / num_products

    results = []
    accumulated_gross_weight = 0
    accumulated_volume = 0
    if execute_type == "Sea":
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

    def safe_round(value, product_naem, default=0.0):
        if not value or value == "/":
            raise ValueError(f"Product '{product_naem}' value is empty or '/'")
        try:
            return round(float(value), 4)
        except (ValueError, TypeError):
            return default

    def safe_float(value, product_naem, default=0.0):
        if not value or value == "/":
            raise ValueError(f"Product '{product_naem}' value is empty or '/'")
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    for idx, product in enumerate(product_list):
        product_name = product.product_name
        box_num = product.box_num
        if execute_type == "Air":
            if export_country == "Vietnam":
                product_record = db.products.find_one({"中文品名": product_name,"country":"Vietnam"})
            else:
                product_record = db.products.find_one({"中文品名": product_name,"country":"China"})
        else:
            if export_country == "Vietnam":
                product_record = db.products_sea.find_one({"中文品名": product_name,"country":"Vietnam"})
            else:
                product_record = db.products_sea.find_one({"中文品名": product_name,"country":"China"})
        if not product_record:
            raise ValueError(f"Product '{product_name}' not found in database")

        origin_duty = product_record.get("Duty")
        additional_duty_dict = product_record.get("加征", {})
        origin_additional_duty = sum(
            float(val) for val in additional_duty_dict.values() if val and val != "/"
        )

        duty = safe_round(origin_duty, product_name)
        additional_duty = safe_round(origin_additional_duty, product_name)

        detail_data_log = {
            "hs_code": product_record.get("HS_CODE"),
            "chinese_name": product_record.get("中文品名"),
            "transport_mode": execute_type,
            "master_bill_number": master_bill_no,
            "total_tax_rate": duty + additional_duty,
            "exemption_code": product_record.get("豁免代码"),
            "category": product_record.get("类别"),
            "box_nums": box_num,
        }

        if product.single_price:
            # sea海运
            single_price = product.single_price
            detail_data_log = {
                "hs_code": product_record.get("HS_CODE"),
                "chinese_name": product_record.get("中文品名"),
                "transport_mode": execute_type,
                "master_bill_number": master_bill_no,
                "total_tax_rate": duty + additional_duty,
                "exemption_code": product_record.get("豁免代码"),
                "category": product_record.get("类别"),
                "box_nums": box_num,
                "single_price": single_price,
                "packing": product.packing,
            }
        else:
            # air空运
            single_price = product_record.get("单价")
            detail_data_log = {
                "hs_code": product_record.get("HS_CODE"),
                "chinese_name": product_record.get("中文品名"),
                "transport_mode": execute_type,
                "master_bill_number": master_bill_no,
                "total_tax_rate": duty + additional_duty,
                "exemption_code": product_record.get("豁免代码"),
                "category": product_record.get("类别"),
                "box_nums": box_num,
                "single_price": product_record.get("单价"),
                "packing":product_record.get("件箱"),

            }

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
            logger.warning(f"{product_record.get('中文品名')}-{product_type}-工厂地址数据库中没有对应的属性")
            return {
                "product_attribute": f"{product_record.get('中文品名')}-{product_type}-工厂地址数据库中没有对应的属性"
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
            "quanity": box_num * safe_float(packing, product_name),
            "danwei": "PCS" if product_record.get("HS_CODE") else "",
            "unit_price": single_price,
            "total_price": safe_float(single_price, product_name)
            * box_num
            * safe_float(packing, product_name),
            "HS_CODE": product_record.get("HS_CODE"),
            "DESCRIPTION": product_record.get("英文品名"),
            "ChineseName": product_record.get("中文品名"),
            "GrossWeight": gross_weight_for_this_product,
            "net_weight": net_weight_for_this_product,
            "single_weight": product_record.get("single_weight",""),
            "rate": exchange_rate["rate"],
            "estimated_tax_rate_cny_per_kg": predict_tax_price,

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
            "duty": duty,
            "additional_duty": additional_duty,
            "estimated_tax_amount": totalyugutax,
        }

        detail_data_log['net_weight'] = net_weight_for_this_product
        detail_data_log['gross_weight'] = gross_weight_for_this_product
        detail_data_log['volume'] = volume_for_this_product

        detail_data_log_list.append(detail_data_log)

        results.append(product_data)

    total_price_all = sum(i.get("total_price", 0) for i in results)
    good_type_totals = {}
    for p in results:
        good_type = p.get("good_type", "未知")
        good_type_totals[good_type] = good_type_totals.get(good_type, 0) + p.get(
            "total_price", 0
        )

    # 按金额排序取前3
    sorted_types = sorted(good_type_totals.items(), key=lambda x: x[1], reverse=True)[
        :3
    ]
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
        "gross_weight_kg": origin_gross_weight,
        "volume_cbm": volume,
        "total_boxes": sum([i.box_num for i in product_list]),
        "estimated_tax_rate_cny_per_kg": predict_tax_price,
        "details": detail_data_log_list,
        "rate": exchange_rate["rate"],
        "total_price_sum": total_price_all,
        "good_type": good_type_percentages,
    }
    return results, summary_log_data


def process_shipping_data_canada(
    shipper_name: str,
    receiver_name: str,
    master_bill_no: str,
    gross_weight: int,
    volume: int,
    product_list: List[ProductData],
    totalyugutax: float,
    predict_tax_price: float,
    session: MongoClient = Depends(get_session),
    execute_type:str="Air",
):
    db = session
    exchange_rate = db.exchange_rates.find_one({"version": "latest","type":"加币人民币汇率"})
    num_products = sum([i.box_num for i in product_list])
    if num_products == 0:
        raise ValueError("Product list cannot be empty")

    origin_gross_weight = gross_weight
    avg_gross_weight = gross_weight / num_products
    avg_volume = volume / num_products

    results = []
    accumulated_gross_weight = 0
    accumulated_volume = 0
    # if product_list[0].single_price or product_list[0].packing:
    # if product_list[0].single_price:
    #     execute_type = "Sea"
    # else:
    #     execute_type = "Air"
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

    def safe_round(value, product_naem, default=0.0):
        if not value or value == "/":
            raise ValueError(f"Product '{product_naem}' value is empty or '/'")
        try:
            return round(float(value), 4)
        except (ValueError, TypeError):
            return default

    def safe_float(value, product_naem, default=0.0):
        if not value or value == "/":
            raise ValueError(f"Product '{product_naem}' value is empty or '/'")
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    for idx, product in enumerate(product_list):
        product_name = product.product_name
        box_num = product.box_num
        if execute_type == "Air":
            product_record = db.products.find_one(
                {"中文品名": product_name, "country": "Canada"}
            )
        else:
            product_record = db.products_sea.find_one(
                {"中文品名": product_name, "country": "Canada"}
            )
        if not product_record:
            raise ValueError(f"Product '{product_name}' not found in database")

        origin_duty = product_record.get("Duty")
        additional_duty_dict = product_record.get("加征", {})
        origin_additional_duty = sum(
            float(val) for val in additional_duty_dict.values() if val and val != "/"
        )

        duty = safe_round(origin_duty, product_name)
        additional_duty = safe_round(origin_additional_duty, product_name)

        detail_data_log = {
            "hs_code": product_record.get("HS_CODE"),
            "chinese_name": product_record.get("中文品名"),
            "transport_mode": execute_type,
            "master_bill_number": master_bill_no,
            "total_tax_rate": duty + additional_duty,
            "exemption_code": product_record.get("豁免代码"),
            "category": product_record.get("类别"),
            "box_nums": box_num,
        }

        if product.single_price:
            # sea海运
            single_price = product.single_price
            detail_data_log = {
                "hs_code": product_record.get("HS_CODE"),
                "chinese_name": product_record.get("中文品名"),
                "transport_mode": execute_type,
                "master_bill_number": master_bill_no,
                "total_tax_rate": duty + additional_duty,
                "exemption_code": product_record.get("豁免代码"),
                "category": product_record.get("类别"),
                "box_nums": box_num,
                "single_price": single_price,
                "packing": product.packing,
            }
        else:
            # air空运
            single_price = product_record.get("单价")
            detail_data_log = {
                "hs_code": product_record.get("HS_CODE"),
                "chinese_name": product_record.get("中文品名"),
                "transport_mode": execute_type,
                "master_bill_number": master_bill_no,
                "total_tax_rate": duty + additional_duty,
                "exemption_code": product_record.get("豁免代码"),
                "category": product_record.get("类别"),
                "box_nums": box_num,
            }
        detail_data_log_list.append(detail_data_log)

        if product.packing:
            packing = product.packing
        else:
            packing = product_record.get("件箱")

        # product_type = product_record.get("属性绑定工厂")
        # address_name_list = list(
        #     db.factories.find(
        #         {"地址": {"$ne": None}, "属性": product_type}, {"地址": 1, "英文": 1}
        #     )
        # )

        # if not address_name_list:
        #     logger.warning(f"{product_type}工厂地址数据库中没有对应的属性")
        #     return {
        #         "product_attribute": f"{product_type}工厂在地址数据库中不存在"
        #     }, None

        # random_address_name = random.choice(address_name_list)
        # address = random_address_name.get("地址")
        # address_name = random_address_name.get("英文")

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
            "quanity": box_num * safe_float(packing, product_name),
            "danwei": "PCS" if product_record.get("HS_CODE") else "",
            "unit_price": single_price,
            "total_price": safe_float(single_price, product_name)
            * box_num
            * safe_float(packing, product_name),
            "HS_CODE": product_record.get("HS_CODE"),
            "DESCRIPTION": product_record.get("英文品名"),
            "ChineseName": product_record.get("中文品名"),
            "GrossWeight": gross_weight_for_this_product,
            "net_weight": net_weight_for_this_product,
            "single_weight": product_record.get("single_weight",""),
            "Volume": volume_for_this_product,
            "usage": product_record.get("用途"),
            "texture": product_record.get("材质"),
            "rate": exchange_rate["rate"],
            "estimated_tax_rate_cny_per_kg": predict_tax_price,

            # "address_name": address_name or "",
            # "address": address or "",
            "note": product_record.get("豁免代码"),
            "note_explaination": product_record.get("豁免代码含义"),
            "execute_type": execute_type,
            "huomian_file_name": product_record.get("huomian_file_name"),
            "good_type": product_record.get("类别"),
            "duty": duty,
            "additional_duty": additional_duty,
            "estimated_tax_amount": totalyugutax,
        }

        results.append(product_data)

    total_price_all = sum(i.get("total_price", 0) for i in results)
    good_type_totals = {}
    for p in results:
        good_type = p.get("good_type", "未知")
        good_type_totals[good_type] = good_type_totals.get(good_type, 0) + p.get(
            "total_price", 0
        )

    # 按金额排序取前3
    sorted_types = sorted(good_type_totals.items(), key=lambda x: x[1], reverse=True)[
        :3
    ]
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
        "gross_weight_kg": origin_gross_weight,
        "volume_cbm": volume,
        "total_boxes": sum([i.box_num for i in product_list]),
        "estimated_tax_rate_cny_per_kg": predict_tax_price,
        "details": detail_data_log_list,
        "rate": exchange_rate["rate"],
        "total_price_sum": total_price_all,
        "good_type": good_type_percentages,
    }
    return results, summary_log_data


web_vba_router = APIRouter(tags=["清关"],prefix='/qingguan')


dalei_router = APIRouter(tags=["大类"],prefix='/dalei')
@web_vba_router.post("/dalei/", response_model=Dalei, summary="创建大类")
def create_dalei(dalei: Dalei, session: MongoClient = Depends(get_session)):
    db = session
    dalei_dict = dalei.dict()
    dalei_dict.pop("id", None)
    result = db.dalei.insert_one(dalei_dict)
    dalei_dict["id"] = str(result.inserted_id)
    return dalei_dict


@web_vba_router.get("/dalei/", summary="获取大类列表")
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


@web_vba_router.get("/dalei/{id}", response_model=Dalei, summary="根据ID获取大类")
def read_dalei_by_id(id: str, session: MongoClient = Depends(get_session)):
    db = session
    dalei = db.dalei.find_one({"_id": ObjectId(id)})
    if not dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")
    dalei["id"] = str(dalei["_id"])
    dalei.pop("_id", None)
    return dalei


@web_vba_router.put("/dalei/{id}", response_model=Dalei, summary="更新大类")
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


@web_vba_router.delete("/dalei/{id}", response_model=Dalei, summary="删除大类")
def delete_dalei(id: str, session: MongoClient = Depends(get_session)):
    db = session
    dalei = db.dalei.find_one({"_id": ObjectId(id)})
    if not dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")
    db.dalei.delete_one({"_id": ObjectId(id)})
    dalei["id"] = str(dalei["_id"])
    dalei.pop("_id", None)
    return dalei


@web_vba_router.get("/products/", response_model=dict, summary="获取产品列表")
def read_products(
    skip: int = 0,
    limit: int = 10,
    名称: Optional[str] = None,
    get_all: bool = False,
    country: str = "China",
    zishui: bool = None,
    is_hidden:bool=None,
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


@web_vba_router.get("/products/upload_huomian_file", summary="上传货免文件")
def upload_huomian_file(file: UploadFile = File(...)):
    save_directory = Path("./file/huomian_file/")
    save_directory.mkdir(parents=True, exist_ok=True)
    file_name = f"{uuid.uuid4()}-{file.filename}"
    file_path = save_directory / file_name

    with file.file as file_content:
        with open(file_path, "wb") as buffer:
            buffer.write(file_content.read())
    return {"file_name": file_name}


@web_vba_router.post("/products/", summary="创建产品")
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


@web_vba_router.get("/products/{pic_name}", summary="下载货免文件")
def download_pic(pic_name: str):
    file_path = os.path.join("./file/huomian_file/", pic_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        file_path, media_type="application/octet-stream", filename=pic_name
    )


@web_vba_router.put("/products/{product_id}", summary="更新产品")
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
@web_vba_router.post("/products/update_batch", summary="批量更新产品信息")
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


@web_vba_router.delete("/products/{product_id}", summary="删除产品")
def delete_product(product_id: str, session: MongoClient = Depends(get_session)):
    db = session
    product = db.products.find_one({"_id": ObjectId(product_id)})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    db.products.delete_one({"_id": ObjectId(product_id)})
    product["id"] = str(product["_id"])
    product.pop("_id", None)
    return product


@web_vba_router.get("/output_products", summary="导出产品Excel")
def output_products(
    session: MongoClient = Depends(get_session),
    transport_type: str = "",
    country: str = "China",
):
    try:
        db = session
        if transport_type == "sea":
            products = list(db.products_sea.find({"country": country}))
        else:
            products = list(db.products.find({"country": country}))

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


@web_vba_router.post("/process-shipping-data", summary="处理清关数据并生成文件")
async def process_shipping_data_endpoint(
    request: ShippingRequest,
    context_request: Request,
    session: MongoClient = Depends(get_session),
):
    try:
        logger.info(f"request: {request.dict()}")
        if request.import_country == "Canada":
            results, summary_log = process_shipping_data_canada(
                shipper_name=request.shipper_name,
                receiver_name=request.receiver_name,
                master_bill_no=request.master_bill_no,
                gross_weight=request.gross_weight,
                volume=request.volume,
                product_list=request.product_list,
                totalyugutax=request.totalyugutax,
                predict_tax_price=request.predict_tax_price,
                execute_type=request.execute_type,
                session=session,
        )
        else:   
            results, summary_log = process_shipping_data(
                shipper_name=request.shipper_name,
                receiver_name=request.receiver_name,
                master_bill_no=request.master_bill_no,
                gross_weight=request.gross_weight,
                volume=request.volume,
                product_list=request.product_list,
                totalyugutax=request.totalyugutax,
                predict_tax_price=request.predict_tax_price,
                execute_type=request.execute_type,
                export_country=request.export_country,
                session=session,
        )

        if isinstance(results, dict) and results.get("product_attribute"):
            return JSONResponse(
                {"status": "False", "content": results.get("product_attribute")}
            )
        if (
            isinstance(results, dict)
            and results.get("type") == "net_weight大于gross_weight"
        ):
            return JSONResponse({"status": "False", "content": results.get("msg")})
        results[0]["export_country"] = request.export_country
        if request.import_country == "Canada":
            pdf_path = generate_excel_from_template_canada(results, request.totalyugutax,request.currency_type)
            shenhe_excel_path = generate_admin_shenhe_canada_template(
            results, request.totalyugutax
        )
        else:
            pdf_path = generate_excel_from_template_test(results, request.totalyugutax,request.port)
            shenhe_excel_path = generate_admin_shenhe_template(
            results, request.totalyugutax
        )
       
        
        # 获取上下文之中的user
        user = context_request.state.user
        summary_log["user_id"] = user["sub"]
        summary_log["packing_type"] = request.packing_type
        summary_log["port"] = request.port
        summary_log["filename"] = Path(pdf_path).name
        summary_log["shenhe_excel_path"] = Path(shenhe_excel_path).name
        if   "TEST"  not in Path(pdf_path).name.split("-")[1].upper():
       
            try:
                minio_client = MinioClient(
                    os.getenv("MINIO_ENDPOINT"),
                    os.getenv("MINIO_ACCESS_KEY"),
                    os.getenv("MINIO_SECRET_KEY"),
                    os.getenv("MINIO_BUCKET_NAME"),
                    secure=False,
                )
                minio_client.connect()
                minio_client.upload_file(pdf_path, f"qingguan_pdf/{Path(pdf_path).name}")
                minio_client.upload_file(
                    shenhe_excel_path,
                    f"qingguan_shenhe_excel/{Path(shenhe_excel_path).name}",
                )
            except Exception as e:
                logger.error(f"上传minio失败，错误为:{e}")
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
            path=pdf_path,
            filename=f"{request.master_bill_no} CI&PL{pdf_path.split('CI&PL')[-1]}",
        )
    except ValueError as e:
        logger.error(f"Value Error: {traceback.format_exc()}")
        return JSONResponse({"status": "False", "content": f"Value Error: {str(e)}"})
    except Exception as e:
        logger.error(f"Internal Server Error: {str(e)}---{traceback.format_exc()}")
        return JSONResponse(
            {"status": "False", "content": f"Internal Server Error: {str(e)}"}
        )


@web_vba_router.get("/products_sea/", response_model=dict, summary="获取海运产品列表")
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


@web_vba_router.get("/products_sea/upload_huomian_file", summary="上传海运货免文件")
def upload_huomian_file_sea(file: UploadFile = File(...)):
    save_directory = Path("./file/huomian_file/")
    save_directory.mkdir(parents=True, exist_ok=True)
    file_name = f"{uuid.uuid4()}-{file.filename}"
    file_path = save_directory / file_name

    with file.file as file_content:
        with open(file_path, "wb") as buffer:
            buffer.write(file_content.read())
    return {"file_name": file_name}


@web_vba_router.post("/products_sea/", summary="创建海运产品")
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


@web_vba_router.get("/products_sea/{pic_name}", summary="下载海运货免文件")
def download_pic_sea(pic_name: str):
    file_path = os.path.join("./file/huomian_file/", pic_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        file_path, media_type="application/octet-stream", filename=pic_name
    )


@web_vba_router.put("/products_sea/{product_id}", summary="更新海运产品")
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
@web_vba_router.post("/products_sea/bulk_hide", summary="批量隐藏/显示海运产品")
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


@web_vba_router.delete("/products_sea/{product_id}", summary="删除海运产品")
def delete_product_sea(product_id: str, session: MongoClient = Depends(get_session)):
    db = session
    product = db.products_sea.find_one({"_id": ObjectId(product_id)})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    db.products_sea.delete_one({"_id": ObjectId(product_id)})
    product["id"] = str(product["_id"])
    product.pop("_id", None)
    return product


# ... (rest of the CRUD operations would follow similar MongoDB conversion pattern)


@web_vba_router.get("/shippersandreceivers/", response_model=dict, summary="获取发货人和收货人列表")
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


@web_vba_router.post("/shippersandreceivers/", response_model=ShippersAndReceivers, summary="创建发货人或收货人")
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


@web_vba_router.put("/shippersandreceivers/{id}", response_model=ShippersAndReceivers, summary="更新发货人或收货人")
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


@web_vba_router.delete(
    "/shippersandreceivers/{id}", response_model=ShippersAndReceivers, summary="删除发货人或收货人"
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

@web_vba_router.post("/packing_types/", response_model=PackingType, summary="创建包装类型")
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


@web_vba_router.get("/packing_types/", summary="获取包装类型列表")
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


@web_vba_router.get("/packing_types/{packing_type_id}", summary="获取包装类型详情")
def read_packing_type(packing_type_id: str, session: MongoClient = Depends(get_session)):
    db = session
    packing_type = db.packing_types.find_one({"_id": ObjectId(packing_type_id)})
    if not packing_type:
        raise HTTPException(status_code=404, detail="PackingType not found")
    packing_type["id"] = str(packing_type["_id"])
    packing_type.pop("_id", None)
    return packing_type


@web_vba_router.put("/packing_types/{packing_type_id}", summary="更新包装类型")
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


@web_vba_router.delete("/packing_types/{packing_type_id}", summary="删除包装类型")
def delete_packing_type(packing_type_id: str, session: MongoClient = Depends(get_session)):
    db = session
    packing_type = db.packing_types.find_one({"_id": ObjectId(packing_type_id)})
    if not packing_type:
        raise HTTPException(status_code=404, detail="PackingType not found")
    db.packing_types.delete_one({"_id": ObjectId(packing_type_id)})
    packing_type["id"] = str(packing_type["_id"])
    packing_type.pop("_id", None)
    return packing_type


@web_vba_router.post("/ports/", summary="创建港口")
def create_port(port: dict, session: MongoClient = Depends(get_session)):
    db = session
    port_dict = port
    port_dict.pop("id", None)
    result = db.ports.insert_one(port_dict)
    port_dict["id"] = str(result.inserted_id)
    return port_dict


@web_vba_router.get("/ports/", summary="获取港口列表")
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


@web_vba_router.get("/ports/{port_id}", summary="获取港口详情")
def read_port(port_id: str, session: MongoClient = Depends(get_session)):
    db = session
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    port["id"] = str(port["_id"])
    port.pop("_id", None)
    return port


@web_vba_router.put("/ports/{port_id}", summary="更新港口")
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


@web_vba_router.delete("/ports/{port_id}", summary="删除港口")
def delete_port(port_id: str, session: MongoClient = Depends(get_session)):
    db = session
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    db.ports.delete_one({"_id": ObjectId(port_id)})
    port["id"] = str(port["_id"])
    port.pop("_id", None)
    return port


# 工厂数据CRUD操作
@web_vba_router.post("/factory/", response_model=FactoryData, summary="创建工厂")
def create_factory(factory: FactoryData, session: MongoClient = Depends(get_session)):
    db = session
    factory_dict = factory.dict()
    factory_dict.pop("id", None)
    result = db.factories.insert_one(factory_dict)
    factory_dict["id"] = str(result.inserted_id)
    return factory_dict


@web_vba_router.get("/factory/", summary="获取工厂列表")
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


@web_vba_router.get("/factory/{factory_id}", response_model=FactoryData, summary="获取工厂详情")
def read_factory(factory_id: str, session: MongoClient = Depends(get_session)):
    db = session
    factory = db.factories.find_one({"_id": ObjectId(factory_id)})
    if not factory:
        raise HTTPException(status_code=404, detail="Factory not found")
    factory["id"] = str(factory["_id"])
    factory.pop("_id", None)
    return factory


@web_vba_router.put("/factory/{factory_id}", response_model=FactoryData, summary="更新工厂")
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


@web_vba_router.delete("/factory/{factory_id}", response_model=FactoryData, summary="删除工厂")
def delete_factory(factory_id: str, session: MongoClient = Depends(get_session)):
    db = session
    factory = db.factories.find_one({"_id": ObjectId(factory_id)})
    if not factory:
        raise HTTPException(status_code=404, detail="Factory not found")
    db.factories.delete_one({"_id": ObjectId(factory_id)})
    factory["id"] = str(factory["_id"])
    factory.pop("_id", None)
    return factory


# 收发货人CRUD操作
@web_vba_router.post("/consignee/", response_model=ConsigneeData, summary="创建收货人")
def create_consignee(
    consignee: ConsigneeData, session: MongoClient = Depends(get_session)
):
    db = session
    consignee_dict = consignee.dict()
    consignee_dict.pop("id", None)
    result = db.consignees.insert_one(consignee_dict)
    consignee_dict["id"] = str(result.inserted_id)
    return consignee_dict


@web_vba_router.get("/consignee/", summary="获取收货人列表")
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


@web_vba_router.get("/consignee/{consignee_id}", response_model=ConsigneeData, summary="获取收货人详情")
def read_consignee(consignee_id: str, session: MongoClient = Depends(get_session)):
    db = session
    consignee = db.consignees.find_one({"_id": ObjectId(consignee_id)})
    if not consignee:
        raise HTTPException(status_code=404, detail="Consignee not found")
    consignee["id"] = str(consignee["_id"])
    consignee.pop("_id", None)
    return consignee


@web_vba_router.put("/consignee/{consignee_id}", response_model=ConsigneeData, summary="更新收货人")
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


@web_vba_router.delete("/consignee/{consignee_id}", response_model=ConsigneeData, summary="删除收货人")
def delete_consignee(consignee_id: str, session: MongoClient = Depends(get_session)):
    db = session
    consignee = db.consignees.find_one({"_id": ObjectId(consignee_id)})
    if not consignee:
        raise HTTPException(status_code=404, detail="Consignee not found")
    db.consignees.delete_one({"_id": ObjectId(consignee_id)})
    consignee["id"] = str(consignee["_id"])
    consignee.pop("_id", None)
    return consignee


@web_vba_router.get("/api/exchange-rate", summary="获取汇率")
def get_exchange_rate(rate_type: str="USDCNY",session: MongoClient = Depends(get_session)):
    url = "https://finance.pae.baidu.com/selfselect/sug?wd=%E7%BE%8E%E5%85%83%E4%BA%BA%E6%B0%91%E5%B8%81&skip_login=1&finClientType=pc"
    db = session
    if rate_type == "USDCNY":
        exchange_rate = db.exchange_rates.find_one({"version": "latest","type":"美金人民币汇率"})
    elif rate_type == "CADCNY":
        exchange_rate = db.exchange_rates.find_one({"version": "latest","type":"加币人民币汇率"})
    else:
        raise HTTPException(status_code=400, detail="汇率类型不支持")
    print(exchange_rate)
    if exchange_rate:
        rate = exchange_rate["rate"]
        return {"USDCNY": rate}
    # 12月上汇率
    rate = "7.3000"
    return {"USDCNY": rate}



@web_vba_router.post("/ip_white_list/", response_model=IpWhiteList, summary="添加IP白名单")
def create_ip_white_list(
    ip_white_list: IpWhiteList, session: MongoClient = Depends(get_session)
):
    db = session
    if db.ip_white_list.find_one({"ip": ip_white_list.ip}):
        raise HTTPException(status_code=400, detail="IP already exists")
    result = db.ip_white_list.insert_one(ip_white_list.dict())
    ip_white_list.id = str(result.inserted_id)
    return ip_white_list


@web_vba_router.get("/ip_white_list/", response_model=List[IpWhiteList], summary="获取所有IP白名单")
def get_all_ip_white_list(session: MongoClient = Depends(get_session)):
    db = session
    ip_white_lists = list(db.ip_white_list.find())
    for item in ip_white_lists:
        item["id"] = str(item["_id"])
    return ip_white_lists


@web_vba_router.get("/ip_white_list/{ip_white_list_id}", response_model=IpWhiteList, summary="获取IP白名单详情")
def get_ip_white_list(
    ip_white_list_id: str, session: MongoClient = Depends(get_session)
):
    db = session
    ip_white_list = db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})
    if not ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")
    ip_white_list["id"] = str(ip_white_list["_id"])
    return ip_white_list


@web_vba_router.put("/ip_white_list/{ip_white_list_id}", response_model=IpWhiteList, summary="更新IP白名单")
def update_ip_white_list(
    ip_white_list_id: str,
    ip_white_list: IpWhiteList,
    session: MongoClient = Depends(get_session),
):
    db = session
    db_ip_white_list = db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})
    if not db_ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")

    update_data = ip_white_list.dict(exclude_unset=True)
    db.ip_white_list.update_one(
        {"_id": ObjectId(ip_white_list_id)}, {"$set": update_data}
    )
    return db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})


@web_vba_router.delete("/ip_white_list/{ip_white_list_id}", response_model=IpWhiteList, summary="删除IP白名单")
def delete_ip_white_list(
    ip_white_list_id: str, session: MongoClient = Depends(get_session)
):
    db = session
    ip_white_list = db.ip_white_list.find_one({"_id": ObjectId(ip_white_list_id)})
    if not ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")
    db.ip_white_list.delete_one({"_id": ObjectId(ip_white_list_id)})
    return ip_white_list


@web_vba_router.get("/files", response_model=List[FileInfo], summary="获取文件列表")
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

@web_vba_router.get("/download_origin_excel/{file_name}", summary="下载原始Excel文件")
async def download_origin_excel(file_name: str):
    path = f"./file/{file_name}"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    encoded_filename = quote(file_name)
    with open(path, "rb") as file:
        file_bytes = file.read()
    # 创建响应对象，返回文件数据
    bytes_io = BytesIO(file_bytes)
    return StreamingResponse(
        bytes_io,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={encoded_filename}"},
    )

@web_vba_router.get("/download/{file_name}", summary="下载文件（MinIO）")
async def download_file(file_name: str):
    local_path = f"./file/{file_name}"
    if os.path.exists(local_path):
        # 如果本地存在文件，则直接下载本地文件
        encoded_filename = quote(file_name)
        return FileResponse(
            path=local_path,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={encoded_filename}"}
        )
    
    minio_client = MinioClient(
        os.getenv("MINIO_ENDPOINT"),
        os.getenv("MINIO_ACCESS_KEY"),
        os.getenv("MINIO_SECRET_KEY"),
        os.getenv("MINIO_BUCKET_NAME"),
        secure=False,
    )

    # 连接到 MinIO
    minio_client.connect()
    # 根据文件扩展名判断文件类型
    file_extension = file_name.split(".")[-1].lower()

    if file_extension == "pdf":
        file_bytes = minio_client.download_file(f"qingguan_pdf/{file_name}")
        media_type = "application/pdf"
    elif file_extension in ["xlsx", "xls"]:
        file_bytes = minio_client.download_file(f"qingguan_shenhe_excel/{file_name}")
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type")
    encoded_filename = quote(file_name)

    # 创建响应对象，返回文件数据
    bytes_io = BytesIO(file_bytes)
    return StreamingResponse(
        bytes_io,
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={encoded_filename}"},
    )

@web_vba_router.get("/user_download/{file_name}", summary="用户下载文件（带处理）")
async def user_download_file(file_name: str):
    minio_client = MinioClient(
        os.getenv("MINIO_ENDPOINT"),
        os.getenv("MINIO_ACCESS_KEY"),
        os.getenv("MINIO_SECRET_KEY"),
        os.getenv("MINIO_BUCKET_NAME"),
        secure=False,
    )

    # 连接到 MinIO
    minio_client.connect()
    # 根据文件扩展名判断文件类型
    file_extension = file_name.split(".")[-1].lower()

    if file_extension == "pdf":
        file_bytes = minio_client.download_file(f"qingguan_pdf/{file_name}")
        media_type = "application/pdf"
        bytes_io = BytesIO(file_bytes)
    elif file_extension in ["xlsx", "xls"]:
        file_bytes = minio_client.download_file(f"qingguan_shenhe_excel/{file_name}")
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        # 读取原始excel，跳过前12行，只保留指定列
        excel_io = BytesIO(file_bytes)
        try:
            df = pd.read_excel(
                excel_io,
                skiprows=12,
                dtype=str,
                engine="openpyxl" if file_extension == "xlsx" else None,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"读取Excel失败: {str(e)}")
        # 只保留指定列
        columns_to_keep = ["LINE", "HS CODE", "DUTY", "总加征", "TOTAL PRICE (USD)"]
        # 兼容列名可能有空格或大小写问题
        df.columns = [str(col).strip() for col in df.columns]
        # 去除HS CODE为空的行
        df = df[df["HS CODE"].notna()]
        keep_cols = []
        for col in columns_to_keep:
            for df_col in df.columns:
                if df_col.strip().upper() == col.strip().upper():
                    keep_cols.append(df_col)
                    break
        if not keep_cols:
            raise HTTPException(status_code=400, detail="Excel中未找到指定列")
        df = df[keep_cols]
        # 修改LINE列为自动填充1,2,3...
        if "LINE" in columns_to_keep:
            # 找到实际的LINE列名
            line_col_name = None
            for df_col in df.columns:
                if df_col.strip().upper() == "LINE":
                    line_col_name = df_col
                    break
            if line_col_name:
                df[line_col_name] = range(1, len(df) + 1)
        # 重新写入到BytesIO
        output_io = BytesIO()
        with pd.ExcelWriter(output_io, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        output_io.seek(0)
        bytes_io = output_io
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type")
    encoded_filename = quote(file_name)

    # 创建响应对象，返回文件数据
    return StreamingResponse(
        bytes_io,
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={encoded_filename}"},
    )


@web_vba_router.post(
    "/cumstom_clear_history_summary/",
    summary="创建清关历史汇总记录"
)
async def create_summary(summary: dict, session: MongoClient = Depends(get_session)):
    db = session
    summary_dict = {k: v for k, v in summary.items() if k != "id"}
    # print(summary_dict)
    result = db.custom_clear_history_summary.insert_one(summary_dict)
    summary_dict["id"] = str(result.inserted_id)
    # logger.info(f"新增清理历史汇总成功: {summary_dict}")
    # 货值/重量
    money_per_kg = summary_dict["total_price_sum"] / summary_dict["gross_weight_kg"]
    port_or_packing = (
        summary_dict["port"] if summary_dict["port"] else summary_dict["packing_type"]
    )
    # if summary_dict["estimated_tax_rate_cny_per_kg"] >= 1.2 or money_per_kg < 0.46:
    #     email_data = {
    #         "receiver_email": "caitlin.fang@hubs-scs.com",
    #         "subject": f"{summary_dict['user_id']}-{'-'.join(summary_dict['filename'].split('-')[1:-1]).replace('CI&PL','').strip()}-{round(money_per_kg,2)}-{summary_dict['estimated_tax_rate_cny_per_kg']} CNY/Kg-{port_or_packing}-税金{summary_dict['estimated_tax_amount']}-{summary_dict['gross_weight_kg']}Kg-货值{summary_dict['total_price_sum']}",
    #         "body": "",
    #         "status": 0,
    #         "create_time": datetime.now()
    #     }
    #     db.email_queue.insert_one(email_data)
    # for detail in summary_dict['details']:
    #     detail['summary_log_id'] = summary_dict['id']
    #     detail['generation_time'] = summary_dict['generation_time']
    #     db.custom_clear_history_detail.insert_one(detail)

    return summary_dict
@web_vba_router.get('/cumstom_clear_history_summary/download_shuidan_file/{id}/{filename}', summary="下载税单文件")
async def download_shuidan_file(
    id: str,
    filename: str,
    session: MongoClient = Depends(get_session)
):
    try:
        db = session
        # 从MongoDB获取文件信息
        summary = db.custom_clear_history_summary.find_one({"_id": ObjectId(id)})
        if not summary or "shuidan" not in summary:
            raise HTTPException(status_code=404, detail="税单文件不存在")
            
        # 查找指定文件名的文件
        file_info = None
        for item in summary["shuidan"]:
            if item["filename"] == filename:
                file_info = item
                break
                
        if not file_info:
            raise HTTPException(status_code=404, detail=f"未找到文件名为 {filename} 的税单文件")
            
        # 先尝试从本地获取文件
        local_path = f"./file/shuidan/{id}/{filename}"
        if os.path.exists(local_path):
            return FileResponse(
                local_path,
                filename=filename,
                media_type='application/octet-stream'
            )
            
        # 本地不存在则从MinIO下载
        minio_client = MinioClient(
            os.getenv("MINIO_ENDPOINT"),
            os.getenv("MINIO_ACCESS_KEY"),
            os.getenv("MINIO_SECRET_KEY"),
            os.getenv("MINIO_BUCKET_NAME"),
            secure=False
        )
        minio_client.connect()
        
        # 确保本地目录存在
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # 从MinIO下载到本地
        minio_client.download_file(file_info['file_path'], local_path)
        
        return FileResponse(
            local_path,
            filename=filename,
            media_type='application/octet-stream'
        )
        
    except Exception as e:
        logger.error(f"下载税单文件失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
@web_vba_router.delete('/cumstom_clear_history_summary/delete_shuidan_file/{id}/{filename}', summary="删除税单文件")
async def delete_shuidan_file(
    id: str,
    filename: str,
    session: MongoClient = Depends(get_session)
):
    try:
        db = session
        # 从MongoDB获取文件信息
        summary = db.custom_clear_history_summary.find_one({"_id": ObjectId(id)})
        if not summary or "shuidan" not in summary:
            raise HTTPException(status_code=404, detail="税单文件不存在")
            
        # 查找指定文件名的文件
        file_info = None
        file_index = -1
        for i, item in enumerate(summary["shuidan"]):
            if item["filename"] == filename:
                file_info = item
                file_index = i
                break
                
        if not file_info:
            raise HTTPException(status_code=404, detail=f"未找到文件名为 {filename} 的税单文件")
        
        # 删除本地文件(如果存在)
        local_path = f"./file/shuidan/{id}/{filename}"
        if os.path.exists(local_path):
            os.remove(local_path)
            
        # 从MinIO删除文件
        try:
            minio_client = MinioClient(
                os.getenv("MINIO_ENDPOINT"),
                os.getenv("MINIO_ACCESS_KEY"),
                os.getenv("MINIO_SECRET_KEY"),
                os.getenv("MINIO_BUCKET_NAME"),
                secure=False
            )
            minio_client.connect()
            
            # 从MinIO删除文件
            minio_client.client.remove_object(
                minio_client.bucket_name, 
                file_info['file_path']
            )
        except Exception as e:
            logger.warning(f"从MinIO删除文件失败: {str(e)}")
            # 继续执行，即使MinIO删除失败
        
        # 从MongoDB中移除文件记录
        shuidan_data = summary.get("shuidan", [])
        if file_index >= 0:
            shuidan_data.pop(file_index)
            
            # 更新MongoDB
            db.custom_clear_history_summary.update_one(
                {"_id": ObjectId(id)},
                {"$set": {"shuidan": shuidan_data}}
            )
            
        return {"message": "文件删除成功"}
        
    except Exception as e:
        logger.error(f"删除税单文件失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@web_vba_router.post('/cumstom_clear_history_summary/upload_shuidan_file', summary="上传税单文件")
async def upload_shuidan_file(
    file: UploadFile = File(...),
    id: str = Form(...),
    file_type: str = Form(...),
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
        local_path = f"./file/shuidan/{id}/"
        if not os.path.exists(local_path):
            os.makedirs(local_path)
            
        minio_client = MinioClient(
            os.getenv("MINIO_ENDPOINT"),
            os.getenv("MINIO_ACCESS_KEY"), 
            os.getenv("MINIO_SECRET_KEY"),
            os.getenv("MINIO_BUCKET_NAME"),
            secure=False,
        )
        minio_client.connect()
        
        # 保存文件到本地
        file_path = os.path.join(local_path, file.filename)
        contents = await file.read()
        with open(file_path, "wb") as f:
            f.write(contents)
            
        # 上传到minio
        minio_path = f"shuidan/{id}/{Path(file_path).name}"
        minio_client.upload_file(file_path, minio_path)
        
        # 更新MongoDB中的shuidan字段
        # 先获取现有的shuidan数据
        summary = db.custom_clear_history_summary.find_one({"_id": ObjectId(id)})
        shuidan_data = summary.get("shuidan", []) if summary else []
        
        # 添加或更新文件信息
        new_file = {
            "type": file_type,
            "file_path": minio_path,
            "filename": file.filename
        }
        
        # 更新逻辑：
        # 1. 如果是abnormal类型，只有文件名完全相同才覆盖
        # 2. 其他类型，按照type覆盖
        updated = False
        for item in shuidan_data:
            if file_type == "abnormal":
                # abnormal类型只在文件名完全相同时才覆盖
                if item["filename"] == file.filename:
                    item.update(new_file)
                    updated = True
                    break
            else:
                # 其他类型按照type覆盖
                if item["type"] == file_type:
                    item.update(new_file)
                    updated = True
                    break
                
        # 如果不存在则添加新的
        if not updated:
            shuidan_data.append(new_file)
            
        # 更新MongoDB
        db.custom_clear_history_summary.update_one(
            {"_id": ObjectId(id)},
            {"$set": {"shuidan": shuidan_data}}
        )
            
        return {
            "message": "success",
            "uploaded_file": file.filename,
            "file_path": minio_path
        }
        
    except Exception as e:
        logger.error(f"上传税单文件失败: {str(e)}")
        # 删除本地文件
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))

@web_vba_router.post(
    "/update_cumstom_clear_history_summary_remarks/",
    summary="更新清关历史汇总备注/异常"
)
async def update_summary(
    request_body: dict,
    context_request: Request,
    session: MongoClient = Depends(get_session),
):
    db = session
    try:
        # 先查找该记录是否被锁定
        summary = db.custom_clear_history_summary.find_one(
            {"_id": ObjectId(request_body["id"])}
        )
        if not summary:
            return {"code": 500, "msg": "未找到该记录", "data": None}
        if summary.get("lock", False):
            return {"code": 500, "msg": "该记录已被锁定，不能修改", "data": None}
        user = context_request.state.user
        reviewer = user["sub"]
        update_data = {"latest_update_time": datetime.now(), "reviewer": reviewer}
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
        return {"code": 200, "msg": "更新成功", "data": result}
    except Exception as e:
        return {"code": 500, "msg": f"更新失败: {str(e)}", "data": None}


@web_vba_router.post(
    "/lock_cumstom_clear_history_summary_remarks/",
    summary="批量锁定/解锁清关历史汇总记录"
)
async def lock_summary(request_body: dict, session: MongoClient = Depends(get_session)):
    """
    批量锁定custom_clear_history_summary记录，summary_id为id列表
    参数通过json传入，格式为{"summary_id": [...], "lock": true/false}
    """
    db = session
    summary_id = request_body.get("summary_id", [])
    lock = request_body.get("lock", False)
    object_ids = [ObjectId(sid) for sid in summary_id]
    result = db.custom_clear_history_summary.update_many(
        {"_id": {"$in": object_ids}},
        {"$set": {"lock": lock}},
    )
    # 返回详细结果，包括修改数量和受影响的ID
    return {
        "modified_count": result.modified_count,
        "matched_count": result.matched_count,
        "locked": lock,
        "summary_ids": summary_id,
    }


@web_vba_router.get("/cumstom_clear_history_summary/", summary="获取清关历史汇总列表")
def read_summaries(
    context_request: Request,
    enable_pagination: bool = Query(False, description="Enable pagination"),
    page: int = Query(1, description="Page number", ge=1),
    pageSize: int = Query(10, description="Number of items per page", ge=1, le=100),
    file_name: Optional[str] = Query(None, description="File name to filter by"),
    convey_type: Optional[str] = Query(None, description="convey_type to filter by"),
    remarks: Optional[str] = Query(None, description="remarks filter by"),
    abnormal: Optional[str] = Query(None, description="abnormal filter by"),
    abnormal_type: Optional[str] = Query(None, description="abnormal查询类型: equals/startswith/not_startswith"),
    port: Optional[str] = Query(None, description="port filter by"),
    start_time: datetime = Query(None, description="开始时间"),
    end_time: datetime = Query(None, description="结束时间"),
    generation_time_sort: Optional[str] = Query(
        None, description="生成时间排序 asc/desc"
    ),
    latest_update_time_sort: Optional[str] = Query(
        None, description="最后更新时间排序 asc/desc"
    ),
    user_id: Optional[str] = Query(None, description="user_id filter by"),
    reviewer: Optional[str] = Query(None, description="reviwer filter by"),
    lock: Optional[bool] = Query(None, description="lock filter by"),
    chinese_product_name: Optional[str] = Query(None, description="中文品名"),
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
        collection = db.custom_clear_history_summary

        query = {"$and": [{"$or": [{"remarks": {"$ne": "删除"}}, {"remarks": None}]}]}
        if chinese_product_name:
            query["$and"].append({
                "details": {
                    "$elemMatch": {
                        "chinese_name": {"$regex": f".*{chinese_product_name}.*", "$options": "i"}
                    }
                }
            })
            
        if file_name:
            # 将中文逗号替换为英文逗号
            file_name = file_name.replace('，', ',')
            file_names = file_name.split(',')
            file_name_conditions = [{"filename": {"$regex": f".*{name.strip()}.*", "$options": "i"}} for name in file_names]
            query["$and"].append({"$or": file_name_conditions})

        if remarks:
            query["remarks"] = {"$regex": f".*{remarks}.*", "$options": "i"}
        if abnormal:
            if abnormal_type == "equals":
                query["abnormal"] = abnormal
            elif abnormal_type == "startswith":
                query["abnormal"] = {"$regex": f"^{abnormal}", "$options": "i"}
            elif abnormal_type == "not_startswith":
                query["abnormal"] = {"$not": {"$regex": f"^{abnormal}", "$options": "i"}}
            else:
                query["abnormal"] = {"$regex": f".*{abnormal}.*", "$options": "i"}
        if port:
            # 将中文逗号替换为英文逗号
            port = port.replace('，', ',')
            ports = port.split(',')
            port_conditions = [{"port": port.strip()} for port in ports]
            query["$and"].append({"$or": port_conditions})
        if convey_type:
            # 如果运输方式为海运，则查询packing_type不为空的，如果为空运，则port不为空的
            if convey_type == "海运":
                query["packing_type"] = {"$ne": ""}
            elif convey_type == "空运":
                if port:
                    query["$and"].append({"$or": port_conditions})
                else:
                    query["port"] = {"$ne": ""}
            elif convey_type == "整柜":
                query["packing_type"] = {"$regex": "整柜"}
            elif convey_type == "拼箱":
                query["packing_type"] = {"$regex": "拼箱"}
        if start_time:
            query["generation_time"] = {"$gte": start_time, "$lte": end_time}

        # 设置排序
        sort_field = None
        sort_order = None

        if latest_update_time_sort:
            sort_field = "latest_update_time"
            sort_order = 1 if latest_update_time_sort == "asc" else -1
        elif generation_time_sort:
            sort_field = "generation_time"
            sort_order = 1 if generation_time_sort == "asc" else -1
        else:
            sort_field = "generation_time"
            sort_order = -1
        if user_id:
            if user_id == "admin":
                query["$and"].append(
                    {"$or": [
                        {"user_id": ""},
                        {"user_id": "admin"},
                        {"user_id": {"$exists": False}},
                    ]}
                )
            else:
                query["user_id"] = user_id
        if reviewer:
            query["reviewer"] = reviewer
        if lock is not None:
            if lock is False:
                query["$and"].append(
                    {"$or": [
                        {"lock": False},
                        {"lock": {"$exists": False}}
                    ]}
                )
            else:
                query["lock"] = lock

        user = context_request.state.user["sub"]
        if user != "admin":
            query["$and"].append(
                    {"$or": [
                        {"lock": False},
                        {"lock": {"$exists": False}}
                    ]}
                )

        # 如果排序字段是latest_update_time但记录中不存在该字段,则使用generation_time
        sort_conditions = (
            [(sort_field, sort_order), ("generation_time", sort_order)]
            if sort_field == "latest_update_time"
            else [(sort_field, sort_order)]
        )

        if enable_pagination:
            offset = (page - 1) * pageSize
            summaries = list(
                collection.find(query).sort(sort_conditions).skip(offset).limit(pageSize)
            )
            # Convert ObjectId to string
            for summary in summaries:
                summary["id"] = str(summary.pop("_id"))
                # 处理可能的无穷大值和NaN值
                for key, value in summary.items():
                    if isinstance(value, float):
                        if math.isinf(value) or math.isnan(value):
                            summary[key] = str(value)

            total = collection.count_documents(query)
            total_pages = (total + pageSize - 1) // pageSize

            return {"summaries": summaries, "total": total, "total_pages": total_pages}
        else:
            summaries = list(collection.find(query).sort(sort_conditions))
            # Convert ObjectId to string
            for summary in summaries:
                summary["id"] = str(summary.pop("_id"))
                # 处理可能的无穷大值和NaN值
                for key, value in summary.items():
                    if isinstance(value, float):
                        if math.isinf(value) or math.isnan(value):
                            summary[key] = str(value)

            return {"summaries": summaries, "total": len(summaries), "total_pages": 1}
    except Exception as e:
        return {"code": 500, "msg": f"查询失败: {str(e)}", "data": None}

@web_vba_router.get("/cumstom_clear_history_summary/batch_hide_test_data", summary="批量隐藏测试数据")
def batch_hide_test_data(
    session: MongoClient = Depends(get_session),
):
    db = session
    # 查找filename中包含test的记录并更新remarks为"删除"(不区分大小写)
    db.custom_clear_history_summary.update_many(
        {"filename": {"$regex": r"-[Tt][Ee][Ss][Tt]"}},
        {"$set": {"remarks": "删除"}}
    )
    return {"message": "success"}

@web_vba_router.get(
    "/cumstom_clear_history_original_summary/",
    summary="获取清关历史数据（原始）汇总详情"
)
async def read_original_summary(
    context_request: Request,
    type: str = Query(..., description="运输类型:空运|海运"),
    session: MongoClient = Depends(get_session)
):
    user = context_request.state.user
    db = session
    summary = list(db.custom_clear_history_original_summary.find(
        {"type": type, "user": user["sub"]}
    ).sort("created_at", -1))
    
    # 转换ObjectId为字符串
    for doc in summary:
        doc["_id"] = str(doc["_id"])
        
    return summary

@web_vba_router.post(
    "/cumstom_clear_history_original_summary/",
    summary="添加清关历史数据（原始）汇总详情"
)
async def create_original_summary(
    context_request: Request,
    type: str = Query(..., description="运输类型:空运|海运"),
    data: dict = Body(...),
    session: MongoClient = Depends(get_session),
   
):
    user = context_request.state.user
    db = session
    # 检查用户已有记录数
    count = db.custom_clear_history_original_summary.count_documents({
        "user": user["sub"],
        "type": type
    })
    
    if count >= 5:
        # 找到最早的记录并删除
        oldest_record = db.custom_clear_history_original_summary.find_one(
            {"user": user["sub"], "type": type},
            sort=[("created_at", 1)]
        )
        if oldest_record:
            db.custom_clear_history_original_summary.delete_one({"_id": oldest_record["_id"]})
    
    data["type"] = type
    data["user"] = user["sub"]
    data["created_at"] = datetime.now()
    result = db.custom_clear_history_original_summary.insert_one(data)
    return {"id": str(result.inserted_id)}

@web_vba_router.delete(
    "/cumstom_clear_history_original_summary/{summary_id}",
    summary="删除清关历史数据（原始）汇总详情"
)
async def delete_original_summary(
    context_request: Request,
    summary_id: str,
    session: MongoClient = Depends(get_session),
   
):
    user = context_request.state.user
    db = session
    if not ObjectId.is_valid(summary_id):
        raise HTTPException(status_code=400, detail="Invalid ID format")
        
    result = db.custom_clear_history_original_summary.delete_one({
        "_id": ObjectId(summary_id),
        "user": user["sub"]
    })
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Summary not found or unauthorized")
        
    return {"message": "Successfully deleted"}

# 查询单个 Summary 记录
@web_vba_router.get(
    "/cumstom_clear_history_summary/{summary_id}",
    summary="获取单个清关历史汇总详情"
)
async def read_summary(summary_id: str, session: MongoClient = Depends(get_session)):
    try:
        db = session
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


@web_vba_router.get("/output_cumtoms_clear_log/", summary="导出清关历史日志Excel")
async def output_log(
    start_time: str = Query(None, description="开始时间"),
    end_time: str = Query(None, description="结束时间"),
    file_name: Optional[str] = Query(None, description="File name to filter by"),
    convey_type: Optional[str] = Query(None, description="convey_type to filter by"),
    remarks: Optional[str] = Query(None, description="remarks filter by"),
    abnormal: Optional[str] = Query(None, description="abnormal filter by"),
    port: Optional[str] = Query(None, description="port filter by"),
):
    file_path = output_custom_clear_history_log(
        start_date=start_time,
        end_date=end_time,
        filename=file_name,
        convey_type=convey_type,
        remarks=remarks,
        abnormal=abnormal,
        port=port,
    )
    # 将文件路径转换为文件流
    file_stream = open(file_path, "rb")
    # 返回 Excel 文件
    return StreamingResponse(
        file_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=custom_clear_history_log.xlsx"
        },
    )

@web_vba_router.post("/output_selected_cumtoms_clear_log/", summary="导出选中清关历史日志Excel")
async def output_selected_log(
  request_body:OutputSelectedLogRequest,
):
    print(request_body)
    id_list = request_body.id_list
    start_time = request_body.start_time
    end_time = request_body.end_time
    file_path = output_custom_clear_history_log(
        id_list=id_list,
        start_date=start_time,
        end_date=end_time,
    )
    # 将文件路径转换为文件流
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
    summary="创建海运自税记录"
)
async def create_haiyunzishui(
    haiyunzishui: HaiYunZiShui, session: MongoClient = Depends(get_session)
):
    try:
        db = session

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


@web_vba_router.get("/haiyunzishui/", response_model=List[HaiYunZiShui], summary="获取海运自税列表")
async def read_haiyunzishuis(
    session: MongoClient = Depends(get_session),
    skip: int = 0,
    limit: Optional[int] = None,
):
    try:
        db = session
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
    summary="获取海运自税详情"
)
async def read_haiyunzishui(
    haiyunzishui_id: str, session: MongoClient = Depends(get_session)
):
    try:
        db = session
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
    summary="更新海运自税"
)
async def update_haiyunzishui(
    haiyunzishui_id: str,
    updated_haiyunzishui: HaiYunZiShui,
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
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
    summary="删除海运自税"
)
async def delete_haiyunzishui(
    haiyunzishui_id: str, session: MongoClient = Depends(get_session)
):
    try:
        db = session
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
@web_vba_router.post("/shipment_logs/", summary="创建运单日志")
async def create_shipment_log(
    shipment_log: ShipmentLog, session: MongoClient = Depends(get_session)
):
    try:
        db = session
        # 检查是否已存在相同提单号的记录
        # existing_log = db.shipment_logs.find_one(
        #     {"master_bill_no": shipment_log.master_bill_no,"all_english_name":shipment_log.all_english_name}
        # )
        # if existing_log:
        #     raise HTTPException(
        #         status_code=400,
        #         detail="Shipment log with this bill number already exists",
        #     )

        shipment_log_dict = shipment_log.model_dump()
        result = db.shipment_logs.insert_one(shipment_log_dict)

        if result.inserted_id:
            shipment_log_dict["_id"] = str(result.inserted_id)
            return shipment_log_dict
        raise HTTPException(status_code=500, detail="Failed to create shipment log")
    except Exception as e:
        logger.error(f"Error creating shipment log: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@web_vba_router.put("/shipment_logs/{shipment_log_id}", summary="更新运单日志")
async def update_shipment_log(
    shipment_log_id: str,
    shipment_log: ShipmentLog,
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
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


@web_vba_router.get("/shipment_logs/", response_model=dict, summary="获取运单日志列表")
async def read_shipment_logs(
    status: Optional[int] = Query(None, description="Filter by status"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Limit for pagination"),
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
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


@web_vba_router.get("/shipment_logs/{master_bill_no}", response_model=dict, summary="根据提单号获取运单日志")
async def read_shipment_log(
    master_bill_no: str, session: MongoClient = Depends(get_session)
):
    try:
        db = session
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


@web_vba_router.get("/get_tidan_pdf_again/{id}", summary="重新生成提单PDF")
async def get_tidan_pdf(id: str, session: MongoClient = Depends(get_session)):
    # 获取 ShipmentLog 数据
    db = session
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


@web_vba_router.get("/5_letters_hscode/", response_model=dict, summary="获取5位码列表")
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


@web_vba_router.post("/5_letters_hscode/", summary="创建5位码")
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


@web_vba_router.put("/5_letters_hscode/{five_letters_hscode_id}", summary="更新5位码")
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


@web_vba_router.delete("/5_letters_hscode/{five_letters_hscode_id}", summary="删除5位码")
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


@web_vba_router.post("/process_excel_usp_data", summary="处理USPS报价Excel并生成结果")
async def process_excel_usp_data(file: UploadFile = File(...)):
    """处理上传的Excel文件"""
    try:
        # 读取上传的文件内容

        contents = await file.read()

        # 使用pandas读取Excel文件
        xls = pd.ExcelFile(io.BytesIO(contents))

        # 检查必要的工作表是否存在
        required_sheets = ["数据粘贴", "LAX分区", "燃油", "尾程25年非旺季报价单"]
        missing_sheets = [
            sheet for sheet in required_sheets if sheet not in xls.sheet_names
        ]

        if missing_sheets:
            raise ValueError(f"缺少工作表: {', '.join(missing_sheets)}")

        # 读取各个工作表
        sheet_data = pd.read_excel(xls, sheet_name="数据粘贴", header=1)
        if "邮编" in sheet_data.columns:
            sheet_data["邮编"] = sheet_data["邮编"].astype(str).str.zfill(5)
        sheet_lax_partition = pd.read_excel(xls, sheet_name="LAX分区", skiprows=5)
        sheet_fuel = pd.read_excel(xls, sheet_name="燃油")
        sheet_fuel = sheet_fuel.dropna(subset=[sheet_fuel.columns[0]])

        # sheet_usp_raw = pd.read_excel(xls, sheet_name='USPS报价单',skiprows=1)
        sheet_usp_25 = pd.read_excel(xls, sheet_name="尾程25年非旺季报价单", skiprows=1)

        # 检查数据有效性
        if sheet_data.empty or sheet_lax_partition.empty or sheet_fuel.empty:
            raise ValueError("一个或多个工作表为空")

        # 处理燃油数据
        fuel_data = []
        for _, row in sheet_fuel.iterrows():
            date_range = str(row[0]).split("~")
            start_date = date_range[0].strip()
            end_date = date_range[1].strip()

            # 将日-月-年转换为年-月-日
            start_parts = start_date.split("-")
            end_parts = end_date.split("-")

            if len(start_parts) == 3 and len(end_parts) == 3:
                try:
                    start_date = pd.to_datetime(
                        f"{start_parts[2]}-{start_parts[1]}-{start_parts[0]}",
                        format="%Y-%m-%d",
                    )
                    end_date = pd.to_datetime(
                        f"{end_parts[2]}-{end_parts[1]}-{end_parts[0]}",
                        format="%Y-%m-%d",
                    )
                except ValueError:
                    print(f"燃油数据日期转换错误: {start_date}, {end_date}")
                    continue

                fuel_data.append(
                    {
                        "startDate": start_date,
                        "endDate": end_date,
                        "rate": float(row[1]),
                    }
                )

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
        sheet_data["第一枪\n扫描时间时间"] = sheet_data["第一枪\n扫描时间时间"].apply(
            lambda x: pd.to_datetime("1899-12-30") + pd.to_timedelta(x, unit="D")
            if pd.notna(x) and isinstance(x, (int, float))
            else x
        )
        invalid_dates = sheet_data[sheet_data["第一枪\n扫描时间时间"].isna()]

        if not invalid_dates.empty:
            raise ValueError("日期格式不对可能为空")

        # # 获取联邦快递的邮政编码
        # fedex_pdf_path = os.path.join(
        #     os.getcwd(),
        #     "file",
        #     "remoteaddresscheck",
        #     "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf",
        # )  # 确保PDF文件名正确
        # fedex_zip_codes_by_category = extract_zip_codes_from_pdf(fedex_pdf_path)
        fedex_excel_path = os.path.join(
            os.getcwd(),
            "file",
            "remoteaddresscheck",
            "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_20250702.xlsx",
        )
        fedex_zip_codes_by_category = extract_zip_codes_from_excel(fedex_excel_path)

        ups_zip_data = get_ups_zip_data()
        # 处理每一行数据
        for index, row in sheet_data.iterrows():
            # 计算计费重量
            jifei_weight = np.ceil(row["重量\n(LB)"])
            if pd.isna(jifei_weight):
                jifei_weight = 0

            # 获取邮编前五位
            zip_code = str(row["邮编"]).zfill(5)
            zip_code_prefix = zip_code[:5] if len(zip_code) >= 5 else zip_code

            # 查找分区
            partition = "未找到分区"
            for _, partition_row in sheet_lax_partition.iterrows():
                dest_zip = str(partition_row["Dest. ZIP"]).strip()
                if "-" in dest_zip:
                    zip_range = dest_zip.split("-")
                    if len(zip_range) == 2:
                        try:
                            start_zip = int(zip_range[0])
                            end_zip = int(zip_range[1])
                            zip_prefix = int(zip_code_prefix)
                            if start_zip <= zip_prefix <= end_zip:
                                partition = partition_row["Ground"]
                                break  # 找到分区后退出循环
                        except ValueError:
                            continue  # 如果转换失败，则跳过此行
                else:
                    if dest_zip.startswith(zip_code_prefix):
                        partition = partition_row["Ground"]
                        break  # 找到分区后退出循环

            # 获取订单日期
            order_date = row["第一枪\n扫描时间时间"]

            # 查找燃油费率
            fuel_rate = 0
            for fuel in fuel_data:
                if order_date and fuel["startDate"] <= order_date <= fuel["endDate"]:
                    fuel_rate = fuel["rate"]
                    break

            # 根据月份选择不同的报价单
            # current_sheet_usp = sheet_usp_25 #默认使用25年
            # 只需要到71行的数据
            current_sheet_usp = sheet_usp_25.iloc[:50]

            # 查找价格
            money = 0
            if int(jifei_weight) in [
                int(i) for i in current_sheet_usp["Ibs"].values
            ] and int(partition) in [
                int(i) for i in list(current_sheet_usp.columns)[2:]
            ]:
                partition = str(int(float(partition))).zfill(3)
                money = current_sheet_usp.loc[int(jifei_weight) - 1, partition]

            # 计算总金额
            all_money = np.ceil(money * (1 + fuel_rate) * 100) / 100

            # 更新数据
            sheet_data.at[index, "计费重量（美制）"] = jifei_weight
            sheet_data.at[index, "分区"] = partition
            sheet_data.at[index, "燃油"] = f"{fuel_rate * 100:.2f}%"
            sheet_data.at[index, "总金额"] = all_money

            # 格式化日期
            sheet_data.at[index, "第一枪\n扫描时间时间"] = (
                order_date.strftime("%Y-%m-%d") if pd.notna(order_date) else None
            )

            # 处理其他日期字段
            for date_field in ["美国出库\n时间", "送达时间"]:
                if date_field in sheet_data.columns:
                    # 使用pd.to_datetime转换日期，允许无法解析的值
                    date_value = pd.to_datetime(row[date_field], errors="coerce")
                    # 格式化日期，如果无法解析则设为None
                    sheet_data.at[index, date_field] = (
                        date_value.strftime("%Y-%m-%d")
                        if pd.notna(date_value)
                        else None
                    )

            # 计算是否偏远，根据快递单号 列来判断是fedex还是ups(1z开头)
            if str(row["快递单号"]).startswith("1Z"):
                for property_name, codes in ups_zip_data.items():
                    if row["邮编"] in codes:
                        sheet_data.at[index, "是否偏远"] = property_name
                        break
            else:
                for property_name, codes in fedex_zip_codes_by_category.items():
                    if row["邮编"] in codes:
                        sheet_data.at[index, "是否偏远"] = property_name
                        break

        # 创建输出文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            sheet_data.to_excel(writer, sheet_name="结果", index=False)

        output.seek(0)

        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"output-{timestamp}.xlsx"

        # 保存到本地
        output_dir = os.path.join(os.getcwd(), "output")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_path = os.path.join(output_dir, filename)
        with open(output_path, "wb") as f:
            f.write(output.getvalue())

        print(f"文件已保存到: {output_path}")

        # 返回文件流
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")


@web_vba_router.get("/get_ups_excel_template", summary="获取UPS报价模板Excel")
def get_ups_excel_template():
    excel_path = next(
        (
            os.path.join(os.getcwd(), "excel_template", f)
            for f in os.listdir(os.path.join(os.getcwd(), "excel_template"))
            if f.startswith("LAX发出-HTT")
        ),
        None,
    )
    if not excel_path:
        raise HTTPException(
            status_code=404, detail="未找到LAX发出-HTT开头的Excel模板文件"
        )
    return FileResponse(excel_path)


@web_vba_router.post("/fedex_remoteaddresscheck", summary="联邦快递偏远地址校验Excel处理")
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
        pdf_path = os.path.join(
            os.getcwd(),
            "file",
            "remoteaddresscheck",
            "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf",
        )  # 确保PDF文件名正确

        # 检查PDF文件是否存在
        if not os.path.exists(pdf_path):
            raise HTTPException(
                status_code=404, detail="未找到Delivery Area Surcharge.pdf文件"
            )
        excel_path = os.path.join(
            os.getcwd(),
            "file",
            "remoteaddresscheck",
            "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_20250702.xlsx",
        )
        # 使用process_excel_with_zip_codes函数处理Excel数据
        result_df = fedex_process_excel_with_zip_codes(excel_file, pdf_path,excel_path=excel_path)

        # 创建输出文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            result_df.to_excel(writer, sheet_name="结果", index=False)
        output.seek(0)

        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"processed-{timestamp}.xlsx"

        # 返回文件流
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")


@web_vba_router.get("/get_fedex_remoteaddresscheck_effective_date", summary="获取联邦快递偏远地址PDF生效日期")
def get_fedex_remoteaddresscheck_effective_date():
    pdf_path = os.path.join(
        os.getcwd(),
        "file",
        "remoteaddresscheck",
        "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf",
    )
    effective_date = None
    try:
        with open(pdf_path, "rb") as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            page = pdf_reader.pages[0]
            text = page.extract_text()
            # 在这里添加提取日期的逻辑，例如使用正则表达式
            # 这里只是一个示例，你需要根据PDF的具体格式来提取
            # 示例：假设日期格式为 "Effective Date: YYYY-MM-DD"
            # 先尝试匹配Updated日期
            match = re.search(r"Updated\s*([A-Za-z]+\s*\d{1,2},\s*\d{4})", text)
            if not match:
                # 如果没有Updated日期,则匹配Effective日期
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


@web_vba_router.post("/ups_remoteaddresscheck", summary="UPS偏远地址校验Excel处理")
async def ups_remoteaddresscheck(file: UploadFile = File(...)):
    """
    上传Excel文件，根据PDF中的邮政编码信息进行处理，并返回处理后的Excel文件。
    """
    try:
        # 保存上传的Excel文件
        excel_file = io.BytesIO(await file.read())

        # 获取property定义Excel文件路径
        property_excel_path = os.path.join(
            os.getcwd(), "file", "remoteaddresscheck", "area-surcharge-zips-us-en.xlsx"
        )

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
                        codes = re.findall(r"\b\d+\b", cell)
                        for code in codes:
                            if code == "00000":
                                continue
                            data.append(code)

                code_property_map[sheet_name] = data

        # 添加property列
        def get_property(code):
            # 检查邮编长度
            if len(str(code)) != 5:
                return "邮编错误，不足五位"

            for property_name, codes in code_property_map.items():
                if str(code) in codes:
                    return property_name
            return "Unknown"

        input_df["property"] = input_df["code"].apply(get_property)

        # 创建输出文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            input_df.to_excel(writer, index=False)
        output.seek(0)

        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"zip_codes_processed_{timestamp}.xlsx"

        # 返回文件流
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        print(f"处理文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")


@web_vba_router.get("/get_ups_remoteaddresscheck_effective_date", summary="获取UPS偏远地址Excel生效日期")
def get_ups_remoteaddresscheck_effective_date():
    pdf_path = os.path.join(
        os.getcwd(), "file", "remoteaddresscheck", "area-surcharge-zips-us-en.xlsx"
    )
    # 读取active sheet的B8单元格
    wb = pd.ExcelFile(pdf_path)
    active_sheet = wb.sheet_names[0]  # 获取第一个sheet作为active sheet
    df = pd.read_excel(pdf_path, sheet_name=active_sheet)
    return {"effective_date": df.iloc[6, 1].replace("Effective", "").strip()}


@web_vba_router.post("/all_remoteaddresscheck_process", summary="批量校验Fedex/UPS偏远地址")
async def all_remoteaddresscheck_process(zip_code_str: str = Form(...)):
    # pdf_path = os.path.join(
    #     os.getcwd(),
    #     "file",
    #     "remoteaddresscheck",
    #     "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf",
    # )  # 确保PDF文件名正确
    excel_path = os.path.join(
        os.getcwd(),
        "file",
        "remoteaddresscheck",
        "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_20250702.xlsx",
    )
    # 检查PDF文件是否存在
    # if not os.path.exists(pdf_path):
    #     raise HTTPException(
    #         status_code=404, detail="未找到Delivery Area Surcharge.pdf文件"
    #     )  # 调用ups_process_excel_with_zip_codes函数
    if not os.path.exists(excel_path):
        raise HTTPException(
            status_code=404, detail="未找到DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.xlsx文件"
        )
    fedex_result = fedex_process_excel_with_zip_codes(zip_code_str,excel_path=excel_path)
    # fedex_result = extract_zip_codes_from_excel(zip_code_str)
    ups_result = ups_process_excel_with_zip_codes(zip_code_str)
    # 合并两个结果列表并按zip_code排序
    combined_result = sorted(fedex_result + ups_result, key=lambda x: x["zip_code"])
    usa_state_chinese = pd.read_excel(
        os.path.join(os.getcwd(), "file", "remoteaddresscheck", "美国州名.xlsx")
    )

    # 定义 property 中文映射
    property_chinese_mapping = {
        # "FEDEX": {
        #     "Contiguous U.S.": "普通偏远",
        #     "Contiguous U.S.: Extended": "超偏远",
        #     "Contiguous U.S.: Remote": "超级偏远",
        #     "Alaska": "阿拉斯加偏远",
        #     "Hawaii": "夏威夷偏远",
        #     "Intra-Hawaii": "夏威夷内部偏远",
        # },
         "FEDEX": {
            "DAS_ContUS": "普通偏远",
            "DAS_ContUSExt": "超偏远",
            "DAS_ContUSRem": "超级偏远",
            "DAS_Alaska": "阿拉斯加偏远",
            "DAS_Hawaii": "夏威夷偏远",
            "DAS_IntraHawaii": "夏威夷内部偏远",
        },
        "UPS": {
            "US 48 Zip": "普通偏远",
            "US 48 Zip DAS Extended": "超偏远",
            "Remote HI Zip": "夏威夷偏远",
            "Remote AK Zip": "阿拉斯加偏远",
            "Remote US 48 Zip": "超级偏远",
        },
    }

    # 遍历结果添加USPS信息和中文 property
    for item in combined_result:
        if item["property"] != "邮编错误,不足五位" and item["property"] != "Unknown":
            # usps_info = query_usps_zip(item['zip_code'])
            usps_info = None
            if usps_info and usps_info.get("resultStatus") == "SUCCESS":
                item["city"] = usps_info.get("defaultCity", "")
                item["state"] = usps_info.get("defaultState", "")
                if item["state"] in usa_state_chinese["美国州名缩写"].values:
                    # 找到对应的 列 ‘中文译名'
                    item["state"] += (
                        f'\n{usa_state_chinese[usa_state_chinese["美国州名缩写"] == item["state"]]["中文译名"].values[0]}'
                    )

                # 获取避免使用的城市名称列表
                avoid_cities = [x["city"] for x in usps_info.get("nonAcceptList", [])]
                item["avoid_city"] = avoid_cities

            # 添加中文 property
            carrier_type = item["type"].upper()  # 获取承运商类型 (FEDEX 或 UPS)
            english_property = item["property"]  # 获取英文 property

            if (
                carrier_type in property_chinese_mapping
                and english_property in property_chinese_mapping[carrier_type]
            ):
                item["property_chinese"] = property_chinese_mapping[carrier_type][
                    english_property
                ]
            else:
                item["property_chinese"] = "未知偏远"  # 默认值
    return combined_result


@web_vba_router.post("/get_city_by_zip", summary="根据邮编获取美国城市信息")
async def get_city_by_zip(request: Request):
    """根据邮编获取城市信息"""
    data = await request.json()
    zip_code = data.get("zip_code")

    if not zip_code:
        raise HTTPException(status_code=400, detail="邮编不能为空")

    usps_info = query_usps_zip(zip_code)

    if not usps_info:
        raise HTTPException(status_code=404, detail="未找到城市信息")
    usa_state_chinese = pd.read_excel(
        os.path.join(os.getcwd(), "file", "remoteaddresscheck", "美国州名.xlsx")
    )

    item = {}
    if usps_info and usps_info.get("resultStatus") == "SUCCESS":
        item["city"] = usps_info.get("defaultCity", "")
        item["state"] = usps_info.get("defaultState", "")
        if item["state"] in usa_state_chinese["美国州名缩写"].values:
            # 找到对应的 列 ‘中文译名'
            item["state"] += (
                f'\n{usa_state_chinese[usa_state_chinese["美国州名缩写"] == item["state"]]["中文译名"].values[0]}'
            )

        # 获取避免使用的城市名称列表
        avoid_cities = [x["city"] for x in usps_info.get("nonAcceptList", [])]
        item["avoid_city"] = avoid_cities

    return item


@web_vba_router.get("/tiles/{z}/{x}/{y}.png", summary="地图瓦片代理与本地缓存")
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
            return Response(content=f.read(), media_type="image/png")

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
            status_code=response.status_code,
        )
@web_vba_router.get("/query_tracking", summary="查询运单跟踪信息")
def query_tracking(mawb_no: str):
    """查询运单跟踪信息
    
    Args:
        mawb_no: 运单号
        
    Returns:
        响应JSON数据
    """
    url = "https://www.mawb.cn:8443/Webservice/WSMawbSystem.asmx/WSTrackTrace"
    
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "zh-CN,zh;q=0.7",
        "content-type": "application/json; charset=UTF-8", 
        "origin": "https://www.mawb.cn",
        "referer": "https://www.mawb.cn/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    }
    
    data = {
        "data": {
            "TypeStr": "TRACK",
            "UserLanguage": "zh-cn",
            "MawbNo": mawb_no
        }
    }
    pre_url = "https://www.mawb.cn/"
    try:
        with httpx.Client(verify=False) as client:
            response = client.post(
                url=url,
                headers=headers,
                json=data
            )
            resp_json = response.json()
            # 解析返回的json数据获取url路径
            url_path = json.loads(resp_json['d'])[0]['src']
            # 拼接完整url
            full_url = pre_url.rstrip('/') + url_path
            return JSONResponse(
                status_code=200,
                content={
                    "code": 200,
                    "message": "success",
                    "data": full_url
                }
            )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "code": 500,
                "message": str(e),
                "data": None
            }
        )
    

# 用于存储验证码的内存字典,格式为 {code: expire_time}
verification_codes = {}

@web_vba_router.post("/generate_verification_code", summary="生成验证码并发送邮件")
def generate_verification_code():
    """生成6位数验证码,有效期5分钟"""
    # 生成6位随机数字验证码
    verification_code = ''.join(random.choices('0123456789', k=6))
    
    # 设置过期时间为5分钟后
    expire_time = datetime.utcnow() + timedelta(minutes=5)
    
    # 保存到内存字典中
    verification_codes[verification_code] = expire_time
    
    # 清理过期的验证码
    current_time = datetime.utcnow()
    expired_codes = [code for code, exp_time in verification_codes.items() 
                    if exp_time < current_time]
    for code in expired_codes:
        verification_codes.pop(code)
    send_email(
        receiver_email="cissifang@qq.com",
        # receiver_email="yu.luo@hubs-scs.com",
        subject="验证码",
        body=f"您的验证码是：{verification_code},时效5分钟"
    )
    return {
        "code": 200,
        "message": "验证码生成成功",
        # "data": {
        #     "verification_code": verification_code,
        #     "expire_time": expire_time
        # }
    }

@web_vba_router.post("/verify_code", summary="校验验证码")
def verify_code(verification_code: str = Form(...)):
    """验证验证码"""
    current_time = datetime.utcnow()
    
    # 验证码不存在
    if verification_code not in verification_codes:
        return {
            "code": 400,
            "message": "验证码无效",
            "data": None
        }
    
    # 验证码已过期
    if verification_codes[verification_code] < current_time:
        verification_codes.pop(verification_code)
        return {
            "code": 400, 
            "message": "验证码已过期",
            "data": None
        }
        
    # 验证成功后删除该验证码
    verification_codes.pop(verification_code)
    
    return {
        "code": 200,
        "message": "验证成功", 
        "data": None
    }


@web_vba_router.post("/get_morelink_zongdan", summary="获取morelink总单的件毛体")
def get_morelink_zongdan(master_bill_no: str):
    """获取件毛体数据"""

    morelink_client = MoreLinkClient(node_path=find_playwright_node_path())
    data = morelink_client.zongdan_api_httpx()
    
   
    filter_data = [
        row for row in data
        if row.get('billno') == master_bill_no
    ]
    if not filter_data:
        return {
            "code": 400,
            "message": "总单号不存在",
            "data": None
        }
    print(filter_data)
    result = {
        "num":filter_data[0]['yjnum'],
        'weight':filter_data[0]['yjweight'],
        'volume':filter_data[0]['yjvolume'],
        'flight_no':filter_data[0]['flightno'],
        'shipcompany':filter_data[0]['shipcompany'],
        "startland":filter_data[0]['startland'],
        'destination':filter_data[0]['destination'],
        'etd':filter_data[0]['etd']
        
    
    }
    return result
   

@web_vba_router.post("/fencangdan_file_generate", summary="生成分舱单文件")
def generate_fencangdan_file_result(upload_data:FenDanUploadData):
    upload_data_dict = upload_data.model_dump()
    result_path = generate_fencangdan_file(upload_data_dict)
    return FileResponse(
            path=result_path,
            filename=f"{upload_data.orderNumber}",
        )



from auto_or import get_qingguan_access_token,optimize_packing_selection,fetch_products_from_api

from pydantic import BaseModel
from typing import List, Dict, Optional
class PackingOptimizationRequest(BaseModel):
    products_data: List[Dict] = []
    W_target: float = 3537
    B_target: int = 214
    alpha: float = 0.46
    beta_cny: float = 1.27
    exchange_rate: float = 7.22
    k: int = 3
    min_boxes_per_product: int = 20
    expansion_factor:Optional[float] = None
@web_vba_router.post("/packing_selection_optimize", summary="最优箱数调整")
def packing_selection_optimize(upload_data: PackingOptimizationRequest):
    # 如果products_data为空，则从API获取
    if not upload_data.products_data:
        token = get_qingguan_access_token()
        products_data = fetch_products_from_api(api_token=token)
    else:
        products_data = upload_data.products_data
    
    # 调用优化函数，传入所有参数
    result = optimize_packing_selection(
        products_data=products_data,
        W_target=upload_data.W_target,
        B_target=upload_data.B_target,
        alpha=upload_data.alpha,
        beta_cny=upload_data.beta_cny,
        exchange_rate=upload_data.exchange_rate,
        k=upload_data.k,
        min_boxes_per_product=upload_data.min_boxes_per_product,
        expansion_factor=upload_data.expansion_factor

    )
    
    return result