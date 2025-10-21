from io import BytesIO
from typing import Dict, List, Optional

from fastapi import (
    APIRouter,
    HTTPException,
)
from pydantic import BaseModel

from auto_or import (
    fetch_products_from_api,
    get_qingguan_access_token,
    optimize_packing_selection,
)

from .air_product import air_product_router
from .consignee import consignee_router
from .custom_clear_history_origin_summary import (
    custom_clear_history_origin_summary_router,
)
from .custom_clear_history_summary import customer_clear_history_summary_router,create_summary
from .factory import factory_router
from .fedex_ups import express_delivery_router
from .fencangdan import fencangdan_router
from .five_letters_hscode import five_letters_hscode_router
from .haiyunzishui import haiyunzishui_router
from .ip_white_list import ip_white_list_router
from .packing_types import packing_type_router
from .ports import ports_router
from .sea_product import sea_product_router
from .shipper_receiver import shipperandreceiver_router
from .sea_tidan_log import sea_tidan_log_router,create_shipment_log
from .tariff import tariff_router
import os
from pathlib import Path
import random
import traceback
from datetime import datetime
from pymongo import MongoClient

from fastapi import (
    Depends,
    Request
)
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from loguru import logger

from app.dadan.models import (
    ShipmentLog,
)
from app.schemas import (
    ProductData,
    ShippingRequest,
)
from app.utils import (
    MinioClient,
    generate_admin_shenhe_canada_template,
)

from app.utils_aspose import (
    generate_excel_from_template_canada,
    generate_excel_from_template_test,
    generate_admin_shenhe_template
)
from app.db_mongo import get_session
from urllib.parse import quote

qingguan_router = APIRouter(tags=["清关"], prefix='/qingguan')

# 包含所有子路由
qingguan_router.include_router(air_product_router)
qingguan_router.include_router(consignee_router)
qingguan_router.include_router(custom_clear_history_origin_summary_router)
qingguan_router.include_router(customer_clear_history_summary_router)
# qingguan_router.include_router(dalei_router)
qingguan_router.include_router(express_delivery_router)  # fedex_ups 模块的路由
qingguan_router.include_router(factory_router)
qingguan_router.include_router(fencangdan_router)
qingguan_router.include_router(five_letters_hscode_router)
qingguan_router.include_router(haiyunzishui_router)
qingguan_router.include_router(ip_white_list_router)
qingguan_router.include_router(packing_type_router)
qingguan_router.include_router(ports_router)
qingguan_router.include_router(sea_product_router)
qingguan_router.include_router(shipperandreceiver_router)
qingguan_router.include_router(sea_tidan_log_router)
qingguan_router.include_router(tariff_router)



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
                product_record = db.products.find_one({"中文品名": product_name,"startland":"Vietnam"})
            else:
                product_record = db.products.find_one({"中文品名": product_name,"startland":"China"})
        else:
            if export_country == "Vietnam":
                product_record = db.products_sea.find_one({"中文品名": product_name,"startland":"Vietnam"})
            else:
                product_record = db.products_sea.find_one({"中文品名": product_name,"startland":"China"})
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
                {"中文品名": product_name, "destination": "Canada"}
            )
        else:
            product_record = db.products_sea.find_one(
                {"中文品名": product_name, "destination": "Canada"}
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
@qingguan_router.post("/packing_selection_optimize", summary="最优箱数调整")
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
@qingguan_router.post("/process-shipping-data", summary="处理清关数据并生成文件")
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


@qingguan_router.get("/download/{file_name}", summary="下载文件（MinIO）")
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

@qingguan_router.get("/api/exchange-rate", summary="获取汇率")
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