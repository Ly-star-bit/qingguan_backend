import os
import random
import traceback
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote

# # 将项目根目录添加到 Python 路径，以便导入 excel_ai 模块
# project_root = Path(__file__).resolve().parent.parent.parent.parent
# if str(project_root) not in sys.path:
#     sys.path.insert(0, str(project_root))
import pandas as pd
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    UploadFile,
)
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from loguru import logger
from pydantic import BaseModel
from pymongo import MongoClient

from app.dadan.models import (
    ShipmentLog,
)
from app.db_mongo import get_session
from app.schemas import (
    ProductData,
    ShippingRequest,
)
from app.utils import (
    MinioClient,
    generate_admin_shenhe_canada_template,
)
from app.utils_aspose import (
    generate_admin_shenhe_template,
    generate_excel_from_template_canada,
    generate_excel_from_template_test,
)
from auto_or import (
    fetch_products_from_api,
    get_qingguan_access_token,
    optimize_packing_selection,
)
from excel_ai.autogen_extractor import AutoGenExcelExtractor

from .air_product import air_product_router
from .consignee import consignee_router
from .custom_clear_history_origin_summary import (
    custom_clear_history_origin_summary_router,
)
from .custom_clear_history_summary import (
    create_summary,
    customer_clear_history_summary_router,
)
from .factory import factory_router
from .fedex_ups import express_delivery_router
from .fencangdan import fencangdan_router
from .five_letters_hscode import five_letters_hscode_router
from .haiyunzishui import haiyunzishui_router
from .ip_white_list import ip_white_list_router
from .packing_types import packing_type_router
from .ports import ports_router
from .sea_product import sea_product_router
from .sea_tidan_log import create_shipment_log, sea_tidan_log_router
from .shipper_receiver import shipperandreceiver_router
from .tariff import tariff_router


def process_unit_price(value):
    # 1. 处理空值或非数字情况
    if value is None or value == "":
        return ""

    try:
        # 转换为 Decimal 以保证精度
        # 如果输入是字符串，直接转；如果是 float，先转字符串再转 Decimal 避免浮点误差
        if isinstance(value, float):
            num = Decimal(str(value))
        else:
            num = Decimal(str(value))

        # 第一步：标准四舍五入保留两位小数
        # quantize(Decimal('0.01')) 配合 ROUND_HALF_UP 实现标准的四舍五入
        rounded_num = num.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # 将结果拆分为整数部分和小数部分以便处理第二位小数
        # 例如 0.96 -> sign=1, digits=(9, 6), exponent=-2
        sign, digits, exponent = rounded_num.as_tuple()

        # 确保至少有两位小数（防止出现类似 5.0 的情况，虽然 quantize 后通常是两位，但防御性编程）
        # digits 是一个元组，比如 (0, 9, 6) 代表 0.96 (取决于 exponent)
        # 更简单的做法是直接操作字符串或利用数学取余，但为了严谨我们提取最后一位小数

        # 获取当前两位小数的数值 (0 到 99)
        # 方法：将数字乘以 100 取整
        cents = int(rounded_num * 100)
        last_digit = cents % 10  # 获取第二位小数 (0-9)

        new_last_digit = 0

        # 第二步：应用特殊规则
        if 1 <= last_digit <= 5:
            new_last_digit = 0
        elif 6 <= last_digit <= 9:
            new_last_digit = 5
        else:
            # 如果是 0，保持不变
            new_last_digit = 0

        # 重新构建数值
        # 去掉原来的个位小数，加上新的个位小数
        base_cents = cents - last_digit + new_last_digit

        final_num = Decimal(base_cents) / 100

        # 如果转换后为0，保持原始值
        if final_num == 0:
            return f"{num:.2f}"

        # 格式化为字符串，确保始终显示两位小数 (例如 0.20 而不是 0.2)
        return f"{final_num:.2f}"

    except Exception:
        # 如果转换失败（例如原本就是非数字字符串），返回原值或空字符串
        return ""


qingguan_router = APIRouter(tags=["清关"], prefix="/qingguan")

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
    port_name: str = "",
    operate_type: str = "",
    session: MongoClient = Depends(get_session),
):
    db = session
    exchange_rate = db.exchange_rates.find_one({
        "version": "latest",
        "type": "美金人民币汇率",
    })
    port_detail = None
    if operate_type == "auto" and port_name:
        # if port_name:
        port_detail = db.ports.find_one({"port_name": port_name})
        logger.info(f"port_detail:{port_detail}")
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
    shipper_record = db.consignees.find_one({
        "发货人": {"$regex": f"^{shipper_name}$", "$options": "i"}
    })
    # 查询收件人地址

    if not shipper_record:
        raise ValueError(f"Shipper '{shipper_name}' not found in database")
    if not receiver_record:
        raise ValueError(f"Receiver '{receiver_name}' not found in database")

    shipper_address = shipper_record.get("发货人详细地址")
    receiver_address = receiver_record.get("发货人详细地址")
    detail_data_log_list = []

    def safe_round(value, product_naem, default=0.0):
        if not value:
            return default
        if value == "/":
            raise ValueError(f"Product '{product_naem}' value is empty or '/'")
        try:
            return round(float(value), 4)
        except (ValueError, TypeError):
            return default

    def safe_float(value, product_naem, default=0.0):
        if not value:
            return default
        if value == "/":
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
                product_record = db.products.find_one({
                    "中文品名": product_name,
                    "startland": "Vietnam",
                })
            else:
                product_record = db.products.find_one({
                    "中文品名": product_name,
                    "startland": "China",
                })
        else:
            if export_country == "Vietnam":
                product_record = db.products_sea.find_one({
                    "中文品名": product_name,
                    "startland": "Vietnam",
                })
            else:
                product_record = db.products_sea.find_one({
                    "中文品名": product_name,
                    "startland": "China",
                })
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
            # single_price = process_unit_price (product.single_price)
            if (
                port_detail
                and port_detail.get("expansion_factor")
                and float(port_detail.get("expansion_factor")) != 1
            ):
                single_price = float(product.single_price) * float(
                    port_detail["expansion_factor"]
                )
                single_price = process_unit_price(single_price)
            else:
                single_price = process_unit_price(product.single_price)

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
            # single_price = process_unit_price(product_record.get("单价"))
            if (
                port_detail
                and port_detail.get("expansion_factor")
                and float(port_detail.get("expansion_factor")) != 1
            ):
                single_price = float(single_price) * float(
                    port_detail["expansion_factor"]
                )
                single_price = process_unit_price(single_price)
            else:
                single_price = process_unit_price(product_record.get("单价"))

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
                "packing": product_record.get("件箱"),
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
            logger.warning(
                f"{product_record.get('中文品名')}-{product_type}-工厂地址数据库中没有对应的属性"
            )
            return {
                "product_attribute": f"{product_record.get('中文品名')}-{product_type}-工厂地址数据库中没有对应的属性"
            }, None

        random_address_name = random.choice(address_name_list)
        address = random_address_name.get("地址")
        address_name = random_address_name.get("英文")

        if idx == len(product_list) - 1:  # 处理最后一个产品，不round确保总和精确
            gross_weight_for_this_product = gross_weight - accumulated_gross_weight
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
                gross_weight_for_this_product = round(avg_gross_weight * box_num, 1)
                accumulated_gross_weight += gross_weight_for_this_product
            volume_for_this_product = round(avg_volume * box_num, 2)
            accumulated_volume += volume_for_this_product

        net_weight_for_this_product = round(
            gross_weight_for_this_product * random.uniform(0.68, 0.72), 1
        )

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
            "single_weight": product_record.get("single_weight", ""),
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

        detail_data_log["net_weight"] = net_weight_for_this_product
        detail_data_log["gross_weight"] = gross_weight_for_this_product
        detail_data_log["volume"] = volume_for_this_product

        detail_data_log_list.append(detail_data_log)

        results.append(product_data)

    total_price_all = sum(i.get("total_price", 0) for i in results)
    good_type_totals = {}
    for p in results:
        good_type = p.get("good_type", "未知")
        if isinstance(good_type, list):
            good_type = good_type[0]
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
    execute_type: str = "Air",
):
    db = session
    exchange_rate = db.exchange_rates.find_one({
        "version": "latest",
        "type": "加币人民币汇率",
    })
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
    shipper_record = db.consignees.find_one({
        "发货人": {"$regex": f"^{shipper_name}$", "$options": "i"}
    })
    # 查询收件人地址

    if not shipper_record:
        raise ValueError(f"Shipper '{shipper_name}' not found in database")
    if not receiver_record:
        raise ValueError(f"Receiver '{receiver_name}' not found in database")

    shipper_address = shipper_record.get("发货人详细地址")
    receiver_address = receiver_record.get("发货人详细地址")
    detail_data_log_list = []

    def safe_round(value, product_naem, default=0.0):
        if not value:
            return default
        if value == "/":
            raise ValueError(f"Product '{product_naem}' value is empty or '/'")
        try:
            return round(float(value), 4)
        except (ValueError, TypeError):
            return default

    def safe_float(value, product_naem, default=0.0):
        if not value:
            return default
        if value == "/":
            raise ValueError(f"Product '{product_naem}' value is empty or '/'")
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    for idx, product in enumerate(product_list):
        product_name = product.product_name
        box_num = product.box_num
        if execute_type == "Air":
            product_record = db.products.find_one({
                "中文品名": product_name,
                "destination": "Canada",
            })
        else:
            product_record = db.products_sea.find_one({
                "中文品名": product_name,
                "destination": "Canada",
            })
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

        net_weight_for_this_product = round(
            gross_weight_for_this_product * random.uniform(0.68, 0.72), 1
        )

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
            "single_weight": product_record.get("single_weight", ""),
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
        if isinstance(good_type, list):
            good_type = good_type[0]
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
    expansion_factor: Optional[float] = None
    startland: str
    destination: str
    transport_type: str  # "air" 或 "sea"


@qingguan_router.post("/packing_selection_optimize", summary="最优箱数调整")
def packing_selection_optimize(upload_data: PackingOptimizationRequest):
    # 如果products_data为空，则从API获取
    if not upload_data.products_data:
        token = get_qingguan_access_token()
        products_data = fetch_products_from_api(
            api_token=token,
            startland=upload_data.startland,
            destination=upload_data.destination,
            transport_type=upload_data.transport_type,
        )
        # 检查API返回的数据
        if not products_data:
            return {
                "status": "Failed",
                "success": False,
                "error": "无法从API获取产品数据",
                "selected_products": [],
                "summary": {},
            }
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
        expansion_factor=upload_data.expansion_factor,
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
            logger.info(f"request_data:{request}")
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
                port_name=request.port,
                operate_type=request.operate_type,
            )

        if isinstance(results, dict) and results.get("product_attribute"):
            return JSONResponse({
                "status": "False",
                "content": results.get("product_attribute"),
            })
        if (
            isinstance(results, dict)
            and results.get("type") == "net_weight大于gross_weight"
        ):
            return JSONResponse({"status": "False", "content": results.get("msg")})
        results[0]["export_country"] = request.export_country
        if request.import_country == "Canada":
            pdf_path = generate_excel_from_template_canada(
                results, request.totalyugutax, request.currency_type
            )
            shenhe_excel_path = generate_admin_shenhe_canada_template(
                results, request.totalyugutax
            )
        else:
            pdf_path = generate_excel_from_template_test(
                results, request.totalyugutax, request.port
            )
            # pdf_path = generate_excel_from_new_template_test(results, request.totalyugutax,request.port)

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
        if "TEST" not in Path(pdf_path).name.split("-")[1].upper():
            try:
                minio_client = MinioClient(
                    os.getenv("MINIO_ENDPOINT"),
                    os.getenv("MINIO_ACCESS_KEY"),
                    os.getenv("MINIO_SECRET_KEY"),
                    os.getenv("MINIO_BUCKET_NAME"),
                    secure=False,
                )
                minio_client.connect()
                minio_client.upload_file(
                    pdf_path, f"qingguan_pdf/{Path(pdf_path).name}"
                )
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
        return JSONResponse({
            "status": "False",
            "content": f"Internal Server Error: {str(e)}",
        })


@qingguan_router.get("/download/{file_name}", summary="下载文件（MinIO）")
async def download_file(file_name: str):
    local_path = f"./file/{file_name}"
    if os.path.exists(local_path):
        # 如果本地存在文件，则直接下载本地文件
        encoded_filename = quote(file_name)
        return FileResponse(
            path=local_path,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={encoded_filename}"},
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
def get_exchange_rate(
    rate_type: str = "USDCNY", session: MongoClient = Depends(get_session)
):
    url = "https://finance.pae.baidu.com/selfselect/sug?wd=%E7%BE%8E%E5%85%83%E4%BA%BA%E6%B0%91%E5%B8%81&skip_login=1&finClientType=pc"
    db = session
    if rate_type == "USDCNY":
        exchange_rate = db.exchange_rates.find_one({
            "version": "latest",
            "type": "美金人民币汇率",
        })
    elif rate_type == "CADCNY":
        exchange_rate = db.exchange_rates.find_one({
            "version": "latest",
            "type": "加币人民币汇率",
        })
    else:
        raise HTTPException(status_code=400, detail="汇率类型不支持")
    print(exchange_rate)
    if exchange_rate:
        rate = exchange_rate["rate"]
        return {"USDCNY": rate}
    # 12月上汇率
    rate = "7.3000"
    return {"USDCNY": rate}


@qingguan_router.get("/update-md/latest", summary="获取最新的清关更新MD文档")
def get_latest_update_md():
    """获取 file/qingguan_update_md 目录下最新的 md 文件内容"""
    md_dir = Path("./file/qingguan_update_md")
    if not md_dir.exists():
        return JSONResponse({"status": "False", "content": "更新文档目录不存在"})

    # 筛选 qingguan_update_ 开头的 .md 文件，按文件名排序取最新
    md_files = sorted(
        [f for f in md_dir.glob("qingguan_update_*.md") if f.is_file()],
        key=lambda f: f.name,
        reverse=True,
    )
    if not md_files:
        return JSONResponse({"status": "False", "content": "暂无更新文档"})

    latest_file = md_files[0]
    try:
        content = latest_file.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"读取MD文件失败: {e}")
        return JSONResponse({"status": "False", "content": f"读取文件失败: {str(e)}"})

    return JSONResponse({
        "status": "True",
        "file_name": latest_file.name,
        "content": content,
    })


class ExcelExtractRequest(BaseModel):
    """Excel提取请求参数"""

    use_cache: bool = True
    confidence_threshold: float = 0.8


@qingguan_router.post("/extract-excel-data", summary="从Excel文件提取产品数据")
async def extract_excel_data(
    file: UploadFile,
    use_cache: bool = True,
    confidence_threshold: float = 0.8,
):
    """
    从上传的Excel文件中提取产品数据（HSCODE、品名、件数、单价、材质）

    返回 DataFrame 格式的数据，包含5列：HSCODE、品名、件数、单价、材质

    Args:
        file: 上传的Excel文件（.xlsx, .xls）
        use_cache: 是否使用数据地图缓存
        confidence_threshold: 置信度阈值

    Returns:
        JSON格式的DataFrame数据
    """
    # 检查文件类型
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="仅支持 .xlsx 或 .xls 格式的文件")

    # 读取 API 配置
    api_key = os.environ.get("OPENAI_API_KEY", "")
    base_url = os.environ.get("OPENAI_BASE_URL", "https://api.deepseek.com")
    model = os.environ.get("OPENAI_MODEL", "deepseek-v4-flash")

    if not api_key:
        raise HTTPException(status_code=500, detail="未配置 OPENAI_API_KEY")

    # 保存上传的文件到临时位置
    temp_dir = Path("./file/temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_file_path = temp_dir / file.filename

    try:
        contents = await file.read()
        with open(temp_file_path, "wb") as f:
            f.write(contents)

        # 创建提取器
        extractor = AutoGenExcelExtractor(
            api_key=api_key, base_url=base_url, model=model
        )

        # 调用智能提取方法
        records = extractor.extract_smart(
            file_path=str(temp_file_path),
            output_path=str(
                temp_dir / f"{file.filename}_output.xlsx"
            ),  # 临时输出路径，不会实际使用
            use_cache=use_cache,
            confidence_threshold=confidence_threshold,
        )

        # 将 List[ProductRecord] 转换为 DataFrame
        if records:
            df = pd.DataFrame([r.to_dict() for r in records])
        else:
            df = pd.DataFrame(columns=["HSCODE", "品名", "件数", "单价", "材质"])

        # 返回 DataFrame 数据（JSON 格式）
        return JSONResponse({
            "status": "success",
            "total": len(records),
            "data": df.to_dict(orient="records"),
            "columns": df.columns.tolist(),
        })

    except Exception as e:
        logger.error(f"Excel提取失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Excel提取失败: {str(e)}")
    finally:
        # 强制垃圾回收释放文件句柄，然后清理临时文件
        import gc
        import time

        gc.collect()
        time.sleep(0.2)  # 等待文件句柄释放

        for _ in range(3):
            try:
                if temp_file_path.exists():
                    temp_file_path.unlink()
                output_path = temp_dir / "output.xlsx"
                if output_path.exists():
                    output_path.unlink()
                break
            except PermissionError:
                time.sleep(0.2)
        else:
            logger.warning(f"临时文件无法删除，可能被占用: {temp_file_path}")
