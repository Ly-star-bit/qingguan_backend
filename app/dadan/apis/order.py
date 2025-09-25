import asyncio
from datetime import datetime, timedelta
from functools import lru_cache
import io
import json
import os
from pipes import quote
import re
import traceback
from typing import List
import uuid
import zipfile
import time
from urllib.parse import urlencode
from fastapi.responses import JSONResponse, StreamingResponse
from loguru import logger
import pandas as pd
from app.db_mongo import get_session
from fastapi import APIRouter, Body, HTTPException, Request, Response
import httpx
from app.schemas import DownloadOrderListRequest
from morelink_api import MoreLinkClient
from feapder.db.mysqldb import MysqlDB
from dotenv import load_dotenv
from rpa_tools import find_playwright_node_path
load_dotenv()

order_router = APIRouter(prefix="/order", tags=["订单"])

# 存储token信息
token_info = {
    "access_token": None,
    "expires_at": None
}

async def get_valid_token():
    """
    获取有效的token,如果过期则重新获取
    """
    current_time = time.time()
    
    # 检查token是否存在且未过期
    if (token_info["access_token"] and token_info["expires_at"] 
        and current_time < token_info["expires_at"]):
        return token_info["access_token"]
        
    # token不存在或已过期,重新获取
    login_data = {
        "userName": os.getenv("USER_NAME"),
        "secretKey": os.getenv("SECRET_KEY")
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://47.103.138.130:8081/api/Login/gettoken",
            json=login_data
        )
        
        if response.status_code == 200:
            result = response.json()
            if result["success"]:
                # 更新token信息
                token_info["access_token"] = result["data"]["access_token"]
                token_info["expires_at"] = current_time + result["data"]["expires_in"]
                return token_info["access_token"]
                
        raise HTTPException(status_code=401, detail="获取token失败")

@order_router.get("/product_list", summary="获取产品列表")
async def get_product_list(area: str):
    """
    获取产品列表接口
    """
    # 获取token
    token = await get_valid_token()
    
    headers = {
        "Accept": "text/plain",
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Bearer {token}"
    }
    params = {"area": area}
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "http://47.103.138.130:8081/api/Order/GetProductList",
            headers=headers,
            params=params,
        )
        if response.status_code == 200:
            return response.json()
        else:
            return {"code": 500, "message": "获取产品列表失败", "data": None}


@order_router.post("/get_a_number_data", summary="获取一个产品的基本数据")
async def get_a_number_data(worknum: str):
    """
    获取一个产品数据
    """
    morelink_client = MoreLinkClient()
    dahuo_data = morelink_client.get_dahuo_data_by_id(worknum)
    if dahuo_data:
        d_code = dahuo_data["d_code"]
        qty = dahuo_data["GoodsNum"] if dahuo_data["GoodsNum"] else dahuo_data["yjnum"]
        weight_kg = dahuo_data["ckweight"] if dahuo_data["ckweight"] else dahuo_data["yjweight"]
        destination = dahuo_data["destination"]
        weight_lbs = weight_kg * 2.20462  # 将公斤转换为磅
        if weight_lbs / qty > 50:
           logger.error(f"单件重量超过50LBS: {worknum}-{d_code}-{qty}X")

        # sono = dahuo_data["sono"]
        # res = morelink_client.search_warehouse_size(sono, cid='1891')
        # if res and res.get("success"):
        #     data = json.loads(res["data"])[0]['tb']
        #     length_width_height = [[i["length"],i["width"],i["height"]] for i in data]
            
        #     # 尺寸转换：厘米转换为英寸
        #     length_width_height_in = [[dim / 2.54 for dim in dims] for dims in length_width_height]
            
        #     # 获取最长边，第二长边和高
        #     max_length = max(max(dims) for dims in length_width_height_in)
        #     sorted_dims = sorted([dim for dims in length_width_height_in for dim in dims], reverse=True)
        #     second_length = sorted_dims[1] if len(sorted_dims) > 1 else 0
        #     height = min(min(dims) for dims in length_width_height_in)
            
        #     # 条件1判断
        #     if max_length > 48 and second_length > 30 and max_length + 2 * (second_length + height) > 105 and weight_lbs < 40:
        #         weight_lbs = 40
            
        #     # 条件2判断
        #     elif max_length > 96 and max_length + 2 * (second_length + height) > 130 and weight_lbs < 90:
        #         weight_lbs = 90

        

        
        return {
            "code": 200,
            "message": "获取成功",
            "a_number": f"{worknum}-{d_code}-{qty}X",
            "data": {
                "qty": qty,
                "weight": weight_kg,
                "d_code": d_code,
            },
        }
 
        return {"code": 500, "message": "获取尺寸数据失败", "data": None}
    else:
        return {"code": 500, "message": "获取工作单号失败", "data": None}

@order_router.post("/get_a_number_data_new", summary="获取一个产品的详细数据（含尺寸和重量判断）")
async def get_a_number_data_new(worknum: str):
    """
    获取一个产品数据
    """
    morelink_client = MoreLinkClient()
    dahuo_data = morelink_client.get_dahuo_data_by_id(worknum)
    if dahuo_data:
        warehouseid = dahuo_data["warehouseid"]
        d_code = dahuo_data["d_code"]
        qty = dahuo_data["GoodsNum"] if dahuo_data["GoodsNum"] else dahuo_data["yjnum"]
        weight_kg = dahuo_data["ckweight"] if dahuo_data["ckweight"] else dahuo_data["yjweight"]
        destination = dahuo_data["destination"]
        weight_lbs = weight_kg * 2.20462  # 将公斤转换为磅
        is_overweight = False
        is_ahs = False
        if weight_lbs / qty > 50:
           logger.error(f"单件重量超过50LBS: {worknum}-{d_code}-{qty}X")
           is_overweight = True

        sono = dahuo_data["sono"]
        res = morelink_client.search_warehouse_size(sono, cid=warehouseid)
        if res and res.get("success"):
            data = json.loads(res["data"])[0]['tb']
            # logger.info(data)
            length_width_height = [[i["length"],i["width"],i["height"]] for i in data]
            
            # 尺寸转换：厘米转换为英寸
            length_width_height_in = [[dim / 2.54 for dim in dims] for dims in length_width_height]
            logger.info(length_width_height_in)
            if length_width_height_in:
            # 获取最长边，第二长边和高
                max_length = max(max(dims) for dims in length_width_height_in)
                sorted_dims = sorted([dim for dims in length_width_height_in for dim in dims], reverse=True)
                second_length = sorted_dims[1] if len(sorted_dims) > 1 else 0
                height = min(min(dims) for dims in length_width_height_in)
                
                # 条件1判断
                if max_length > 48 and second_length > 30 and max_length + 2 * (second_length + height) > 105 and weight_lbs < 40:
                    weight_lbs = 40
                    is_ahs = True

                # 条件2判断
                elif max_length > 96 and max_length + 2 * (second_length + height) > 130 and weight_lbs < 90:
                    weight_lbs = 90
                    is_ahs = True
        

        
        return {
            "code": 200,
            "message": "获取成功",
            "a_number": f"{worknum}-{d_code}-{qty}X",
            "data": {
                "qty": qty,
                "weight": weight_lbs/2.20462,
                "d_code": d_code,
                "is_overweight": is_overweight,
                "port":destination,
                "is_ahs": is_ahs
            },
        }
 
        return {"code": 500, "message": "获取尺寸数据失败", "data": None}
    else:
        return {"code": 500, "message": "获取工作单号失败", "data": None}

@lru_cache(maxsize=128)
def get_warehouse_data():
    morelink_client = MoreLinkClient()
    us_province_two = pd.read_excel(r"file/remoteaddresscheck/美国州名.xlsx")
    warehouse_data = morelink_client.fba_warehouse_search()
    return warehouse_data,us_province_two
@order_router.post("/try_calculate", summary="尝试计算订单运费（自动匹配仓库地址）")
async def try_calculate(request: Request):
    """
    尝试计算订单
    """
    request_data = await request.json()
    # print(request_data)
    request_orders = request_data["orders"]
    warehouse_data,us_province_two = get_warehouse_data()
    # morelink_client = MoreLinkClient()
    # warehouse_data = None
    # if warehouse_data is None:
    #     warehouse_data = morelink_client.fba_warehouse_search()
    # us_province_two = pd.read_excel(r"file/remoteaddresscheck/美国州名.xlsx")
    fba_warehouse_data = [
        i for i in warehouse_data if i.get("d_code") == request_orders.get("d_code")
    ][0]
    if fba_warehouse_data:
        country = "US"
        zip = fba_warehouse_data["zip"]
        # 将省份全称转为二字码，全部转为大写
        province_full = fba_warehouse_data["province"]
        if len(province_full) == 2:
            province = province_full
        else:
            # logger.info(province_full.replace(" ", "").upper())
            try:
                province = us_province_two[
                    us_province_two.iloc[:, 1].str.replace("\xa0", " ").str.upper() == province_full.upper()
                ].iloc[0, 0]
            except Exception as e:
                logger.error(f"获取省份二字码失败: {e}")
                return {"code": 500, "message": f"获取省份二字码失败{fba_warehouse_data['d_code']}", "data": None}
        city = fba_warehouse_data["city"]
        address1 = ""
        telephone = "0123456789"
        cjaddr = fba_warehouse_data["cjaddr"]
        d_code = fba_warehouse_data["d_code"]
        if len(d_code.split("-")) == 2:
            name = cjaddr.split("\n")[0].replace(":", "").replace("：", "").strip()
            address1 = cjaddr.split("\n")[1].replace(":", "").replace("：", "").strip()
            company_name = "Amazon"
            # FBA仓
        else:
            name = ""
            company_name = ""
            if "RECEIVER & COMPANY NAME" in cjaddr:
                # 获取RECEIVER & COMPANY NAME右边的部分
                receiver_company_name_index = cjaddr.find("RECEIVER & COMPANY NAME")
                receiver_company_name_right = cjaddr[
                    receiver_company_name_index + len("RECEIVER & COMPANY NAME") :
                ].strip()

                # 判断右边是否有'/'
                if "/" in receiver_company_name_right:
                    parts = receiver_company_name_right.split(
                        "/", 1
                    )  # 只分割第一个斜杠
                    name = parts[0].replace(":", "").replace("：", "").strip()
                    company_name = (
                        parts[1]
                        .split("\n")[0]
                        .replace(":", "")
                        .replace("：", "")
                        .strip()
                    )  # 忽略换行符右边的数据
                else:
                    name = (
                        receiver_company_name_right.replace(":", "")
                        .replace("：", "")
                        .split("\n")[0]
                        .strip()
                    )
                    company_name = name
            # 因为字符串中没有TEL，所以匹配不到。修改pattern只匹配ADDRESS后面的内容
            pattern = r"ADDRESS:\s*(.*?)(?:\r\n|$)"

            # 使用 re.search 进行匹配
            match = re.search(pattern, cjaddr, re.DOTALL)

            # 获取匹配的结果
            if match:
                address1 = match.group(1).replace(":", "").replace("：", "").strip()
            if not address1:
                return {"code": 500, "message": "获取仓库地址失败", "data": None}
            # 获取TEL右边的电话
            tel_index = cjaddr.find("TEL")
            if tel_index != -1:
                telephone_part = (
                    cjaddr[tel_index + len("TEL") :]
                    .replace(":", "")
                    .replace("：", "")
                    .strip()
                )
                telephone = telephone_part.split("\n")[
                    0
                ].strip()  # 忽略换行符右边的数据
    try_calculate_result = []

    token = await get_valid_token()
    for product in request_orders["children"]:
        logger.info(f"正在计算{request_orders['a_number']}-{product['channelName']}的运费")
        try_calculate_request_data = {
            "orderNumber":  re.sub(r'[()（）]', '', request_orders["a_number"]) ,
            "poZipCode": "",
            "arrivePortCode": "",
            "expressType": product["expressType"],
            "expressSupplier": product["expressSupplier"],
            "channelName": product["channelName"],
            "channelCode": product["channelCode"],
            "shipperFrom": {
                "name": "",
                "companyName": "",
                "phone": "",
                "postalCode": "",
                "country": "",
                "province": "",
                "city": "",
                "address1": "",
                "address2": "",
            },
            "shipperTo": {
                "name": name,
                "companyName": company_name,
                "phone": telephone,
                "postalCode": zip,
                "country": country,
                "province": province,
                "city": city,
                "address1": address1,
                "address2": "",
            },
            "productDetailList": [
                {
                    "length": "50",
                    "width": "30",
                    "height": "30",
                    "weight": str(
                        round(request_orders["weight"] / request_orders["qty"], 2)
                    ),
                    "number": request_orders["qty"],
                }
            ],
        }

        response = httpx.post(
            "http://47.103.138.130:8081/api/Order/FeeRates",
            json=try_calculate_request_data,
            headers={"Authorization": f"Bearer {token}"},
        )
        response_json = response.json()
        if response.status_code == 200 and response_json["data"]:
            response_data = {
                "a_number": request_orders["a_number"],
                "child_id": product["key"],
                "totalFee": response_json["data"]["totalFee"],
                "channelName": product["channelName"],
                "supplier": product["expressSupplier"],

                "shipperTo": try_calculate_request_data["shipperTo"],
                "productDetailList": try_calculate_request_data["productDetailList"],
            }
            try_calculate_result.append(response_data)
        else:
            logger.error(try_calculate_request_data)
            logger.error(response.text)
            if '已存在' in response_json.get("msg"):
                totalFee = 1
            else:
                #失败
                totalFee = -1

            response_data = {
                "key": str(uuid.uuid4()),#随机生成key
                "a_number": request_orders["a_number"],
                "child_id": product["key"],
                "totalFee": totalFee,
                "channelName": product["channelName"],
                "supplier": product["expressSupplier"],
            }
            try_calculate_result.append(response_data)
    if try_calculate_result:
        return {"code": 200, "message": "获取成功", "data": try_calculate_result}
    else:
        return {"code": 500, "message": "获取失败", "data": None}
@order_router.post("/try_calculate_hand", summary="尝试计算订单运费（手动传入收件人信息）")
async def try_calculate_hand(request: Request):
    """
    尝试计算订单
    """
    request_data = await request.json()
    # print(request_data)
    request_orders = request_data["orders"]
    area = request_orders["area"]
    product_list = await get_product_list(area)
    product_list = product_list["data"]
    children = []
    for product_type_data in product_list:
        for product in product_type_data["productsList"]:

            new_product = {
                "key": str(uuid.uuid4()),#随机生成key
                "expressType": product_type_data["productType"],
                "expressSupplier": product["expressSupplier"],
                "channelName": product["expressChannelName"],
                "channelCode": product["expressChannelCode"],
            }
            children.append(new_product)
    request_orders["children"] = children
    try_calculate_result = []

    token = await get_valid_token()
    for product in request_orders["children"]:
        logger.info(f"正在计算{request_orders['a_number']}-{product['channelName']}的运费")
        try_calculate_request_data = {
            "orderNumber":  re.sub(r'[()（）]', '', request_orders["a_number"]) ,
            "poZipCode": "",
            "arrivePortCode": "",
            "expressType": product["expressType"],
            "expressSupplier": product["expressSupplier"],
            "channelName": product["channelName"],
            "channelCode": product["channelCode"],
            "shipperFrom": {
                "name": "",
                "companyName": "",
                "phone": "",
                "postalCode": "",
                "country": "",
                "province": "",
                "city": "",
                "address1": "",
                "address2": "",
            },
            "shipperTo": {
                "name": request_orders["shipperTo"]["name"],
                "companyName": request_orders["shipperTo"]["companyName"],
                "phone": request_orders["shipperTo"]["phone"],
                "postalCode": request_orders["shipperTo"]["postalCode"],
                "country": request_orders["shipperTo"]["country"],
                "province": request_orders["shipperTo"]["province"],
                "city": request_orders["shipperTo"]["city"],
                "address1": request_orders["shipperTo"]["address1"],
                "address2": request_orders["shipperTo"]["address2"],
            },
            "productDetailList": [
                {
                    "length": str(request_orders["productDetailList"][0]["length"]),
                    "width": str(request_orders["productDetailList"][0]["width"]),
                    "height": str(request_orders["productDetailList"][0]["height"]),
                    "weight": str(
                        round(request_orders["productDetailList"][0]["weight"], 2)
                    ),
                    "number": request_orders["productDetailList"][0]["number"],
                }
            ],
        }

        response = httpx.post(
            "http://47.103.138.130:8081/api/Order/FeeRates",
            json=try_calculate_request_data,
            headers={"Authorization": f"Bearer {token}"},
        )
        response_json = response.json()
        if response.status_code == 200 and response_json["data"]:
            response_data = {
                "a_number": request_orders["a_number"],
                "child_id": product["key"],
                "totalFee": response_json["data"]["totalFee"],
                "channelName": product["channelName"],
                "supplier": product["expressSupplier"],
                "expressType": product["expressType"],
                "channelCode": product["channelCode"],
                "shipperTo": try_calculate_request_data["shipperTo"],
                "productDetailList": try_calculate_request_data["productDetailList"],
            }
            try_calculate_result.append(response_data)
        else:
            logger.error(try_calculate_request_data)
            logger.error(response.text)
            if '已存在' in response_json.get("msg"):
                totalFee = 1
            else:
                #失败
                totalFee = -1

            response_data = {
                "key": str(uuid.uuid4()),#随机生成key
                "a_number": request_orders["a_number"],
                "totalFee": totalFee,
                "channelName": product["channelName"],
                "supplier": product["expressSupplier"],
            }
            try_calculate_result.append(response_data)
    if try_calculate_result:
        return {"code": 200, "message": "获取成功", "data": try_calculate_result}
    else:
        return {"code": 500, "message": "获取失败", "data": None}
@order_router.post("/try_calculate_new", summary="尝试计算订单运费（新版，自动匹配产品和仓库地址）")
async def try_calculate_new(request: Request):
    """
    尝试计算订单
    """
    request_data = await request.json()
    area = request_data["area"]
    order_item = request_data["order_item"]
    product_list = await get_product_list(area)
    product_list = product_list["data"]
    children = []
    for product_type_data in product_list:
        for product in product_type_data["productsList"]:

            new_product = {
                "key": str(uuid.uuid4()),#随机生成key
                "expressType": product_type_data["productType"],
                "expressSupplier": product["expressSupplier"],
                "channelName": product["expressChannelName"],
                "channelCode": product["expressChannelCode"],
            }
            children.append(new_product)
    order_item['orders']["children"] = children
    request_orders =order_item['orders']
    warehouse_data,us_province_two = get_warehouse_data()

    fba_warehouse_data = [
        i for i in warehouse_data if i.get("d_code") == request_orders.get("d_code")
    ][0]
    if fba_warehouse_data:
        country = "US"
        zip = fba_warehouse_data["zip"]
        # 将省份全称转为二字码，全部转为大写
        province_full = fba_warehouse_data["province"]
        if len(province_full) == 2:
            province = province_full
        else:
            # logger.info(province_full.replace(" ", "").upper())
            try:
                province = us_province_two[
                    us_province_two.iloc[:, 1].str.replace("\xa0", " ").str.upper() == province_full.upper()
                ].iloc[0, 0]
            except Exception as e:
                logger.error(f"获取省份二字码失败: {e}")
                return {"code": 500, "message": f"获取省份二字码失败{fba_warehouse_data['d_code']}", "data": None}
        city = fba_warehouse_data["city"]
        address1 = ""
        telephone = "0123456789"
        cjaddr = fba_warehouse_data["cjaddr"]
        d_code = fba_warehouse_data["d_code"]
        if len(d_code.split("-")) == 2:
            name = cjaddr.split("\n")[0].replace(":", "").replace("：", "").strip()
            address1 = cjaddr.split("\n")[1].replace(":", "").replace("：", "").strip()
            company_name = "Amazon"
            # FBA仓
        else:
            name = ""
            company_name = ""
            if "RECEIVER & COMPANY NAME" in cjaddr:
                # 获取RECEIVER & COMPANY NAME右边的部分
                receiver_company_name_index = cjaddr.find("RECEIVER & COMPANY NAME")
                receiver_company_name_right = cjaddr[
                    receiver_company_name_index + len("RECEIVER & COMPANY NAME") :
                ].strip()

                # 判断右边是否有'/'
                if "/" in receiver_company_name_right:
                    parts = receiver_company_name_right.split(
                        "/", 1
                    )  # 只分割第一个斜杠
                    name = parts[0].replace(":", "").replace("：", "").strip()
                    company_name = (
                        parts[1]
                        .split("\n")[0]
                        .replace(":", "")
                        .replace("：", "")
                        .strip()
                    )  # 忽略换行符右边的数据
                else:
                    name = (
                        receiver_company_name_right.replace(":", "")
                        .replace("：", "")
                        .split("\n")[0]
                        .strip()
                    )
                    company_name = name
            # 因为字符串中没有TEL，所以匹配不到。修改pattern只匹配ADDRESS后面的内容
            pattern = r"ADDRESS:\s*(.*?)(?:\r\n|$)"

            # 使用 re.search 进行匹配
            match = re.search(pattern, cjaddr, re.DOTALL)

            # 获取匹配的结果
            if match:
                address1 = match.group(1).replace(":", "").replace("：", "").strip()
            if not address1:
                return {"code": 500, "message": "获取仓库地址失败", "data": None}
            # 获取TEL右边的电话
            tel_index = cjaddr.find("TEL")
            if tel_index != -1:
                telephone_part = (
                    cjaddr[tel_index + len("TEL") :]
                    .replace(":", "")
                    .replace("：", "")
                    .strip()
                )
                telephone = telephone_part.split("\n")[
                    0
                ].strip()  # 忽略换行符右边的数据
    try_calculate_result = []

    token = await get_valid_token()
    logger.info(request_orders)

    async def calculate_fee(product):
        logger.info(f"正在计算{request_orders['a_number']}-{product['channelName']}的运费")
        try_calculate_request_data = {
            "orderNumber":  re.sub(r'[()（）]', '', request_orders["a_number"]) ,
            "poZipCode": "",
            "arrivePortCode": "",
            "expressType": product["expressType"],
            "expressSupplier": product["expressSupplier"],
            "channelName": product["channelName"],
            "channelCode": product["channelCode"],
            "shipperFrom": {
                "name": "",
                "companyName": "",
                "phone": "",
                "postalCode": "",
                "country": "",
                "province": "",
                "city": "",
                "address1": "",
                "address2": "",
            },
            "shipperTo": {
                "name": name.strip(','),
                "companyName": company_name.strip(','),
                "phone": telephone,
                "postalCode": zip,
                "country": country,
                "province": province,
                "city": city,
                "address1": address1.strip(','),
                "address2": "",
            },
            "productDetailList": [
                {
                    "length": "50",
                    "width": "30",
                    "height": "30",
                    "weight": str(
                        round(request_orders["weight"] / request_orders["qty"], 2)
                    ),
                    "number": request_orders["qty"],
                }
            ],
        }
        logger.info(try_calculate_request_data)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                "http://47.103.138.130:8081/api/Order/FeeRates",
                json=try_calculate_request_data,
                headers={"Authorization": f"Bearer {token}"},
            )
            response_json = response.json()
            if response.status_code == 200 and response_json["data"]:
                response_data = {
                    "a_number": request_orders["a_number"],
                    "child_id": product["key"],
                    "totalFee": response_json["data"]["totalFee"],
                    "channelName": product["channelName"],
                    "supplier": product["expressSupplier"],
                    "expressType": product["expressType"],
                    "channelCode": product["channelCode"],
                    "shipperTo": try_calculate_request_data["shipperTo"],
                    "productDetailList": try_calculate_request_data["productDetailList"],
                }
                return response_data
            else:
                logger.error(try_calculate_request_data)
                logger.error(response.text)
                if '已存在' in response_json.get("Msg",""):
                    totalFee = 1
                else:
                    #失败
                    totalFee = -1

                response_data = {
                    "key": str(uuid.uuid4()),#随机生成key
                    "a_number": request_orders["a_number"],
                    "child_id": product["key"],
                    "totalFee": totalFee,
                    "channelName": product["channelName"],
                    "supplier": product["expressSupplier"],
                }
                return response_data

    tasks = [calculate_fee(product) for product in request_orders["children"]]
    try_calculate_result = await asyncio.gather(*tasks)

    if try_calculate_result:
        return {"code": 200, "message": "获取成功", "data": try_calculate_result}
    else:
        return {"code": 500, "message": "获取失败", "data": None}

@order_router.post("/TuffyOrder", summary="提交订单进行下单")
async def create_order(request: Request):
    """
    开始下单
    """
    request_data = await request.json()
    # print(request_data)
    token = await get_valid_token()
    create_order_result = []

    for product in request_data['orders']:
        create_order_request_data = {
            "orderNumber": product["a_number"].replace("(", "").replace(")", ""),
            "poZipCode": "",
            "arrivePortCode": "",
            "expressType": product["expressType"],
            "expressSupplier": product["expressSupplier"],
            "channelName": product["channelName"],
            "channelCode": product["channelCode"],
            "shipperFrom": {
                "name": "",
                "companyName": "",
                "phone": "",
                "postalCode": "",
                "country": "",
                "province": "",
                "city": "",
                "address1": "",
                "address2": "",
            },
            "shipperTo": product["shipperTo"],
            "productDetailList": product["productDetailList"],
        }

        response = httpx.post(
            "http://47.103.138.130:8081/api/Order/TuffyOrder",
            json=create_order_request_data,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30
        )
        response_json = response.json()
        logger.info(response_json)
        if response.status_code == 200 and response_json.get("success") :
            create_order_result.append(product["a_number"])
       
    if create_order_result:
        return {"code": 200, "message": "获取成功", "data": create_order_result}
    else:
        return {"code": 500, "message": "获取失败", "data": response_json}


@order_router.get("/get_order_list", summary="获取订单列表")
async def get_order_list(
    orderNumber: str = None,
    startTime: str = None,
    endTime: str = None,
    page: int = 1,
    size: int = 10,
):
    """
    获取订单列表
    """
    params = {
        "orderNumber": orderNumber,
        "startTime": startTime,
        "endTime": endTime,
        "page": page,
        "size": size,
    }
    token = await get_valid_token()

    headers = {"Content-Type": "application/x-www-form-urlencoded","Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                "http://47.103.138.130:8081/api/Order/getorderlist",
                params=params,
                headers=headers,
            )

            if response.status_code == 200:
                response_json = response.json()

                return {
                    "code": 200,
                    "message": "获取成功",
                    "data": response_json.get("data"),
                }

            else:
                logger.error(f"获取订单列表失败: {response.text}")
                return {
                    "code": response.status_code,
                    "message": f"获取失败-{response.text}",
                    "data": None,
                }
        except Exception as e:
            logger.error(f"获取订单列表异常: {str(e)}")
            return {"code": 500, "message": f"获取失败-{str(e)}", "data": None}


@order_router.post("/download_order_list_pdf", summary="下载订单列表PDF或打包ZIP")
async def download_order_list_pdf(request: DownloadOrderListRequest):
    """
    下载订单列表PDF
    如果是单个URL则直接返回PDF文件
    如果是多个URL则返回ZIP文件（无密码）
    """
    if not request.urls:
        raise HTTPException(status_code=400, detail="URL列表不能为空")

    async with httpx.AsyncClient() as client:
        try:
            if len(request.urls) == 1:
                # 单个URL直接返回PDF
                response = await client.get(request.urls[0])
                if response.status_code != 200:
                    raise HTTPException(status_code=response.status_code, detail="PDF下载失败")
                    
                return StreamingResponse(
                    io.BytesIO(response.content),
                    media_type="application/pdf",
                    headers={"Content-Disposition": "attachment; filename=order.pdf"}
                )
            else:
                # 多个URL打包成ZIP，明确不设置密码
                zip_buffer = io.BytesIO()
                successful_files = 0
                
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    for i, url in enumerate(request.urls):
                        response = await client.get(url)
                        name = url.split("/")[-1]
                        if response.status_code == 200:
                            if response.headers.get('Content-Type') == 'application/pdf':
                                zip_file.writestr(f"{name}.pdf", response.content)
                                successful_files += 1
                            else:
                                logger.warning(f"URL {url} 返回的不是 PDF，Content-Type: {response.headers.get('Content-Type')}")
                        else:
                            logger.warning(f"URL {url} 下载失败，状态码: {response.status_code}")
                
                logger.info(f"成功添加 {successful_files} 个文件到 ZIP")
                
                if successful_files == 0:
                    return JSONResponse(status_code=400, content={"error": "没有成功下载任何 PDF 文件"})
                
                zip_buffer.seek(0)
                zip_content = zip_buffer.getvalue()
                
                # 调试：保存 ZIP 文件到本地
                with open("test.zip", "wb") as f:
                    f.write(zip_content)
                
                return Response(
                    content=zip_content,
                    media_type="application/zip",
                    headers={
                        "Content-Disposition": "attachment; filename=orders.zip",
                        "Content-Length": str(len(zip_content))
                    }
                )
                
        except Exception as e:
            logger.error(f"下载PDF文件异常: {str(e)}")
            raise HTTPException(status_code=500, detail=f"下载失败: {str(e)}")
        




@order_router.get("/getfbatrackinglist", summary="获取FBA箱号清单列表")
async def get_fba_tracking_list(
    sonno: str = None,
    startTime: str = None,
    endTime: str = None,
    page: int = 1,
    size: int = 10,
    customerId: int = 0
):
    """获取FBA箱号清单列表"""
    try:
        token = await get_valid_token()
        
        # 构建表单数据
        form_data = {
            "page": str(page),
            "size": str(size),
            "customerId": str(customerId)
        }
        if sonno:
            form_data["sonno"] = sonno
        if startTime:
            form_data["startTime"] = startTime  
        if endTime:
            form_data["endTime"] = endTime

        # 转换为 x-www-form-urlencoded 格式
        encoded_data = urlencode(form_data)

        # 发送请求
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "http://47.103.138.130:8081/api/Fba/getfbatrackinglist",
                params=encoded_data,  # 直接使用编码后的字符串
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/x-www-form-urlencoded"  # 明确指定内容类型
                },
                timeout=30
            )

            if response.status_code == 200:
                response_data = response.json()
                return {
                    "status": response_data.get("status", 0),
                    "success": response_data.get("success", True),
                    "msg": response_data.get("msg", ""),
                    "data": response_data.get("data", {
                        "page": page,
                        "pageCount": 0,
                        "dataCount": 0, 
                        "pageSize": size,
                        "data": []
                    })
                }
            else:
                logger.error(f"获取FBA箱号清单列表失败: {response.text}")
                return {
                    "status": 500,
                    "success": False,
                    "msg": "获取FBA箱号清单列表失败",
                    "data": None
                }

    except Exception as e:
        logger.error(f"获取FBA箱号清单列表异常: {str(e)}")
        return {
            "status": 500,
            "success": False,
            "msg": f"获取FBA箱号清单列表异常: {str(e)}",
            "data": None
        }

@order_router.get("/export_fba_tracking", summary="导出FBA箱号跟踪信息为Excel")
async def export_fba_tracking(
    sonno: str = None,
    startTime: str = None, 
    endTime: str = None,
    customerId: int = 0
):
    """导出FBA箱号跟踪信息"""
    try:
        token = await get_valid_token()
        
        # 如果没有传入日期,默认最近30天
        if not startTime or not endTime:
            #end为明天
            end = datetime.now() + timedelta(days=1)
            start = end - timedelta(days=30)
            startTime = start.strftime("%Y-%m-%d")
            endTime = end.strftime("%Y-%m-%d")

        # 构建查询参数
        params = {
            "page": 1,
            "size": 10000,  # 设置较大的size以获取所有数据
            "customerId": customerId
        }
        if sonno:
            params["sonno"] = sonno
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        encoded_data = urlencode(params)
        logger.info(encoded_data)
        # 发送请求获取数据
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "http://47.103.138.130:8081/api/Fba/getfbatrackinglist",
                params=encoded_data,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/x-www-form-urlencoded"  # 明确指定内容类型
                },                timeout=30
            )
            response_data = response.json()

            if response.status_code != 200:
                logger.error(f"获取FBA跟踪信息失败: {response.text}")
                return JSONResponse(status_code=500, content={"error": response_data.get("Msg", "获取数据失败")})
               

            if not response_data.get("success"):
                return JSONResponse(status_code=500, content={"error": response_data.get("Msg", "获取数据失败")})
               

            # 创建DataFrame
            data = []
            for item in response_data["data"]["data"]:
                if item['customerName'] == "ALGJ-傲雷国际-SZ":
                    tracking_id = item["fullTrackingId"]
                else:
                    tracking_id = item["trackingId"]
                data.append({
                    "订单号": item["sono"],
                    "FBA箱号": item["fbaShipmentBoxId"],
                    "跟踪号": tracking_id,
                    "A单号": item.get("operNo",None)
                })

            #如果A单号有空的
            sono_list = [item["订单号"] for item in data if item["A单号"] is None]
            if sono_list:
                morelink_client = MoreLinkClient()
                so_str = ",".join(sono_list)
                operNo = morelink_client.dahuodingdan_worknum_search_httpx(so_str,"SO")
                all_data = [{'operNo': item['operNo'],'sono': item['sono']} for item in operNo]
                for item in all_data:
                    for data_item in data:
                        if data_item["订单号"] == item["sono"]:
                            data_item["A单号"] = item["operNo"]
            df = pd.DataFrame(data)

            # 保存到内存
            # 创建目录
            os.makedirs('./excel/fba_box_list', exist_ok=True)
            
            # 生成文件名
            filename = f"FBA_tracking_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
            file_path = os.path.join('./excel/fba_box_list', filename)
            
            # 保存Excel文件
            df.to_excel(file_path, index=False)
            
            # 读取文件并返回
            with open(file_path, 'rb') as f:
                excel_file = io.BytesIO(f.read())
            
            encoded_filename = quote(filename)
            
            return StreamingResponse(
                excel_file,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"}
            )

    except Exception as e:
        logger.error(f"导出FBA跟踪信息异常: {traceback.format_exc()}")
        return {
            "code": 500,
            "message": f"导出异常: {str(e)}",
            "data": None
        }

@order_router.get("/getcustomerslist", summary="获取客户列表")
async def get_customers_list():
    try:
        token = await get_valid_token()
        
        # 发送请求获取数据
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "http://47.103.138.130:8081/api/Fba/getcustomerslist",
                headers={"Authorization": f"Bearer {token}"},
                timeout=30
            )   
            if response.status_code != 200:
                return {
                    "code": 500,
                    "message": "获取数据失败",
                    "data": None
                }
            response_data = response.json()
            if not response_data.get("success"):
                return {
                    "code": 500,
                    "message": response_data.get("msg", "获取数据失败"),
                    "data": None
                }
            return response_data
    except Exception as e:
        logger.error(f"获取客户列表异常: {str(e)}")
        return {
            "code": 500,
            "message": f"获取客户列表异常: {str(e)}",
            "data": None
        }



@order_router.get("/mannual_update_order", summary="手动更新订单信息（同步MoreLink数据到本地数据库）")
async def mannual_update_order():
    try:
        logger.info("开始获取数据")
        db = MysqlDB(
            ip=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT")),
            db="fbatms",
            user_name=os.getenv("MYSQL_USER"),
            user_pass=os.getenv("MYSQL_PASS")
        )
        logger.info("数据库连接成功")

        # 获取原始数据
        raw_data = db.find("select * from tb_fbatracking where (sono='' or customerId=0 or operNo is null) and type = 2", to_json=True)
        if not raw_data:
            logger.info("没有需要更新的数据")
            return {
                "code": 200,
                "message": "没有需要更新的数据",
                "data": None
            }
        
        logger.info(f"获取到{len(raw_data)}条需要更新的数据")
        customers_data = db.find("select id,customerName from tb_customers", to_json=True)
        customers_data_dict = {item["customerName"]:item["id"] for item in customers_data}
        
        # 按fbaShipmentBoxId的U分隔前面字符串分组
        grouped_data = {}
        for item in raw_data:
            if item.get('fbaShipmentBoxId'):
                # 以U分隔,取前面部分作为分组key
                group_key = item['fbaShipmentBoxId'].split('U')[0] if 'U' in item['fbaShipmentBoxId'] else item['fbaShipmentBoxId']
                if group_key not in grouped_data:
                    grouped_data[group_key] = []
                grouped_data[group_key].append(item)
        
        logger.info("开始获取MoreLink数据")
        
        morelink_client = MoreLinkClient(node_path=find_playwright_node_path())
        molink_dahuo_data = morelink_client.dahuodingdan_all_data()
        logger.info(f"获取到{len(molink_dahuo_data)}条MoreLink数据")

        data = grouped_data
        update_count = 0
        for key, value in data.items():
            try:
                group_tracking_number = value[0]['trackingId']
                customerId = None
                for molink_dahuo_item in molink_dahuo_data:
                    # if group_tracking_number in str(molink_dahuo_item["CourierNumber"]):
                    if key in str(molink_dahuo_item["fbano"]):
                        sono = molink_dahuo_item["sono"]
                        operNo = molink_dahuo_item["operNo"]
                        customername = molink_dahuo_item["customername"]
                        customerId = customers_data_dict[customername]
                        break

                if customerId:
                    db.update_smart("tb_fbatracking", {"sono": sono, "customerId": customerId,"operNo": operNo}, f"trackingId = '{group_tracking_number}' or fbaShipmentBoxId like '{key}%'")
                    update_count += 1
                logger.debug(f"处理分组 {key}: {value}")
            except Exception as e:
                logger.error(f"处理分组{key}时出错: {str(e)}")
                continue
                
        logger.info(f"数据更新完成,共更新{update_count}条数据")
        return {
            "code": 200,
            "message": "获取数据成功",
            "data": data
        }
    except Exception as e:
        logger.error(f"获取数据出错: {traceback.format_exc()}")
        return {
            "code": 500,
            "message": f"获取数据失败: {str(e)}",
            "data": None
        }