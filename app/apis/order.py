import json
import re

from loguru import logger
import pandas as pd
from app.db_mongo import get_session
from fastapi import APIRouter, Request
import httpx
from morelink_api import MoreLinkClient

order_router = APIRouter(prefix="/order", tags=["订单"])


@order_router.get("/product_list")
async def get_product_list(area: str):
    """
    获取产品列表接口
    """
    headers = {
        "Accept": "text/plain",
        "Content-Type": "application/x-www-form-urlencoded",
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


@order_router.post("/get_a_number_data")
async def get_a_number_data(worknum: str):
    """
    获取一个产品数据
    """
    morelink_client = MoreLinkClient()
    dahuo_data = morelink_client.get_dahuo_data_by_id(worknum)
    if dahuo_data:
        d_code = dahuo_data["d_code"]
        qty = dahuo_data["GoodsNum"] if dahuo_data["GoodsNum"] else dahuo_data["yjnum"]
        weight = dahuo_data["ckweight"] if dahuo_data["ckweight"] else dahuo_data["yjweight"]
        return {
            "code": 200,
            "message": "获取成功",
            "a_number": f"{worknum}-{d_code}-{qty}X",
            "data": {
                "qty": qty,
                "weight": weight,
                "d_code": d_code,
            },
        }
        # res = morelink_client.search_warehouse_size(sono, cid)
        # if res and res.get("success"):
        #     data = json.loads(res["data"])[0]['tb']
        #     return {
        #         "code": 200,
        #         "message": "获取成功",
        #         'a_number':worknum,
        #         "data": [{
        #             "qty": i ["qty"],  # 数量
        #             "length": i ["length"],  # 长
        #             "width": i ["width"],  # 宽
        #             "height": i ["height"],  # 高
        #             "weight": i ["weight"]  # 重量
        #         }
        #         for i in data
        #     ]
        #     }
        return {"code": 500, "message": "获取尺寸数据失败", "data": None}
    else:
        return {"code": 500, "message": "获取工作单号失败", "data": None}


@order_router.post("/try_calculate")
async def try_calculate(request: Request):
    """
    尝试计算订单
    """
    request_data = await request.json()
    # print(request_data)
    request_orders = request_data["orders"]
    morelink_client = MoreLinkClient()
    warehouse_data = None
    if warehouse_data is None:
        warehouse_data = morelink_client.fba_warehouse_search()
    us_province_two = pd.read_excel(r"file/remoteaddresscheck/美国州名.xlsx")
    fba_warehouse_data = [
        i for i in warehouse_data if i.get("d_code") == request_orders.get("d_code")
    ][0]
    if fba_warehouse_data:
        country = "US"
        zip = fba_warehouse_data["zip"]
        # 将省份全称转为二字码，全部转为大写
        province_full = fba_warehouse_data["province"].upper()
        province = us_province_two[
            us_province_two.iloc[:, 1].str.upper() == province_full
        ].iloc[0, 0]
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
    for product in request_orders["children"]:
        try_calculate_request_data = {
            "orderNumber": request_orders["a_number"],
            "poZipCode": "",
            "arrivePortCode": "",
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

            response_data = {
                "a_number": request_orders["a_number"],
                "child_id": product["key"],
                "totalFee": "失败",
                "channelName": product["channelName"],
                "supplier": product["expressSupplier"],
            }
            try_calculate_result.append(response_data)
    if try_calculate_result:
        return {"code": 200, "message": "获取成功", "data": try_calculate_result}
    else:
        return {"code": 500, "message": "获取失败", "data": None}


@order_router.post("/TuffyOrder")
async def start_order(request: Request):
    """
    开始下单
    """
    request_data = await request.json()
    # print(request_data)
  
    try_calculate_result = []
    for product in request_data['orders']:
        create_order_request_data = {
            "orderNumber": product["a_number"],
            "poZipCode": "",
            "arrivePortCode": "",
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
        )
        response_json = response.json()
        if response.status_code == 200 and response_json["data"]:
            try_calculate_result.append(response_json["data"])
       
    if try_calculate_result:
        return {"code": 200, "message": "获取成功", "data": try_calculate_result}
    else:
        return {"code": 500, "message": "获取失败", "data": None}


@order_router.get("/get_order_list")
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

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

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
