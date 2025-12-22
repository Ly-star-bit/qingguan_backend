"""
# @ Author: luoyu
# @ Create Time: 2024-06-04 10:57:29
# @ Modified by: luoyu
# @ Modified time: 2024-06-04 10:57:35
# @ Description: 工作单号打单 -> 完整打和部分打
"""

from io import BytesIO
import os
from collections import defaultdict
import re
import sqlite3
import traceback
import shutil
import time
import zipfile
import pandas as pd
import httpx
from morelink_api import MoreLinkClient
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
from loguru import logger
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import fitz  # PyMuPDF
import hashlib

import json
from rpa_tools import find_playwright_node_path
import openpyxl
import setting
from tenacity import retry, stop_after_attempt, wait_fixed
import share

load_dotenv()

# 自定义处理器，用于在日志记录时发送邮件

# os.environ["PLAYWRIGHT_BROWSERS_PATH"] = r"C:\Users\Administrator\AppData\Local\ms-playwright"


class UPSRoyalDadan:
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.token = None
        self.api_context = None
        # self.chrome = None
        self.playwright = sync_playwright().start()

    def launch_browser(self):
        # chrome = ChromeLauncher(multi_chrome=True)
        # chrome.launch_chrome()
        # self.chrome = chrome
        # from rpa_tools import WindowSetter

        # self.browser = self.playwright.chromium.connect_over_cdp(
        #     f"http://localhost:{chrome.debug_port}"
        # )
        self.browser = self.playwright.chromium.launch(
            headless=True,
        )
        self.context = self.browser.new_context()
        self.page = self.context.new_page()
        # self.context = self.browser.contexts[0]
        # self.page = self.context.pages[0]
        # session = self.context.new_cdp_session(self.page)
        # window_setter = WindowSetter(session)
        # window_setter.full()
        self.page.goto("http://47.57.143.95")
        self.page.wait_for_load_state("networkidle")

    def login(self):
        login_button_tag = self.page.locator("//button/span[text()='登录']")
        if login_button_tag.all():
            # 需要登陆
            self.page.locator("//input[contains(@placeholder,'用户名')]").fill(
                os.getenv("ups_royal_username")
            )
            self.page.locator("//input[contains(@placeholder,'密码')]").fill(
                os.getenv("ups_royal_pass")
            )
            while True:
                if not login_button_tag.all():
                    break
                verficy_code_buffer = self.page.locator(
                    "//img[contains(@src,'api')]"
                ).screenshot()
                if verficy_code_buffer:
                    verify_resp = httpx.post(
                        url=setting.OCR_URL,
                        files={"image": ("verify_picture", verficy_code_buffer)},
                    )

                self.page.locator("//input[contains(@placeholder,'验证码')]").fill(
                    verify_resp.text.strip()
                )
                login_button_tag.click()
                self.page.wait_for_load_state("networkidle")
                time.sleep(2)

            print("登陆成功")
        self.api_context = self.context.request
        self.token = [
            cookie["value"]
            for cookie in self.context.cookies()
            if cookie["name"] == "cs_token"
        ][0]

    def upload_file(self, upload_file):
        if not self.page.locator("//span[text()='批量导入']").nth(0).is_visible():
            self.page.click("//span[text()='尾程快递订单管理']")
            time.sleep(1)
        self.page.locator("//span[text()='批量导入']").nth(0).click()

        self.page.locator("//input[@placeholder='请选择']").click()
        self.page.locator("//span[text()='尾程快递-多票批量导入-按总箱数']").click()

        # Listen for the network response
        with self.page.expect_response(
            "http://47.57.143.95/its-api/fba/order/batchOrderImport?node=11&templateType=2"
        ) as response_info:
            with self.page.expect_file_chooser() as fc_info:
                self.page.locator("//div[@class='el-upload-dragger']").nth(0).click()
                file_chooser = fc_info.value
                file_chooser.set_files(upload_file)
            self.page.wait_for_load_state("networkidle")
            response = response_info.value

        return response.json()  # or response.text() depending on the response content

    def get_range(self, value):
        ranges = [10, 100, 200, 500, 1000]
        for r in ranges:
            if value <= r:
                return r
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
    def get_draft_list(self, customerOrderNos: str):
        # self.page.locator("//span[text()='订单列表']").nth(0).click()

        # 获取今天的日期
        today = datetime.today()

        # 获取两个月前的日期
        two_months_ago = today - timedelta(days=60)

        # 格式化日期为所需的字符串格式
        createDate2 = today.strftime("%Y-%m-%d 23:59:59")
        createDate1 = two_months_ago.strftime("%Y-%m-%d 00:00:00")
        params = {
            "order": "desc",
            "orderField": "id",
            "page": 1,
            "limit": self.get_range(len(customerOrderNos.split(",")) + 1),
            "customerOrderNos": customerOrderNos,
            "customerOrderNoLike": "",
            "waybillNos": "",
            "deliveryNos": "",
            "consignee": "",
            "logisticsProductCode": "",
            "consigneeCountry": "",
            "createDate1": createDate1,
            "createDate2": createDate2,
            "customerId": "1722171699027361794",
            "status": 10,
            "orderByMode": 11,
            "orderLogisticsType": 12,
            "print": "",
            "createDate": None,
        }

        headers = {"Content-Type": "application/json", "Token": self.token}
        draft_url = "http://47.57.143.95/its-api/co/order/page"
        draft_list_res = self.api_context.post(
            url=draft_url, headers=headers, data=params
        )
        return draft_list_res.json()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
    def forecast(self, upload_file):
        upload_file_pd = pd.read_excel(upload_file, skiprows=4)
        customerOrderNo_list = upload_file_pd["客户单号/入库单号"].tolist()

        draft_list = self.get_draft_list(",".join(customerOrderNo_list))
        if draft_list["code"] == 500:
            logger.warning(draft_list["msg"])
            return
        if not draft_list:
            logger.warning("没有获取到草稿中的数据")
            return
        matched_ids = []
        for draft in draft_list["data"]["list"]:
            if draft["customerOrderNo"] in customerOrderNo_list:
                matched_ids.append(
                    {"id": draft["id"], "customerOrderNo": draft["customerOrderNo"]}
                )

        headers = {"Content-Type": "application/json", "Token": self.token}
        if not matched_ids:
            logger.warning("没有匹配到需要预报的数据")
            return
        success_id = []
        for item in matched_ids:
            try:
                forecast_url = "http://47.57.143.95/its-api/co/order/forecast"
                forecast_res = self.api_context.put(
                    url=forecast_url, headers=headers, data=str(item["id"])
                )
                forecast_res_json = forecast_res.json()
                if (
                    forecast_res.status == 200
                    and forecast_res_json["data"]["isSuccess"]
                ):
                    success_id.append(item)
                else:
                    logger.warning(forecast_res_json)
                time.sleep(1)
            except Exception as e:
                logger.error(f"预报错误为:{e}")
                continue
        return success_id

    def forecast_ui(self, upload_file):
        self.page.locator("//span[text()='订单列表']").nth(0).click()
        upload_file_pd = pd.read_excel(upload_file, skiprows=4)
        upload_file_pd["客户单号/入库单号"].tolist()
        self.page.locator("//li[text()='客户单号']").click()

        self.page.locator("//textarea").fill(
            "S240600393-15213US-5X,S240400109-08518US-6X"
        )

        # self.page.locator("//textarea").fill(",".join(customerOrderNo_list))
        self.page.locator(
            '//span[@class="el-pagination__sizes"]//span[@class="el-input__suffix"]'
        ).click()
        self.page.locator("//span[text()='1000条/页']").click()

        self.page.locator("//span[text()='查询']").click()

        # 预报
        self.page.locator('//span[@title="全选/取消"]').nth(1).click()
        self.page.locator("//span[text()='预报']").nth(0).click()

        self.page.wait_for_load_state("networkidle")
        time.sleep(1)
        self.page.keyboard.press("Escape")

        self.page.locator("//div[contains(text(),'已预报')]").click()

        # 获取所有派送单号

    def get_queryid(self, limit=10, status=11, customerOrderNos=""):
        """获取查询id

        Args:
            limit (int): 限制每页多少条数据
            status (int): 查询什么类型，11为已预报，10为草稿，20为已取消
            customerOrderNos(str): 通过具体的客户单号来过滤

        Returns:
            str: query_id
        """

        url = "http://47.57.143.95/its-api/common/cacheQueryParams"
        headers = {"Content-Type": "application/json", "Token": self.token}
        # 获取今天的日期
        today = datetime.today()

        # 获取两个月前的日期
        two_months_ago = today - timedelta(days=60)

        # 格式化日期为所需的字符串格式
        createDate2 = today.strftime("%Y-%m-%d")
        createDate1 = two_months_ago.strftime("%Y-%m-%d")
        payload = {
            "order": "desc",
            "orderField": "id",
            "page": 1,
            "limit": limit,
            "customerOrderNos": customerOrderNos,
            "customerOrderNoLike": "",
            "waybillNos": "",
            "deliveryNos": "",
            "consignee": "",
            "logisticsProductCode": "",
            "consigneeCountry": "",
            "createDate1": f"{createDate1} 00:00:00",
            "createDate2": f"{createDate2} 23:59:59",
            "customerId": "1722171699027361794",
            "status": status,
            "orderByMode": 11,
            "orderLogisticsType": 12,
            "createDate": None,
        }
        queryid_res = self.api_context.post(url=url, data=payload, headers=headers)

        if queryid_res.status == 200 and queryid_res.json()["msg"] == "success":
            return queryid_res.json()["data"]

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
    def get_other_tracking_num(self, work_num, headers=None):
        """通过工作单号获取所有的tracking_num,这里的工作单号不是A单号，而是打单平台的复合单号

        Args:
            work_num (str): 工作单号
            headers (dict, optional): 请求头. Defaults to None.

        Returns:
            list: tracking_num 列表, 如果失败则返回 None
        """
        tracking_list = []
        page = 1
        limit = 20
        while True:
            url = f"http://47.57.143.95/its-api/co/orderpackage/page?orderId={work_num}&page={page}&limit={limit}&_t={int(time.time() * 1000)}"
            res = self.api_context.get(url, headers=headers, timeout=30 * 1000)

            if res.status == 200 and res.json()["msg"] == "success":
                data = res.json()["data"]
                if not data["list"]:
                    break  # 如果没有数据，退出循环
                tracking_list.extend([i["packageDeliveryNo"] for i in data["list"]])
                if data["total"] <= page * limit:
                    break  # 如果已经获取所有数据，退出循环
                page += 1
            else:
                logger.warning(res.text)  # 打印响应文本，而不是响应体
                return None
        return tracking_list

    def print_already_yubao_get_tracking_num(self, customerOrderNos):
        """打印已预报的面单和获取单号"""
        # 先获取所有已预报的数据
        # customerOrderNos = "S240600393-15213US-5X,S240400109-08518US-6X"
        if not customerOrderNos:
            return None, None
        # 获取今天的日期
        today = datetime.today()

        # 获取两个月前的日期
        two_months_ago = today - timedelta(days=60)

        # 格式化日期为所需的字符串格式
        createDate2 = today.strftime("%Y-%m-%d")
        createDate1 = two_months_ago.strftime("%Y-%m-%d")
        url = "http://47.57.143.95/its-api/co/order/page"

        params = {
            "order": "desc",
            "orderField": "id",
            "page": 1,
            "limit": self.get_range(len(customerOrderNos.split(",")) + 1),
            "customerOrderNos": customerOrderNos,
            "customerOrderNoLike": "",
            "waybillNos": "",
            "deliveryNos": "",
            "consignee": "",
            "logisticsProductCode": "",
            "consigneeCountry": "",
            "createDate1": createDate1,
            "createDate2": createDate2,
            "customerId": "1722171699027361794",
            "status": 11,
            "orderByMode": 11,
            "orderLogisticsType": 12,
            "print": "",
            "createDate": None,
        }
        headers = {"Content-Type": "application/json", "Token": self.token}

        already_data_res = self.api_context.post(url=url, data=params, headers=headers)
        if (
            already_data_res.status == 200
            and already_data_res.json()["msg"] == "success"
        ):
            already_data_list = already_data_res.json()["data"]["list"]
        if already_data_list:
            # 根目录路径
            root_path = Path(f"file/tools_auto/ups_pdf_file/{time.time()}")
            # 创建根目录（如果不存在）
            root_path.mkdir(parents=True, exist_ok=True)
            return_data = []
            delivery_type_list = []

            for item in already_data_list:
                id = item["id"]

                # 下载pdf
                @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
                def download_pdf():
                    pdf_url = f"http://47.57.143.95/its-api/co/order/customer/label?nos={id}&orderByMode=11"
                    return self.api_context.post(
                        url=pdf_url, headers=headers, timeout=30 * 1000
                    )

                pdf_res = download_pdf()
                # 构建文件路径
                file_path = root_path / f"{item['customerOrderNo']}.pdf"
                # 写入文件
                file_path.write_bytes(pdf_res.body())
                # 获取快递单号
                if "FedEx" in item["logisticsProductCode"]:
                    deliver_type = "FedEx"
                elif "UPS" in item["logisticsProductCode"]:
                    delivery_type_list.append("UPS")

                    deliver_type = "UPS"
                    # 裁剪UPS面单
                    crop_bottom(
                        input_pdf_path=file_path,
                        output_pdf_path=root_path
                        / "crop"
                        / f"{item['customerOrderNo']}-crop.pdf",
                    )
                all_tracking_num = self.get_other_tracking_num(
                    item["id"], headers=headers
                )
                return_data.append(
                    {
                        "工作单号": item["customerOrderNo"].split("-")[0],
                        "快递单号": ",".join(all_tracking_num)
                        if all_tracking_num
                        else item["deliveryNo"],
                        # "快递单号": item["deliveryNo"],
                        "打单方式": deliver_type,
                    }
                )
            if "UPS" in delivery_type_list:
                # 获取所有已经裁剪的PDF文件
                existing_cropped_files = {
                    file.stem.replace("-crop", "")
                    for file in (root_path / "crop").glob("*-crop.pdf")
                }

                # 找出没有被裁剪的文件并复制到crop目录中
                all_files = {file.stem for file in root_path.glob("*.pdf")}
                uncropped_files = all_files - existing_cropped_files

                for uncropped_file in uncropped_files:
                    original_pdf_path = root_path / f"{uncropped_file}.pdf"
                    crop_pdf_path = root_path / "crop" / f"{uncropped_file}.pdf"
                    shutil.copy(original_pdf_path, crop_pdf_path)

                # 打包PDF文件夹为zip
                crop_path = root_path / "crop"
                if crop_path.exists() and any(crop_path.glob("*.pdf")):
                    # 创建zip文件
                    zip_path = f"{root_path}.zip"
                    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                        for pdf_file in crop_path.glob("*.pdf"):
                            zipf.write(pdf_file, pdf_file.name)
                    logger.info(f"PDF已打包为: {zip_path}")
                    return return_data, zip_path

        # 最后返回customerOrderNo和deliveryNo
        return return_data, root_path

    def get_yubao_data(self, client_num_list: list):
        customerOrderNos = ",".join(client_num_list)
        today = datetime.today()

        # 获取两个月前的日期
        two_months_ago = today - timedelta(days=60)
        headers = {"Content-Type": "application/json", "Token": self.token}
        url = "http://47.57.143.95/its-api/co/order/page"
        # 格式化日期为所需的字符串格式
        createDate2 = today.strftime("%Y-%m-%d 23:59:59")
        createDate1 = two_months_ago.strftime("%Y-%m-%d 00:00:00")
        params = {
            "order": "desc",
            "orderField": "id",
            "page": 1,
            "limit": self.get_range(len(customerOrderNos.split(",")) + 1),
            "customerOrderNos": customerOrderNos,
            "customerOrderNoLike": "",
            "waybillNos": "",
            "deliveryNos": "",
            "consignee": "",
            "logisticsProductCode": "",
            "consigneeCountry": "",
            "createDate1": createDate1,
            "createDate2": createDate2,
            "customerId": "1722171699027361794",
            "status": 11,
            "orderByMode": 11,
            "orderLogisticsType": 12,
            "print": "",
            "createDate": None,
        }
        res = self.api_context.post(url=url, data=params, headers=headers)
        if res.status == 200 and res.json()["msg"] == "success":
            return res.json()
        else:
            logger.warning(res.text)
            return None

    def close(self):
        self.playwright.stop()
        # self.chrome.close_chrome()


def generate_md5_signature(apiKey, apiSecret, usertoken, requestBody=""):
    timestamp = str(int(time.time() * 1000))
    # 拼接字符串
    data = f"{apiKey}{apiSecret}{usertoken}{timestamp}{requestBody}"
    # 生成MD5签名
    signature = hashlib.md5(data.encode("utf-8")).hexdigest()
    return signature, timestamp


def crop_bottom(input_pdf_path, output_pdf_path, crop_height: int = 40):
    # Ensure the output directory exists
    output_path = Path(output_pdf_path)
    output_dir = output_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Open the input PDF file
    doc = fitz.open(input_pdf_path)

    for page_num in range(len(doc)):
        page = doc[page_num]
        # Get the page's bounding box
        bbox = page.rect

        # Define the new bounding box by adjusting the height
        new_bbox = fitz.Rect(bbox.x0, bbox.y0, bbox.x1, bbox.y1 - crop_height)
        page.set_cropbox(new_bbox)

    # Save the cropped PDF to the output file
    doc.save(output_pdf_path)


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
def getTransPrice(
    work_num: str,
    logisticsProductCode: str,
    consigneeProvince: str,
    consigneeCity: str,
    consigneeAddress: str,
    consigneePostcode: str,
    packageWeightD: int,
    packageLengthD: int,
    box_num: int,
):
    """获取运输价格

    Args:
        logisticsProductCode (str): 产品代码
        consigneeProvince (str): 收货人省
        consigneeCity (str): 收货人city
        consigneeAddress (str): 收货人具体地址
        consigneePostcode (str): 收货人邮编
        packageWeightD (int): 包裹重量
        packageLengthD (int): 箱子长度
        box_num (int): 箱子数目

    Returns:
        float :   价格
    """
    apiKey = "d0c53916fdc94beabfd8d9ff07e57a46"
    userToken = "a5639002120a43d39237eb576b25ebfe"
    apiSecret = "106265d4b91b45e3956a451aea3d7967"
    payload = {
        "logisticsProductCode": logisticsProductCode,
        "taxPayMode": 11,  # 税金支付方式,取值范围[10:DDU/11:DDP]
        "parcelType": 10,  # 包裹类型 取值范围[10:包裹/11:PAK 袋/12:文件]
        "goodsCategory": 15,  # 货物类别。取值范围[11:礼物/12:文件/13:商业样本/14: 回货品/15:其他]
        "consignee": {
            "consigneeCountry": "US",
            "consigneeProvince": consigneeProvince,
            "consigneeCity": consigneeCity,
            "consigneeAddress": consigneeAddress,
            "consigneePostcode": consigneePostcode,
        },
        "packages": [
            {
                "packageWeightD": packageWeightD,
                "packageLengthD": packageLengthD,
                "packageWidthD": 30,
                "packageHeightD": 30,
            }
            for num in range(int(box_num))
        ],
    }
    requestBody = json.dumps(payload)  # 确保requestBody是JSON字符串

    signature, timestamp = generate_md5_signature(
        apiKey, apiSecret, userToken, requestBody
    )
    url = "http://47.57.143.95/its-api/api/fba/getTransSum"  # 替换为实际的API URL
    headers = {
        "Content-Type": "application/json",
        "apikey": apiKey,
        "signature": signature,
        "timestamp": timestamp,
        "usertoken": userToken,
    }
    with httpx.Client(headers=headers) as client:
        response = client.post(url, content=requestBody)
        if response.status_code == 200 and response.json()["msg"] == "success":
            data = response.json()["data"]
            logger.info(f"{logisticsProductCode}价格为:{data['totalSum']}")

            return data["totalSum"]
        else:
            logger.log("ALERT", response.json()["msg"] + f"工作单号为:{work_num}")
            share.error_messages = "错误信息为:" + response.json()["msg"]


def get_state_abbreviation(file_path, state_name):
    # 读取Excel文件
    df = pd.read_excel(file_path)

    # 获取"美国州名英文"和"美国州名缩写"两列
    state_data = df[["美国州名英文", "美国州名缩写"]]

    # 标准化输入的州名英文（去除前后空格并转换为小写）
    state_name = state_name.strip().lower()

    # 查找对应的州名缩写
    abbreviation = state_data[state_data["美国州名英文"].str.lower() == state_name][
        "美国州名缩写"
    ].values

    if len(abbreviation) > 0:
        return abbreviation[0]
    else:
        return "未找到对应的州名缩写"


def get_fill_data(
    deliver_type,
    weight,
    box_num,
    work_num,
    warehouse_d_code,
    origin_weight,
    receipt_data,
):
    data = []
    data.append((1, f"{work_num}-{warehouse_d_code}-{int(box_num)}X"))
    abbreviation = receipt_data["province"]  # 默认值

    if deliver_type.upper() == "FEDEX":
        # CA01和CA02修改为CA03
        # if origin_weight * 2.2 * box_num < 200:
        #     data.append((6, "FedEx-CA03"))
        # else:
        #     data.append((6, "FedEx-CA03"))
        data.append((6, "FedEx-CA03"))
        # 转换州二字码
        file_path = "美国州省二字码.xlsx"  # 替换为你的Excel文件路径
        state_name = receipt_data["province"]  # 替换为你要查询的州名英文
        abbreviation = get_state_abbreviation(file_path, state_name)
        if abbreviation == "未找到对应的州名缩写":
            abbreviation = receipt_data["province"]
    elif deliver_type.upper() == "UPS":
        data.append((6, "UPS CA07"))
        if int(origin_weight * 2.2) > 20 and int(weight * 2.2) < 20:
            weight = 9
            # 转换州二字码
        file_path = "美国州省二字码.xlsx"  # 替换为你的Excel文件路径
        state_name = receipt_data["province"]  # 替换为你要查询的州名英文
        abbreviation = get_state_abbreviation(file_path, state_name)
        if abbreviation == "未找到对应的州名缩写":
            abbreviation = receipt_data["province"]
    data.append((7, box_num))
    data.append((8, weight))
    data.append((9, weight))
    if weight > 12:
        data.append((10, "50"))
    else:
        data.append((10, "40"))
    data.append((11, "30"))
    data.append((12, "30"))
    data.append((13, "cloth"))
    data.append((14, "衣服"))
    data.append((15, box_num))
    data.append((16, "2"))
    data.append((25, receipt_data["d_code"]))
    data.append((26, receipt_data["d_code"]))
    data.append((27, receipt_data["country"]))

    data.append((28, abbreviation))

    data.append((29, receipt_data["city"]))
    lines = receipt_data["cjaddr"].split("\n")[1:]

    data.append((31, "8662038372"))
    data.append((32, receipt_data["zip"][:5]))
    # 私人地址
    # if receipt_data["d_code"].count("-") >= 2 and "TEL:" in receipt_data["cjaddr"]:
    if "COMPANY NAME" in receipt_data["cjaddr"]:
        # 获取前三个元素和包含“TEL:”的元素
        text = "\n".join(lines).strip().strip(",")
        company_pattern = r"RECEIVER & COMPANY NAME:\s*(.+?)(?:\n|$)"
        company_match = re.search(company_pattern, text)
        company_name = company_match.group(1).strip() if company_match else ""
        if company_name:
            data.append((26, company_name))
        # 提取地址到city
        address_pattern = r"ADDRESS:\s*(.+?)(?:\n|$)"
        address_match = re.search(address_pattern, text)
        address = address_match.group(1).strip() if address_match else ""

        # 提取TEL
        tel_pattern = r"TEL:\s*(.+?)(?:\n|$)"
        tel_match = re.search(tel_pattern, text)
        tel = tel_match.group(1).strip() if tel_match else ""
        if tel and not any(c.isdigit() for c in tel):
            tel = ""
        if tel:
            data.append((31, tel))

        extract_result = address.strip().strip(",")
    else:
        extract_result = "\n".join(lines[:-1]).strip().strip(",")

    # if lines:
    #     lines = lines[:-1]
    data.append((30, extract_result))

    return data


def write_data_to_template(
    template_path,
    data,
    output_path,
    all_warehouse_info,
    start_row=6,
):
    """
    将数据写入Excel模板文件，并保存为新的Excel文件。

    :param template_path: 模板文件路径
    :param data: 传入的数据列表，每个元素是一个包含客户单号、仓库代码、箱数和重量的字典
    :param output_path: 输出文件路径
    :param start_row: 数据开始写入的行号（默认为6）
    """
    # 读取模板文件
    wb = openpyxl.load_workbook(template_path)
    ws = wb.active

    current_row = start_row

    # 用于存储需要填充的FedEx和UPS数据
    fill_data_list = []

    # 遍历数据，收集FedEx和UPS的数据
    for record in data:
        deliver_type = record["快递类型"]

        work_num = record["operNo"]
        warehouse_d_code = record["d_code"]

        if record.get("件数（如果完整打可不填）"):
            average_weight = (
                record.get("ckweight")
                or record.get("yjweight") / record.get("GoodsNum")
                or record.get("yjnum")
            )
            box_num = int(record["件数（如果完整打可不填）"])
        else:
            box_num = record.get("GoodsNum") or record.get("yjnum")

        box_num2 = record.get("GoodsNum") or record.get("yjnum")

        volume = record.get("ckcbm") or record.get("yjvolume")
        origin_weight = round(
            (record.get("ckweight") or record.get("yjweight")) / box_num2, 2
        )
        receipt_data = None
        for warehouse in all_warehouse_info["rows"]:
            if warehouse["d_code"] == warehouse_d_code:
                receipt_data = warehouse
                break

        if not receipt_data:
            logger.warning(f"没有找到收货人数据: {warehouse_d_code}")
            continue

        if deliver_type.upper() == "FEDEX":
            weight = round(
                (record.get("ckweight") or record.get("yjweight")) / box_num2 * 0.95, 2
            )
            fill_data_list.append(
                get_fill_data(
                    "FEDEX",
                    weight,
                    box_num,
                    work_num,
                    warehouse_d_code,
                    origin_weight,
                    receipt_data,
                )
            )
            current_row += 1

        elif deliver_type.upper() == "UPS":
            weight = round(
                (record.get("ckweight") or record.get("yjweight")) / box_num2 * 0.75, 2
            )
            fill_data_list.append(
                get_fill_data(
                    "UPS",
                    weight,
                    box_num,
                    work_num,
                    warehouse_d_code,
                    origin_weight,
                    receipt_data,
                )
            )
            current_row += 1

        elif deliver_type.strip() == "快递":
            # 生成FedEx数据
            weight_fedex = round(
                (record.get("ckweight") or record.get("yjweight")) / box_num2 * 0.95, 2
            )
            fill_data_list.append(
                get_fill_data(
                    "FEDEX",
                    weight_fedex,
                    box_num,
                    work_num,
                    warehouse_d_code,
                    origin_weight,
                    receipt_data,
                )
            )
            current_row += 1
            # 生成UPS数据
            weight_ups = round(
                (record.get("ckweight") or record.get("yjweight")) / box_num2 * 0.75, 2
            )
            fill_data_list.append(
                get_fill_data(
                    "UPS",
                    weight_ups,
                    box_num,
                    work_num,
                    warehouse_d_code,
                    origin_weight,
                    receipt_data,
                )
            )
            current_row += 1

    # 填充收集到的数据
    grouped_data = defaultdict(list)
    for item in fill_data_list:
        _, value = item[0]
        grouped_data[value].append(item)
    for _, items in grouped_data.items():
        # 使用字典去重，保证items唯一性
        unique_items = {
            tuple((col, value) for col, value in item): item for item in items
        }
        items = list(unique_items.values())
        if len(items) == 1:
            max_row = ws.max_row + 1
            for col, value in items[0]:
                ws.cell(row=max_row, column=col, value=value)

        elif len(items) == 2:
            # 比价
            first_item = items[0]
            second_item = items[1]
            logger.info(f"{first_item[0][1]}比价如下：")
            first_price = getTransPrice(
                work_num=first_item[0][1],
                logisticsProductCode=first_item[1][1],
                consigneeProvince=first_item[15][1],
                consigneeCity=first_item[16][1],
                consigneeAddress=first_item[17][1],
                consigneePostcode=first_item[19][1],
                packageWeightD=first_item[3][1],
                packageLengthD=first_item[5][1],
                box_num=first_item[2][1],
            )
            second_price = getTransPrice(
                work_num=first_item[0][1],
                logisticsProductCode=second_item[1][1],
                consigneeProvince=second_item[15][1],
                consigneeCity=second_item[16][1],
                consigneeAddress=second_item[17][1],
                consigneePostcode=second_item[19][1],
                packageWeightD=second_item[3][1],
                packageLengthD=second_item[5][1],
                box_num=second_item[2][1],
            )
            if not first_price or not second_price:
                logger.error("比价错误，价格都没有获取到")
                raise
            # 谁的价格低使用谁的
            max_row = ws.max_row + 1
            if first_price < second_price:
                for col, value in first_item:
                    ws.cell(row=max_row, column=col, value=value)

            else:
                for col, value in second_item:
                    ws.cell(row=max_row, column=col, value=value)

    wb.save(output_path)
    logger.info(f"文件已成功生成: {output_path}")


def generate_tempalte_file(excel_content, is_binary=True):
    if is_binary:
        bytes_io = BytesIO(excel_content)
        work_num_df = pd.read_excel(bytes_io)
    else:
        work_num_df = pd.read_excel(excel_content)
    # 从文件中获取数据
    work_num_df = work_num_df.fillna("")

    work_num_df = work_num_df.loc[work_num_df["国家"] != ""]
    a_num = ",".join(work_num_df["工作单号"].to_list())
    node_path = find_playwright_node_path()
    morelink_client = MoreLinkClient(node_path=node_path)
    all_warehouse_info = morelink_client.fba_warehouse_search()

    # dahuo_res = morelink_client.dahuodingdan_worknum_search_httpx(
    #     numberno=a_num, signtype="工作单号"
    # )
    dahuo_res = morelink_client.dahuodingdan_worknum_search_httpx_batch(
        numberno=a_num, signtype="工作单号"
    )
    exists_CourierNumber_worknum = []

    # 过滤出 CourierNumber(快递单号) 不存在的条目，并构建 exists_CourierNumber_worknum 列表
    filtered_dahuo_res = []
    filtered_dahuo_res = [i for i in dahuo_res]
    for i in dahuo_res:
        # filtered_dahuo_res.append(i)
        if i.get("CourierNumber"):
            exists_CourierNumber_worknum.append(i["operNo"])
        else:
            filtered_dahuo_res.append(i)
    if not filtered_dahuo_res:
        return None, exists_CourierNumber_worknum
    df_dahuo = pd.DataFrame(filtered_dahuo_res)
    work_num_df = work_num_df.rename(columns={"工作单号": "operNo"})

    # 合并两个 DataFrame
    df_merged = df_dahuo.merge(
        work_num_df, on="operNo", how="left", suffixes=("", "_excel")
    )
    df_merged = df_merged.fillna("")
    output_path = rf"./file/tools_auto/{work_num_df['operNo'].to_list()[0]}.xlsx"
    all_box_num_template = r"尾程快递-多票批量导入-按总箱数 (1).xlsx"

    write_data_to_template(
        all_box_num_template,
        df_merged.to_dict("records"),
        output_path,
        all_warehouse_info,
    )
    return output_path, exists_CourierNumber_worknum


def save_backfilling_data(huitian_data):
    """将回填数据保存到数据库"""
    if not huitian_data:
        return

    # 将数据保存到mysql中
    from dotenv import load_dotenv
    from feapder.db.mysqldb import MysqlDB

    load_dotenv()
    mysql_db = MysqlDB(
        ip=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT")),
        db="fbatms",
        user_name=os.getenv("MYSQL_USER"),
        user_pass=os.getenv("MYSQL_PASS"),
    )

    # 创建表如果不存在
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS `tracking_number_data` (
        `id` int NOT NULL AUTO_INCREMENT,
        `work_num` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
        `tracking_num` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
        `print_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
        `route_content` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
        `status` int DEFAULT '0',
        `all_received` tinyint(1) DEFAULT '0',
        `create_time` datetime DEFAULT NULL,
        `update_time` datetime DEFAULT NULL,
        `earliest_delivery_time` date DEFAULT NULL COMMENT '最早签收时间',
        `earliest_launch_time` date DEFAULT NULL COMMENT '最早上线时间',
        `eta` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_tracking_number_data_work_num` (`work_num`)
    ) ENGINE=InnoDB AUTO_INCREMENT=19068 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
    """
    mysql_db.execute(create_table_sql)

    # 将数据写入sqlite
    conn = sqlite3.connect("tracking.db")
    cursor = conn.cursor()

    # 创建表(如果不存在)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tracking_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_num TEXT,
            tracking_num TEXT, 
            print_type TEXT,
            status INTEGER DEFAULT 0
        )
    """)

    # 插入数据
    for data in huitian_data:
        # 检查是否已存在
        cursor.execute(
            "SELECT id FROM tracking_tasks WHERE work_num = ?", (data["工作单号"],)
        )
        result = cursor.fetchone()
        if result:
            cursor.execute(
                "UPDATE tracking_tasks SET tracking_num = ?, print_type = ?, status = 0 WHERE work_num = ?",
                (data["快递单号"], data["打单方式"], data["工作单号"]),
            )
        else:
            cursor.execute(
                """
                INSERT INTO tracking_tasks (work_num, tracking_num, print_type, status)
                VALUES (?, ?, ?, 0)
            """,
                (data["工作单号"], data["快递单号"], data["打单方式"]),
            )
    conn.commit()

    mysql_data = [
        {
            "work_num": i["工作单号"],
            "tracking_num": i["快递单号"],
            "print_type": i["打单方式"],
            "status": 0,
            "create_time": datetime.utcnow(),
            "update_time": datetime.utcnow(),
        }
        for i in huitian_data
    ]
    mysql_db.add_batch_smart(
        "tracking_number_data",
        mysql_data,
        update_columns=["tracking_num", "print_type", "status", "update_time"],
    )
    conn.close()
    logger.info(f"已保存 {len(huitian_data)} 条回填数据")


def execute_backfilling():
    """从数据库获取未完成的任务并执行回填"""
    conn = sqlite3.connect("tracking.db")
    cursor = conn.cursor()

    # 确保表存在
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tracking_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_num TEXT,
            tracking_num TEXT, 
            print_type TEXT,
            status INTEGER DEFAULT 0
        )
    """)

    # 获取状态为0的任务
    cursor.execute("SELECT * FROM tracking_tasks WHERE status = 0")
    tasks = cursor.fetchall()
    if not tasks:
        logger.info("没有待回填的任务")
        conn.close()
        return

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            page.goto("https://morelink56.com/Master")
            max_attempts = 5
            attempts = 0
            while attempts < max_attempts:
                login_text = page.query_selector("//div[text()='电商物流']")
                if login_text:
                    logger.info("已登录。")
                    break
                logger.info("没有登录，现在登录")
                page.fill("//input[@id='txt_tid']", "10849")
                page.fill("//input[@id='txt_account']", "RPA")
                page.fill("//input[@id='password']", "Hubs@2302")
                page.wait_for_load_state("networkidle")
                verify_code_buffer = page.locator("//img[@id='valiCode']").screenshot()
                resp = httpx.post(
                    url=setting.OCR_URL_JSON,
                    files={"image": ("verify_picture", verify_code_buffer)},
                )
                page.fill("//input[@id='valiCode1']", resp.json()["result"])
                page.click("//button[@id='login-btn']")
                page.wait_for_load_state("networkidle")

                try:
                    page.wait_for_selector("//div[text()='电商物流']")
                    logger.info("登录成功。")
                    break
                except Exception:
                    attempts += 1
                    logger.info(f"登录失败，尝试重新登录。尝试次数：{attempts}")

            if attempts == max_attempts:
                logger.error("达到最大登录尝试次数，登录失败。")
                return

            # 点击快速查单
            page.locator("//div[@id='kscd_div']").click()
            page.frame_locator("//iframe[@id='layui-layer-iframe1']").locator(
                "//input[@id='_easyui_textbox_input4']"
            ).click()
            page.frame_locator("//iframe[@id='layui-layer-iframe1']").locator(
                "//div[@id='_easyui_combobox_i2_1']"
            ).click()

            for task in tasks:
                task_id = task[0]
                work_num = task[1]
                tracking_num = task[2]
                print_type = task[3]

                page.frame_locator("//iframe[@id='layui-layer-iframe1']").locator(
                    "//input[@id='_easyui_textbox_input3']"
                ).fill(work_num)
                page.frame_locator("//iframe[@id='layui-layer-iframe1']").locator(
                    "//form[@id='quickC']//a"
                ).nth(-1).click()

                page.wait_for_load_state("load")

                try:
                    page.frame_locator(
                        "//iframe[@id='layui-layer-iframe1']"
                    ).frame_locator("//iframe[@id='resIframe']").frame_locator(
                        "//iframe[@id='运单信息']"
                    ).locator(
                        "//td[@id='limit_CourierType']//input[contains(@id,'easyui')]"
                    ).wait_for(timeout=10000)
                except Exception:
                    logger.warning(f"无法找到 {work_num} 的运单信息界面，跳过")
                    continue

                page.frame_locator("//iframe[@id='layui-layer-iframe1']").frame_locator(
                    "//iframe[@id='resIframe']"
                ).frame_locator("//iframe[@id='运单信息']").locator(
                    "//td[@id='limit_CourierType']//input[contains(@id,'easyui')]"
                ).clear()
                page.frame_locator("//iframe[@id='layui-layer-iframe1']").frame_locator(
                    "//iframe[@id='resIframe']"
                ).frame_locator("//iframe[@id='运单信息']").locator(
                    "//td[@id='limit_CourierType']//input[contains(@id,'easyui')]"
                ).click()
                page.keyboard.type(print_type, delay=2)
                time.sleep(1)
                page.keyboard.press("Enter")

                tracking_num_str = (
                    page.frame_locator("//iframe[@id='layui-layer-iframe1']")
                    .frame_locator("//iframe[@id='resIframe']")
                    .frame_locator("//iframe[@id='运单信息']")
                    .locator(
                        "//td[@id='limit_CourierNumber']//em[@class='wordBreakEm']"
                    )
                    .all()
                )
                if tracking_num_str:
                    logger.info(f"{work_num}已经存在快递单号")
                    cursor.execute(
                        "UPDATE tracking_tasks SET status = 1 WHERE id = ?", (task_id,)
                    )
                    conn.commit()
                    continue
                page.frame_locator("//iframe[@id='layui-layer-iframe1']").frame_locator(
                    "//iframe[@id='resIframe']"
                ).frame_locator("//iframe[@id='运单信息']").locator(
                    "//td[@id='limit_CourierNumber']"
                ).click()
                page.keyboard.type(tracking_num)
                page.keyboard.press("Enter")

                time.sleep(1)
                # 点击保存
                page.frame_locator("//iframe[@id='layui-layer-iframe1']").frame_locator(
                    "//iframe[@id='resIframe']"
                ).frame_locator("//iframe[@id='运单信息']").locator(
                    "//a[@id='btnsubmit']"
                ).click()
                try:
                    page.frame_locator(
                        "//iframe[@id='layui-layer-iframe1']"
                    ).frame_locator("//iframe[@id='resIframe']").frame_locator(
                        "//iframe[@id='运单信息']"
                    ).get_by_text("确认保存").click(timeout=3 * 1000)
                    page.wait_for_load_state("domcontentloaded")
                    time.sleep(1)
                except Exception:
                    logger.info("没有出现二次确认保存按钮")

                # 更新任务状态为已完成
                cursor.execute(
                    "UPDATE tracking_tasks SET status = 1 WHERE id = ?", (task_id,)
                )
                conn.commit()
                time.sleep(1)

            context.close()
            browser.close()
    finally:
        conn.close()


def backfilling_tracking_no(huitian_data=None):
    """兼容旧接口，先保存再尝试执行一次"""
    if huitian_data:
        save_backfilling_data(huitian_data)
    execute_backfilling()


def work_num_dadan_main(excel_content, is_binary=True, progress_callback=None):
    """
    工作单号打单主函数

    Args:
        excel_content: Excel文件内容（二进制或路径）
        is_binary: 是否为二进制内容
        progress_callback: 进度回调函数，接收 (step_name: str, status: str, message: str) 参数

    Returns:
        tuple: (exists_CourierNumber_worknum, root_path, new_file_path, huitian_data)
    """
    exists_CourierNumber_worknum = []
    huitian_data = None
    root_path = None
    new_file_path = None
    ups_royal = None

    def notify_progress(step_name, status, message=""):
        """通知进度更新"""
        if progress_callback:
            try:
                progress_callback(step_name, status, message)
            except Exception as e:
                logger.warning(f"进度回调失败: {e}")

    try:
        todos = {
            "生成模板文件": "",
            "上传模板文件": "",
            "预报": "",
            "打印": "",
            "回填": "",
        }

        # 生成模板文件
        notify_progress("生成模板文件", "running", "正在生成模板文件...")
        file_path, exists_CourierNumber_worknum = generate_tempalte_file(
            excel_content, is_binary
        )
        if not file_path:
            logger.info("没有生成模板文件")
            todos["生成模板文件"] = "失败"
            notify_progress("生成模板文件", "failed", "没有生成模板文件")
            logger.info(f"[TODO] 状态: {todos}")
            return exists_CourierNumber_worknum, None, None, None
        todos["生成模板文件"] = "完成"
        notify_progress("生成模板文件", "completed", "模板文件生成完成")

        # 登录
        notify_progress("登录", "running", "正在登录打单系统...")
        ups_royal = UPSRoyalDadan()
        ups_royal.launch_browser()
        ups_royal.login()
        notify_progress("登录", "completed", "登录成功")

        # 上传
        notify_progress("上传模板文件", "running", "正在上传模板文件...")
        upload_file_path = file_path
        upload_result = ups_royal.upload_file(upload_file_path)
        if upload_result["code"] == 500:
            msg = upload_result["msg"]
            if "客户单号/入库单号已使用过" in msg:
                todos["上传模板文件"] = "完成(已经上传过一次，pass)"
                notify_progress("上传模板文件", "completed", "文件已存在，跳过上传")

                logger.info("开始尝试预报，处理那些没有预报的内容")
                notify_progress("预报", "running", "正在进行预报...")
                forecast_list = ups_royal.forecast(upload_file_path)
                todos["预报"] = "完成(已经上传过一次，pass)"
                notify_progress("预报", "completed", "预报完成")

                logger.info("已经上传过一次，获取打印数据")
                notify_progress("打印", "running", "正在打印面单...")

                # 获取已预报的客户单号
                if forecast_list:
                    customerOrderNos = ",".join(
                        [item["customerOrderNo"] for item in forecast_list]
                    )
                else:
                    # 如果预报没有返回，从上传文件中读取客户单号
                    upload_file_pd = pd.read_excel(upload_file_path, skiprows=4)
                    customerOrderNos = ",".join(
                        upload_file_pd["客户单号/入库单号"].tolist()
                    )

                # 执行打印，获取打印数据
                huitian_data, root_path = (
                    ups_royal.print_already_yubao_get_tracking_num(
                        customerOrderNos=customerOrderNos
                    )
                )
                todos["打印"] = "完成"
                notify_progress("打印", "completed", "打印完成")

                todos["回填"] = "完成(由后台定时任务处理)"
                logger.info(f"[TODO] 状态: {todos}")
                # 打印完成后立即返回，回填数据作为返回值，由后台定时任务处理回填
                return (
                    exists_CourierNumber_worknum,
                    root_path,
                    new_file_path,
                    huitian_data,
                )

            logger.error(f"上传的文件有问题: {msg}")
            share.error_messages += f"\n上传的文件有问题:{msg}"
            todos["上传模板文件"] = f"失败({msg})"
            notify_progress("上传模板文件", "failed", f"上传失败: {msg}")
            logger.info(f"[TODO] 状态: {todos}")
            return None, upload_file_path, None, None

        todos["上传模板文件"] = "完成"
        notify_progress("上传模板文件", "completed", "上传完成")

        # 预报
        logger.info("开始预报")
        notify_progress("预报", "running", "正在进行预报...")
        forecast_list = ups_royal.forecast(upload_file_path)
        if not forecast_list:
            logger.warning("没有预报数据")
            todos["预报"] = "失败"
            notify_progress("预报", "failed", "没有预报数据")
            logger.info(f"[TODO] 状态: {todos}")
            return None, None, None, None
        todos["预报"] = "完成"
        notify_progress("预报", "completed", "预报完成")

        customerOrderNos = ",".join([item["customerOrderNo"] for item in forecast_list])

        # 打印
        logger.info("开始打印")
        notify_progress("打印", "running", "正在打印面单...")
        huitian_data, root_path = ups_royal.print_already_yubao_get_tracking_num(
            customerOrderNos=customerOrderNos
        )
        todos["打印"] = "完成"
        notify_progress("打印", "completed", "打印完成")

        # 返回处理后的excel
        logger.info("开始generate excel 文件")
        notify_progress("生成Excel", "running", "正在生成Excel文件...")
        # 读取 Excel 文件
        df = pd.read_excel(file_path, skiprows=5)

        # 获取客户订单号列表
        client_num_list = df.iloc[:, 0].to_list()
        # 定义新的列名
        columns = [
            "单号（S号）",
            "Seller ID(FBA)",
            "箱数",
            "重量（KG）",
            "折后重量",
            "仓库",
            "邮编",
            "打单备注信息（REF)",
            "UPS单号",
            "价格（USD)",
        ]

        # 获取预报数据
        data = ups_royal.get_yubao_data(client_num_list)
        new_data = []

        for row_data in data["data"]["list"]:
            worknum_s = row_data["customerOrderNo"].split("-")[0]
            seller_id = ""
            box_num = row_data["packageQty"]
            weight = (
                row_data["forecastWeightD"] / 0.75
                if "UPS" in row_data["logisticsProductCode"]
                else row_data["forecastWeightD"] / 0.95
            )
            weight_discount = row_data["forecastWeightD"] / box_num
            warehouse = "-".join(row_data["customerOrderNo"].split("-")[1:-1])
            postcode = ""
            ref = row_data["customerOrderNo"]
            upd_number = row_data["deliveryNo"]
            price = sum([i["sum"] for i in row_data["sumList"]])

            # 将新数据添加到列表中
            new_data.append(
                [
                    worknum_s,
                    seller_id,
                    box_num,
                    weight,
                    weight_discount,
                    warehouse,
                    postcode,
                    ref,
                    upd_number,
                    price,
                ]
            )

        # 创建 DataFrame
        new_df = pd.DataFrame(new_data, columns=columns)
        new_file_path = f"file/tools_auto/template_整柜{time.time()}.xlsx"
        # 保存为新的 Excel 文件
        new_df.to_excel(new_file_path, index=False)
        notify_progress("生成Excel", "completed", "Excel文件生成完成")

    except Exception as e:
        logger.warning(traceback.format_exc())
        notify_progress("错误", "failed", f"执行失败: {str(e)}")
        raise

    finally:
        if ups_royal:
            ups_royal.close()

    logger.info(f"[TODO] 状态: {todos}")
    # 打印完成后立即返回结果，回填数据作为返回值的一部分
    # 回填将在后台异步执行
    return exists_CourierNumber_worknum, root_path, new_file_path, huitian_data


def test(
    excel_content=r"C:\Users\a1337\Desktop\morelink_worknum_dadan模板.xlsx-1736148393.1750574.xlsx",
    is_binary=False,
):
    #     #生成模板文件
    file_path, exists_CourierNumber_worknum = generate_tempalte_file(
        excel_content, is_binary
    )


def print_huitian(excel_content, ups_royal=None):
    try:
        # 打印
        logger.info("开始打印")
        if ups_royal is None:
            ups_royal = UPSRoyalDadan()
            ups_royal.launch_browser()
            ups_royal.login()
        customerOrderNos = pd.read_excel(excel_content, skiprows=4)
        huitian_data, root_path = ups_royal.print_already_yubao_get_tracking_num(
            customerOrderNos=",".join(customerOrderNos["客户单号/入库单号"].to_list())
        )
        logger.info("获取到打印数据")
        # 返回处理后的excel
        logger.info("开始generate excel 文件")
        # 读取 Excel 文件
        df = pd.read_excel(excel_content, skiprows=5)

        # 获取客户订单号列表
        client_num_list = df.iloc[:, 0].to_list()
        # 定义新的列名
        columns = [
            "单号（S号）",
            "Seller ID(FBA)",
            "箱数",
            "重量（KG）",
            "折后重量",
            "仓库",
            "邮编",
            "打单备注信息（REF)",
            "UPS单号",
            "价格（USD)",
        ]

        # 获取预报数据
        data = ups_royal.get_yubao_data(client_num_list)
        new_data = []

        for row_data in data["data"]["list"]:
            worknum_s = row_data["customerOrderNo"].split("-")[0]
            seller_id = ""
            box_num = row_data["packageQty"]
            weight = (
                row_data["forecastWeightD"] / 0.75
                if "UPS" in row_data["logisticsProductCode"]
                else row_data["forecastWeightD"] / 0.95
            )
            weight_discount = row_data["forecastWeightD"] / box_num
            warehouse = "-".join(row_data["customerOrderNo"].split("-")[1:-1])
            postcode = ""
            ref = row_data["customerOrderNo"]
            upd_number = row_data["deliveryNo"]
            price = sum([i["sum"] for i in row_data["sumList"]])

            # 将新数据添加到列表中
            new_data.append(
                [
                    worknum_s,
                    seller_id,
                    box_num,
                    weight,
                    weight_discount,
                    warehouse,
                    postcode,
                    ref,
                    upd_number,
                    price,
                ]
            )

        # 创建 DataFrame
        new_df = pd.DataFrame(new_data, columns=columns)
        new_file_path = f"file/tools_auto/template_整柜{time.time()}.xlsx"
        # 保存为新的 Excel 文件
        new_df.to_excel(new_file_path, index=False)
    finally:
        # #登陆
        ups_royal.close()

        # huitian_data = [
        #     {"工作单号": "A240801995", "快递单号": "123456", "打单方式": "UPS"},
        #     {"工作单号": "A240801994", "快递单号": "123456", "打单方式": "UPS"},
        # ]
        # 回填morelink
    logger.info("开始回填")

    if huitian_data:
        try:
            backfilling_tracking_no(huitian_data)
        except Exception as e:
            share.error_messages += "\n网络延迟,回填失败,可以检查后重新发送邮件"
            logger.error(e)
    return root_path, new_file_path


if __name__ == "__main__":
    import sys
    import asyncio

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    work_num_dadan_main(
        excel_content=r"C:\Users\a1337\Desktop\morelink_worknum_dadan.xlsx-1766128806.7280135.xlsx",
        is_binary=False,
    )
    # ups_royal = UPSRoyalDadan()
    # ups_royal.launch_browser()
    # ups_royal.login()
    # # print(ups_royal.get_other_tracking_num("S250303299-OKC2-M-16X"))
    # ups_royal.print_already_yubao_get_tracking_num("S250303298-OAK3-W-50X-2")
    # print_huitian(
    #     excel_content=r"C:\Users\a1337\Desktop\A250503899.xlsx"
    # )
    # backfilling_tracking_no()
    # test(
    #     excel_content=r"C:\Users\a1337\Desktop\morelink_worknum_dadan模板.xlsx-1736411705.7249537.xlsx",
    #     is_binary=False,
    # )
