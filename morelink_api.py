import os
import pickle
import time
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import execjs._runner_sources as _runner_sources

import httpx
import urllib
import execjs
from pathlib import Path
import sys
import site
from loguru import logger
import setting
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional


class MoreLinkClient:
    def __init__(
        self,
        node_path="",
        use_playwright_node=True,
        session_file="./session/session.pkl",
    ) -> None:
        self.morelink_session_file = session_file
        self.node_path = node_path
        local_client = self.check_and_use_local_session()
        # local_client = None
        if local_client:
            self.httpx_client = local_client
        else:
            self.httpx_client = self.login_morelink_get_httpx_client()

    def check_and_use_local_session(self):
        client = self.load_session()
        try:
            if client:
                resp = client.post(
                    "https://morelink56.com/Common/IDATA?type=fun&proc=bll.RBTool.QueryFreightPolicyList",
                    timeout=10,
                )
                if resp.status_code == 200 and resp.json()["success"]:
                    logger.info("本地client有效")
                    return client
        except Exception as e:
            return None
        return None

    def save_session(self, httpx_client: httpx.Client):
        cookies = httpx_client.cookies.jar
        serialized_cookies = {cookie.name: cookie.value for cookie in cookies}
        session_data = {
            "cookies": serialized_cookies,
            "headers": dict(httpx_client.headers),
        }
        with open(self.morelink_session_file, "wb") as f:
            pickle.dump(session_data, f)

    def load_session(self):
        if os.path.exists(self.morelink_session_file):
            with open(self.morelink_session_file, "rb") as f:
                session_data = pickle.load(f)
                self.httpx_client = httpx.Client()
                for name, value in session_data["cookies"].items():
                    self.httpx_client.cookies.set(name, value)
                self.httpx_client.headers.update(session_data["headers"])
                self.session_active = True
                return self.httpx_client
        return None

    def login_morelink_get_httpx_client(self, retry_counts: int = 5):
        """获取 morelink httpx client

        Args:
            retry_counts (int, optional): _description_. Defaults to 5.

        Returns:
            _type_: _description_
        """

        login_headers = {
            "Host": "morelink56.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            # "Content-Length": "384",
            "Origin": "https://morelink56.com",
            "Connection": "keep-alive",
            "Referer": "https://morelink56.com/",
            # "Cookie": "ASP.NET_SessionId=trkuuck0ne5zqbubyhih1a4k; ARRAffinity=738806bc03a2c99a973be9d4cbb480ac0731487dd85a43b0e0f9897591611422; language=zh",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "TE": "trailers",
        }
        client = httpx.Client(headers=login_headers)
        res = client.post("https://morelink56.com/Login/index")
        if res.status_code == 200:
            data = res.json()
            print(data)

        # 假设 decrpt.js 文件在当前目录下
        decrpt_js_path = "decret.js"

        # 使用 execjs 加载 JavaScript 文件
        with open(decrpt_js_path, "r", encoding="utf-8") as f:
            js_code = f.read()
        if self.node_path:
            local_node_runtime = execjs.ExternalRuntime(
                name="Node.js (V8) local",
                command="",
                encoding="UTF-8",
                runner_source=_runner_sources.Node,
            )
            # 这里是重点，需要强制性修改
            local_node_runtime._binary_cache = [self.node_path]
            local_node_runtime._available = True
            # 将刚创建好的 JavaScript 运行时 注册至 PyExecJS 中
            execjs.register("local_node", local_node_runtime)

            context = execjs.get("local_node").compile(js_code)

        else:
            # 使用 execjs 编译并执行 JavaScript 代码
            context = execjs.compile(js_code)
        for count in range(1, retry_counts + 1):
            logger.info(f"开始第{count}次尝试登陆")
            # 调用 JavaScript 文件中的函数
            encrypted_password = context.call(
                "passrod_encrypt", data["m1"], data["m2"], "Hubs@2302"
            )

            verify_code_url = (
                f"https://morelink56.com/Login/GetValidateCode?time={time.time()*1000}"
            )

            verficy_code_buffer = client.get(verify_code_url).content
            if verficy_code_buffer:
                verify_resp = httpx.post(
                    url=setting.OCR_URL,
                    files={"image": ("verify_picture", verficy_code_buffer)},
                )

            login_payload = {
                "type": "",
                "txt_account": "RPA",
                "valiCode": verify_resp.text.strip(),
                "password": encrypted_password,
                "tid": "10849",
                "keyString": data["keyString"],
                "browser": "Chrome",
                "OS": "Windows",
            }

            login_encoded_data = urllib.parse.urlencode(login_payload)
            login_resp = client.post(
                "https://morelink56.com/Login/VerificationLogin",
                content=login_encoded_data,
            )
            login_resp_data = login_resp.json()
            if login_resp_data["success"] == False:
                logger.error(f"{count}次登陆失败")
                continue
            break

        if login_resp_data["success"] == True:
            logger.info(f"尝试{count}次登陆成功")

            self.save_session(client)
            return client
        else:
            logger.error("登陆失败")
            return None

    def dahuodingdan_all_data(self):
        """获取最近1个月的大货订单的数据

        Returns:
            list: _description_
        """
        url = "https://morelink56.com/Common/IDATA?type=dsjson&proc=Web_Select_BigWaybill_Back2_10849"
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=1)
        end_date_str = end_date.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")
        payload = {
            "otype": "",
            "timetype": "createtime",
            "starttime": start_date_str,
            "endtime": end_date_str,
            "Supplier_DS": "",
            "Consignor": "",
            "orderStatus": "",
        }

        encoded_data = urllib.parse.urlencode(payload)
        client = self.httpx_client
        response = client.post(url=url, data=encoded_data)

        if response.status_code == 200:
            try:
                json_data = response.json()[0]["tb"]
                return json_data
            except (IndexError, KeyError) as e:
                print(f"Error parsing JSON data: {e}")
                return None
        else:
            print(f"HTTP error: {response.status_code}")
            return None

    def dahuodingdan_worknum_search_httpx(self, numberno: str, signtype: str):
        """Query data of big waybill using the provided numberno and signtype.

        Args:
            numberno (str): The order number.
            signtype (str): The query type: 提单号，工作单号，多单号
            client (httpx.Client): HTTPX client instance.

        Returns:
            dict: JSON data if successful, None otherwise.
        """
        url = "https://morelink56.com/Common/IDATA?type=dsjson&proc=Web_Select_BigWaybill_Back2_10849"

        end_date = datetime.now()
        start_date = end_date - relativedelta(years=1)
        end_date_str = end_date.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")

        payload = {
            "otype": "",
            "timetype": "createtime",
            "starttime": start_date_str,
            "endtime": end_date_str,
            "Supplier_DS": "",
            "Consignor": "",
            "numberNo": numberno.strip(","),
            "more": "1",
            "signtype": signtype,
            "orderStatus": "",
        }

        encoded_data = urllib.parse.urlencode(payload)
        client = self.httpx_client
        response = client.post(url=url, data=encoded_data, timeout=120)

        if response.status_code == 200:
            try:
                json_data = response.json()[0]["tb"]
                return json_data
            except (IndexError, KeyError) as e:
                print(f"Error parsing JSON data: {e}")
                return None
        else:
            print(f"HTTP error: {response.status_code}")
            return None

    def dahuodingdan_worknum_search_httpx_batch(
        self,
        numberno: str,
        signtype: str,
        batch_size: int = 10,
        max_workers: int = 3,
        retry_times: int = 2,
    ) -> Optional[List[Dict[str, Any]]]:
        """批处理查询大订单数据，优化大量订单号的查询性能

        将大量订单号分批处理，避免单次请求超时问题

        Args:
            numberno (str): 订单号字符串，支持逗号分隔
            signtype (str): 查询类型：提单号，工作单号，多单号
            batch_size (int): 每批处理的订单号数量，默认10个
            max_workers (int): 最大并发线程数，默认3个
            retry_times (int): 失败重试次数，默认2次

        Returns:
            List[Dict[str, Any]]: 合并后的所有批次查询结果，失败返回None
        """
        # 解析订单号
        numbers = [num.strip() for num in numberno.strip().split(",") if num.strip()]

        if not numbers:
            logger.warning("没有有效的订单号")
            return None

        # 如果订单号数量较少，直接使用原方法
        if len(numbers) <= batch_size:
            logger.info(f"订单号数量较少({len(numbers)}个)，使用单次查询")
            return self._execute_single_batch(numbers, signtype)

        # 分批处理
        batches = self._create_batches(numbers, batch_size)
        logger.info(
            f"总共{len(numbers)}个订单号，分为{len(batches)}批处理，每批最多{batch_size}个"
        )

        all_results = []
        failed_batches = []

        # 使用线程池并发处理批次
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有批次任务
            future_to_batch = {
                executor.submit(
                    self._execute_single_batch_with_retry, batch, signtype, retry_times
                ): batch
                for batch in batches
            }

            # 收集结果
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    result = future.result()
                    if result:
                        all_results.extend(result)
                        logger.info(f"批次处理成功，获得{len(result)}条记录")
                    else:
                        failed_batches.append(batch)
                        logger.warning(f"批次处理失败: {batch}")
                except Exception as e:
                    failed_batches.append(batch)
                    logger.error(f"批次处理异常: {batch}, 错误: {e}")

        # 如果有失败的批次，尝试使用更小的批次大小重试
        if failed_batches:
            logger.info(f"有{len(failed_batches)}个批次失败，尝试减小批次大小重试")
            smaller_batch_size = max(3, batch_size // 2)
            failed_batches = self._retry_failed_batches(
                failed_batches, signtype, smaller_batch_size, retry_times
            )

        logger.info(f"批处理完成，总共获得{len(all_results)}条记录")
        return all_results if all_results else None

    def _create_batches(self, numbers: List[str], batch_size: int) -> List[List[str]]:
        """将订单号列表分批"""
        batches = []
        for i in range(0, len(numbers), batch_size):
            batch = numbers[i : i + batch_size]
            batches.append(batch)
        return batches

    def _execute_single_batch_with_retry(
        self, batch: List[str], signtype: str, retry_times: int
    ) -> Optional[List[Dict[str, Any]]]:
        """执行单批次查询，支持重试"""
        numberno_str = ",".join(batch)

        for attempt in range(retry_times + 1):
            try:
                result = self._execute_single_batch(batch, signtype)
                if result:
                    return result
                else:
                    logger.warning(f"批次查询第{attempt + 1}次尝试失败")
            except Exception as e:
                logger.error(f"批次查询第{attempt + 1}次尝试异常: {e}")

            if attempt < retry_times:
                time.sleep(1)  # 重试前等待1秒

        return None

    def _execute_single_batch(
        self, batch: List[str], signtype: str
    ) -> Optional[List[Dict[str, Any]]]:
        """执行单批次查询的内部方法"""
        url = "https://morelink56.com/Common/IDATA?type=dsjson&proc=Web_Select_BigWaybill_Back2_10849"

        end_date = datetime.now()
        start_date = end_date - relativedelta(years=1)
        end_date_str = end_date.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")

        payload = {
            "otype": "",
            "timetype": "createtime",
            "starttime": start_date_str,
            "endtime": end_date_str,
            "Supplier_DS": "",
            "Consignor": "",
            "numberNo": ",".join(batch),
            "more": "1",
            "signtype": signtype,
            "orderStatus": "",
        }

        encoded_data = urllib.parse.urlencode(payload)
        client = self.httpx_client

        try:
            response = client.post(url=url, data=encoded_data, timeout=60)

            if response.status_code == 200:
                try:
                    json_data = response.json()[0]["tb"]
                    return json_data if isinstance(json_data, list) else [json_data]
                except (IndexError, KeyError) as e:
                    logger.error(f"解析JSON数据错误: {e}")
                    return None
            else:
                logger.error(f"HTTP错误: {response.status_code}")
                return None
        except httpx.TimeoutException:
            logger.error(f"批次查询超时: {batch}")
            raise
        except Exception as e:
            logger.error(f"批次查询异常: {e}")
            raise

    def _retry_failed_batches(
        self,
        failed_batches: List[List[str]],
        signtype: str,
        smaller_batch_size: int,
        retry_times: int,
    ) -> List[List[str]]:
        """重新处理失败的批次"""
        still_failed = []

        for failed_batch in failed_batches:
            # 将失败的批次进一步拆分
            sub_batches = self._create_batches(failed_batch, smaller_batch_size)

            for sub_batch in sub_batches:
                result = self._execute_single_batch_with_retry(
                    sub_batch, signtype, retry_times
                )
                if not result:
                    still_failed.append(sub_batch)

        return still_failed

    def zongdan_api_httpx(self):
        """Retrieve data of zongdan for the last year.

        Args:
            client (httpx.Client): HTTPX client instance.

        Returns:
            list: List of JSON data if successful, None otherwise.
        """
        zongdan_list_url = "https://morelink56.com/Common/IDATA?type=list&proc=Web_Select_zginfo_cp_10849"

        end_date = datetime.now()
        start_date = end_date - relativedelta(years=1)
        end_date_str = end_date.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")

        zongdan_payload = {
            "orderStatus": "",
            "loadingtype": "",
            "time_type": "ydtime1",
            "startdate": start_date_str,
            "enddate": end_date_str,
            "isrepair": "1",
            "filterRules": [{"field": "billno", "op": "contains", "value": ""}],
        }

        encoded_payload = urllib.parse.urlencode(zongdan_payload)
        client = self.httpx_client

        response = client.post(url=zongdan_list_url, data=encoded_payload, timeout=60)

        if response.status_code == 200:
            try:
                zongdan_json_data = response.json()["rows"]
                return zongdan_json_data
            except Exception as e:
                print(f"Error accessing API: {e}")
                return None
        else:
            print(f"HTTP error: {response.status_code}")
            return None

    def cooperate_client_search(self):
        """获取所有电商客户信息

        Returns:
            _type_: _description_
        """
        cooperate_client_url = "https://morelink56.com/Common/IDATA?type=list&proc=web_select_cooperativecustomer2"

        client = self.httpx_client
        cooperate_client_res = client.post(url=cooperate_client_url)
        if cooperate_client_res.status_code == 200:
            return cooperate_client_res.json()

    def fba_warehouse_search(self):
        """获取所有fba仓库的信息

        Returns:
            _type_: _description_
        """
        fba_warehoust_url = "https://morelink56.com/Common/IDATA?type=list&proc=Web_Select_WarehouseInfo"
        client = self.httpx_client

        fba_warehouse_res = client.post(url=fba_warehoust_url)
        if fba_warehouse_res.status_code == 200:
            return fba_warehouse_res.json()

    @staticmethod
    def zongdan_filter_json_data(data_list, orderno=None, billno=None):
        """Filter data_list based on orderno or billno.

        Args:
            data_list (list): List of zongdan data.
            orderno (str, optional): GQ order number. Defaults to None.
            billno (str, optional): Bill of lading number. Defaults to None.

        Returns:
            list: Filtered data based on the provided parameters.
        """
        return [
            data
            for data in data_list
            if (orderno is None or data.get("orderno") == orderno)
            and (billno is None or data.get("billno") == billno)
        ]
