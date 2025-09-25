
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
class MoreLinkClient:
    def __init__(self,node_path="",use_playwright_node = True,session_file="./session/session.pkl") -> None:
        self.morelink_session_file = session_file
        self.node_path = node_path
        local_client = self.check_and_use_local_session()
        if local_client:
            self.httpx_client = local_client 
        else:
            self.httpx_client = self.login_morelink_get_httpx_client()
    

    def check_and_use_local_session(self):
        client = self.load_session()
        if client:
            resp = client.post("https://morelink56.com/Common/IDATA?type=fun&proc=bll.RBTool.QueryFreightPolicyList")
            if resp.status_code == 200 and resp.json()["success"]:
                logger.info("本地client有效")
                return client
        return None
    def save_session(self, httpx_client: httpx.Client):
        cookies = httpx_client.cookies.jar
        serialized_cookies = {cookie.name: cookie.value for cookie in cookies}
        session_data = {
            'cookies': serialized_cookies,
            'headers': dict(httpx_client.headers)
        }
        with open(self.morelink_session_file, 'wb') as f:
            pickle.dump(session_data, f)

    def load_session(self):
        if os.path.exists(self.morelink_session_file):
            with open(self.morelink_session_file, 'rb') as f:
                session_data = pickle.load(f)
                self.httpx_client = httpx.Client()
                for name, value in session_data['cookies'].items():
                    self.httpx_client.cookies.set(name, value)
                self.httpx_client.headers.update(session_data['headers'])
                self.session_active = True
                return self.httpx_client
        return None
    def login_morelink_get_httpx_client(self,retry_counts:int=5):
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
                command='',
                encoding='UTF-8',
                runner_source=_runner_sources.Node
            )
            # 这里是重点，需要强制性修改
            local_node_runtime._binary_cache = [self.node_path]
            local_node_runtime._available = True
            # 将刚创建好的 JavaScript 运行时 注册至 PyExecJS 中
            execjs.register('local_node', local_node_runtime)

            context =execjs.get('local_node').compile(js_code)

        else:
            # 使用 execjs 编译并执行 JavaScript 代码
            context = execjs.compile(js_code)
        for count in range(1,retry_counts+1):
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
                "valiCode": verify_resp.text.strip() ,
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

        if login_resp_data['success'] == True:
            logger.info(f"尝试{count}次登陆成功")

            self.save_session(client)
            return client
        else:
            logger.error("登陆失败")
            return None
    def dahuodingdan_all_data(self,start_date:str=None,end_date:str=None):
        """获取最近1个月的大货订单的数据

        Returns:
            list: _description_
        """
        url  = "https://morelink56.com/Common/IDATA?type=dsjson&proc=Web_Select_BigWaybill_Back2_10849"
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - relativedelta(months=2)
            
        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime("%Y-%m-%d")
            
        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime("%Y-%m-%d")
        payload = {
            "otype": "",
            "timetype": "createtime",
            "starttime": start_date_str,
            "endtime": end_date_str,
            "Supplier_DS": "",
            "Consignor": "",
            "orderStatus": ""
        }

        encoded_data = urllib.parse.urlencode(payload)
        client = self.httpx_client
        response = client.post(url=url, data=encoded_data)

        if response.status_code == 200:
            try:

                json_data = response.json()[0]["tb"]
                return json_data
            except (IndexError, KeyError) as e:
                print(f"Error parsing JSON data: {e}-{response.json()}")
                return None
        else:
            print(f"HTTP error: {response.status_code}")
            return None  
    def dahuodingdan_worknum_search_httpx(self, numberno: str, signtype: str,start_date:str=None,end_date:str=None):
        """Query data of big waybill using the provided numberno and signtype.

        Args:
            numberno (str): The order number.
            signtype (str): The query type: 提单号，工作单号，多单号
            client (httpx.Client): HTTPX client instance.

        Returns:
            dict: JSON data if successful, None otherwise.
        """
        url = "https://morelink56.com/Common/IDATA?type=dsjson&proc=Web_Select_BigWaybill_Back2_10849"

        if start_date is None:
            start_date = datetime.now() - relativedelta(months=1)
            start_date_str = start_date.strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now()
            end_date_str = end_date.strftime("%Y-%m-%d")

        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime("%Y-%m-%d")

        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime("%Y-%m-%d")
        

        payload = {
            "otype": "",
            "timetype": "createtime",
            "starttime": start_date_str,
            "endtime": end_date_str,
            "Supplier_DS": "",
            "Consignor": "",
            "numberNo": numberno,
            "more": "1",
            "signtype": signtype,
            "orderStatus": ""
        }

        encoded_data = urllib.parse.urlencode(payload)
        client = self.httpx_client
        response = client.post(url=url, data=encoded_data)

        if response.status_code == 200:
            try:
                json_data = response.json()[0]["tb"]
                return json_data
            except (IndexError, KeyError) as e:
                print(f"Error parsing JSON data: {e}-{response.json()}")
                return None
        else:
            print(f"HTTP error: {response.status_code}")
            return None   
    
    def zongdan_api_httpx(self,start_date:str=None,end_date:str=None):
        """Retrieve data of zongdan for the last year.

        Args:
            client (httpx.Client): HTTPX client instance.

        Returns:
            list: List of JSON data if successful, None otherwise.
        """
        zongdan_list_url = "https://morelink56.com/Common/IDATA?type=list&proc=Web_Select_zginfo_cp_10849"
        if end_date is None:
            end_date = datetime.now()
            end_date_str = end_date.strftime("%Y-%m-%d")
        else:
            end_date_str = end_date
            
        if start_date is None:
            start_date = datetime.now() - relativedelta(years=1)
            start_date_str = start_date.strftime("%Y-%m-%d")
        else:
            start_date_str = start_date
        zongdan_payload = {
            "orderStatus": "",
            "loadingtype": "",
            "time_type": "ydtime1",
            "startdate": start_date_str,
            "enddate": end_date_str,
            "isrepair": "1",
            "filterRules": [{"field":"billno","op":"contains","value":""}]
        }

        encoded_payload = urllib.parse.urlencode(zongdan_payload)
        client = self.httpx_client

        response = client.post(url=zongdan_list_url, data=encoded_payload)

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
        fba_warehoust_url = (
        "https://morelink56.com/Common/IDATA?type=list&proc=Web_Select_WarehouseInfo"
        )
        client = self.httpx_client

        fba_warehouse_res = client.post(url=fba_warehoust_url)
        if fba_warehouse_res.status_code == 200:
            return fba_warehouse_res.json()["rows"]
    def search_warehouse_size(self, sono: str, cid: str):
        search_url = (
            "https://morelink56.com/Common/IDATA?type=fun&proc=bll.goodssize.cargo_size"
        )
        search_payload = {"sono": sono, "cid": cid}

        # 将字典转换为查询字符串
        search_payload_encoded = urllib.parse.urlencode(search_payload)

        # 构建完整的 URL
        full_search_url = f"{search_url}&{search_payload_encoded}"

        # 使用完整的 URL 作为 url 参数
        res = self.httpx_client.post(
            url=full_search_url, content=search_payload_encoded
        )
        if res.status_code == 200 and res.json()["success"]:
            return res.json()
    
    def get_dahuo_data_by_id(self,worknum:str):
        """根据工作单号获取大货订单数据

        Args:
            worknum (str): 工作单号

        Returns:
            _type_: _description_
        """

        url = "https://morelink56.com/Common/IDATA?type=dsjson&proc=Web_Select_BigWaybill_ByID"

        payload = {
            'operNo':worknum
        }

        encoded_payload = urllib.parse.urlencode(payload)
        client = self.httpx_client
        response = client.post(url=url, data=encoded_payload)
        if response.status_code == 200:
            return response.json()[0]["tb"][0]
        else:
            return None
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
            data for data in data_list
            if (orderno is None or data.get('orderno') == orderno)
            and (billno is None or data.get('billno') == billno)
        ]
