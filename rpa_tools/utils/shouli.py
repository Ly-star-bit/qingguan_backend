from datetime import datetime, timedelta
import json
from urllib.parse import urlencode

from loguru import logger

from .morelink_api import MoreLinkClient


def zongdan_can_zhuanggui_data(morelink_client: MoreLinkClient):
    """查询可以装柜的数据"""
    url = "https://morelink56.com/Common/IDATA?type=dsjson&proc=web_select_zginfo_bigwaybil"
    # 获取当前时间
    now = datetime.now()

    # 设置 starttime 为一个月前的当前时间
    starttime = (now - timedelta(days=30)).strftime("%Y-%m-%d")

    # 设置 endtime 为当前时间
    endtime = now.strftime("%Y-%m-%d")
    payload = {
        "tb_sono": [{"sono": ""}],
        "starttime": starttime,
        "endtime": endtime,
        "EntrustType": "",
    }
    encoded_data = urlencode(payload)

    res = morelink_client.httpx_client.post(url=url, content=encoded_data)
    if res.status_code == 200:
        return res.json()[0]["tb"]


def zongdan_zhuanggui(
    morelink_client: MoreLinkClient, voyageid, gq_num, tb_zg_depot_order
):
    """装柜"""

    url = (
        "https://morelink56.com/Common/IDATA?type=fun&proc=bll.productcenter._saveorder"
    )
    payload = {
        "voyageid": voyageid,
        "orderno": gq_num,
        "tb_zg_depot_order": tb_zg_depot_order,
        "tb_zg_depot_order_del": [{"sono": "", "operNo": "", "sort": 0}],
    }
    encoded_data = urlencode(payload)

    res = morelink_client.httpx_client.post(url=url, content=encoded_data)

    if res.status_code == 200 and res.json()["success"]:
        logger.info(f"装柜成功：{payload['orderno']}")
        return True


def zongdan_shouli(morelink_client: MoreLinkClient, id, sono, operno):
    url = "https://morelink56.com/Common/IDATA?type=fun&proc=bll.productcenter.save_bigwaybill"
    payload = {
        "types": "0",
        "tb_sono_type": [
            {
                "ID": id,
                "sono": sono,
                "tid": "10849",
                "voyageid": "null",
                "optype": 2,
            }
        ],
        "tb_operNo": [{"operNo": operno}],
        "sail_id": "",
        "tb_BigWaybill_LimitEdit_new": [
            {"operno": operno, "key": "goodinfo", "value": "1", "type": "area"},
            {"operno": operno, "key": "channelid", "value": "1", "type": "filed"},
            {
                "operno": operno,
                "key": "customsdeclaration",
                "value": "1",
                "type": "filed",
            },
            {"operno": operno, "key": "shaddr", "value": "1", "type": "filed"},
        ],
    }
    encoded_data = urlencode(payload)
    res = morelink_client.httpx_client.post(url=url, content=encoded_data)

    if res.status_code == 200 and res.json()["success"]:
        # logger.info("受理成功")
        return True


def zongdan_get_ready_shouli_list(morelink_client: MoreLinkClient):
    """获取最近一个月未受理的数据"""

    url = "https://morelink56.com/Common/IDATA?type=dsjson&proc=Web_Select_zginfo_BigWaybill_%E8%AE%A2%E8%88%B1"
    # 获取当前时间
    now = datetime.now()

    # 设置 starttime 为一个月前的当前时间
    starttime = (now - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

    # 设置 endtime 为当前时间
    endtime = now.strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "state": "未受理",
        "EntrustType": "全部",
        "timetype": "createtime",
        "starttime": starttime,
        "endtime": endtime,
    }
    encoded_data = urlencode(payload)
    res = morelink_client.httpx_client.post(url=url, content=encoded_data)

    if res.status_code == 200:
        logger.info("获取未受理数据成功")
        data = res.json()[0]["tb"]
        return data


def zongdan_upload(morelink_client: MoreLinkClient):
    # 获取当前日期
    today = datetime.today()

    # 格式化日期
    formatted_date = today.strftime("%Y-%m-%d")
    url = "https://morelink56.com/Common/IDATA?type=fun&proc=bll.productcenter.add"
    payload = {
        "voyageid": "0",
        "opentype": "0",
        "types": "",
        "iskc": "",
        "orderno": "",
        "budan": "",
        "shuaigui": "",
        "cuip": "",
        "pass": "",
        "cuip1": "",
        "pass1": "",
        "zgver": "0",
        "order_type": "1",
        "expresstrack": "",
        "kystrack": "",
        "hystrack": "",
        "shipcompanyname": "",
        "shipcompany": "",
        "isydcw": "",
        "csid": "",
        "businesstype": "海运",
        "supname": "",
        "supplierid": "",
        "priority": "0",
        "caozuojd": "",
        "qgtype": "",
        "tgmode": "代理卡车",
        "ordertype": "自拼",
        "country": "美国",
        "customername": "",
        "DSID": "",
        "ckorderno": "",
        "loadingtype": "整柜",
        "DelegateDate": formatted_date,
        "zline": "",
        "billno": "",
        "huodaiid": "",
        "huodai": "",
        "so": "",
        "shipdate": "",
        "flightno": "",
        "cabinetNo": "",
        "startland": "",
        "destination": "",
        "destination_addr": "",
        "shipname": "",
        "flight": "",
        "etd": "",
        "eta": "",
        "arrivalporttime": "",
        "releasetime": "",
        "sjsctime": "",
        "cgfinishtime": "",
        "counterday": "",
        "discharge": "",
        "wharftruck": "",
        "dls_arrivaltime": "",
        "sealno": "",
        "qgcomplete": "",
        "deliveryday": "",
        "qgagent": "ODW",
        "bgportcode": "",
        "route": "",
        "dispatchagent": "",
        "pod": "",
        "dcpayment": "未付款",
        "cabinettype": "40HC",
        "cabinet_weight": "",
        "invno": "",
        "hblno": "",
        "cktzcytime": "",
        "ckcytime": "",
        "ckfxtime": "",
        "ckcytimelen": "",
        "jktzcytime": "",
        "jkcytime": "",
        "jkfxtime": "",
        "jkcytimelen": "",
        "closingtime": "",
        "bltime": "",
        "pgtime": "",
        "cutype": "",
        "cduration": "",
        "suborderno": "",
        "takeofftime": "",
        "arrivetime": "",
        "customsbroker": "",
        "orderchannel": "",
        "ATD": "",
        "ATA": "",
        "realtimeinfo": "",
        "remarks_ydcw": "",
        "cresult": "",
        "creason": "",
        "addr": "",
        "isfba": "FBA",
        "shipper": "",
        "shipperaddr": "",
        "tdnotifier2": "",
        "shiptoaddr": "",
        "tdywhname": "",
        "consignee": "",
        "tdmark": "",
        "tdhwagent": "",
        "tdhpdescribe": "",
        "hbl": "",
        "tdthreedrawee": "",
        "tdzxdabstract": "",
        "tddccontractno": "",
        "tdpono": "",
        "tdhscode": "",
        "tdxtypexl": "",
        "tdpacking": "",
        "num": "0",
        "weight": "0.00",
        "volume": "0.0000",
        "selling_party": "",
        "buying_party": "",
        "bond": "",
        "bondaddr": "",
        "container_location": "",
        "importer_party": "",
        "importer": "",
        "manufacturer": "",
        "payway": "",
        "blnum": "",
        "tctgaddress": "",
        "tcsupname": "",
        "tcsupplierid": "",
        "tcvehicleno": "",
        "sjinfo": "",
        "tconelphone": "",
        "tcsealingpoint": "",
        "yjsctime": "",
        "sjdctime": "",
        "tchgaddress": "",
        "tcxhbgh": "",
        "loadingdate": "",
        "warehouseid": "",
        "warehousename": "",
        "tcmwsuser": "",
        "tcremarks": "",
        "tckxzgdate": "",
        "tczgwcdate": "",
        "tcyh": "0",
        "bw_khchannel_name": "",
        "bw_khchannel_id": "",
        "bw_tihuotype": "自送货",
        "bw_warehousename": "",
        "bw_warehouseid": "",
        "bw_deliveryTime": "",
        "bw_LoadingDriver": "",
        "zgtype": "",
        "tb_zg_depot_order": "[]",
        "tb_zg_depot_order_sort": "[]",
        "gqserver": "",
        "ghserver": "",
        "gzserver": "",
        "salesman": "",
    }
    upload_res = morelink_client.httpx_client.post(url=url, data=payload)
    if upload_res.status_code == 200 and upload_res.json()["success"]:
        json_data = json.loads(upload_res.json()["data"])[0]
        gq_no = json_data["orderno"]
        voyageid = json_data["voyageid"]

        logger.info(json_data)
        return voyageid, gq_no


def main_shouli(dahuo_upload_success_data):
    # 查询总单列表未受理的数据
    zongdan_weishouli = zongdan_get_ready_shouli_list()

    if not zongdan_weishouli:
        logger.error("No pending acceptance data found.")
        return

    # 通过A单号过滤获取需要受理的的数据
    dahuo_a_danhaos = [item["A单号"] for item in dahuo_upload_success_data]
    need_shouli_data = [
        item for item in zongdan_weishouli if item["operNo"] in dahuo_a_danhaos
    ]

    if not need_shouli_data:
        logger.error("No matching data found for acceptance.")
        return

    # 受理need_shouli_data
    for single_shouli_data in need_shouli_data:
        id = single_shouli_data["ID"]
        sono = single_shouli_data["sono"]
        operno = single_shouli_data["operNo"]
        result = zongdan_shouli(id, sono, operno)
        if result:
            logger.info(f"{operno}受理成功")

    # 获取可以装柜的数据
    can_zhuanggui_data = zongdan_can_zhuanggui_data()
    if not can_zhuanggui_data:
        logger.error("没有查询到装柜的数据")
        return
    # 新建总单列表
    voyageid, gq_no = zongdan_upload()
    if not voyageid or not gq_no:
        logger.error("No matching data found for acceptance.")
        return

    # 装柜
    # 1. 处理可装柜的数据格式
    sort_counter = 1
    tb_zg_depot_order = []

    for i in can_zhuanggui_data:
        if i["operNo"] in dahuo_a_danhaos:
            tb_zg_depot_order.append(
                {"sono": i["sono"], "operNo": i["operNo"], "sort": sort_counter}
            )
            sort_counter += 1
    # 2. 装柜
    zongdan_zhuanggui(
        voyageid=voyageid, gq_num=gq_no, tb_zg_depot_order=tb_zg_depot_order
    )
