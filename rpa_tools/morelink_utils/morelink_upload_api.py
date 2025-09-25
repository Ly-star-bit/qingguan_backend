from datetime import datetime
from .morelink_api import MoreLinkClient
import httpx
from lxml import html
import urllib
from loguru import logger

# logger.add("file_{time}.log", rotation="10 MB")

def get_channelid(channelname: str,all_channeli_ids:list) -> str:
    logger.info(f"all_channeli_ids: {all_channeli_ids}")
    for i in all_channeli_ids:
        if i['channelname'] == channelname:
            return i["channelid"]
def get_warehouse_id(warehouse_name:str,all_warehouse_data:list) -> str:
    for i in all_warehouse_data:
        if i["d_name"] == warehouse_name:
            return i["id"]

def get_all_channelid(morelink_httpx_client: httpx.Client, type: str = "空运") -> str:
    """
    此 Python 函数向特定 URL 发送带有有效负载数据的 POST 请求，以根据指定类型检索频道 ID。
    
    :param morelink_httpx_client: `morelink_httpx_client` 参数是 `httpx.Client` 类的一个实例，用于发出 HTTP
    请求。在此函数中，它用于向特定 URL 发送带有一些有效负载数据的 POST 请求。
    :type morelink_httpx_client: httpx.Client
    :param type: `get_all_channelid` 函数中的 `type` 参数是一个字符串，用于指定要检索其通道 ID 的 EntrustType 类型。在本例中，`type`
    的默认值设置为“空运”，英文意思是“air transport”, defaults to 空运
    :type type: str (optional)
    :return: 函数“get_all_channelid”返回从对指定 URL 的 POST 请求的 JSON 响应中检索到的频道 ID 列表。
    """
    url = "https://morelink56.com/Common/IDATA?type=list&proc=web_select_channel_EntrustType"
    payload = {
        "EntrustType": type,
        "filterRules": "[]"
    }
    encoded_data = urllib.parse.urlencode(payload)
    try:
        channelid_res = morelink_httpx_client.post(url=url, content=encoded_data,timeout=30 )
        # logger.info(channelid_res.json())
        channelid_res.raise_for_status()
        return channelid_res.json()['rows']
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def get_all_warehouse_id(morelink_httpx_client: httpx.Client):
    """
    此 Python 函数使用 HTTPX 客户端向特定 URL 发送 POST 请求，以 JSON 格式检索所有仓库 ID。
    
    :param morelink_httpx_client: `morelink_httpx_client` 参数是 `httpx.Client` 类的一个实例，用于发出 HTTP
    请求。在此函数中，它用于向特定 URL 发送 POST 请求，以便从 JSON 响应中检索仓库 ID。
    :type morelink_httpx_client: httpx.Client
    :return: 函数“get_all_warehouse_id”将从 POST 请求获得的 JSON 响应返回到指定的 URL 端点，该端点检索所有仓库 ID。
    """
    url = "https://morelink56.com/Common/IDATA?type=json&proc=web_select_warehouse_by_list"
    try:
        warehouse_all_id_res = morelink_httpx_client.post(url=url,timeout=30)
        warehouse_all_id_res.raise_for_status()
        # logger.info(warehouse_all_id_res.json())
        return warehouse_all_id_res.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

def get_input_consignor(morelink_httpx_client: httpx.Client, CustomerName: str = "测试"):
    """
    该 Python 函数以 HTTPX 客户端和客户名称作为输入，使用客户名称数据向特定 URL 发送 POST 请求，并在成功的情况下从响应 JSON 中返回 DSID。
    
    :param morelink_httpx_client: `morelink_httpx_client` 参数是 `httpx.Client` 类的一个实例，用于发出 HTTP 请求。在提供的函数
    `get_input_consignor` 中，此客户端用于向特定 URL 发送带有编码数据的 POST 请求。
    :type morelink_httpx_client: httpx.Client
    :param CustomerName: `get_input_consignor` 函数接受 `morelink_httpx_client` 对象（类型为 `httpx.Client`）和
    `CustomerName` 参数（类型为 `str`）。该函数向特定 URL 发送 POST 请求，其中编码了 `CustomerName` 数据，并检索, defaults to 测试
    :type CustomerName: str (optional)
    :return: 函数“get_input_consignor”返回“input_Consignor”的值，该值是从对
    URL“https://morelink56.com/Common/IDATA?type=json&proc=Web_Select_DS_Customer_rolekh”发出的 POST 请求的
    JSON 响应中提取的。返回的值是 JSON 响应数组第一个元素中的“DSID”字段。
    """
    data = {"CustomerName": CustomerName}
    encoded_data = urllib.parse.urlencode(data)
    try:
        input_Consignor_res = morelink_httpx_client.post(url="https://morelink56.com/Common/IDATA?type=json&proc=Web_Select_DS_Customer_rolekh", content=encoded_data,timeout=30)
        input_Consignor_res.raise_for_status()
        input_Consignor = input_Consignor_res.json()
        return input_Consignor
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

def get_rcid(morelink_httpx_client: httpx.Client, input_Consignor: str):
    """
    函数“get_rcid”向特定 URL 发送带有有效负载的 POST 请求，以从响应 JSON 数据中检索特定值。
    
    :param morelink_httpx_client: `morelink_httpx_client` 参数是 `httpx.Client` 类的一个实例，用于发出 HTTP 请求。它被传递给
    `get_rcid` 函数，以使用有效负载（`rc
    :type morelink_httpx_client: httpx.Client
    :param input_Consignor: `get_rcid` 函数接受两个参数：
    :type input_Consignor: str
    :return: 函数“get_rcid”返回从使用提供的有效负载向指定 URL 发出 POST 请求后收到的 JSON 响应中提取的“rcid”值。如果成功，它将返回“rcid”值。如果发生
    HTTP 错误，它将记录错误消息。如果发生任何其他异常，它也会记录错误消息。
    """
    rcid_url = "https://morelink56.com/Common/IDATA?type=fun&proc=bll.cooperativecustomer_contract.cooperativecustomer_contract.GetCarrier"
    rcid_payload = {"dsid": input_Consignor}
    encodedrcid_payload = urllib.parse.urlencode(rcid_payload)
    try:
        rcid_res = morelink_httpx_client.post(url=rcid_url, content=encodedrcid_payload,timeout=30)
        rcid_res.raise_for_status()
        rcid = rcid_res.json()["rows"][0]["rcid"]
        return rcid
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

def get_guidoperNo(morelink_httpx_client: httpx.Client):
    """
    此函数使用 HTTP 客户端从特定 URL 检索“guidoperNo”输入字段的值。
    
    :param morelink_httpx_client: `morelink_httpx_client` 参数是 `httpx.Client` 类的一个实例，用于发出 HTTP
    请求。在此函数中，它用于向特定 URL 发送 GET 请求（`https://morelink56.com/BigWaybill/newadd?
    :type morelink_httpx_client: httpx.Client
    :return: 函数 `get_guidoperNo` 正在使用提供的 `httpx.Client` 实例向
    URL“https://morelink56.com/BigWaybill/newadd?repair=3”发出 GET 请求，从响应的 HTML 内容中返回 'guidoperNo'
    输入字段的值。如果在
    """
    try:
        response = morelink_httpx_client.get(url="https://morelink56.com/BigWaybill/newadd?repair=3",timeout=30)
        response.raise_for_status()
        tree = html.fromstring(response.content)
        input_guidoperNo = tree.xpath('//input[@id="guidoperNo"]')
        return input_guidoperNo[0].get('value') if input_guidoperNo else None
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

# Supplier_DS constant
Supplier_DS = "625C2AF5-DF07-4537-9FE3-7749CD7ADA82"



# morelink_client = MoreLinkClient()
# http_client = morelink_client.httpx_client

# # get_all_channelid(http_client)
# get_all_warehouse_id(http_client)