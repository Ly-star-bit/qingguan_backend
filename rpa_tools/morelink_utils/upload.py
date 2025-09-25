from datetime import datetime
import json
import traceback
import httpx
from loguru import logger
import urllib

from .morelink_upload_api import (
    get_all_channelid,
    get_all_warehouse_id,
    Supplier_DS,
    get_channelid,
    get_guidoperNo,
    get_input_consignor,
    get_rcid,
    get_warehouse_id,
)
from .morelink_api import MoreLinkClient
import traceback


def dahuo_upload(orders, morelink_client: MoreLinkClient):
    try:
        all_cooperate_client = morelink_client.cooperate_client_search()
        all_warehouse = morelink_client.fba_warehouse_search()

        # Get all send warehouse IDs
        all_send_warehouse_id = get_all_warehouse_id(morelink_client.httpx_client)
        all_channel_id = get_all_channelid(
            morelink_client.httpx_client, orders[0]["业务类型"]
        )
        if not all_channel_id:
            logger.error("没有找到对应的渠道")
            raise ValueError("没有找到对应的渠道")

    except httpx.TimeoutException as e:
        logger.error(f"网络超时错误: {e}")
        raise ValueError("网络超时错误,请重试")
    except Exception as e:
        logger.error(f"错误： {traceback.format_exc()}")
        return
        # Read the Excel file
    # df = pd.read_excel(r"D:\RPAProject\morelink_dahuo_upload\2024.5.11东莞仓提货数据-赫泊斯-香港悦动.xlsx")
    success_upload_data = []
    fail_upload_data = []
    error_msg = ""
    for order in orders:
        try:
            upload_payload = {
                "guidoperNo": "",
                "repair": "0",
                "tsemail": "0",
                "display": "1",
                "hxqdsh": "0",
                "sup_send": "1",
                "ID": "",
                "country_code": "德国",
                "d_code": "[暂停使用] W-MCI1-M",
                "fbano": "",
                "address1": """
                    [暂停使用] W-MCI1-M
                    RECEIVER & COMPANY NAME: WAL-MART
                    ADDRESS: 1303 SW INNOVATION PKWY, 
                    TOPEKA, KS 66619, US
                    """,
                "zip_code": "28197",
                "tb_GoodsInfo2": '[{"ContainerNo":"","ContaineNum":0,"ContainerWeight":0,"ContainerLength":0,"ContainerWidth":0,"ContainerHeight":0,"GoodsSKU":"","EnglishProduct":"","ChineseProduct":"","DeclaredValue":0,"DeclaredNum":0,"Material":"","Purpose":"","CustomsCode":"","SalesWebsite":"","SellingPice":"","PicturesLink":"","ProductWeight":0,"ProductSize":"","ASIN":"","FNSKU":"","model":"","netweight":0,"roughweight":0,"english_material":"","id":"","isdd":"","isdc":"","GoodsSKUtype":"","custom1":"","custom2":"","custom3":"","custom4":"","custom5":""}]',
                "khCreateTime": "",
                "storehouse": "",
                "warehousename": "广州仓-CAN",
                "expresstrack": "",
                "jdtype": "2",
                "jdorder": "",
                "channelid": "",
                "oksize": "",
                "bw_tid": "",
                "bw_orderno": "",
                "postcode": "de",
                "CustomerName": "测试账号",
                "ordertype": "",
                "version": "0",
                "make": "0",
                "reserveid": "",
                "apptdate": "",
                "rsum": "",
                "cargoagency": "",
                "vat": "",
                "vat_company": "",
                "vat_address": "",
                "vat_contact": "",
                "vat_companyphone": "",
                "eroi": "",
                "eoricompany": "",
                "eoriaddr": "",
                "Consignor": "",
                "ConsignorContacts": "TEST",
                "ConsignorPhone": "",
                "ConsignorAddress": "",
                "hmhc": "",
                "khremarks": "",
                "remarks": "",
                "yjweight": "",
                "yjvolume": "",
                "yjnum": "",
                "xmai": "",
                "issj": "否",
                "buildtime": "",
                "operNo": "",
                "CreateTime": "",
                "sono": "",
                "EntrustType": "海运",
                "importername": "",
                "jks_destination": "",
                "eori": "",
                "address": "",
                "isbx": "是",
                "bxjine": "0",
                "bxpeople": "赫泊斯供应链管理",
                "insure_currency": "",
                "kimsvolume": "",
                "Supplier_DS": Supplier_DS,
                "BigCustomerOperNo": "",
                "extendoperno": "",
                "KOKoperNo": "",
                "channeltype": "以星快递-CBM",
                "Freight": "",
                "OfferPrice": "",
                "trueWeight": "",
                "fjcost": "",
                "channeltype2": "",
                "khchannel_id": "",
                "delivertype": "",
                "channelcode": "",
                "HeavyCharge": "",
                "CostPrice": "",
                "ckvolume": "",
                "fjcbcost": "",
                "channelalias": "",
                "Markerbar": "",
                "jfunit": "",
                "Ykg": "",
                "Continuedheavy": "",
                "khchannelalias": "",
                "CourierType": "",
                "fpweight": "",
                "product": "",
                "GoodsNum": "",
                "ckweight": "0",
                "ckcbm": "",
                "etd": "",
                "eta": "",
                "startland": "",
                "destination": "",
                "cabinetNo": "",
                #
                "tihuotype": "自送货",
                "DeliveryTime": "",
                "warehouseid": "1916",
                "coutru_status": "",
                "ckremarks": "",
                "ckimg": "",
                "yj_enter_time": "",
                "LoadingDriver": "",
                "entertime": "",
                "outtime": "",
                "yytime": "",
                "isfba": "FBA",
                "companyname": "",
                "consignee": "",
                "telephone": "",
                "province": "BREMEN",
                "city": "BREMEN",
                "yyno": "",
                "invoicelink": "",
                "isbdjc": "0",
                "email": "",
                "CourierNumber": "",
                "ProductNature": "",
                "SaleMan": "Caitlin.Fang",
                "opren": "Caitlin.Fang",
                "salesloginaccount": "",
                "zhuli": "",
                "supname": "赫泊斯供应链管理",
                "types": "0",
                "tovoid_id": "",
                "CustomsDeclaration": "PD-待定",
                "vat_clear_customs": "PD-待定",
                "cwsl": "",
                "rcid": "",
                "otype": "",
                "iskims": "否",
            }

            send_customer = order["发货单位"]
            send_thing_remarks = ""
            sender = ""
            phone = ""

            for single_client in all_cooperate_client["rows"]:
                if single_client["customername"].strip() == send_customer:
                    send_thing_remarks = single_client["CompanyAddress"]
                    sender = single_client["PersonInCharge"]
                    phone = single_client["CustomerPhone"]
                    break

            if send_customer == "测试账号":
                send_thing_remarks = "测试"
                phone = "911"

            if send_customer and (send_thing_remarks or sender or phone):
                guidoperNo = get_guidoperNo(morelink_client.httpx_client)
                consigor_data = get_input_consignor(
                    morelink_client.httpx_client, send_customer
                )
                consignor = consigor_data[0]["DSID"]
                rcid = get_rcid(morelink_client.httpx_client, consignor)

                # 填写订单
                upload_payload["CustomerName"] = send_customer
                upload_payload["ConsignorContacts"] = sender
                upload_payload["ConsignorPhone"] = phone
                upload_payload["ConsignorAddress"] = send_thing_remarks
                upload_payload["yjweight"] = "{:.2f}".format(
                    float(order.get("预计重量", 0))
                )
                upload_payload["yjvolume"] = "{:.2f}".format(
                    float(order.get("预计体积", 0))
                )
                upload_payload["yjnum"] = int(float(order.get("预计数量", 0)))
                upload_payload["EntrustType"] = order.get("业务类型", "")
                upload_payload["CustomsDeclaration"] = order.get("报关方式", "")
                upload_payload["vat_clear_customs"] = order.get("清关方式", "")
                upload_payload["channeltype"] = order.get("自营渠道", "")
                upload_payload["channeltype2"] = order.get("客户渠道", "")
                upload_payload["Markerbar"] = order.get("标记栏", "")
                upload_payload["ProductNature"] = order.get("产品性质", "")
                upload_payload["remarks"] = order.get("业务备注", "")
                upload_payload["OfferPrice"] = order.get("报价单价", "")
                upload_payload["CostPrice"] = order.get("成本单价", "")

                upload_payload["ckweight"] = "{:.2f}".format(
                    float(order.get("总KG", 0))
                )
                upload_payload["ckcbm"] = "{:.4f}".format(float(order.get("总CBM", 0)))
                upload_payload["GoodsNum"] = "{:.0f}".format(
                    float(order.get("总箱数", 0))
                )

                # 业务员
                upload_payload["SaleMan"] = consigor_data[0].get("UserName", "")
                # 跟单客服
                upload_payload["opren"] = consigor_data[0].get("LoginAccount", "")
                # 提货方式
                upload_payload["tihuotype"] = order.get("提货方式", "")
                upload_payload["LoadingDriver"] = order.get("司机资料", "")
                upload_payload["DeliveryTime"] = order.get("上门提货日期", "")

                # 预计入仓时间
                upload_payload["yj_enter_time"] = order.get("预计入仓时间", "")

                upload_payload["warehousename"] = order.get("送货仓库", "")
                upload_payload["warehouseid"] = get_warehouse_id(
                    upload_payload["warehousename"], all_send_warehouse_id
                )
                # 收货地址
                upload_payload["country_code"] = order.get("国家", "")
                upload_payload["fbano"] = (
                    order.get("FBA号", "")
                    .replace(",nan", "")
                    .replace("nan", "")
                    .strip()
                )
                upload_payload["extendoperno"] = order.get("客户内部号", "")

                origin_d_code = str(order.get("搜索CODE", ""))

                if origin_d_code.isdigit():
                    if not origin_d_code.startswith("0") and len(origin_d_code) != 5:
                        origin_d_code = "0" + origin_d_code
                    if "SZNE" in send_customer:
                        new_d_code = "SZNE" + "-" + origin_d_code
                    else:
                        new_d_code = send_customer.split("-")[0] + "-" + origin_d_code
                        logger.info(f"new_d_code:{new_d_code}")
                else:
                    new_d_code = origin_d_code
                d_code_exists = False
                cjaddr = ""
                zip_code = ""

                matched_warehouses = []
                for single_warehouse in all_warehouse["rows"]:
                    if (
                        new_d_code in single_warehouse["d_code"]
                        and "暂停使用" not in single_warehouse["d_code"]
                    ):
                        matched_warehouses.append(single_warehouse)

                if len(matched_warehouses) == 1:
                    new_d_code = matched_warehouses[0]["d_code"]
                    cjaddr = matched_warehouses[0]["cjaddr"]
                    zip_code = matched_warehouses[0]["zip"]
                    
                    d_code_exists = True
                elif len(matched_warehouses) > 1:
                    new_d_code = "PD_待定_US"
                    d_code_exists = False

                if not d_code_exists:
                    new_d_code = "PD_待定_US"
                    for single_warehouse in all_warehouse["rows"]:
                        if new_d_code == single_warehouse["d_code"]:
                            cjaddr = single_warehouse["cjaddr"]
                            if order.get("自定义_邮编", ""):
                                zip_code = order.get("自定义_邮编", "")
                            else:
                                zip_code = origin_d_code
                            break
                # if new_d_code == "PD_待定_US":
                #     fail_upload_data.append(order["FBA号"])
                #     continue
                upload_payload["d_code"] = new_d_code
                upload_payload["address1"] = cjaddr
                upload_payload["zip_code"] = zip_code
                # 货箱清单
                # if order.get("货箱清单", ""):
                #     tb_goods_data = [
                #         {
                #             "ContainerNo": row["fba_no"],
                #             "ContaineNum": 0,
                #             "ContainerWeight": "0.00",
                #             "ContainerLength": "0.00",
                #             "ContainerWidth": "0.00",
                #             "ContainerHeight": "0.00",
                #             "GoodsSKU": "",
                #             "EnglishProduct": "",
                #             "ChineseProduct": "",
                #             "DeclaredValue": "0",
                #             "DeclaredNum": 0,
                #             "Material": "",
                #             "Purpose": "",
                #             "CustomsCode": "",
                #             "SalesWebsite": "",
                #             "SellingPice": "0.00",
                #             "PicturesLink": "",
                #             "ProductWeight": "0.00",
                #             "ProductSize": "",
                #             "ASIN": row["po"],
                #             "FNSKU": "",
                #             "model": "",
                #             "netweight": "0.00",
                #             "roughweight": "0.00",
                #             "english_material": "",
                #             "id": f"{index}",
                #             "isdd": "",
                #             "isdc": "",
                #             "GoodsSKUtype": "",
                #             "custom1": "",
                #             "custom2": "",
                #             "custom3": "",
                #             "custom4": "",
                #             "custom5": "",
                #         }
                #         for index, row in enumerate(order["货箱清单"], start=1)
                #     ]

                #     upload_payload["tb_GoodsInfo2"] = str(tb_goods_data)
                # 全局表单
                upload_payload["guidoperNo"] = guidoperNo
                upload_payload["Consignor"] = consignor
                upload_payload["rcid"] = rcid

                now = datetime.now()
                upload_payload["CreateTime"] = now.strftime("%Y-%m-%d %H:%M:%S")

                channelid = get_channelid(upload_payload["channeltype"], all_channel_id)
                if channelid:
                    upload_payload["channelid"] = channelid
                    upload_payload["khchannel_id"] = channelid
                else:
                    logger.error("没有channelid")
                    error_msg = f"{upload_payload['channeltype']} 没有找到对应的渠道"
                    continue

                upload_res = morelink_client.httpx_client.post(
                    url="https://morelink56.com/BigWaybill/order_operation",
                    data=upload_payload,
                )
                upload_res_json = upload_res.json()
                if upload_res_json["success"]:
                    # logger.info(upload_res_json["msg"])
                    ano = json.loads(upload_res_json["msg"])[0]["operNo"]
                    success_upload_data.append(
                        {
                            "shipmendID": upload_payload["fbano"],
                            "A单号": ano,
                            "箱数": order.get("预计数量", ""),
                            "体积": order.get("预计体积", ""),
                            "实重": order.get("预计重量", ""),
                            "fba仓库": upload_payload["d_code"],
                            "邮编": order.get("邮编", ""),
                        }
                    )
                    sono = json.loads(upload_res_json["msg"])[0]["sono"]
                    # tb_goodssize = [
                    #     {
                    #         "sono": sono,
                    #         "qty": str(int(i["总箱数"])),
                    #         "weight": str(round(i["总实重"], 2)),
                    #         "cbm": str(round(i["长"] * i["宽"] * i["高"] / 10**6, 4)),
                    #         "length": str(round(i["长"], 2)),
                    #         "width": str(round(i["宽"], 2)),  # 确保 i['宽'] 是数值类型
                    #         "height": str(round(i["高"], 2)),
                    #         "boxno": "",
                    #         "remarks": "",
                    #         "sumweight": str(round(i["总实重"] * i["总箱数"], 2)),
                    #         "sumcbm": str(
                    #             round(
                    #                 i["长"] * i["宽"] * i["高"] / 10**6 * i["总箱数"], 4
                    #             )
                    #         ),
                    #         "sumvolume": str(
                    #             round(
                    #                 i["长"] * i["宽"] * i["高"] / 6000 * i["总箱数"], 4
                    #             )
                    #         ),  # 确保 i['长'] 是数值类型
                    #     }
                    #     for i in order["cargo_size"]
                    # ]

                    # update_warehoust_size(
                    #     morelink_client,
                    #     sono=sono,
                    #     cid="1920",
                    #     tb_goodssize=tb_goodssize,
                    # )

                else:
                    logger.error(f"Upload failed: {upload_res_json['msg']}")
                    logger.error(upload_payload)
        except Exception as e:
            logger.error(f"Error processing order: {traceback.format_exc()}")
            fail_upload_data.append(order["FBA号"])
    logger.info(
        f"success_upload_data:{success_upload_data},fail_upload_data:{fail_upload_data},error_msg:{error_msg}"
    )
    return success_upload_data, fail_upload_data, error_msg


def update_warehoust_size(
    morelink_client: MoreLinkClient, sono: str, cid: str, tb_goodssize: list
):
    update_url = "https://morelink56.com/BigWaybill_Back/update_goodsize"

    update_payload = {
        "cid": cid,
        "sono": sono,
        "tb_goodssize": tb_goodssize,
    }

    update_payload_encoded = urllib.parse.urlencode(update_payload)

    res = morelink_client.httpx_client.post(
        url=update_url, content=update_payload_encoded
    )
    if res.status_code == 200 and res.json()["success"]:
        logger.info(res.json())
        return res.json()
    else:
        logger.error(res.json())



