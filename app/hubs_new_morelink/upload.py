from datetime import datetime
import json
import traceback
import httpx
from loguru import logger
import urllib

# from .morelink_api import MoreLinkClient
import traceback

from .hubs_client import HubsClient

def exec_generated_code(code, path):
    # 使用 exec 执行提取的代码
    try:
        exec(code, globals())  # 使用 globals() 确保生成的函数在全局命名空间中可用
        # 调用生成的函数
        
        error_msg = None
        generated_file_path = None
        generated_function = globals().get('process_excel_to_json')
        if generated_function:
            generated_file_path = generated_function(path)
            logger.info(f"生成的Json文件路径: {generated_file_path}")
        else:
            logger.error("未找到生成的函数 process_excel_to_json")
            error_msg =  "未找到生成的函数 process_excel_to_json"
    except KeyError as e:
        error_msg = f"没有列名: {str(e)}"
        logger.error(f"没有列名: {str(e)}")
        
    except Exception as e:
        error_msg = f"执行代码时出错: {traceback.format_exc()}"
        logger.error(f"执行代码时出错: {traceback.format_exc()}")
    finally:
        return generated_file_path, error_msg
def dahuo_upload(orders):
    try:
        hubsclient = HubsClient()
        # all_cooperate_client = morelink_client.cooperate_client_search()
        all_warehouse = hubsclient.get_import_warehouses()
        # all_base_country = hubsclient.get_base_countries()
        all_crm_supplier = hubsclient.get_crm_supplier()
        # Get all send warehouse IDs
        all_channel = hubsclient.get_customer_channels(channel_type=1)
        all_client = hubsclient.get_sys_customer()
        all_fbx_address = hubsclient.get_base_fbx_addr()
        current_client = [
                i
                for i in all_client
                if i["abbreviation"] == orders[0]['发货单位']
            ][0]
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
        print(order)
        try:
            upload_payload = {
                "order": {
                    "optype": "",
                    "create_time": "",
                    "sono": "",
                    "operno": "",
                    "transport_type": "",
                    "start_city": "",
                    "country_name": "",
                    "country_code": "",
                    "delivery_mode": 0,
                    "delivery_time": "",
                    "fh_contacts": "",
                    "fh_phone": "",
                    "fh_province": "",
                    "fh_city": "",
                    "fh_area": "",
                    "fh_addr": "",
                    "fh_longitude": "",
                    "fh_latitude": "",
                    "ckid": "",
                    "ck_name": "",
                    "ck_input_expect_time": "",
                    "ck_state": 0,
                    "is_insure": False,
                    "tb_jine": 0,
                    "tb_currency": "",
                    "insured": "",
                    "sup_org_id": "",
                    "sup_org_name": "",
                    "kh_chid": "",
                    "kh_channel_name": "",
                    "supid": "",
                    "sup_name": "",
                    "nb_chid": "",
                    "nb_channel_name": "",
                    "is_tax": False,
                    "fbx_name": "",
                    "addr_type": 0,
                    "fbx_code": "",
                    "ref_id": "",
                    "d_code": "",
                    "fbx_no": "",
                    "sh_province": "",
                    "sh_city": "",
                    "sh_addr": "",
                    "sh_email": "",
                    "sh_zip_code": "",
                    "sh_company": "",
                    "final_delivery": "",
                    "sh_contacts": "",
                    "sh_phone": "",
                    "product": "",
                    "product_nature": "",
                    "packing": "箱",
                    "total_qty": 0,
                    "total_kg": 0,
                    "total_cbm": 0,
                    "yj_qty": 0,
                    "yj_kg": 0,
                    "yj_cbm": 0,
                    "first_weight": 0,
                    "xu_weight": 0,
                    "True_weight": 0,
                    "bg_mode": "",
                    "qg_mode": "",
                    "have_no": "",
                    "ve_vat_tax": "",
                    "ve_eori_tax": "",
                    "ve_company": "",
                    "ve_addr": "",
                    "ve_contacts": "",
                    "ve_tel": "",
                    "billing_unit": "",
                    "cost_price": 0,
                    "offer_price": 0,
                    "price_remark": "",
                    "dsid": "",
                    "customer_name": "",
                    "abbreviation": "",
                    "kh_remark": "",
                    "switch_type": 0,
                    "switch_state": 0,
                    "total_volume": 0,
                    "fp_weigth": 0,
                    "cbm_weigth": 0,
                    "cost_kg": 0,
                    "cb_kg": 0,
                    "infoid": "",
                    "coupon_name": "",
                    "express_type": "",
                    "express_no": "",
                    "express_no_list": "",
                    "is_express": False,
                    "ref_no": "",
                    "org_id": "",
                    "org_name": "",
                    "sale_userid": "",
                    "sale_name": "",
                    "zhuli_id": "",
                    "zhuli": "",
                    "kefu_id": "",
                    "kefu": "",
                    "js_mode": 0,
                    "js_days": 0,
                    "ctype": 0,
                    "tc_remark": "",
                    "size_state": 0,
                    "bizcost": 0,
                    "this_ckid": "",
                    "this_ck_name": "",
                    "pr_no": "",
                    "vid": "",
                    "v_title": "",
                    "bj_remark": "",
                    "is_push_fba": True,
                    "bgtt_code": "",
                    "bgtt_name": "",
                    "bgzlsx_name": "",
                    "qgzlsx_name": "",
                    "ps_type_hope": "",
                    "ps_type_plan": "",
                },
                # "boxgauge_list": [
                #     {
                #         "bgid": "",
                #         "fba_no": "",
                #         "container_no": "",
                #         "tracking_no": "",
                #         "length": 0,
                #         "width": 0,
                #         "height": 0,
                #         "qty": 0,
                #         "build_date": "",
                #         "singlebox_kg": 0,
                #         "refid": "",
                #         "product_list": [
                #             {
                #                 "cn_product_name": "",
                #                 "us_product_name": "",
                #                 "brand": "",
                #                 "model": "",
                #                 "purpose": "",
                #                 "material": "",
                #                 "hs_code": "",
                #                 "single_cost": 0,
                #                 "currency": "",
                #                 "total_declared": 0,
                #                 "asin": "",
                #                 "product_nature": "",
                #                 "sales_links": "",
                #                 "back_map": "",
                #                 "sales_records_map": "",
                #                 "product_map": "",
                #                 "sales_price": 0,
                #                 "attestation": "",
                #                 "product_pcs": 0,
                #                 "sku": "",
                #                 "pt_gross_weight": 0,
                #                 "pt_net_weight": 0,
                #                 "tax_rate": 0,
                #                 "bgid": "",
                #                 "en_purpose": "",
                #                 "en_material": "",
                #                 "ps_type": "",
                #                 "is_tax": True,
                #                 "remark": "",
                #             }
                #         ],
                #     }
                # ],
                # "goods_type": {
                #     "sono": "",
                #     "goods_type": "",
                #     "goods_type_name": "",
                #     "magnetic_performance": "",
                #     "magnetic_performance_name": "",
                #     "un_package": "",
                #     "un_package_name": "",
                #     "have_report_xz": True,
                #     "have_sj": True,
                #     "report": "",
                #     "report_name": "",
                #     "dry_battery": "",
                #     "dry_battery_name": "",
                #     "battery": "",
                #     "battery_name": "",
                #     "report_lithium_battery": "",
                #     "report_lithium_battery_name": "",
                #     "description": "",
                # },
            }
            send_customer = order["发货单位"]

            # 填写订单
            upload_payload["order"]["abbreviation"] = send_customer
            # upload_payload["order"]["dsid"] = [
            #     i["dsid"]
            #     for i in all_client
            #     if i["abbreviation"] == upload_payload["order"]["abbreviation"]
            # ][0]
            upload_payload["order"]["dsid"] = current_client['dsid']
            upload_payload["order"]["sale_userid"] = current_client['sale_userid']
            upload_payload["order"]["sale_name"] = current_client['sale_name']
            upload_payload["order"]["zhuli_id"] = current_client['zhuli_id']
            upload_payload["order"]["zhuli"] = current_client['zhuli']
            upload_payload["order"]["kefu_id"] = current_client['kefu_id']
            upload_payload["order"]["kefu"] = current_client['kefu']
            upload_payload["order"]["org_id"] = current_client['org_id']
            upload_payload["order"]["org_name"] = current_client['org_name']
            # 转接类型
            upload_payload["order"]["switch_type"] = 1
            # upload_payload['order']["ConsignorContacts"] = sender
            # upload_payload['order']["ConsignorPhone"] = phone
            # upload_payload['order']["ConsignorAddress"] = send_thing_remarks
            upload_payload["order"]["yj_kg"] = "{:.2f}".format(
                float(order.get("预计重量", 0))
            )
            upload_payload["order"]["yj_cbm"] = "{:.2f}".format(
                float(order.get("预计体积", 0))
            )
            upload_payload["order"]["yj_qty"] = int(float(order.get("预计数量", 0)))

            upload_payload["order"]["transport_type"] = order.get("业务类型", "")
            upload_payload["order"]["qg_mode"] = order.get("报关方式", "")
            upload_payload["order"]["bg_mode"] = order.get("清关方式", "")
            upload_payload["order"]["nb_channel_name"] = order.get(
                "自营渠道", ""
            )  # 通过名称找id 不是必填
  
            child_id,billing_unit = [
                [i["chid"],i['billing_unit']]
                for i in all_channel
                if i["channel_name"] == upload_payload["order"]["nb_channel_name"]
            ][0]
            upload_payload['order']['billing_unit'] = billing_unit
            upload_payload["order"]["nb_chid"] =child_id
            upload_payload["order"]["kh_channel_name"] = order.get("客户渠道", "")  #
            upload_payload["order"]["kh_chid"] = child_id
           
            # 供应商
            upload_payload["order"]["sup_name"] = "赫泊斯供应链管理（集团）"  #
            upload_payload["order"]["supid"] = [
                i["supid"]
                for i in all_crm_supplier
                if i["sup_name"] == upload_payload["order"]["sup_name"]
            ][0]
            upload_payload["order"]["sup_org_name"] = upload_payload["order"][
                "sup_name"
            ]
            upload_payload["order"]["sup_org_id"] = upload_payload["order"]["supid"]
            upload_payload["order"]["bj_remark"] = order.get("标记栏", "")
            upload_payload["order"]["product_nature"] = order.get("产品性质", "")
            upload_payload["order"]["kh_remark"] = order.get("业务备注", "")  #
            upload_payload["order"]["offer_price"] = order.get("报价单价", "")
            upload_payload["order"]["cost_price"] = order.get("成本单价", "")

            upload_payload["order"]["total_kg"] = "{:.2f}".format(
                float(order.get("总KG", 0))
            )
            upload_payload["order"]["total_cbm"] = "{:.4f}".format(
                float(order.get("总CBM", 0))
            )
            upload_payload["order"]["total_qty"] = "{:.0f}".format(
                float(order.get("总箱数", 0))
            )

            # # 业务员
            # upload_payload['order']["sale_name"] = consigor_data[0].get("UserName", "")
            # # 跟单客服
            # upload_payload['order']["kefu"] = consigor_data[0].get("LoginAccount", "")
            # 提货方式
            upload_payload["order"]["delivery_mode"] = 1
            # upload_payload['order']["LoadingDriver"] = order.get("司机资料", "")#没找到
            upload_payload["order"]["delivery_time"] = order.get("上门提货日期", "")

            # 预计入仓时间
            upload_payload["order"]["ck_input_expect_time"] = order.get(
                "预计入仓时间", ""
            )

            upload_payload["order"]["ck_name"] = order.get("送货仓库", "")
            upload_payload["order"]["ckid"] = [
                i["ckid"]
                for i in all_warehouse
                if i["ck_name"] == upload_payload["order"]["ck_name"]
            ][0]
            # 收货地址
            # upload_payload['order']["country_name"] = order.get("国家", "")
            # upload_payload['order']["country_code"] = [i['code'] for i in all_base_country if i['name_cn'] == upload_payload['order']["country_name"]][0]#需要对应
            # 电商平台
            upload_payload["order"]["fbano"] = (
                order.get("FBA号", "").replace(",nan", "").replace("nan", "").strip()
            )
            # 如果HKLMT-香港兰玛特-SZ存在FBA号，则推送Amazon
            if (
                send_customer == "HKLMT-香港兰玛特-SZ"
                and upload_payload["order"]["fbano"]
                and "FBA" in upload_payload["order"]["fbano"]
            ):
                upload_payload["order"]["is_push_fba"] = True
            upload_payload["order"]["have_no"] = order.get("客户内部号", "")  #

            origin_d_code = str(order.get("搜索CODE", ""))
            upload_payload["order"]["fbx_name"] = "亚马逊"

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
            for single_fbx_warehouse in all_fbx_address:
                if (
                    new_d_code in single_fbx_warehouse["d_code"]
                    and "暂停使用" not in single_fbx_warehouse["d_code"]
                ):
                    matched_warehouses.append(single_fbx_warehouse)

            if len(matched_warehouses) == 1:
                new_d_code = matched_warehouses[0]["d_code"]
                cjaddr = matched_warehouses[0]["addr"]
                zip_code = matched_warehouses[0]["zip_code"]
                fbx_name = matched_warehouses[0]["fbx_name"]
                fbx_code = matched_warehouses[0]["fbx_code"]
                state = matched_warehouses[0]["zhou"]
                city = matched_warehouses[0]["city"]
                country_name = matched_warehouses[0]["country_name"]
                country_code = matched_warehouses[0]["country_code"]
                email = matched_warehouses[0]["email"]

                d_code_exists = True
            elif len(matched_warehouses) > 1:
                new_d_code = "PD_待定_US"
                d_code_exists = False

            if not d_code_exists:
                new_d_code = "PD_待定_US"
                for single_warehouse in all_fbx_address["rows"]:
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
            upload_payload["order"]["d_code"] = new_d_code
            upload_payload["order"]["sh_addr"] = cjaddr
            upload_payload["order"]["sh_zip_code"] = zip_code
            upload_payload["order"]["fbx_name"] = fbx_name
            upload_payload["order"]["fbx_code"] = fbx_code
            upload_payload["order"]["sh_province"] = state
            upload_payload["order"]["sh_city"] = city
            upload_payload["order"]["sh_email"] = email
            # 收货地址
            upload_payload["order"]["country_name"] = country_name
            upload_payload["order"]["country_code"] = country_code
            fbano = upload_payload["order"]["fbano"]
            # 货箱清单
            if order.get("货箱清单", ""):
                upload_payload['boxgauge_list'] = order.get("货箱清单", "")

            # 如果只有一个Fbano，就不用关心多个FBA号，不同箱数了，全部箱数都是同一个fbano的
            elif (
                fbano
                and len(fbano.split(",")) == 1
            ):
                boxgauge_list = [
                    {
                        "bgid": "",
                        "fba_no":  fbano,
                        "container_no": f"{fbano}U{i:05d}" if "FBA" in fbano else fbano ,
                        "tracking_no": "",
                        "length": 0,
                        "width": 0,
                        "height": 0,
                        "qty": 1,
                        "build_date": "",
                        "singlebox_kg": 0,
                        "refid": "",
                    }
                    for i in range(1, int(upload_payload["order"]["yj_qty"]) + 1)
                ]
                upload_payload['boxgauge_list'] =boxgauge_list

            now = datetime.now()
            upload_payload["order"]["create_time"] = now.strftime("%Y-%m-%d %H:%M:%S")

            upload_res_json = hubsclient.post_bigwaybill_order(upload_payload)
            if upload_res_json:
                # logger.info(upload_res_json["msg"])
                success_upload_data.append(
                    {
                        "shipmendID": upload_payload["order"]["fbano"],
                        "A单号": upload_res_json['data']['operno'],
                        "箱数": order.get("预计数量", ""),
                        "体积": order.get("预计体积", ""),
                        "实重": order.get("预计重量", ""),
                        "fba仓库": upload_payload["order"]["d_code"],
                        "邮编": order.get("邮编", ""),
                        'sono':upload_res_json['data']['sono']
                    }
                )
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


if __name__ == "__main__":
    orders = [
        {
            "搜索CODE": "ABE2",
            "FBA号": "FBA123456",
            "预计数量": 3,
            "预计体积": 0.06,
            "预计重量": 15.4,
            "发货单位": "XMHX-厦门和新-JX",
            "业务类型": "空运",
            "报关方式": "正本-校验",
            "清关方式": "LDP",
            "自营渠道": "空运标准",
            "客户渠道": "空运标准",
            "产品性质": "普货",
            "提货方式": "上门提货",
            "上门提货日期": "2025-07-10 00:00",
            "送货仓库": "上海仓",
            "预计入仓时间": "",
            "司机资料": "",
            "国家": "美国",
            "客户内部号": "",
            "报价单价": 43,
            "成本单价": 43,
            "业务备注": "",
            "标记栏": "汉波：15980826732",
        }
    ]

    upload_data = dahuo_upload(orders)
