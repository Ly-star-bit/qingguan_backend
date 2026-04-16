import pandas as pd
import traceback
import os
from datetime import datetime, timedelta

from app.utils_aspose import shenzhen_air_pdf_gennerate, shenzhen_customes_pdf_gennerate
from rpa_tools.email_tools import send_email
from rpa_tools.utils import find_playwright_node_path
from morelink_api import MoreLinkClient
from app.tools_auto.utils.hubs.hubs_client import HubsClient

receiver_emial = "op_sea@hubs-scs.com"


def convert_date_format(date_str):
    """将日期字符串转换为 dd/MMM/yy 格式（如 05/May/24）"""
    if not date_str:
        return ""
    try:
        date_obj = datetime.strptime(str(date_str), '%Y-%m-%d %H:%M')
        return date_obj.strftime('%d/%b/%y')
    except ValueError:
        try:
            date_obj = datetime.strptime(str(date_str), '%Y-%m-%d')
            return date_obj.strftime('%d/%b/%y')
        except ValueError:
            return date_str


def add_bill_index_before_group(df):
    """在分组前为相同提单号添加序号（已废弃）"""
    return df


def process_excel(file_path):
    """读取 Excel 并按提单号 group 合并"""
    df = pd.read_excel(file_path)

    columns = [
        "提单号",
        "提单发货人",
        "提单收货人",
        "箱数",
        "毛重/(KG)",
        "体积/(立方米)",
        "品名",
    ]
    df = df[columns]

    df["提单号"] = df["提单号"].astype(str).str[:12]

    grouped = (
        df.groupby(["提单号", "提单发货人", "提单收货人"])
        .agg(
            {
                "品名": "first",
                "箱数": "sum",
                "毛重/(KG)": "sum",
                "体积/(立方米)": "sum",
            }
        )
        .reset_index()
    )

    # 按分组键生成唯一序号（包含提单号+发货人+收货人的组合）
    group_key_counter = {}
    bill_indices = []
    for idx, row in grouped.iterrows():
        group_key = (row["提单号"], row["提单发货人"], row["提单收货人"])
        if group_key not in group_key_counter:
            group_key_counter[group_key] = 0
        group_key_counter[group_key] += 1
        bill_indices.append(group_key_counter[group_key])
    grouped["bill_index"] = bill_indices

    print(f"DEBUG: 分组后记录数: {len(grouped)}")
    print(f"DEBUG: 分组后提单号: {grouped['提单号'].tolist()[:20]}")
    print(f"DEBUG: 分组后序号: {grouped['bill_index'].tolist()[:20]}")

    result = []
    for _, row in grouped.iterrows():
        result.append(
            {
                "shipper_name": row["提单发货人"],
                "receiver_name": row["提单收货人"],
                "master_bill_no": row["提单号"],
                "gross_weight": int(row["毛重/(KG)"]),
                "volume": float(row["体积/(立方米)"]),
                "total_boxes": int(row["箱数"]),
                "all_english_name": row["品名"],
                "bill_index": row["bill_index"],
            }
        )

    return result


def add_bill_index(excel_data):
    """为相同提单号添加序号（已废弃，用process_excel内置逻辑）"""
    return excel_data


def get_hubs_data():
    """从 Hubs 获取数据"""
    hubs_client = HubsClient()
    today = datetime.now()
    one_month_ago = today - timedelta(days=90)
    s_date = one_month_ago.strftime("%Y-%m-%d")
    e_date = today.strftime("%Y-%m-%d")
    
    hubs_data_list = []
    
    for transport_type in ["海运", "空运"]:
        hubs_payload = {"pageindex":1,"pagesize":1000,"open_search":False,"sorts":[],"s_date":s_date,"e_date":e_date,"transport_type":transport_type,"sel_type":"operno","ids":[],"searchlist":[]}
        hubs_zongdan_data_res = hubs_client.httpx_client.post("https://admin.hubs-scs.com/api/v1/oms/oms_zginfo/getlist",json=hubs_payload)
        hubs_zongdan_data = hubs_zongdan_data_res.json()
        
        if hubs_zongdan_data.get("code") == 200 and hubs_zongdan_data.get("data"):
            hubs_data_list.extend(hubs_zongdan_data['data']['list'])
    
    # 获取端口映射
    port_code_full_english_name_mapping = dict()
    for transport_type in ["海运", "空运"]:
        port_payload = {"keyword":"","pageindex":1,"pagesize":100,"transport_type":transport_type}
        hubs_port_res = hubs_client.httpx_client.post("https://admin.hubs-scs.com/api/v1/pom/base_port/getcomponentlist",json=port_payload)
        hubs_port_data = hubs_port_res.json()
        if hubs_port_data.get("data") and hubs_port_data["data"].get("list"):
            for port in hubs_port_data['data']['list']:
                if port['port_code'] not in port_code_full_english_name_mapping:
                    port_code_full_english_name_mapping[port['port_code']] = port['port_name_yw']
    
    return hubs_client, hubs_data_list, port_code_full_english_name_mapping


def get_morelink_data():
    """从 Morelink 获取数据（备用）"""
    node_path = find_playwright_node_path()
    morelink_client = MoreLinkClient(node_path)
    morelink_data = morelink_client.zongdan_api_httpx()
    return morelink_client, morelink_data


def generate_pdf_from_excel_hubs(file_path):
    """从 Excel 生成 PDF (使用 Hubs 数据)"""
    excel_data = process_excel(file_path)
    print(f"处理完成，共{len(excel_data)}条数据")

    # 获取 Hubs 数据
    hubs_client, hubs_data_list, port_code_full_english_name_mapping = get_hubs_data()
    
    hubs_field_mapping = {
        "hbl_no": "hblno",
        "pol": "startland",
        "pod2": "destination",
        "vsl_voy": "flight",
        "cabinet_type": "cabinettype",
        "seal_no": "sealno",
        "atd": "ATD",
        "cntr_no":"cabinetNo"
    }

    success_count = 0
    fail_count = 0
    failed_bills = []

    for item in excel_data:
        try:
            master_bill_no = item["master_bill_no"].strip()
            bill_index = item.get("bill_index", 1)
            is_air = "-" in master_bill_no

            # 使用 (提单号_发货人简称_收货人简称_序号) 作为文件名，确保唯一性
            shipper_short = item["shipper_name"][:3] if item.get("shipper_name") else "SHP"
            receiver_short = item["receiver_name"][:3] if item.get("receiver_name") else "RCV"
            pdf_name = f"{master_bill_no}_{shipper_short}_{receiver_short}_{bill_index}.pdf"
            pdf_path = f"./pdf/customs/{pdf_name}"
            if os.path.exists(pdf_path):
                success_count += 1
                continue

            filter_data = [i for i in hubs_data_list if i['bl_no'] == master_bill_no]
            if not filter_data:
                print(f"提单号搜索不到：{master_bill_no}")
                fail_count += 1
                failed_bills.append(master_bill_no)
                continue

            mapped_data = []
            for data_item in filter_data:
                mapped_item = {hubs_field_mapping.get(k, k): v for k, v in data_item.items()}
                # mapped_item['startland'] = port_code_full_english_name_mapping.get(mapped_item.get('startland'), mapped_item.get('startland'))
                # mapped_item['destination'] = port_code_full_english_name_mapping.get(mapped_item.get('destination'), mapped_item.get('destination'))
                mapped_data.append(mapped_item)

            if is_air:
                atd_value = mapped_data[0].get("ATD")
                atd_formatted = convert_date_format(atd_value) if atd_value else ""
                input_data = [
                    master_bill_no, master_bill_no, "", "", "",
                    item["shipper_name"], "", item["receiver_name"],
                    mapped_data[0]["startland"], mapped_data[0]["destination"],
                    mapped_data[0].get('ship_ari_code', ''), atd_formatted,
                    mapped_data[0]["flight"], item["total_boxes"], item["gross_weight"],
                    "", item["volume"], 'CLOTHING', "", "", "", "", "", "", "", ""
                ]
                pdf_file = shenzhen_air_pdf_gennerate(input_data)
            else:
                # 使用 (提单号_发货人简称_收货人简称_序号) 作为文件名，确保唯一性
                shipper_short = item["shipper_name"][:3] if item.get("shipper_name") else "SHP"
                receiver_short = item["receiver_name"][:3] if item.get("receiver_name") else "RCV"
                pdf_name = f"{master_bill_no}_{shipper_short}_{receiver_short}_{bill_index}.pdf"
                request_data = {
                    "shipper_name": item["shipper_name"],
                    "receiver_name": item["receiver_name"],
                    "master_bill_no": master_bill_no,
                    "gross_weight": item["gross_weight"],
                    "volume": item["volume"],
                    "total_boxes": item["total_boxes"],
                    "all_english_name": "CLOTHING",
                    "pdf_name": pdf_name,
                }
                pdf_file = shenzhen_customes_pdf_gennerate(request_data, mapped_data[0])

            print(f"已生成 pdf 文件->{pdf_file}")
            success_count += 1

        except Exception as e:
            print(f"处理异常：{traceback.format_exc()}")
            fail_count += 1
            failed_bills.append(item.get("master_bill_no", "未知"))
            continue

    print(f"完成！成功：{success_count}, 失败：{fail_count}")
    if failed_bills:
        print("\n========== 失败的提单号列表 ==========")
        for bill_no in failed_bills:
            print(f"  - {bill_no}")
        print(f"失败提单号共计：{len(failed_bills)} 个")


def generate_pdf_from_excel_morelink(file_path):
    """从 Excel 生成 PDF (使用 Morelink 数据)"""
    excel_data = process_excel(file_path)
    print(f"处理完成，共{len(excel_data)}条数据")

    morelink_client, morelink_data = get_morelink_data()

    morelink_field_mapping = {
        "billno": "hblno",
        "startland": "startland",
        "destination": "destination",
        "flight": "flight",
        "cabinetNo": "cabinetNo",
        "sealno": "sealno",
        "ATD": "ATD",
    }

    success_count = 0
    fail_count = 0
    failed_bills = []

    for item in excel_data:
        try:
            master_bill_no = item["master_bill_no"].strip()
            bill_index = item.get("bill_index", 1)
            is_air = "-" in master_bill_no

            # 使用 (提单号_发货人简称_收货人简称_序号) 作为文件名，确保唯一性
            shipper_short = item["shipper_name"][:3] if item.get("shipper_name") else "SHP"
            receiver_short = item["receiver_name"][:3] if item.get("receiver_name") else "RCV"
            pdf_name = f"{master_bill_no}_{shipper_short}_{receiver_short}_{bill_index}.pdf"
            pdf_path = f"./pdf/customs/{pdf_name}"
            if os.path.exists(pdf_path):
                success_count += 1
                continue

            filter_data = [row for row in morelink_data if row.get("billno") == master_bill_no]
            if not filter_data:
                print(f"提单号搜索不到：{master_bill_no}")
                fail_count += 1
                failed_bills.append(master_bill_no)
                continue

            mapped_data = []
            for data_item in filter_data:
                mapped_item = {morelink_field_mapping.get(k, k): v for k, v in data_item.items()}
                mapped_data.append(mapped_item)

            if is_air:
                atd_value = mapped_data[0].get("ATD")
                atd_formatted = convert_date_format(atd_value) if atd_value else ""
                input_data = [
                    master_bill_no, master_bill_no, "", "", "",
                    item["shipper_name"], "", item["receiver_name"],
                    mapped_data[0]["startland"], mapped_data[0]["destination"],
                    mapped_data[0].get('shipcompany', ''), atd_formatted,
                    mapped_data[0]["flight"], item["total_boxes"], item["gross_weight"],
                    "", item["volume"], 'CLOTHING', "", "", "", "", "", "", "", ""
                ]
                pdf_file = shenzhen_air_pdf_gennerate(input_data)
            else:
                # 使用 (提单号_发货人简称_收货人简称_序号) 作为文件名，确保唯一性
                shipper_short = item["shipper_name"][:3] if item.get("shipper_name") else "SHP"
                receiver_short = item["receiver_name"][:3] if item.get("receiver_name") else "RCV"
                pdf_name = f"{master_bill_no}_{shipper_short}_{receiver_short}_{bill_index}.pdf"
                request_data = {
                    "shipper_name": item["shipper_name"],
                    "receiver_name": item["receiver_name"],
                    "master_bill_no": master_bill_no,
                    "gross_weight": item["gross_weight"],
                    "volume": item["volume"],
                    "total_boxes": item["total_boxes"],
                    "all_english_name": "CLOTHING",
                    "pdf_name": pdf_name,
                }
                pdf_file = shenzhen_customes_pdf_gennerate(request_data, mapped_data[0])

            print(f"已生成 pdf 文件->{pdf_file}")
            success_count += 1

        except Exception as e:
            print(f"处理异常：{traceback.format_exc()}")
            fail_count += 1
            failed_bills.append(item.get("master_bill_no", "未知"))
            continue

    print(f"完成！成功：{success_count}, 失败：{fail_count}")
    if failed_bills:
        print("\n========== 失败的提单号列表 ==========")
        for bill_no in failed_bills:
            print(f"  - {bill_no}")
        print(f"失败提单号共计：{len(failed_bills)} 个")


def generate_pdf_from_excel(file_path):
    """从 Excel 生成 PDF (默认使用 Hubs)"""
    generate_pdf_from_excel_hubs(file_path)


if __name__ == "__main__":
    generate_pdf_from_excel(r"C:\Users\a1337\Desktop\1-2月 赫泊斯 出货清单.xlsx")
