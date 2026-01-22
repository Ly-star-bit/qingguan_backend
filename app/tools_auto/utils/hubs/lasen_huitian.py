
from io import BytesIO

import pandas as pd

from app.tools_auto.utils.hubs.hubs_client import HubsClient


def safe_float(value):
    try:
        if value == "" or value is None:
            return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def format_zip_code(zip_code):
    try:
        if zip_code is None or zip_code == "":
            return ""
        return str(int(float(zip_code)))
    except (ValueError, TypeError):
        return ""


def convert_search_code(origin_d_code: str, all_fbx_address: list) -> dict:
    origin_d_code = str(origin_d_code).strip()

    if origin_d_code.isdigit():
        if not origin_d_code.startswith("0") and len(origin_d_code) != 5:
            origin_d_code = "0" + origin_d_code
        new_d_code = origin_d_code
    else:
        new_d_code = origin_d_code

    matched_warehouses = []
    for single_fbx_warehouse in all_fbx_address:
        if (
            new_d_code == single_fbx_warehouse["d_code"]
            and "暂停使用" not in single_fbx_warehouse["d_code"]
        ):
            matched_warehouses.append(single_fbx_warehouse)

    result = {
        "d_code": new_d_code,
        "fbx_name": "亚马逊",
        "fbx_code": "",
        "sh_addr": "",
        "sh_zip_code": "",
        "sh_province": "",
        "sh_city": "",
        "sh_email": "",
        "country_name": "美国",
        "country_code": "US",
        "addr_type": 0,
        "found": False,
    }

    if len(matched_warehouses) == 1:
        matched = matched_warehouses[0]
        result["d_code"] = matched.get("d_code", "") or ""
        result["fbx_name"] = matched.get("fbx_name", "") or ""
        result["fbx_code"] = matched.get("fbx_code", "") or ""
        result["sh_addr"] = matched.get("addr", "") or ""
        result["sh_zip_code"] = format_zip_code(matched.get("zip_code"))
        result["sh_province"] = matched.get("zhou", "") or ""
        result["sh_city"] = matched.get("city", "") or ""
        result["sh_email"] = matched.get("email", "") or ""
        result["country_name"] = matched.get("country_name", "") or "美国"
        result["country_code"] = matched.get("country_code", "") or "US"
        result["found"] = True
    elif len(matched_warehouses) > 1:
        result["d_code"] = "TBD"
        result["fbx_name"] = "TBD"
        result["fbx_code"] = "TBD"
        result["addr_type"] = 2
        result["found"] = False
    else:
        result["d_code"] = "TBD"
        result["fbx_name"] = "TBD"
        result["fbx_code"] = "TBD"
        result["addr_type"] = 2
        result["found"] = False

    return result


def generate_boxgauge_list(fbano: str, yj_qty: int, po_number: str = "") -> list:
    """根据 fba_no 和 yj_qty 生成箱清单列表"""
    fbano_list = [f.strip() for f in fbano.split(",") if f.strip()]
    
    if len(fbano_list) == 1:
        single_fbano = fbano_list[0]
        if "FBA" in single_fbano.upper():
            return [
                {
                    "bgid": "",
                    "fba_no": single_fbano,
                    "container_no": f"{single_fbano}U{i:06d}",
                    "tracking_no": "",
                    "length": 0,
                    "width": 0,
                    "height": 0,
                    "qty": 1,
                    "build_date": "",
                    "singlebox_kg": 0,
                    "refid": po_number or "",
                }
                for i in range(1, int(yj_qty) + 1)
            ]
        else:
            return [
                {
                    "bgid": "",
                    "fba_no": single_fbano,
                    "container_no": single_fbano,
                    "tracking_no": "",
                    "length": 0,
                    "width": 0,
                    "height": 0,
                    "qty": int(yj_qty),
                    "build_date": "",
                    "singlebox_kg": 0,
                    "refid": po_number or "",
                }
            ]
    return []


def process_excel_to_json2(excel_content, is_binary=True, all_fbx_address: list = None, po_number: str = ""):
    if is_binary:
        bytes_io = BytesIO(excel_content)
        df = pd.read_excel(bytes_io)
    else:
        df = pd.read_excel(excel_content)
    df = df[~df.isin(df.columns).all(axis=1)]
    df = df[df["仓库"].notna()]
    df.columns = df.columns.str.strip()
    df["仓库代码"] = df["仓库"].astype(str).str.strip()
    df["订单单号"] = df["发货批次"].astype(str)
    df["数量"] = df["箱数"]
    df["重量"] = df["总实重"]
    df = df.dropna(how="all")

    df["进仓号"] = df["进仓号"].ffill()
    grouped = df.groupby("进仓号")

    result_list = []

    for name, group in grouped:
        fba_no = ",".join(group["订单单号"].unique())
        yj_qty = int(safe_float(group["数量"].sum()))
        yj_kg = round(safe_float(group["重量"].sum()), 2)
        a_number = group["进仓号"].dropna().unique()[0]
        take_goods = "\n".join(map(str, group["总箱数"].dropna().unique()))
        origin_code = group["仓库代码"].dropna().unique()[0]

        converted = {"d_code": origin_code, "found": False}
        if all_fbx_address:
            converted = convert_search_code(origin_code, all_fbx_address)

       
        current_boxgauge = []
        for sub_fba, sub_df in group.groupby("订单单号"):
            sub_fba = str(sub_fba).strip()
            if not sub_fba:
                continue
            sub_qty = int(safe_float(sub_df["数量"].sum()))
            current_boxgauge.extend(generate_boxgauge_list(sub_fba, sub_qty, po_number))

        result = {
            "order": {
                "fba_no": fba_no,
                "yj_qty": yj_qty,
                "yj_kg": yj_kg,
                "have_no": a_number,
                "packing": take_goods,
                "d_code": converted["d_code"],
                "fbx_name": converted.get("fbx_name", ""),
                "fbx_code": converted.get("fbx_code", ""),
                "sh_addr": converted.get("sh_addr", ""),
                "sh_zip_code": converted.get("sh_zip_code", ""),
                "sh_province": converted.get("sh_province", ""),
                "sh_city": converted.get("sh_city", ""),
                "sh_email": converted.get("sh_email", ""),
                "country_name": converted.get("country_name", "美国"),
                "country_code": converted.get("country_code", "US"),
                "addr_type": converted.get("addr_type", 0),
            },
            "boxgauge_list": current_boxgauge,
        }

        result_list.append(result)

    return result_list

# hubs_client = HubsClient()
# all_fbx_address = hubs_client.get_base_fbx_addr()

# data = process_excel_to_json2(
#     r"C:\Users\a1337\Desktop\数据汇总-赫泊斯美空1.12.xlsx",
#     False,
#     all_fbx_address=all_fbx_address,
# )

# # print(data)

# hubs_client.update_bigwaybill_order(data[0]['have_no'], data[0])
