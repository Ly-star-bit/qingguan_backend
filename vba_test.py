import pandas as pd
import traceback
import os
from datetime import datetime

from app.utils_aspose import shenzhen_air_pdf_gennerate, shenzhen_customes_pdf_gennerate
from rpa_tools.email_tools import send_email
from rpa_tools.utils import find_playwright_node_path
from morelink_api import MoreLinkClient

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

    grouped = (
        df.groupby("提单号")
        .agg(
            {
                "品名": "first",
                "提单发货人": "first",
                "提单收货人": "first",
                "箱数": "sum",
                "毛重/(KG)": "sum",
                "体积/(立方米)": "sum",
            }
        )
        .reset_index()
    )

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
            }
        )

    return result


def generate_pdf_from_excel(file_path):
    """从 Excel 生成 PDF"""
    excel_data = process_excel(file_path)
    print(f"处理完成，共{len(excel_data)}条数据")

    # 初始化 Morelink 客户端
    node_path = find_playwright_node_path()
    morelink_client = MoreLinkClient(node_path)
    morelink_data = morelink_client.zongdan_api_httpx()

    if not morelink_data:
        print("Morelink 数据获取失败")
        return

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
    failed_bills = []  # 记录失败的提单号

    for item in excel_data:
        try:
            master_bill_no = item["master_bill_no"].strip()
            is_air = "-" in master_bill_no  # 有 - 是空运，没有是海运

            # 检查 PDF 是否已存在
            pdf_path = f"./pdf/customs/{master_bill_no}.pdf"
            if os.path.exists(pdf_path):
                # print(f"PDF 已存在，跳过：{master_bill_no}")
                success_count += 1
                continue

            # 从 Morelink 查询
            filter_data = [
                row for row in morelink_data if row.get("billno") == master_bill_no
            ]

            if not filter_data:
                print(f"提单号搜索不到：{master_bill_no}")
                fail_count += 1
                failed_bills.append(master_bill_no)
                continue

            # 字段映射
            mapped_data = []
            for data_item in filter_data:
                mapped_item = {
                    morelink_field_mapping.get(k, k): v for k, v in data_item.items()
                }
                mapped_data.append(mapped_item)

            # 生成 PDF
            if is_air:
                # 空运 - 使用 shenzhen_air_pdf_gennerate
                atd_value = mapped_data[0].get("ATD")
                atd_formatted = convert_date_format(atd_value) if atd_value else ""

                input_data = [
                        master_bill_no,
                        master_bill_no,
                        "",
                        "",
                        "",
                        item["shipper_name"],
                        "",
                        item["receiver_name"],
                        mapped_data[0]["startland"],
                        mapped_data[0]["destination"],
                        mapped_data[0]['shipcompany'],
                        atd_formatted,
                        mapped_data[0]["flight"],
                        item["total_boxes"],
                        item["gross_weight"],
                        "",
                        item["volume"],
                        'CLOTHING',
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                    ]
                pdf_file = shenzhen_air_pdf_gennerate(input_data)
            else:
                # 海运 - 使用 shenzhen_customes_pdf_gennerate(data, mapped_data)
                request_data = {
                    "shipper_name": item["shipper_name"],
                    "receiver_name": item["receiver_name"],
                    "master_bill_no": master_bill_no,
                    "gross_weight": item["gross_weight"],
                    "volume": item["volume"],
                    "total_boxes": item["total_boxes"],
                    "all_english_name": "CLOTHING",
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

    # 输出所有失败的提单号
    if failed_bills:
        print("\n========== 失败的提单号列表 ==========")
        for bill_no in failed_bills:
            print(f"  - {bill_no}")
        print(f"=====================================")
        print(f"失败提单号共计：{len(failed_bills)} 个")


if __name__ == "__main__":
    generate_pdf_from_excel(r"C:\Users\a1337\Desktop\赫泊斯-(1123).xlsx")
