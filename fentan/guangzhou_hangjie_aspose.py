"""
# @ Author: luoyu
# @ Create Time: 2024-05-13 09:46:01
# @ Modified by: luoyu
# @ Modified time: 2024-05-13 09:46:43
# @ Description: 广州航捷报关费核对
"""

import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
from loguru import logger
import clr
from rpa_tools.utils.morelink_api import MoreLinkClient

from rpa_tools import find_playwright_node_path

# Add the AsposeCells_net directory to the path
aspose_dir = os.path.join(os.getcwd(), "AsposeCells_net")

license_path = os.path.join(aspose_dir, "Aspose.Total.NET.lic")
sys.path.append(aspose_dir)
sys.path.append(os.path.join(aspose_dir, "net40"))
# Load Aspose.Cells.dll
clr.AddReference("Aspose.Cells")

# Import Aspose.Cells namespaces
from Aspose.Cells import Workbook, SaveFormat,License
# # 创建License对象
# license = License()

# # 设置许可证
# license.SetLicense(license_path)
# print("Aspose.Cells .NET 许可证设置成功！")

def column_letter_to_number(column_letter):
    """将列字母转换为对应的列号（从1开始）。"""
    number = 0
    for letter in column_letter:
        number = number * 26 + (ord(letter.upper()) - ord("A") + 1)
    return number


def write_data_to_excel_aspose(
    data,
    start_row: int,
    start_column: str,
    sheet_name: str,
    file_path: str,
    save_new_file: bool = False,
):
    """
    使用 Aspose.Cells.NET 写入 Excel 文件，取代 win32com 的实现。

    Args:
        data (list of list): 要写入的二维列表数据。
        start_row (int): 开始写入的行号（1-based）。
        start_column (str): 开始写入的列名（如 "A"）。
        sheet_name (str): 工作表名称。
        file_path (str): Excel 文件路径。
        save_new_file (bool): 是否另存为新文件，默认 False。

    Returns:
        str: 保存后的文件路径（如果是新文件）。
    """

    # 加载现有工作簿
    wb = Workbook(file_path)

    # 获取或创建工作表
    try:
        sheet = wb.Worksheets[sheet_name]
    except Exception:
        # 如果工作表不存在，新建一个
        sheet = wb.Worksheets.Add(sheet_name)

    # 将列字母转为数字（1-based）
    start_col_num = column_letter_to_number(start_column)

    # 写入数据
    for i, row_data in enumerate(data):
        for j, value in enumerate(row_data):
            sheet.Cells[start_row + i - 1, start_col_num + j - 1].PutValue(value)

    # 保存文件
    output_path = None
    if save_new_file:
        base, ext = os.path.splitext(file_path)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_path = f"{base}_{timestamp}{ext}"
        wb.Save(output_path, SaveFormat.Xlsx)
    else:
        wb.Save(file_path, SaveFormat.Xlsx)

    return output_path if save_new_file else file_path





def check_missing_worknums(work_num_str, dahuo_data):
    # 将逗号分隔的字符串转换为工作号集合
    expected_worknums = set(work_num_str.strip().split(","))

    # 创建一个集合来存储dahuo_data中存在的工作号
    existing_worknums = {worknum["operNo"] for worknum in dahuo_data}

    # 找出缺失的工作号
    missing_worknums = expected_worknums - existing_worknums

    # 返回不存在的工作号
    if missing_worknums:
        logger.info(f"缺失的大货号: {', '.join(missing_worknums)}")
        return list(missing_worknums)
    else:
        logger.info("所有大货号都存在")
        return []






def zongdan_filter_json_data(data_list, orderno=None, billno=None):
    filtered_data = [
        data
        for data in data_list
        if (orderno is None or data.get("orderno") == orderno)
        and (billno is None or data.get("billno") == billno)
    ]
    return filtered_data


def get_with_default(container, key, default):
    value = container.get(key, default)
    return default if value == 0.0 else value



def execute(file_path):
    client = MoreLinkClient(node_path=find_playwright_node_path())


    zongdan_json_data = client.zongdan_api_httpx()
    end_date = datetime.now()
    # 获取过去六个月的大货数据
    start_date = end_date - relativedelta(months=6)
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date_str = start_date.strftime("%Y-%m-%d")
    dahuo_all_data = client.dahuodingdan_all_data(
        start_date_str=start_date_str, end_date_str=end_date_str
    )

    # filter_data = zongdan_filter_json_data(zongdan_json_data,billno="999-05966881")
    # print(123)
    # file_path = r"D:\YD数据\报关费-广州航捷\广州航捷-赫伯斯-初审.xlsx"

    # 使用 ExcelFile 来打开文件
    xls = pd.ExcelFile(file_path)

    # 现在使用 ExcelFile 对象来指定 sheet_name
    sample_sheet = pd.read_excel(xls, sheet_name="sample", skiprows=2)
    # 系数
    xishu = sample_sheet.columns[-6]
    all_fix_data = []
    for index, row in sample_sheet.iterrows():
        row = row.fillna(0).infer_objects(copy=False)
        if not row["提单号"]:
            continue
        fix_data = {
            "费用": row["费用"],
            "need_fentan_A_dict": [],
            # 原始提单号
            "origin_billno": "",
            # 提单号
            "billno": "",
            # 提单号是否存在
            "billno_exist": "不存在",
            # 总KG
            "all_kg": "",
            # 大货单号
            "dahuo_no": "",
            # 大货单号是否存在
            "dahuo_no_exist": "",
            # 大货单号KG
            "dahuo_no_kg": "",
            "n*q": "",
            # 报关方式
            "custom_type": "",
            # 重量差
            "weight_diff": "",
            # 报关方式是否正确
            "custom_type_right": True,
            # A#是否正确
            "A#_right": True,
            # 需要分摊的A#
            "need_fentan_A#": "",
        }

        # 总kg
        all_kilogram = row.iloc[3]
        # 提单号
        origin_billno = str(row.iloc[1])
        fix_data["origin_billno"] = origin_billno
        if isinstance(origin_billno, str) and "_" in origin_billno:
            origin_billno = str(origin_billno).strip().replace("，", ",")

            bill_no = (
                origin_billno.split("_")[0][:3] + "-" + origin_billno.split("_")[0][3:]
            )
            fix_data["billno"] = bill_no
            # 分单号，大货单号
            work_num_str = origin_billno.split("_")[1]
            fix_data["dahuo_no"] = work_num_str

            # 判断提单号是否存在
            filter_zongdan_data = zongdan_filter_json_data(
                zongdan_json_data, billno=bill_no
            )
            if filter_zongdan_data:
                logger.info(f"{bill_no}存在")
                fix_data["billno_exist"] = "存在"
            # dahuo_data = dahuoyundan_worknum_search(work_num_str,api_context=api_request,sign_type="多单号")
            dahuo_data = [
                data
                for data in dahuo_all_data
                if data.get("operNo") in set(work_num_str.split(","))
            ]

            # 判断分单号是否存在
            missing = check_missing_worknums(work_num_str, dahuo_data)
            if missing:
                logger.warning(f"存在没有大货号{missing}")
                fix_data["dahuo_no_exist"] = f"{missing}不存在"
                # 录入 ‘报错’ 到空单元格
            else:
                fix_data["dahuo_no_exist"] = "存在"
            # 判断origin_billno中是否包含A2
            if "A2" in origin_billno:
                # 获取报关方式
                CustomsDeclaration = {
                    ("买单" if i["CustomsDeclaration"] == "无单证" else "单证")
                    for i in dahuo_data
                }

                fix_data["custom_type"] = ",".join(list(CustomsDeclaration))
                # 报关方式判断是否正确
                if ",".join(list(CustomsDeclaration)) != row["类型"]:
                    # 默认就是True
                    fix_data["custom_type_right"] = False
                # 获取大货单号的总KG
                all_kilogram_dahuo = sum(get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0)) for i in dahuo_data)
                fix_data["dahuo_no_kg"] = all_kilogram_dahuo
                # 大货号总kg和execl中总kg之差
                weight_diff = all_kilogram_dahuo - all_kilogram
                fix_data["weight_diff"] = weight_diff
                # 判断 A#是否正确
                work_num_list = work_num_str.split(",")
                dahuo_data_bill_no_list = [
                    i["operNo"]
                    for i in [
                        data
                        for data in dahuo_all_data
                        if data.get("billno") in set(bill_no.split(","))
                    ]
                ]
                # 将列表转换为集合并比较
                if set(work_num_list) != set(dahuo_data_bill_no_list):
                    fix_data["A#_right"] = False
                else:
                    fix_data["A#_right"] = True

                dahuo_data_bill_no = ",".join([i["operNo"] for i in dahuo_data])
                # 分摊A
                fix_data["need_fentan_A#"] = dahuo_data_bill_no
                #
                fix_data["need_fentan_A_dict"] = [
                    [i["operNo"], get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0))] for i in dahuo_data
                ]

            elif "A2" not in origin_billno and row["类型"] == "买单":
                # 获取单货运单中报关方式 为'无单证' 的 A#的总KG求和
                all_kilogram_dahuo = sum(
                    get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0))
                    for i in dahuo_data
                    if i["CustomsDeclaration"] == "无单证"
                )
                fix_data["all_kg"] = all_kilogram_dahuo

                # 获取所有需要分摊的A#
                fix_data["need_fentan_A#"] = ",".join([i["operNo"] for i in dahuo_data])
                fix_data["need_fentan_A_dict"] = [
                    [i["operNo"], get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0))] for i in dahuo_data
                ]

                # 重量差N-D
                weight_diff = all_kilogram_dahuo - all_kilogram
                fix_data["weight_diff"] = weight_diff
            elif "A2" not in origin_billno and row["类型"] == "单证":
                # 获取单货运单中报关方式 不为'无单证' 的 A#，然后删除原来O列的A#大货号，求剩余的A#的总KG
                # 1.之前处理过的billno
                index_front_bill_no_list = sample_sheet.loc[:index, "提单号"].tolist()
                already_process_worknums = []
                for i in index_front_bill_no_list:
                    i_billno = i.split("_")[0][:3] + "-" + i.split("_")[0][3:]
                    if i_billno == bill_no:
                        i_work_nums = i.split("_")[0][1]
                        already_process_worknums.extend(",".join(i_work_nums))
                all_kilogram_dahuo = sum(
                    get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0))
                    for i in dahuo_data
                    if i["CustomsDeclaration"] == "无单证"
                    and i["operNo"] not in already_process_worknums
                )
                fix_data["all_kg"] = all_kilogram_dahuo
                # 获取所有需要分摊的A#
                fix_data["need_fentan_A#"] = ",".join([i["operNo"] for i in dahuo_data])
                fix_data["need_fentan_A_dict"] = [
                    [i["operNo"], get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0))] for i in dahuo_data
                ]

                # 重量差N-D
                weight_diff = all_kilogram_dahuo - all_kilogram
                fix_data["weight_diff"] = weight_diff
        else:
            origin_billno = str(origin_billno).strip().replace("，", ",")
            bill_no = (
                origin_billno.split("_")[0][:3] + "-" + origin_billno.split("_")[0][3:]
            )
            fix_data["billno"] = bill_no

            # 判断提单号是否存在
            filter_zongdan_data = zongdan_filter_json_data(
                zongdan_json_data, billno=bill_no
            )
            if filter_zongdan_data:
                logger.info(f"{bill_no}存在")
                fix_data["billno_exist"] = "存在"

                dahuo_data = [
                    data
                    for data in dahuo_all_data
                    if data.get("billno") in set(bill_no.split(","))
                ]

            if "A2" not in origin_billno and row["类型"] == "买单":
                # 获取单货运单中报关方式 为'无单证' 的 A#的总KG求和
                all_kilogram_dahuo = sum(
                    get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0))
                    for i in dahuo_data
                    if i["CustomsDeclaration"] == "无单证"
                )
                fix_data["all_kg"] = all_kilogram_dahuo

                # 获取所有需要分摊的A#
                fix_data["need_fentan_A#"] = ",".join([i["operNo"] for i in dahuo_data])
                fix_data["need_fentan_A_dict"] = [
                    [i["operNo"], get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0))] for i in dahuo_data
                ]

                # 重量差N-D
                weight_diff = all_kilogram_dahuo - all_kilogram
                fix_data["weight_diff"] = weight_diff
            elif "A2" not in origin_billno and row["类型"] == "单证":
                # 获取单货运单中报关方式 不为'无单证' 的 A#，然后删除原来O列的A#大货号，求剩余的A#的总KG
                # 1.之前处理过的billno
                index_front_bill_no_list = sample_sheet.loc[
                    : index - 1, "提单号"
                ].tolist()
                already_process_worknums = []
                for i in index_front_bill_no_list:
                    if origin_billno not in str(i):
                        continue
                    i_billno = i.split("_")[0][:3] + "-" + i.split("_")[0][3:]
                    if i_billno == bill_no:
                        i_work_nums = i.split("_")[0][1]
                        already_process_worknums.extend(",".join(i_work_nums))
                all_kilogram_dahuo = sum(
                    [
                        get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0))
                        for i in dahuo_data
                        if i["CustomsDeclaration"] == "无单证"
                        and i["operNo"] not in already_process_worknums
                    ]
                )
                fix_data["all_kg"] = all_kilogram_dahuo

                # 获取所有需要分摊的A#
                fix_data["need_fentan_A#"] = ",".join([i["operNo"] for i in dahuo_data])
                fix_data["need_fentan_A_dict"] = [
                    [i["operNo"], get_with_default(i, "ckweight", get_with_default(i, "yjweight", 0))] for i in dahuo_data
                ]

                # 重量差N-D
                weight_diff = all_kilogram_dahuo - all_kilogram
                fix_data["weight_diff"] = weight_diff

        all_fix_data.append(fix_data)
    all_data = [list(i.values())[3:] for i in all_fix_data]
    new_file_path = write_data_to_excel_aspose(
        all_data, 4, "L", "sample", file_path=file_path, save_new_file=True
    )
    # 以下为分摊
    all_fentan_data = [
        [
            "费用类型",
            "SO号/工作单号/参考号",
            "客户名称",
            "费用名称",
            "单价",
            "单位",
            "数量",
            "币种",
            "汇率",
            "备注",
        ]
    ]
    for row in all_fix_data:
        if row["need_fentan_A#"]:
            fentan_a_num_list = row["need_fentan_A_dict"]
            total_price = row["费用"]
            all_kg_actually = row["dahuo_no_kg"]
            fentan_data_list = []
            all_money = 0
            for i in fentan_a_num_list[:-1]:  # Exclude the last element for now
                fendan_data = [
                    "应付",
                    i[0],
                    "GZHJ-广州航捷",
                    "OCLR-起运港出口报关费",
                    round(total_price / all_kg_actually * float(i[1]), 2) if all_kg_actually != 0 else 0,
                    "",
                    1,
                    "RMB",
                    1,
                ]
                fentan_data_list.append(fendan_data)
                all_money += fendan_data[4]

            # Process the last element separately to ensure the total matches
            if fentan_a_num_list:
                last_element = fentan_a_num_list[-1]
                last_fendan_data = [
                    "应付",
                    last_element[0],
                    "GZHJ-广州航捷",
                    "OCLR-起运港出口报关费",
                    round(
                        total_price - all_money, 2
                    ),  # Adjust the last amount to match total_price
                    "",
                    1,
                    "RMB",
                    1,
                ]
                fentan_data_list.append(last_fendan_data)
                all_money += last_fendan_data[4]

            all_fentan_data.extend(fentan_data_list)

    write_data_to_excel_aspose(
        data=all_fentan_data,
        start_row=1,
        start_column="A",
        sheet_name="报关费分摊模板",
        file_path=new_file_path,
        save_new_file=False,
    )
    return new_file_path


if __name__ == "__main__":

    execute(file_path=r"C:\Users\a1337\Downloads\广州航捷-赫泊斯2025年6月账单.xlsx")
