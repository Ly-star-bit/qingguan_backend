'''
 # @ Author: luoyu
 # @ Create Time: 2024-05-13 09:46:01
 # @ Modified by: luoyu
 # @ Modified time: 2024-05-13 09:46:43
 # @ Description: 上海平政报关费核对
 '''

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





def check_missing_worknums(work_num_str, dahuo_data):
    # 将逗号分隔的字符串转换为工作号集合
    expected_worknums = set(work_num_str.strip().split(','))

    # 创建一个集合来存储dahuo_data中存在的工作号
    existing_worknums = {worknum["operNo"] for worknum in dahuo_data}

    # 找出缺失的工作号
    missing_worknums = expected_worknums - existing_worknums

    # 返回不存在的工作号
    if missing_worknums:
        logger.info(f"缺失的大货号: {', '.join(missing_worknums)}")
        return list(missing_worknums)
    else:
        # logger.info("所有大货号都存在")
        return []





 
  

def zongdan_filter_json_data(data_list, orderno=None, billno=None):
    filtered_data = [
        data for data in data_list
        if (orderno is None or data.get('orderno') == orderno)
        and (billno is None or data.get('billno') == billno)
    ]
    return filtered_data



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

    try:

        # 使用 ExcelFile 来打开文件
        xls = pd.ExcelFile(file_path)

        # 现在使用 ExcelFile 对象来指定 sheet_name
        sample_sheet = pd.read_excel(xls, sheet_name='sample',skiprows=2)
        # #系数
        # xishu = sample_sheet.columns[-6]
        all_fix_data = []
        for index,row in sample_sheet.iterrows():
            row = row.fillna(0).infer_objects(copy=False)
            # logger.info(row["运单号"])
            if not row["运单号"]:
                continue
            fix_data = {

                #提单号
                "billno":"",
                #提单号是否存在
                "billno_exist": "不存在",
                #大货单号
                "dahuo_no":"",
                #大货单号是否存在
                "dahuo_no_exist":"",
                  #A#是否属于提单号
                "A#_right":False,
                #报关方式
                "custom_type":"",
                #需要分摊的a#个数
                "need_fentan_a#_count":0
              

            }



            #运单号
            origin_billno = str(row.iloc[2])
            
            if  "_" in origin_billno:
                #提单号
                bill_no = origin_billno.split("_")[0][:3] + "-"  + origin_billno.split("_")[0][3:]
                fix_data["billno"] = bill_no
                #分单号，大货单号
                work_num_str = origin_billno.split("_")[1]
                if "A" in work_num_str:
                    fix_data["dahuo_no"] = work_num_str
                else:
                    work_num_str = "A" + work_num_str
                    fix_data["dahuo_no"] = work_num_str


                #判断提单号是否存在
                filter_zongdan_data = zongdan_filter_json_data(zongdan_json_data,billno=bill_no)
                if filter_zongdan_data:
                    # logger.info(f"{bill_no}存在")
                    fix_data["billno_exist"] = "存在"
                # dahuo_data = dahuoyundan_worknum_search(work_num_str,api_context=api_request,sign_type="多单号")
                dahuo_data = [data for data in dahuo_all_data if data.get('operNo') in set(work_num_str.split(','))]
                #判断分单号是否存在
                missing = check_missing_worknums(work_num_str, dahuo_data)
                if missing:
                    logger.warning(f"存在没有大货号{missing}")
                    fix_data["dahuo_no_exist"] = "不存在"
                    #录入 ‘报错’ 到空单元格
                    all_fix_data.append(fix_data)
                    continue
                fix_data["dahuo_no_exist"] = "存在"
                #a#是否属于提单号
                work_num_list = work_num_str.split(',')
                dahuo_data_bill_no_list = [i['operNo'] for i in [data for data in dahuo_all_data if data.get('billno') in set(bill_no.split(','))]]
                # 将列表转换为集合并比较
                # 将列表转换为集合并比较
                if set(work_num_list).issubset(set(dahuo_data_bill_no_list)):
                    fix_data["A#_right"] = True
                else:
                    fix_data["A#_right"] = False
                #获取报关方式
                CustomsDeclaration = {i["CustomsDeclaration"] for i in dahuo_data}
                fix_data["custom_type"] =    ",".join(list(CustomsDeclaration))

                #需要分摊的个数
                fix_data["need_fentan_a#_count"] = len(work_num_list)
                #需要分摊的A#

            else:
                #提单号
                bill_no = origin_billno.strip()[:3] + "-"  + origin_billno.strip()[3:]
                fix_data["billno"] = bill_no
                #判断提单号是否存在
                filter_zongdan_data = zongdan_filter_json_data(zongdan_json_data,billno=bill_no)
                if filter_zongdan_data:
                    # logger.info(f"{bill_no}存在")
                    fix_data["billno_exist"] = "存在"
                if row["备注"] == '买单费':
                    # dahuo_data = dahuoyundan_worknum_search(bill_no,api_request,"提单号")
                    dahuo_data = [data for data in dahuo_all_data if data.get('billno') in set(bill_no.split(','))]

                    if dahuo_data:
                        ##需要分摊的个数
                        fix_data["need_fentan_a#_count"] = len(dahuo_data)
                        #需要分摊的A#
                  
                    
            all_fix_data.append(fix_data)
            # 确保数据类型兼容性，将值转换为字符串类型
            values_list = [str(value) for value in fix_data.values()]
            # 使用 .astype(object) 确保列类型兼容性，避免 FutureWarning
            for i, value in enumerate(values_list):
                sample_sheet.iloc[index, 22 + i] = value
        #写入sample表
        all_data = [list(i.values())for i in all_fix_data]
        new_file_path = write_data_to_excel_aspose(data=all_data,start_row=4,start_column="w",sheet_name="sample",file_path=file_path,save_new_file=True)

        #分摊
        new_sample_sheet = pd.read_excel(rf"{new_file_path}",sheet_name="sample",skiprows=2)
        new_sample_sheet = new_sample_sheet.fillna("")
        # new_sample_sheet.to_dict(orient='records')
        fentan_results = []
        for data in  new_sample_sheet.to_dict(orient='records'):
            fentan_data = {
                "总费用":data["小计"],
                "个数":data["需要分摊的A#个数"],
                "需要分摊的A":data["抽A#"],
                "已分摊的费用":0.0
            }

            if fentan_data["个数"] == 0 or not fentan_data["个数"] or not fentan_data["总费用"]:
                # logger.info(data["运单号"],"没有需要分摊的")
                continue


            fentan_money = round(fentan_data["总费用"]/fentan_data["个数"],2)
            
            if "," in fentan_data["需要分摊的A"]:
                splited_list = fentan_data["需要分摊的A"].split(",")
            elif not fentan_data["需要分摊的A"]:
                splited_list = []
            else:
                splited_list = [fentan_data["需要分摊的A"]]
            for loop_item_index, loop_item in enumerate(splited_list):
                if len(splited_list) == 1:
                    fentan_money = fentan_data["总费用"]
                elif loop_item_index + 1 == len(splited_list):
                    fentan_money = fentan_data["总费用"] - fentan_data["已分摊的费用"]
                else:
                    fentan_money = fentan_money
                    fentan_data["已分摊的费用"] += fentan_money 

                fentan_results.append(['应付',loop_item,'SHPZ-上海平政','OCLR-起运港出口报关费',fentan_money,'',1,'RMB',1])

        write_data_to_excel_aspose(data=fentan_results,start_row=2,start_column="A",sheet_name="报关费分摊模板",file_path=new_file_path)
        return new_file_path

    finally:
        pass
        

if __name__ == "__main__":
    execute(file_path = r"C:\Users\a1337\Downloads\账单-上海平政-2507.xlsx")