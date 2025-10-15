import time
import pandas as pd

from rpa_tools import find_playwright_node_path
from rpa_tools.utils.morelink_api import MoreLinkClient
from .download_maitou import maitou_export_api,fujian_maitou_export_api
from .shouli import main_shouli
from loguru import logger
def execute_tools(tools: list,success_data: list,origin_file_path:str,client_name:str):
    node_path = find_playwright_node_path()
    morelink_client = MoreLinkClient(node_path)
    a_number_list = [i["A单号"] for i in success_data]

    
    a_number_output_file = None
    maitou_path = None
    if 'A单号'  in tools:
        # 保存结果到新的 Excel 文件
        # 将数据转换为 DataFrame
        df = pd.DataFrame(success_data)

        # 保存为 Excel 文件
        a_number_output_file = f"./excel_file/{client_name}_{time.time()}.xlsx"
        with pd.ExcelWriter(a_number_output_file) as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)

            # 读取原始Excel文件的默认sheet
            df_origin = pd.read_excel(origin_file_path)

            # 将a_number_list添加到原始数据的DataFrame中
            # df_origin['A单号'] = a_number_list

            # 将原始数据写入到新的Excel文件的Sheet2中
            df_origin.to_excel(writer, sheet_name='原始文件', index=False)
    if '唛头' in tools:
        try:
            # 唛头
            
            maitou_path = maitou_export_api(
                    a_number_list, morelink_client,client_name
                )
        except Exception as e:
            logger.error(f"唛头导出失败: {e}")
            maitou_path = None
    if '装箱' in tools:
        main_shouli(success_data)
    return a_number_output_file,maitou_path




