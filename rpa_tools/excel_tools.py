

from datetime import datetime

from pathlib import Path

import win32com.client
def column_letter_to_number(column_letter):
    """将列字母转换为对应的列号（从1开始）。"""
    number = 0
    for letter in column_letter:
        number = number * 26 + (ord(letter.upper()) - ord('A') + 1)
    return number
def write_data_to_excel(visible: bool, data, start_row: int, start_column: str, sheet_name:str,file_path:str):
    excel = get_excel_application()
    excel.Visible = visible  # 根据参数决定是否可视化Excel

    # workbook_path = Path(r"D:\YD数据\报关费-广州航捷\广州航捷-赫伯斯-初审.xlsx")
    workbook_path = Path(rf"{file_path}")


    # 打开工作簿
    wb = excel.Workbooks.Open(str(workbook_path))

    # 访问指定的工作表，可以通过名称或索引指定
    sheet = wb.Sheets(sheet_name)

    # 转换列字母到数字
    start_col_num = column_letter_to_number(start_column)

    # 从start_row行和start_col_num列开始写入二维list数据
    for i, row in enumerate(data, start_row):
        for j, value in enumerate(row):
            col_num = start_col_num + j
            sheet.Cells(i, col_num).Value = value

    # 生成新文件名，包含当前时间戳
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    new_file_name = workbook_path.stem + "_" + timestamp + workbook_path.suffix
    new_file_path = workbook_path.parent / new_file_name

    # 另存为新文件
    wb.SaveAs(str(new_file_path))

    # 关闭工作簿
    wb.Close()
    excel.Quit()
def get_excel_application():
    try:

        # 尝试连接到 WPS Office 电子表格
        # excel = win32com.client.gencache.EnsureDispatch('Ket.Application')
        excel = win32com.client.Dispatch('Ket.Application')

        print("Connected to WPS Office.")
    except Exception as e:
        print("Trying to connect to Microsoft Excel because:", str(e))
        # 如果连接 WPS 失败，尝试连接到 Microsoft Excel
        try:
            excel = win32com.client.Dispatch('Excel.Application')
            print("Connected to Microsoft Excel.")
        except Exception as ex:
            raise Exception("Neither WPS Office nor Microsoft Excel is installed.") from ex
    return excel