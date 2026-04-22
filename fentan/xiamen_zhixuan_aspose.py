"""
# @ Author: luoyu
# @ Create Time: 2026-04-22
# @ Description: 厦门致萱报关费分摊
"""

import os
import sys
from datetime import datetime

# Add the AsposeCells_net directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
aspose_dir = os.path.join(current_dir, "..", "AsposeCells_net")
sys.path.append(aspose_dir)
sys.path.append(os.path.join(aspose_dir, "net40"))
import clr
dll_path = os.path.abspath(os.path.join(aspose_dir, "net40", "Aspose.Cells.dll"))
clr.AddReference(dll_path)
from Aspose.Cells import Workbook, SaveFormat, License

# 设置许可证
license_path = os.path.join(aspose_dir, "Aspose.Total.NET.lic")
license = License()
license.SetLicense(license_path)


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
    """使用 Aspose.Cells.NET 写入 Excel 文件。"""
    wb = Workbook(file_path)
    try:
        sheet = wb.Worksheets[sheet_name]
    except Exception:
        sheet = wb.Worksheets.Add(sheet_name)

    start_col_num = column_letter_to_number(start_column)

    for i, row_data in enumerate(data):
        for j, value in enumerate(row_data):
            sheet.Cells[start_row + i - 1, start_col_num + j - 1].PutValue(value)

    output_path = None
    if save_new_file:
        base, ext = os.path.splitext(file_path)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_path = f"{base}_{timestamp}{ext}"
        wb.Save(output_path, SaveFormat.Xlsx)
    else:
        wb.Save(file_path, SaveFormat.Xlsx)

    return output_path if save_new_file else file_path


def xiamen_zhixuan_fentan_execute(file_path: str):
    """厦门致萱报关费分摊主函数"""
    wb = Workbook(file_path)

    # ========== 读取 outfile1 sheet ==========
    sheet = wb.Worksheets["outfile1"]
    max_row = sheet.Cells.MaxRow
    max_col = sheet.Cells.MaxColumn  # 14

    # 列索引（0-based）:
    # 0: 单号, 1: 发货人, 2: 件数, 3: 重量kg, 4: 体积CBM
    # 5: 目的港, 6: 日期, 7: 目的港2, 8: 附加费, 9: 附加费金额
    # 10: 客户, 11: 合计, 12: 总合计, 13: A#大货号

    COL_DANHAO = 0
    COL_FAHUO = 1
    COL_JIANSHU = 2
    COL_ZHONGLIANG = 3
    COL_TIJI = 4
    COL_MUDIGANG = 5
    COL_RIQI = 6
    COL_MUDIGANG2 = 7
    COL_FUJIAFEI = 8
    COL_FUJIAFEI_JINE = 9
    COL_KEHU = 10
    COL_HEJI = 11
    COL_ZONGHEJI = 12
    COL_AHAO = 13

    # ========== 按单号分组 ==========
    # 每个单号有多行，每行都有独立的A#和体积
    # 总合计只在第一行
    all_groups = []
    i = 5  # 从第5行开始（1-indexed）

    while i <= max_row:
        # 读取单号
        danhao = sheet.Cells[i - 1, COL_DANHAO].Value
        if danhao is None or str(danhao).strip() == "":
            i += 1
            continue

        danhao = str(danhao).strip()

        # 找同一单号的所有连续行
        same_bill_rows = []
        j = i
        while j <= max_row:
            curr_danhao = sheet.Cells[j - 1, COL_DANHAO].Value
            if curr_danhao is None or str(curr_danhao).strip() == "":
                break
            if str(curr_danhao).strip() != danhao:
                break
            same_bill_rows.append(j)
            j += 1

        if len(same_bill_rows) < 1:
            i = j
            continue

        # 第一行有总合计，其他行没有
        first_row_idx = same_bill_rows[0]
        total_heji = sheet.Cells[first_row_idx - 1, COL_ZONGHEJI].Value

        if total_heji is None or str(total_heji).strip() == "":
            # 没有总合计，跳过这组
            i = j
            continue

        total_heji = float(total_heji)

        # 收集每行的A#和体积
        ahao_list = []
        detail_volumes = []
        for row_idx in same_bill_rows:
            ahao = sheet.Cells[row_idx - 1, COL_AHAO].Value
            tiji = sheet.Cells[row_idx - 1, COL_TIJI].Value
            if ahao:
                ahao_list.append(str(ahao).strip())
            if tiji is not None:
                detail_volumes.append(float(tiji))
            else:
                detail_volumes.append(0.0)

        total_tiji = sum(detail_volumes)

        # 获取客户名称（从第一行）
        kehu = ""
        kehu_val = sheet.Cells[first_row_idx - 1, COL_KEHU].Value
        if kehu_val:
            kehu = str(kehu_val).strip()

        all_groups.append({
            "danhao": danhao,
            "total_heji": total_heji,
            "total_tiji": total_tiji,
            "ahao_list": ahao_list,
            "detail_volumes": detail_volumes,
            "kehu": kehu,
        })

        i = j

    # ========== 生成分摊数据 ==========
    # 导入明细表头（11列，与原始文件一致）
    header = [
        "费用类型",           # A
        "SO号/工作单号/参考号",  # B
        "客户/供应商名称",      # C
        "费用名称",            # D
        "单位",               # E
        "单价",               # F
        "数量",               # G
        "币种",               # H
        "汇率",               # I
        "发票号码",            # J
        "备注",               # K
    ]
    all_fentan_data = [header]

    for group in all_groups:
        danhao = group["danhao"]
        total_price = group["total_heji"]
        total_tiji = group["total_tiji"]
        ahao_list = group["ahao_list"]
        detail_volumes = group["detail_volumes"]
        kehu = group["kehu"]

        if not ahao_list or total_tiji == 0:
            continue

        # 分摊数量校验
        if len(ahao_list) != len(detail_volumes):
            print(f"警告: {danhao} A#数({len(ahao_list)}) != 明细行数({len(detail_volumes)})")
            continue

        # 分摊计算
        all_money = 0
        for idx, ahao in enumerate(ahao_list):
            tiji = detail_volumes[idx]

            if idx < len(ahao_list) - 1:
                # 前N-1个按比例计算
                money = round(total_price / total_tiji * tiji, 2)
            else:
                # 最后一个用余数确保总额精确
                money = round(total_price - all_money, 2)

            all_money += money

            if not kehu:
                kehu = "厦门致萱供应链"

            fentan_row = [
                "应付",           # 费用类型
                ahao,             # SO号/工作单号/参考号
                kehu,             # 客户/供应商名称
                "报关费",         # 费用名称
                "",               # 单位（留空）
                money,            # 单价
                1,                # 数量
                "CNY",            # 币种
                1,                # 汇率
                "",               # 发票号码（留空）
                "",               # 备注
            ]
            all_fentan_data.append(fentan_row)

    # ========== 写入导入明细 sheet ==========
    output_path = write_data_to_excel_aspose(
        data=all_fentan_data,
        start_row=1,
        start_column="A",
        sheet_name="导入明细",
        file_path=file_path,
        save_new_file=True,
    )



    print(f"分摊完成，输出文件: {output_path}")
    print(f"共处理 {len(all_groups)} 个单号，分摊 {len(all_fentan_data) - 1} 条记录")


    return output_path


if __name__ == "__main__":
    file_path = r"C:\Users\a1337\Downloads\账单-厦门致萱-2603.xls"
    xiamen_zhixuan_fentan_execute(file_path)
