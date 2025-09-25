from datetime import datetime
import pandas as pd
import os
from pathlib import Path

def process_data(history_file, directory, output_file):
    """
    处理数据，将历史明细数据与指定目录下的Excel文件数据合并，并保存到新的Excel文件。

    Args:
        history_file (str): 历史明细Excel文件路径。
        directory (str): 包含带有主单号Excel文件的目录路径。
        output_file (str): 输出Excel文件路径。
    """

    # 读取历史明细数据
    history_df = pd.read_excel(history_file, sheet_name='历史明细')
    # history_df = history_df.groupby('主单号').first().reset_index()  # 根据主单号分组，取第一个

    # 存储合并后的数据
    merged_data = []

    # 遍历目录下的所有Excel文件
    for file_path in Path(directory).glob('*.xlsx'):
        # 获取文件修改时间
        file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        
        # 检查文件是否在2025年1月
        if file_modified_time.year != 2025 or file_modified_time.month != 1:
            continue

        # 从文件名中提取主单号
        file_name = file_path.name
        try:
            parts = file_name.split('-')
            if len(parts) > 1:
                master_bill_no = parts[1]+'-' + parts[2].replace('CI&PL','').strip()
            else:
                master_bill_no = None
        except:
            continue

        # 查找匹配的历史明细数据
        matching_history = history_df[history_df['主单号'] == master_bill_no].copy()

        if not matching_history.empty:
            try:
                # 读取CIV sheet
                civ_df = pd.read_excel(file_path, sheet_name='CIV', header=12)
                civ_df = civ_df.iloc[:, [1, 3, 5, 6]]
                civ_df.columns = ['hscode', '数量', '单价', '总价']
                civ_df = civ_df[civ_df['hscode'].notna()]

                # 读取PL sheet
                pl_df = pd.read_excel(file_path, sheet_name='PL', header=11)
                pl_df = pl_df.iloc[:, [1, 5, 7, 8, 9]]
                pl_df.columns = ['hscode', 'carton', 'net_weight', 'gross_weight', 'M3']
                pl_df = pl_df[pl_df['hscode'].notna()]

                # 合并CIV和PL数据
                merged_df = pd.merge(civ_df, pl_df, left_on='hscode', right_on='hscode', how='inner')

                # 将合并后的数据添加到历史明细数据中
                for history_index, history_row in matching_history.iterrows():
                    # 获取当前历史记录的 HS Code
                    hs_code = history_row['HS Code']

                    # 在 merged_df 中找到 col2_civ (或 col2_pl) 与 HS Code 相匹配的行
                    matching_merged_rows = merged_df[merged_df['hscode'] == hs_code]

                    # 如果找到了匹配的行，则合并数据
                    if not matching_merged_rows.empty:
                        for _, merged_row in matching_merged_rows.iterrows():
                            combined_data = {**history_row.to_dict(), **merged_row.to_dict()}
                            merged_data.append(combined_data)
                    else:
                        # 如果没有找到匹配的行，也把history_row放进去，避免数据丢失
                        combined_data = history_row.to_dict()
                        merged_data.append(combined_data)

            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

    # 创建DataFrame
    result_df = pd.DataFrame(merged_data)

    # 保存到新的Excel文件
    result_df.to_excel(output_file, index=False)
    print(f"Data merged and saved to {output_file}")


if __name__ == '__main__':
    import clr
    print(clr.__file__)
    print(dir(clr))
