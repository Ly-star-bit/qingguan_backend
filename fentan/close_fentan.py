from datetime import datetime
import numpy as np
import pandas as pd
import os

def main(file_path):
    try:
        # 读取Excel文件
        df = pd.read_excel(file_path)

        # 删除B列中的空白行
        df.dropna(subset=['装柜单号'], inplace=True)
        
        # 获取需要分摊的列
        GQList = df['装柜单号'].unique().tolist()
        
        # 插入空列
        df.insert(len(df.columns), 'P', np.nan)
        
        # 遍历GQList
        for GQ in GQList:
            if pd.isna(GQ) or GQ in ["装柜单号", "", "G"]:
                continue

            A号码 = ""
            总KG = 0.0
            港前港后AP = 0.0
            first_GQ_index = None

            # 循环df
            for index, row in df.iterrows():
                if row['装柜单号'] == GQ:
                    if first_GQ_index is None:
                        first_GQ_index = index
                    A号码 += f"{row['工作单号']},"
                    总KG += row['总KG']
                    港前港后AP += row['港前港后AP']
            
            if 总KG == 0:
                continue

            # 计算单KG
            单KG = round(港前港后AP / 总KG, 2)

            # 分割A号码
            splited_list = A号码[:-1].split(',')

            # 以下为分摊
            if 港前港后AP != 0:
                if len(splited_list) == 1:
                    df.loc[first_GQ_index, 'P'] = 港前港后AP
                else:
                    剩下的值 = 0.0
                    for i, A in enumerate(splited_list):
                        if i == len(splited_list) - 1:
                            # df.loc[first_GQ_index, 'P'] = 港前港后AP - 剩下的值
                            df.loc[first_GQ_index, 'P'] = 港前港后AP - 剩下的值
                        else:
                            A号码的值 = df.loc[first_GQ_index, '总KG'] * 单KG
                            df.loc[first_GQ_index, 'P'] = A号码的值
                            剩下的值 += A号码的值
                            first_GQ_index += 1

        # 获取文件目录和文件名
        file_dir = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)
        current_datetime = datetime.now()
        时间 = current_datetime.strftime('%Y%m%d%H%M')
        new_file_name = f"{file_name.split('.')[0]}-{时间}.xlsx"
        new_file_path = os.path.join(file_dir, new_file_name)

        # 保存修改后的文件
        df.to_excel(new_file_path, index=False)
        return new_file_path
    finally:
        pass

if __name__ == "__main__":
    main(r"C:\Users\a1337\Desktop\vscode_vba\CLOSE系统测试列.xlsx")
