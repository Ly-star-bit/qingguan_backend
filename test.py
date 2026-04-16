import pandas as pd
import numpy as np
from datetime import datetime, time

def process_excel_to_json(excel_path):
    # 读取 Excel 文件
    df = pd.read_excel(excel_path)
    
    # 删除和列名相同的行 (参考代码逻辑)
    df = df[~df.isin(df.columns).all(axis=1)]
    df = df.fillna('')
    
    # 确保数值列能正常计算，先统一转为字符串再处理，避免混合类型错误
    # 但为了后续计算，保留原始 df 用于分组，计算时再转换
    
    groups = []  # 存储所有票的数据
    current_group = []  # 当前票的数据

    # 保留原有的分组逻辑 (目标代码核心逻辑)
    for _, row in df.iterrows():
        # 如果 "货代箱号" 包含 "箱"，或者 "邮寄地址" 为空，表示当前票结束
        # 注意：这里需要确保列存在，防止报错
        box_no_str = str(row.get("货代箱号", ""))
        address_str = str(row.get('邮寄地址', ""))
        
        if "箱" in box_no_str or not address_str:
            if current_group:  # 避免空组
                groups.append(current_group)
                current_group = []
        else:
            if address_str:
                current_group.append(row.to_dict())  # 否则加入当前票

    # 处理最后一组（如果数据不以 "X 箱" 结尾）
    if current_group:
        groups.append(current_group)
    
    # 获取全局信息 (增加安全性检查)
    business_type = df['业务类型'].iloc[0] if '业务类型' in df.columns and len(df) > 0 else '海运'
    qudao = df['渠道'].iloc[0] if '渠道' in df.columns and len(df) > 0 else '海运整柜 - 标准'
    
    # 处理每个分组数据
    result_rows = []
    
    for group in groups:
        group_df = pd.DataFrame(group)
        
        # --- 开始生成货箱清单 (参考代码逻辑适配) ---
        boxgauge_list = []
        fba_index_map = {}  # 用于记录每个 FBA 号当前的箱号序号，保证同一 FBA 下箱号不重复
        
        # 检查是否有 PO 列
        has_po = 'PO' in group_df.columns
        
        for _, row in group_df.iterrows():
            fba_num = str(row.get('订单单号', '')).strip()
            if not fba_num:
                continue
                
            # 获取箱数，确保是数字
            try:
                box_count = int(float(row.get('箱数', 1)))
            except:
                box_count = 1
            
            # 获取尺寸和重量
            length = row.get('箱长（CM）', 0)
            width = row.get('箱宽（CM）', 0)
            height = row.get('箱高（CM）', 0)
            weight = row.get('箱重（KG）', 0)
            
            # 获取 PO (如果有)
            po_value = ""
            if has_po:
                po_val = row.get('PO', '')
                po_value = str(po_value) if po_val else ""

            # 初始化该 FBA 号的计数器
            if fba_num not in fba_index_map:
                fba_index_map[fba_num] = 0
            
            # 循环生成每个箱子的详情
            for i in range(box_count):
                fba_index_map[fba_num] += 1
                current_index = fba_index_map[fba_num]
                
                boxgauge_list.append({
                    "bgid": "",
                    "fba_no": fba_num,
                    "container_no": f"{fba_num}U{current_index:06d}", # 参考代码格式
                    "tracking_no": "",
                    "length": length,
                    "width": width,
                    "height": height,
                    "qty": 1,
                    "build_date": "",
                    "singlebox_kg": weight,
                    "refid": po_value,
                })
        # --- 货箱清单生成结束 ---

        # 获取仓库代码前 4 位 (搜索 CODE)
        warehouse_code = group_df['邮寄地址'].iloc[0] if not group_df.empty else ""
        
        # 获取 FBA 号 (去重)
        fba_numbers = ', '.join(group_df['订单单号'].drop_duplicates().astype(str))
        
        # 计算体积 (目标代码逻辑)
        # 确保尺寸列存在且为数值
        for col in ['箱长（CM）', '箱宽（CM）', '箱高（CM）']:
            if col in group_df.columns:
                group_df[col] = pd.to_numeric(group_df[col], errors='coerce').fillna(0)
            else:
                group_df[col] = 0
                
        group_df['体积'] = group_df['箱长（CM）'] * group_df['箱宽（CM）'] * group_df['箱高（CM）']
        
        # 计算预计数量、体积和重量
        total_volume = group_df['体积'].sum()
        
        # 将箱数转换为数值类型
        if '箱数' in group_df.columns:
            group_df['箱数'] = pd.to_numeric(group_df['箱数'], errors='coerce').fillna(0)
            box_count = group_df[group_df['体积'].notna()]['箱数'].sum()
        else:
            box_count = 0
            
        # 重量列处理
        if '箱重（KG）' in group_df.columns:
            group_df['箱重（KG）'] = pd.to_numeric(group_df['箱重（KG）'], errors='coerce').fillna(0)
            total_weight = group_df['箱重（KG）'].sum()
        else:
            total_weight = 0
        
        # 判断是否带电/带磁
        has_battery = '电' in str(group_df['是否带电'].tolist()) if '是否带电' in group_df.columns else False
        product_property = '带电' if has_battery else '普货'
        
        if '产品中文名' in group_df.columns:
            if "磁" in str(group_df['产品中文名'].tolist()):
                product_property += ",带磁"
        
        # 构建结果行
        result_row = {
            '发货单位': 'SZNE-深圳纽尔 -SZ',
            '业务类型': business_type,
            '报关方式': '正本 - 校验',
            '清关方式': 'LDP',
            '自营渠道': qudao,
            '客户渠道': qudao,
            '产品性质': product_property,
            '业务备注': '加急 - 红单' if business_type == '快递' else '',
            '提货方式': '上门提货',
            '上门提货日期': datetime.combine(datetime.today(), time(17, 0)).strftime("%Y-%m-%d %H:%M"),
            '送货仓库': '深圳仓',
            '预计入仓时间': datetime.today().strftime("%Y-%m-%d"),
            '国家': '美国',
            '搜索 CODE': warehouse_code,
            'FBA 号': fba_numbers,
            '预计数量': int(box_count),
            '预计体积': total_volume / 1000000,  # 转换为 CBM
            '预计重量': total_weight
        }
        
        # 将生成的货箱清单加入结果 (参考代码逻辑)
        # 这里无论是否有 PO 都加入清单，因为尺寸数据是有价值的，但参考了参考代码的键名
        if boxgauge_list:
            result_row['货箱清单'] = boxgauge_list
            
        result_rows.append(result_row)
    
    # 转换为 DataFrame 并返回
    result_df = pd.DataFrame(result_rows)
    # 如果需要直接返回 JSON 格式字典，可以取消下面这行的注释，但函数签名通常返回 DataFrame
    # return result_df.to_dict(orient="records") 
    return result_df

process_excel_to_json(r"C:\Users\a1337\Desktop\新增美国FBA空运箱单2.12.xls")