import pandas as pd
from datetime import datetime, time

def process_excel_to_json(excel_path):
    # 读取Excel文件
    df = pd.read_excel(excel_path)

    # 去除列名空格
    df.columns = df.columns.str.strip()
    
    # 删除和列名相同的行
    df = df[~df.isin(df.columns).all(axis=1)]

    # 去除仓库代码为空的row
    df = df[df['目的仓'].notna()]

    # 获取仓库代码前4位
    df['仓库代码'] = (
    df['目的仓']
    .astype(str)
    .str.strip()
    .str.replace('（', '(', regex=False)
    .str.replace('）', ')', regex=False)
)
    # 处理FBA号
    df['订单单号'] = df['FBA号'].fillna('')

    # 处理体积
    df['体积'] = pd.to_numeric(df['预计体积'], errors='coerce').fillna(0)

    # 处理数量
    df['数量'] = pd.to_numeric(df['预计件数'], errors='coerce').fillna(0)

    # 处理重量
    df['重量'] = pd.to_numeric(df['预计毛重'], errors='coerce').fillna(0)

    # 去除全部为nan的行
    df = df.dropna(how='all')
    df['预计入仓时间'] = df['预计入仓时间'].fillna("")
    df['提货时间'] = df['提货时间'].fillna("")
    # 【填写订单】
    df['发货单位'] = df['发货单位']
    df['业务类型'] = df['业务类型']
    df['报关方式'] = df['报关方式']
    df['清关方式'] = df['清关方式']
    df['自营渠道'] = df['自营和客户渠道']
    df['客户渠道'] = df['自营和客户渠道']
    df['产品性质'] = df['产品性质']
    df['提货方式'] = '上门提货'

    # 提货时间处理
    if '提货时间' in df.columns:
        if pd.api.types.is_numeric_dtype(df['提货时间']) and not df['预计入仓时间'].isna().all() :
            df['上门提货日期'] = pd.to_datetime(df['提货时间'], unit='D', origin='1899-12-30').strftime('%Y-%m-%d %H:%M')
        elif pd.api.types.is_datetime64_any_dtype(df['提货时间']):
            df['上门提货日期'] = df['提货时间'].dt.strftime('%Y-%m-%d %H:%M')
        else:
            df['上门提货日期'] = df['提货时间'].astype(str)
    else:
        df['上门提货日期'] = None  # 或者其他默认值

    df['送货仓库'] = df['送货仓']

    # 预计入仓时间处理
    if '预计入仓时间' in df.columns:
        if pd.api.types.is_numeric_dtype(df['预计入仓时间']) and not df['预计入仓时间'].isna().all():
            df['预计入仓时间'] = pd.to_datetime(df['预计入仓时间'], unit='D', origin='1899-12-30').strftime('%Y-%m-%d %H:%M')
        elif pd.api.types.is_datetime64_any_dtype(df['预计入仓时间']):
            df['预计入仓时间'] = df['预计入仓时间'].dt.strftime('%Y-%m-%d %H:%M')
        else:
            df['预计入仓时间'] = df['预计入仓时间'].astype(str)
    else:
        df['预计入仓时间'] = None  # 或者其他默认值
        
    df['司机资料'] = df['司机资料']

    # 【收货地址】
    df['国家'] = df['国家']
    df['客户内部号'] = df['客户内部号']
    df['报价单价'] = df['报价单价']
    df['成本单价'] = df['报价单价']
    df['业务备注'] = df['业务备注']
    df['标记栏'] = df['FOM 标记']
    

    # 创建结果DataFrame
    result_df = pd.DataFrame({
        '搜索CODE': df['仓库代码'],
        'FBA号': df['订单单号'],
        '预计数量': df['数量'],
        '预计体积': df['体积'],
        '预计重量': df['重量'],
        '发货单位': df['发货单位'],
        '业务类型': df['业务类型'],
        '报关方式': df['报关方式'],
        '清关方式': df['清关方式'],
        '自营渠道': df['自营渠道'],
        '客户渠道': df['客户渠道'],
        '产品性质': df['产品性质'],
        '提货方式': df['提货方式'],
        '上门提货日期': df['上门提货日期'],
        '送货仓库': df['送货仓库'],
        '预计入仓时间': df['预计入仓时间'],
        '司机资料': df['司机资料'],
        '国家': df['国家'],
        '客户内部号': df['客户内部号'],
        '报价单价': df['报价单价'],
        '成本单价': df['成本单价'],
        '业务备注': df['业务备注'],
        '标记栏': df['标记栏']
    })
    result_df = result_df.fillna("")
    result_df.to_dict(orient='records')
    return result_df

process_excel_to_json(r"C:\Users\a1337\Desktop\通用客户_1758681874.3716567.xlsx")