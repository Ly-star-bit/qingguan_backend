
import pandas as pd
from datetime import datetime, time, timedelta

def process_excel_to_json(excel_path):
    """
    读取 excel 表格，进行逻辑处理，返回 dataframe。
    """
    df = pd.read_excel(excel_path)
    syk_prefix = f'SYK-{datetime.now().year}'

    # 1. 删除和列名相同的行，并去除列名空格
    df = df[~df.isin(df.columns).all(axis=1)]
    df.columns = df.columns.str.strip()

    # 删除批次号为空的行
    order_number_column = '批次号'  # 根据分析，确定订单单号列
    df = df[df[order_number_column].notna() & (df[order_number_column] != '') & (df[order_number_column] != '合计')]
    # 检查 'Unnamed: 1' 列是否存在
    if 'Unnamed: 1' in df.columns:
        # 向上填充 'Unnamed: 1' 列的缺失值
        df['Unnamed: 1'] = df['Unnamed: 1'].ffill()
        df['自定义_邮编'] = df['Unnamed: 1']
    else:
        df['自定义_邮编'] = ''
    # 2. 获取仓库代码,去除仓库代码为空的row
    warehouse_code_column = 'Unnamed: 2'  # 根据分析，确定仓库代码列

    # 如果批次号列是以syk_prefix开头的，则默认’HUBS-90250-W‘，否则则读取warehouse_code_column
    df['搜索CODE'] = df.apply(
        lambda row: 'HUBS-90250-W' if str(row['批次号']).startswith(syk_prefix)
        else str(row['仓库']).strip() if str(row['批次号']).startswith('HBSHY')
        else str(row[warehouse_code_column]).strip(),
        axis=1
    )

    # 先处理 NaN 值, 并去除仓库代码为空的row
    df['搜索CODE'] = df['搜索CODE'].fillna('').astype(str)
    df = df[df['搜索CODE'].notna() & (df['搜索CODE'] != '')]

    # 3. 获取订单单号(FBA Number)
    df['FBA号'] = ''

    # 4. 获取每一行体积
    volume_column = '合计'  # 根据要求，体积从“合计”列获取
    df['预计体积'] = df[volume_column].fillna(0)

    # 5. 获取每一行数量
    quantity_column = '总箱数'  # 根据分析，确定数量列
    df['预计数量'] = df[quantity_column].fillna(0)

    # 6. 获取每一行重量
    weight_column = '重量'  # 根据分析，确定重量列
    df['预计重量'] = df[weight_column].fillna(0)

    # 7. 去除row全部为nan的行
    df = df.dropna(how='all')

    # 8. 添加新列
    df['发货单位'] = 'FJXSY-福建新时颖-SH'
    df['业务类型'] = df['批次号'].apply(lambda x: '海运' if str(x).startswith('HBSHY') else '空运')
    df['报关方式'] = df['报关方式'] if df['业务类型'][0] == '空运' else '正本-校验'
    df['清关方式'] = 'LDP'
    # 根据批次号设置自营渠道
    # df['自营渠道'] = df['批次号'].apply(
    #     lambda x: '空运定制专线' if str(x).startswith(syk_prefix) 
    #     else '美森卡派-KG' if str(x).startswith('HBSHY')
    #     else '空运标准'
    # )
    # df['客户渠道'] = df['批次号'].apply(
    #     lambda x: '空运定制专线' if str(x).startswith(syk_prefix)
    #     else '美森卡派-KG' if str(x).startswith('HBSHY') 
    #     else '空运标准'
    # )
    df['自营渠道'] = df['渠道']
    df['客户渠道'] = df['渠道']

    df['产品性质'] = '普货'
    df['提货方式'] = '上门提货'
    # 修改上门提货日期，先转换为字符串，避免datetime转换错误
    df['上门提货日期'] = datetime.combine(datetime.today(), time(17, 0)).strftime("%Y-%m-%d %H:%M")
    df['上门提货日期'] = df['上门提货日期'].astype(str)
    df['送货仓库'] = df['送货地址']
    df['预计入仓时间'] = df['批次号'].apply(lambda x: datetime.now().strftime('%Y-%m-%d') if str(x).startswith('HBSHY') else '')
    df['国家'] = '美国'
    df['业务备注'] = ''

    # 客户内部号
    df['颜色'] = df['颜色'].astype(str)
    df['批次号'] = df['批次号'].astype(str)
    df['客户内部号'] = df['颜色'] + ' ' + df['批次号']

    # Apply the modifications based on '批次号'
    df.loc[df['批次号'].str.startswith(syk_prefix), '客户内部号'] = df['批次号']
    df.loc[df['批次号'].str.startswith(syk_prefix), '业务备注'] = '时颖中转货物'
    df.loc[df['批次号'].str.startswith(syk_prefix), '送货仓库'] = '晨阳仓-CY'
    df.loc[df['批次号'].str.startswith(syk_prefix), '预计入仓时间'] = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    df.loc[df['批次号'].str.startswith(syk_prefix), '搜索CODE'] = 'HUBS-90250-W'


    # 9. 只保留需要的列，并重命名
    df = df[['搜索CODE', 'FBA号', '预计体积', '预计数量', '预计重量', '发货单位', '业务类型', '报关方式', '清关方式',
             '自营渠道', '客户渠道', '产品性质', '提货方式', '上门提货日期', '送货仓库', '预计入仓时间', '国家', '客户内部号', '业务备注', '自定义_邮编']]

    df.to_dict(orient='records')
    return df