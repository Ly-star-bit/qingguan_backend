import pandas as pd
from datetime import datetime, time, timedelta


def process_excel_to_json(excel_path):
    """
    读取excel表格进行逻辑处理,返回一个dataframe
    """
    # 定义仓库代码映射字典
    warehouse_mapping = {
        "EGL芝加哥仓": "HKLMT-60608-M",
        "出口易美东三仓": "HKLMT-08837-E(A)",
        "GC美东10仓": "HKLMT-08505-E(A)",
    }
    df = pd.read_excel(excel_path)

    # 1. 删除和列名相同的行
    df = df[~df.isin(df.columns).all(axis=1)]

    # 重新处理列名,用strip去除前后空格
    df.columns = df.columns.str.strip()

    # 2. 获取仓库代码
    # 仓库代码列名
    warehouse_code_col = "仓库代码"
    df.dropna(subset=[warehouse_code_col], inplace=True)
    df["搜索CODE"] = df[warehouse_code_col].astype(str).str.strip()

    # 对需要映射的仓库代码进行映射,其他的取前4位
    df["搜索CODE"] = df[warehouse_code_col].apply(
        lambda x: warehouse_mapping.get(x.strip(), x.strip()[:4])
    )

    # 去除仓库代码为空的row
    df = df[df["搜索CODE"].notna() & (df["搜索CODE"] != "")]

    # 3. 获取订单单号(FBA Number)
    # 订单单号列名,如果表格中没有明显的FBA Number，则置为空字符串
    order_number_col = "SHIPMENT ID"
    df["FBA号"] = df[order_number_col].astype(str)

    # 4. 获取每一行体积
    volume_col = "体积"
    df["预计体积"] = df[volume_col].astype(float)

    # 5. 获取每一行数量
    quantity_col = "总箱数"
    df["预计数量"] = df[quantity_col].astype(int)

    # 6. 获取每一行重量
    weight_col = "总实重（KG）"
    df["预计重量"] = df[weight_col].astype(float)

    # 7. 去除row全部为nan的行
    df = df.dropna(how="all")

    # 新增列
    df["发货单位"] = "HKLMT-香港兰玛特-SZ"
    df["业务类型"] = "空运"
    df["报关方式"] = "9710"

    # 清关方式：匹配表格'发货方式'，空运慢线BS是LDP,空运慢线自税是DUTY
    shipment_method_col = "发货方式"  # 假设发货方式的列名为'发货方式'
    df["清关方式"] = df[shipment_method_col].apply(
        lambda x: "LDP"
        if str(x).endswith("BS")
        else ("DUTY" if str(x).endswith("自税") else "")
    )
    df["flight_type"] = df[shipment_method_col].apply(
        lambda x: "快线" if "快线" in str(x) else "慢线"
    )
    df["自营渠道"] = "空运标准"
    df["客户渠道"] = "空运标准"
    df["产品性质"] = "普货"
    df["提货方式"] = "上门提货"
    df["司机资料"] = "货拉拉深圳 / / /"
    df['提货地址条件']=df['配货新/旧仓']
    # 上门提货日期：新建订单当天17：00
    df["上门提货日期"] = datetime.combine(datetime.today(), time(17, 0)).strftime(
        "%Y-%m-%d %H:%M"
    )

    df["送货仓库"] = "深圳仓"

    # 预计入仓时间：新建订单当天
    df["预计入仓时间"] = datetime.today().strftime("%Y-%m-%d")

    df["国家"] = "美国"
    df["报价单价"] = ""

    # 客户内部号：匹配 表格 '备货单'
    df["客户内部号"] = df["备货单"].fillna("")  # 假设备货单的列名为'备货单'
    df["PO"] = df['REFERENCE ID']
    # 业务备注：DEV:提货时间（上门提货日期）+13天
    df["业务备注"] = (
        "DEV:"
        + (pd.to_datetime(df["上门提货日期"]) + timedelta(days=13)).dt.strftime(
            "%Y-%m-%d"
        )
        + "\n如后段快递安排打FEDEX"
    )


    df = df[
        [
            "搜索CODE",
            "清关方式",
            "flight_type",
            "FBA号",
            "预计体积",
            "预计数量",
            "预计重量",
            "发货单位",
            "业务类型",
            "报关方式",
            "自营渠道",
            "客户渠道",
            "产品性质",
            "提货方式",
            "提货地址条件",
            "司机资料",
            "上门提货日期",
            "送货仓库",
            "预计入仓时间",
            "国家",
            "客户内部号",
            "业务备注",
            "报价单价",
            "PO"
        ]
    ]
    df.to_dict(orient="records")
    return df



process_excel_to_json(r"C:\Users\a1337\Desktop\3-9 赫泊斯.xlsx")