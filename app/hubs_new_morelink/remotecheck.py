import os

import pandas as pd

from app.utils import fedex_process_excel_with_zip_codes, ups_process_excel_with_zip_codes


def all_remoteaddresscheck_process(zip_code_str: str ):

    excel_path = os.path.join(
        os.getcwd(),
        "file",
        "remoteaddresscheck",
        "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_20250702.xlsx",
    )
  
    if not os.path.exists(excel_path):
        
        raise("未找到DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.xlsx文件")
       
    fedex_result = fedex_process_excel_with_zip_codes(zip_code_str,excel_path=excel_path)
    # fedex_result = extract_zip_codes_from_excel(zip_code_str)
    ups_result = ups_process_excel_with_zip_codes(zip_code_str)
    # 合并两个结果列表并按zip_code排序
    combined_result = sorted(fedex_result + ups_result, key=lambda x: x["zip_code"])
    usa_state_chinese = pd.read_excel(
        os.path.join(os.getcwd(), "file", "remoteaddresscheck", "美国州名.xlsx")
    )

    # 定义 property 中文映射
    property_chinese_mapping = {
        # "FEDEX": {
        #     "Contiguous U.S.": "普通偏远",
        #     "Contiguous U.S.: Extended": "超偏远",
        #     "Contiguous U.S.: Remote": "超级偏远",
        #     "Alaska": "阿拉斯加偏远",
        #     "Hawaii": "夏威夷偏远",
        #     "Intra-Hawaii": "夏威夷内部偏远",
        # },
         "FEDEX": {
            "DAS_ContUS": "普通偏远",
            "DAS_ContUSExt": "超偏远",
            "DAS_ContUSRem": "超级偏远",
            "DAS_Alaska": "阿拉斯加偏远",
            "DAS_Hawaii": "夏威夷偏远",
            "DAS_IntraHawaii": "夏威夷内部偏远",
        },
        "UPS": {
            "US 48 Zip": "普通偏远",
            "US 48 Zip DAS Extended": "超偏远",
            "Remote HI Zip": "夏威夷偏远",
            "Remote AK Zip": "阿拉斯加偏远",
            "Remote US 48 Zip": "超级偏远",
        },
    }

    # 遍历结果添加USPS信息和中文 property
    for item in combined_result:
        if item["property"] != "邮编错误,不足五位" and item["property"] != "Unknown":
            # usps_info = query_usps_zip(item['zip_code'])
            usps_info = None
            if usps_info and usps_info.get("resultStatus") == "SUCCESS":
                item["city"] = usps_info.get("defaultCity", "")
                item["state"] = usps_info.get("defaultState", "")
                if item["state"] in usa_state_chinese["美国州名缩写"].values:
                    # 找到对应的 列 ‘中文译名'
                    item["state"] += (
                        f'\n{usa_state_chinese[usa_state_chinese["美国州名缩写"] == item["state"]]["中文译名"].values[0]}'
                    )

                # 获取避免使用的城市名称列表
                avoid_cities = [x["city"] for x in usps_info.get("nonAcceptList", [])]
                item["avoid_city"] = avoid_cities

            # 添加中文 property
            carrier_type = item["type"].upper()  # 获取承运商类型 (FEDEX 或 UPS)
            english_property = item["property"]  # 获取英文 property

            if (
                carrier_type in property_chinese_mapping
                and english_property in property_chinese_mapping[carrier_type]
            ):
                item["property_chinese"] = property_chinese_mapping[carrier_type][
                    english_property
                ]
            else:
                item["property_chinese"] = "未知偏远"  # 默认值
    return combined_result

