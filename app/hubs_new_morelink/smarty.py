from smartystreets_python_sdk import StaticCredentials, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup
from dotenv import load_dotenv
import os
load_dotenv()


def validate_address(street=None, city=None, state=None, zipcode=None, candidates=1):
    """
    Validates an address using SmartyStreets API and returns search data and RDI results
    
    Args:
        street (str): Street address
        city (str): City name
        state (str): State abbreviation
        zipcode (str): ZIP code
        candidates (int): Number of candidate results to return (default: 1)
    
    Returns:
        dict: Contains validation results including standardized address and RDI
    """
    AUTH_ID = os.getenv("AUTH_ID")
    AUTH_TOKEN = os.getenv("AUTH_TOKEN")
    
    # Validate that required parameters are provided
    if not street:
        return {"error": "Street address is required"}
    
    # 1) 构建客户端
    credentials = StaticCredentials(AUTH_ID, AUTH_TOKEN)
    client = ClientBuilder(credentials).build_us_street_api_client()

    # 2) 组装查询
    lookup = Lookup()
    lookup.street = street
    lookup.city = city
    lookup.state = state
    lookup.zipcode = zipcode
    lookup.candidates = candidates  # 只要最优结果

    # 3) 发送请求
    try:
        client.send_lookup(lookup)
    except Exception as e:
        return {"error": f"API request failed: {str(e)}"}

    # 4) 读取结果（含 RDI）
    if lookup.result:
        first = lookup.result[0]
        result = {
            "success": True,
            # "standardized_address": {
            #     "delivery_line_1": first.delivery_line_1,
            #     "last_line": first.last_line,
            #     "components": {
            #         "primary_number": getattr(first.components, 'primary_number', None),
            #         "street_name": getattr(first.components, 'street_name', None),
            #         "street_suffix": getattr(first.components, 'street_suffix', None),
            #         "city_name": getattr(first.components, 'city_name', None),
            #         "state_abbreviation": getattr(first.components, 'state_abbreviation', None),
            #         "zipcode": getattr(first.components, 'zipcode', None),
            #         "plus4_code": getattr(first.components, 'plus4_code', None),
            #     }
            # },
            "rdi": getattr(first.metadata, 'rdi', None),  # Residential / Commercial
            # "dpv_match_code": getattr(first.analysis, 'dpv_match_code', None),
            # "is_deliverable": getattr(first.analysis, 'dpv_footnotes', None) is not None
        }
        return result
    else:
        return {"success": False, "error": "未找到匹配结果"}


# Example usage:
if __name__ == "__main__":
    result = validate_address(
        street="1600 Amphitheatre Pkwy",
        city="Mountain View", 
        state="CA", 
        zipcode="94043"
    )
    
    if result.get("success"):
        print("标准化地址：", result["standardized_address"]["delivery_line_1"], 
              ",", result["standardized_address"]["last_line"])
        print("RDI（住宅/商业）：", result["rdi"])   # Residential / Commercial
        print("DPV 匹配码：", result["dpv_match_code"])
    else:
        print(result.get("error", "未知错误"))
