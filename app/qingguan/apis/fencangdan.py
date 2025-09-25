
from morelink_api import MoreLinkClient

from fastapi import (
    APIRouter
)
from fastapi.responses import FileResponse

from app.schemas import (
    FenDanUploadData,
)

from app.utils_aspose import (
    generate_fencangdan_file
)
from rpa_tools import find_playwright_node_path

fencangdan_router = APIRouter(tags=["分舱单"],prefix="/fencangdan")

@fencangdan_router.post("/get_morelink_zongdan", summary="获取morelink总单的件毛体")
def get_morelink_zongdan(master_bill_no: str):
    """获取件毛体数据"""

    morelink_client = MoreLinkClient(node_path=find_playwright_node_path())
    data = morelink_client.zongdan_api_httpx()
    
   
    filter_data = [
        row for row in data
        if row.get('billno') == master_bill_no
    ]
    if not filter_data:
        return {
            "code": 400,
            "message": "总单号不存在",
            "data": None
        }
    print(filter_data)
    result = {
        "num":filter_data[0]['yjnum'],
        'weight':filter_data[0]['yjweight'],
        'volume':filter_data[0]['yjvolume'],
        'flight_no':filter_data[0]['flightno'],
        'shipcompany':filter_data[0]['shipcompany'],
        "startland":filter_data[0]['startland'],
        'destination':filter_data[0]['destination'],
        'etd':filter_data[0]['etd']
        
    
    }
    return result
   

@fencangdan_router.post("/fencangdan_file_generate", summary="生成分舱单文件")
def generate_fencangdan_file_result(upload_data:FenDanUploadData):
    upload_data_dict = upload_data.model_dump()
    result_path = generate_fencangdan_file(upload_data_dict)
    return FileResponse(
            path=result_path,
            filename=f"{upload_data.orderNumber}",
        )
