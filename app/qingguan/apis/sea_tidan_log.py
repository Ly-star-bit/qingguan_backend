from typing import Optional
from bson import ObjectId
from pymongo import MongoClient
from morelink_api import MoreLinkClient

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Response
)
from loguru import logger



from app.utils_aspose import (
    shenzhen_customes_pdf_gennerate
)
from rpa_tools import find_playwright_node_path
from app.db_mongo import get_session
from pydantic import BaseModel,Field
class ShipmentLog(BaseModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    shipper_name: str
    receiver_name: str
    master_bill_no: str
    gross_weight: float
    volume: float
    total_boxes: int
    all_english_name: str   # 使用 SQLAlchemy 的 Text 类型
    status: int = Field(default=0)  # 0: 未完成, 1: 已完成, -1: 失败
    other_data: str   # 使用 SQLAlchemy 的 JSON 类型存储 JSON 数据
sea_tidan_log_router = APIRouter(tags=["sea_tidan_log"],prefix='/sea_tidan_log')
# 增：创建 ShipmentLog
@sea_tidan_log_router.post("/shipment_logs/", summary="创建运单日志")
async def create_shipment_log(
    shipment_log: ShipmentLog, session: MongoClient = Depends(get_session)
):
    try:
        db = session
        # 检查是否已存在相同提单号的记录
        # existing_log = db.shipment_logs.find_one(
        #     {"master_bill_no": shipment_log.master_bill_no,"all_english_name":shipment_log.all_english_name}
        # )
        # if existing_log:
        #     raise HTTPException(
        #         status_code=400,
        #         detail="Shipment log with this bill number already exists",
        #     )

        shipment_log_dict = shipment_log.model_dump()
        result = db.shipment_logs.insert_one(shipment_log_dict)

        if result.inserted_id:
            shipment_log_dict["_id"] = str(result.inserted_id)
            return shipment_log_dict
        raise HTTPException(status_code=500, detail="Failed to create shipment log")
    except Exception as e:
        logger.error(f"Error creating shipment log: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@sea_tidan_log_router.put("/shipment_logs/{shipment_log_id}", summary="更新运单日志")
async def update_shipment_log(
    shipment_log_id: str,
    shipment_log: ShipmentLog,
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
        if not ObjectId.is_valid(shipment_log_id):
            raise HTTPException(
                status_code=400, detail="Invalid shipment log ID format"
            )

        existing_log = db.shipment_logs.find_one({"_id": ObjectId(shipment_log_id)})
        if not existing_log:
            raise HTTPException(status_code=404, detail="ShipmentLog not found")

        update_data = shipment_log.model_dump(exclude_unset=True)
        db.shipment_logs.update_one(
            {"_id": ObjectId(shipment_log_id)}, {"$set": update_data}
        )
        updated_log = db.shipment_logs.find_one({"_id": ObjectId(shipment_log_id)})
        updated_log["_id"] = str(updated_log["_id"])
        updated_log["id"] = str(updated_log["_id"])
        return updated_log
    except Exception as e:
        logger.error(f"Error updating shipment log: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@sea_tidan_log_router.get("/shipment_logs/", response_model=dict, summary="获取运单日志列表")
async def read_shipment_logs(
    status: Optional[int] = Query(None, description="Filter by status"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Limit for pagination"),
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
        query = {}
        if status is not None:
            query["$or"] = [{"status": status}, {"status": -1}]

        cursor = db.shipment_logs.find(query).skip(offset).limit(limit)
        shipment_logs = []
        for log in cursor:
            log["_id"] = str(log["_id"])
            log["id"] = str(log["_id"])
            shipment_logs.append(log)

        total_count = db.shipment_logs.count_documents(query)
        total_pages = (total_count + limit - 1) // limit

        return {
            "shipment_logs": shipment_logs,
            "total": total_count,
            "total_pages": total_pages,
        }
    except Exception as e:
        logger.error(f"Error reading shipment logs: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@sea_tidan_log_router.get("/shipment_logs/{master_bill_no}", response_model=dict, summary="根据提单号获取运单日志")
async def read_shipment_log(
    master_bill_no: str, session: MongoClient = Depends(get_session)
):
    try:
        db = session
        if not master_bill_no or len(master_bill_no.strip()) == 0:
            raise HTTPException(
                status_code=400, detail="Master bill number cannot be empty"
            )

        shipment_log = db.shipment_logs.find_one({"master_bill_no": master_bill_no})
        if not shipment_log:
            raise HTTPException(status_code=404, detail="ShipmentLog not found")

        shipment_log["_id"] = str(shipment_log["_id"])
        return {"shipment_logs": shipment_log, "total": 1, "total_pages": 1}
    except Exception as e:
        logger.error(f"Error reading shipment log: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@sea_tidan_log_router.get("/get_tidan_pdf_again/{id}", summary="重新生成提单PDF")
async def get_tidan_pdf(id: str, session: MongoClient = Depends(get_session)):
    # 获取 ShipmentLog 数据
    db = session
    request_data = db.shipment_logs.find_one({"_id": ObjectId(id)})
    if not request_data:
        raise HTTPException(status_code=404, detail="ShipmentLog not found")
    node_path = find_playwright_node_path()
    morelink_client = MoreLinkClient(node_path)
    # morelink_client = MoreLinkClient()
    data = morelink_client.zongdan_api_httpx()

    filter_data = [
        row for row in data if row.get("billno") == request_data["master_bill_no"]
    ]

    if not filter_data:
        logger.log("ALERT", f"morelink提单号搜索不到：{request_data['master_bill_no']}")
        return

    pdf_file = shenzhen_customes_pdf_gennerate(request_data, filter_data[0])
    logger.info(f"已生成pdf文件->{pdf_file}")

    # 更新 ShipmentLog 的状态
    db.shipment_log.update_one({"_id": ObjectId(id)}, {"$set": {"status": 1}})

    # 读取 PDF 文件内容
    with open(pdf_file, "rb") as file:
        pdf_content = file.read()

    # 返回 PDF 文件
    response = Response(content=pdf_content, media_type="application/pdf")
    response.headers["Content-Disposition"] = f"attachment; filename={pdf_file}"
    return response