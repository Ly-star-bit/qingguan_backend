import os
import traceback
from fastapi import (
    FastAPI,
    File,
    Request,
    HTTPException,
    Header,
    UploadFile,
    status as status_code,
)
from fastapi import APIRouter

from loguru import logger
import pandas as pd
from pydantic import BaseModel
from typing import List, Optional
import hashlib
from fastapi.responses import FileResponse, JSONResponse
from pymongo import MongoClient
from app.db_mongo import get_db
from collections import defaultdict

import json
from datetime import datetime
from sqlmodel import Field, Session, SQLModel, select,or_


from fastapi import Depends
from app.db import get_session
from sqlalchemy import func


router_17track = APIRouter(prefix="/17track", tags=["17track"])
track17_logger = logger.bind(name="17track")
track17_logger = track17_logger.patch(lambda record: record.update(name="17track"))

track17_logger.add(
    "log/17track.log", rotation="10 MB", retention="10 days", compression="zip"
)
# Replace with your actual secret key from 17TRACK
SECRET_KEY = "E993A64609F7F52F0842B2A36A8F0108"


class WebhookEvent(BaseModel):
    event: str
    data: dict


def verify_signature(request_body: str, received_signature: str) -> bool:
    """Verify the signature from 17TRACK"""
    try:
        string_to_hash = f"{request_body}/{SECRET_KEY}"
        sha256_hash = hashlib.sha256(string_to_hash.encode("utf-8")).hexdigest()
        track17_logger.info(f"计算的签名: {sha256_hash}")
        track17_logger.info(f"收到的签名: {received_signature}")
        return sha256_hash == received_signature
    except Exception as e:
        track17_logger.error(f"验证签名时出错: {str(e)}\n{traceback.format_exc()}")
        return False


async def process_webhook(
    event_type: str,
    tracking_number: str,
    status: Optional[str] = None,
    milestome: Optional[str] = None,
    tracking: Optional[str] = None,
    estimated_delivery_date: Optional[str] = None,
):
    """Process the webhook data"""
    try:
        with get_db() as db:
            if event_type == "TRACKING_STOPPED":
                # track17_logger.info(f"Tracking stopped for number: {tracking_number}")
                db.track17_tracking_data.update_one(
                    {"tracking_number": tracking_number},
                    {
                        "$set": {
                            "status": status,
                            "tracking_stopped": True,
                            # "tracking": tracking,
                        }
                    },
                    upsert=True,
                )

            # Add your business logic for stopped tracking
            elif event_type == "TRACKING_UPDATED":
                # track17_logger.info(f"Tracking updated - Number: {tracking_number}, Status: {status},milestome:{milestome}")
                db.track17_tracking_data.update_one(
                    {"tracking_number": tracking_number},
                    {
                        "$set": {
                            "status": status,
                            "milestome": milestome,
                            "tracking": tracking,
                            "estimated_delivery_date": estimated_delivery_date,
                        }
                    },
                    upsert=True,
                )
                # Add your business logic for updated tracking
            else:
                track17_logger.warning(f"Unknown event type: {event_type}")
    except Exception as e:
        track17_logger.error(
            f"处理webhook数据时出错: {str(e)}\n{traceback.format_exc()}"
        )
        raise


@router_17track.post("/notify", summary="17track webhook")
async def notify(
    request: Request,
    sign: str = Header(None),
):
    """Webhook endpoint for 17TRACK notifications"""
    try:
        # Read the raw request body
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8")

        track17_logger.info(f"收到webhook请求: {body_str}")
        track17_logger.info(f"请求头: {request.headers}")

        # Parse the JSON
        try:
            payload = json.loads(body_str)
        except json.JSONDecodeError as e:
            track17_logger.error(f"无效的JSON格式: {str(e)}\n{traceback.format_exc()}")
            raise HTTPException(
                status_code=status_code.HTTP_400_BAD_REQUEST, detail="无效的JSON格式"
            )

        # Validate the event type
        if "event" not in payload or "data" not in payload:
            track17_logger.error("请求体缺少必需字段")
            raise HTTPException(
                status_code=status_code.HTTP_400_BAD_REQUEST,
                detail="请求体缺少必需字段",
            )

        event_type = payload["event"]
        tracking_number = payload["data"].get("number")

        if not tracking_number:
            track17_logger.error("请求体缺少运单号")
            raise HTTPException(
                status_code=status_code.HTTP_400_BAD_REQUEST, detail="请求体缺少运单号"
            )

        # For TRACKING_UPDATED events, verify signature and get status
        status = None
        milestome = None
        tracking = None
        estimated_delivery_date = None
        if event_type == "TRACKING_UPDATED":
            if not sign:
                track17_logger.warning("TRACKING_UPDATED事件缺少签名")
                raise HTTPException(
                    status_code=status_code.HTTP_401_UNAUTHORIZED,
                    detail="TRACKING_UPDATED事件需要签名",
                )

            if not verify_signature(body_str, sign):
                track17_logger.warning("签名验证失败")
                raise HTTPException(
                    status_code=status_code.HTTP_401_UNAUTHORIZED, detail="无效的签名"
                )

            # Extract the latest status if available
            if (
                "track_info" in payload["data"]
                and "latest_status" in payload["data"]["track_info"]
            ):
                status = payload["data"]["track_info"]["latest_status"].get("status")
                milestome = payload["data"]["track_info"].get("milestone")
                tracking = payload["data"]["track_info"].get("tracking")
                estimated_delivery_date = (
                    payload["data"]["track_info"]
                    .get("time_metrics")
                    .get("estimated_delivery_date")
                )
        # Process the webhook data
        await process_webhook(
            event_type,
            tracking_number,
            status,
            milestome,
            tracking,
            estimated_delivery_date,
        )

        # Return 200 OK if everything is processed successfully
        return JSONResponse(
            content={"status": "success"}, status_code=status_code.HTTP_200_OK
        )

    except HTTPException as he:
        track17_logger.error(f"HTTP异常: {str(he)}\n{traceback.format_exc()}")
        raise he
    except Exception as e:
        track17_logger.error(f"未预期的错误: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=status_code.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"服务器内部错误: {str(e)}",
        )


# region TrackingNumberData CRUD
# 定义数据模型
class TrackingNumberData(SQLModel, table=True):
    __tablename__ = "tracking_number_data"
    id: Optional[int] = Field(default=None, primary_key=True)
    work_num: Optional[str] = Field(default=None)
    tracking_num: Optional[str] = Field(default=None, max_length=65535)  # 使用Text类型
    print_type: Optional[str] = Field(default=None)
    route_content: Optional[str] = Field(default=None)
    status: Optional[int] = Field(default=0)
    all_received: Optional[bool] = Field(default=False)
    create_time: Optional[datetime] = Field(default_factory=datetime.utcnow)
    update_time: Optional[datetime] = Field(default_factory=datetime.utcnow)
    earliest_launch_time: Optional[datetime] = Field(default=None)
    earliest_delivery_time: Optional[datetime] = Field(default=None)
    eta: Optional[str] = Field(default=None)


class TrackingNumberDataCreate(BaseModel):
    work_num: Optional[str] = None
    tracking_num: Optional[str] = None
    print_type: Optional[str] = None
    route_content: Optional[str] = None
    status: Optional[int] = 0
    all_received: Optional[bool] = False
    create_time: Optional[datetime] = Field(default_factory=datetime.utcnow)
    update_time: Optional[datetime] = Field(default_factory=datetime.utcnow)


class TrackingNumberDataUpdate(BaseModel):
    work_num: Optional[str] = None
    tracking_num: Optional[str] = None
    print_type: Optional[str] = None
    route_content: Optional[str] = None
    status: Optional[int] = None
    all_received: Optional[bool] = None
    update_time: Optional[datetime] = Field(default_factory=datetime.utcnow)


class PaginatedResponse(BaseModel):
    total: int
    page: int
    size: int
    items: List[TrackingNumberData]


@router_17track.post("/tracking_data/upload", summary="批量上传追踪数据")
async def upload_tracking_data(
    file: UploadFile = File(...), session: Session = Depends(get_session)
):
    """
    通过Excel文件批量上传追踪数据。
    文件必须包含以下列:
    - 工作单号
    - 追踪号码
    - 打印类型
    """
    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="只支持Excel文件格式(.xls, .xlsx)")

    try:
        # 读取Excel文件
        df = pd.read_excel(file.file)

        # 验证必要的列是否存在
        required_columns = ["工作单号", "追踪号码", "打印类型"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Excel文件缺少必要的列: {', '.join(missing_columns)}",
            )

        # 处理每一行数据
        success_count = 0
        error_rows = []

        for index, row in df.iterrows():
            try:
                tracking_data = TrackingNumberDataCreate(
                    work_num=str(row["工作单号"]),
                    tracking_num=str(row["追踪号码"]),
                    print_type=str(row["打印类型"]),
                )

                db_data = TrackingNumberData.model_validate(tracking_data)
                db_data.create_time = datetime.now()
                db_data.update_time = datetime.now()

                session.add(db_data)
                success_count += 1

            except Exception as e:
                error_rows.append(
                    {
                        "row": index + 2,  # Excel行号从1开始，且有标题行
                        "error": str(e),
                    }
                )

        session.commit()

        return {
            "status": "success",
            "message": f"成功导入 {success_count} 条数据",
            "errors": error_rows if error_rows else None,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"文件处理失败: {str(e)}")


@router_17track.post(
    "/tracking_data/query-by-excel",
    response_model=List[TrackingNumberData],
    summary="通过Excel批量查询追踪数据",
)
async def query_tracking_data_by_excel(
    file: UploadFile = File(...), session: Session = Depends(get_session)
):
    """
    通过Excel文件批量查询追踪数据。
    文件必须包含 '工作单号' 列。
    根据'工作单号'列中的值，查询并返回所有匹配的追踪数据记录。
    """
    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="只支持Excel文件格式(.xls, .xlsx)")

    try:
        # 读取Excel文件
        df = pd.read_excel(file.file)

        # 验证'工作单号'列是否存在
        if "工作单号" not in df.columns:
            raise HTTPException(
                status_code=400, detail="Excel文件缺少必要的列: 工作单号"
            )

        # 获取工作单号列表并去重
        work_nums = df["工作单号"].dropna().astype(str).unique().tolist()

        if not work_nums:
            return []

        # 根据工作单号查询数据
        query = select(TrackingNumberData).where(
            TrackingNumberData.work_num.in_(work_nums)
        )
        results = session.exec(query).all()

        # 创建结果字典以便快速查找
        results_dict = {result.work_num: result for result in results}

        # 在原始DataFrame中添加新列
        # 从路由内容中提取各状态数量
        def extract_status_counts(route_content):
            if not route_content:
                return None, None, None
            parts = route_content.split()[2:]  # 跳过前两个部分
            text = " ".join(parts)
            try:
                # 提取数字
                signed = int(text.split("已签收: ")[1].split(",")[0])
                in_transit = int(text.split("在途: ")[1].split(",")[0])
                not_online = int(text.split("未上线: ")[1].split(",")[0])
                return signed, in_transit, not_online
            except:
                return None, None, None

        # 添加三列
        df["已签收"] = df["工作单号"].apply(
            lambda x: extract_status_counts(results_dict[x].route_content)[0]
            if x in results_dict
            else None
        )
        df["在途"] = df["工作单号"].apply(
            lambda x: extract_status_counts(results_dict[x].route_content)[1]
            if x in results_dict
            else None
        )
        df["未上线"] = df["工作单号"].apply(
            lambda x: extract_status_counts(results_dict[x].route_content)[2]
            if x in results_dict
            else None
        )
        df["最早上线时间"] = df["工作单号"].apply(
            lambda x: results_dict[x].earliest_launch_time
            if x in results_dict
            else None
        )
        df["最早签收时间"] = df["工作单号"].apply(
            lambda x: results_dict[x].earliest_delivery_time
            if x in results_dict
            else None
        )

        def check_eta(x):
            if x not in results_dict:
                return None
            counts = extract_status_counts(results_dict[x].route_content)
            if None in counts:
                return None
            if counts[2] == sum(counts):  # 如果未上线数等于总数
                return None
            return results_dict[x].eta

        df["预计送达时间"] = df["工作单号"].apply(check_eta)
        # 生成Excel文件
        filename = f"tracking_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
        file_path = os.path.join("./excel", filename)
        os.makedirs("./excel", exist_ok=True)

        df.to_excel(file_path, index=False)

        # 返回Excel文件
        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=filename,
        )

    except Exception as e:
        track17_logger.error(f"处理Excel查询时出错: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"文件处理失败: {str(e)}")


@router_17track.post(
    "/tracking_data", response_model=TrackingNumberData, summary="创建新的追踪数据"
)
def create_tracking_data(
    *, session: Session = Depends(get_session), data_in: TrackingNumberDataCreate
):
    """
    创建一个新的 tracking_number_data 记录。
    """
    db_data = TrackingNumberData.model_validate(data_in)
    db_data.create_time = datetime.now()
    db_data.update_time = datetime.now()
    session.add(db_data)
    session.commit()
    session.refresh(db_data)
    return db_data


@router_17track.get(
    "/tracking_data", response_model=PaginatedResponse, summary="查询追踪数据（分页）"
)
def read_tracking_data(
    work_num: Optional[str] = None,
    all_received: Optional[bool] = None,
    print_type: Optional[str] = None,
    status: Optional[int] = None,
    session: Session = Depends(get_session),
    page: int = 1,
    size: int = 10,
):
    """
    分页查询 tracking_number_data 数据。
    """
    if page < 1:
        page = 1
    if size < 1:
        size = 1
    offset = (page - 1) * size

    # 构建基础查询
    query = select(TrackingNumberData)
    count_query = select(func.count()).select_from(TrackingNumberData)

    # 添加过滤条件
    if work_num:
        work_num_list = [num.strip() for num in work_num.split(",")]
        query = query.where(TrackingNumberData.work_num.in_(work_num_list))
        count_query = count_query.where(TrackingNumberData.work_num.in_(work_num_list))

    if all_received is not None:
        query = query.where(TrackingNumberData.all_received == all_received)
        count_query = count_query.where(TrackingNumberData.all_received == all_received)

    if print_type:
        query = query.where(TrackingNumberData.print_type == print_type)
        count_query = count_query.where(TrackingNumberData.print_type == print_type)

    if status is not None:
        query = query.where(TrackingNumberData.status == status)
        count_query = count_query.where(TrackingNumberData.status == status)

    # 获取总数
    total = session.exec(count_query).one()

    # 获取分页数据
    statement = query.offset(offset).limit(size)
    items = session.exec(statement).all()

    return PaginatedResponse(total=total, page=page, size=size, items=items)


@router_17track.get(
    "/tracking_data/{data_id}",
    response_model=TrackingNumberData,
    summary="根据ID获取追踪数据",
)
def read_tracking_data_by_id(*, session: Session = Depends(get_session), data_id: int):
    """
    通过ID获取单个 tracking_number_data 数据。
    """
    db_data = session.get(TrackingNumberData, data_id)
    if not db_data:
        raise HTTPException(status_code=404, detail="数据未找到")
    return db_data


@router_17track.put(
    "/tracking_data/{data_id}",
    response_model=TrackingNumberData,
    summary="更新追踪数据",
)
def update_tracking_data(
    *,
    session: Session = Depends(get_session),
    data_id: int,
    data_in: TrackingNumberDataUpdate,
):
    """
    更新指定ID的 tracking_number_data 数据。
    """
    db_data = session.get(TrackingNumberData, data_id)
    if not db_data:
        raise HTTPException(status_code=404, detail="数据未找到")

    update_data = data_in.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_data, key, value)

    db_data.update_time = datetime.now()
    session.add(db_data)
    session.commit()
    session.refresh(db_data)
    return db_data


@router_17track.delete("/tracking_data/{data_id}", summary="删除追踪数据")
def delete_tracking_data(*, session: Session = Depends(get_session), data_id: int):
    """
    删除指定ID的 tracking_number_data 数据。
    """
    db_data = session.get(TrackingNumberData, data_id)
    if not db_data:
        raise HTTPException(status_code=404, detail="数据未找到")

    session.delete(db_data)
    session.commit()
    return {"ok": True, "detail": "数据删除成功"}




@router_17track.post("/tracking_data/finance_upload", summary="财务数据上传和处理")
async def finance_upload(
    file: UploadFile = File(...), session: Session = Depends(get_session)
):
    """
    通过上传包含"快递单号"的Excel文件，查询并处理财务相关数据，最终返回包含处理结果的Excel文件。
    """
    if not file.filename.endswith((".xls", ".xlsx")):
        raise HTTPException(status_code=400, detail="只支持Excel文件格式(.xls, .xlsx)")
    try:
        # 连接MongoDB
        MONGO_CONFIG = {
            "host": os.getenv("MONGO_HOST"),
            "port": int(os.getenv("MONGO_PORT")),
            "username": os.getenv("MONGO_USER"),
            "password": os.getenv("MONGO_PASS"),
            "database": "dadan",
        }
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
        client = MongoClient(uri)
        db = client[MONGO_CONFIG["database"]]
        
        df = pd.read_excel(file.file)
        if "快递单号" not in df.columns:
            raise HTTPException(
                status_code=400, detail="Excel文件缺少必要的列: 快递单号"
            )
        tracking_nums = df["快递单号"].dropna().astype(str).unique().tolist()
        if not tracking_nums:
            return JSONResponse(
                content={"message": "Excel文件中没有有效的快递单号"}, status_code=400
            )

        # 修改SQL查询方式
        query = select(TrackingNumberData).where(
            or_(*[TrackingNumberData.tracking_num.contains(num) for num in tracking_nums])
        )
        tracking_data_results = session.exec(query).all()
        
        # --- 修改逻辑开始 ---
        # 即使 tracking_data_results 为空，也继续处理，以标识未找到的单号
        # if not tracking_data_results:
        #     return JSONResponse(
        #         content={"message": "未找到对应的工作单号"}, status_code=400
        #     )
        # --- 修改逻辑结束 ---

        # --- 修改逻辑开始 ---
        # 建立快递单号到工作单号的映射，并记录在Excel中但未在MySQL中找到的单号
        tracking_to_work_map = {}
        work_num_to_all_tracking = defaultdict(list)  # 存储工作单号对应的所有快递单号
        found_tracking_nums = set() # 记录在MySQL中找到的快递单号

        if tracking_data_results:
            # 如果有查询结果，才进行映射
            for result in tracking_data_results:
                if not result.tracking_num:
                    continue
                tracking_nums_list = result.tracking_num.split(",")
                work_num_to_all_tracking[result.work_num].extend(tracking_nums_list)
                for tn in tracking_nums_list:
                    if tn in tracking_nums:  # 只映射Excel中的快递单号
                        tracking_to_work_map[tn] = result.work_num
                        found_tracking_nums.add(tn) # 记录找到的单号

        # 处理在Excel中但未在MySQL中找到的快递单号
        not_found_tracking_nums = set(tracking_nums) - found_tracking_nums
        for tn in not_found_tracking_nums:
            tracking_to_work_map[tn] = "未找到" # 对于未找到的，工作单号标记为 "未找到"
        # --- 修改逻辑结束 ---
        
        # --- 修改逻辑开始 ---
        # 如果没有任何快递单号被映射（包括标记为“未找到”），则返回错误
        if not tracking_to_work_map:
             return JSONResponse(
                 content={"message": "未找到对应的工作单号"}, status_code=400
             )
        # --- 修改逻辑结束 ---

        all_tracking_details_cursor = db.track17_tracking_data.find(
            {"tracking_number": {"$in": tracking_nums}},
            {"tracking_number": 1, "tracking": 1, "estimated_delivery_date": 1,"milestome":1,"tracking_stopped":1},
        )
        work_num_details = defaultdict(list)
        for detail in all_tracking_details_cursor:
            tracking_number = detail["tracking_number"]
            work_num = tracking_to_work_map.get(tracking_number)
            # 修改：只处理有映射关系的（包括“未找到”），但MongoDB详情只关联到有效work_num
            if work_num and work_num != "未找到": 
                work_num_details[work_num].append(detail)

        summary_results = []
        detail_results = []
        
        # --- 修改逻辑开始 ---
        # 先处理有有效 work_num 的情况，并生成明细
        for work_num in set(tracking_to_work_map.values()):
            if work_num == "未找到":
                continue # 跳过标记为未找到的，稍后单独处理
            
            tracking_details = work_num_details.get(work_num, [])
            launch_dates = defaultdict(int)
            delivery_dates = defaultdict(int)
            not_launched = 0
            not_delivered = 0
            
            # 获取属于当前 work_num 且在本次上传中的快递单号
            current_tracking_nums_for_work = [tn for tn, wn in tracking_to_work_map.items() if wn == work_num and tn in tracking_nums]
            
            # 为每个属于此 work_num 的快递单号生成明细行
            for tracking_number in current_tracking_nums_for_work:
                 # 在 tracking_details 中查找该快递单号的具体信息
                 detail_for_tracking = next((d for d in tracking_details if d["tracking_number"] == tracking_number), None)
                 
                 eta = None
                 launch_time = None
                 delivery_time = None
                 
                 if detail_for_tracking:
                     estimated_delivery_date = detail_for_tracking.get("estimated_delivery_date", {})
                     if estimated_delivery_date and estimated_delivery_date.get("from"):
                         eta = datetime.fromisoformat(
                             estimated_delivery_date["from"].replace("Z", "+00:00")
                         ).strftime("%Y-%m-%d")
                     
                     # 检查是否已送达但tracking为None的情况
                     if detail_for_tracking.get("tracking") is None and detail_for_tracking.get("tracking_stopped") is True:
                         milestone = detail_for_tracking.get("milestome", []) # 注意原代码拼写是 milestome
                         if milestone:
                             # 检查发货时间
                             for m in milestone:
                                 if m["key_stage"] == "PickedUp" and m["time_utc"]:
                                     event_time = datetime.fromisoformat(
                                         m["time_utc"].replace("Z", "+00:00")
                                     )
                                     launch_date = event_time.strftime("%d-%m-%Y")
                                     launch_dates[launch_date] += 1
                                     launch_time = event_time.strftime("%Y-%m-%d %H:%M:%S")
                                 # 检查送达时间
                                 elif m["key_stage"] == "Delivered" and m["time_utc"]:
                                     event_time = datetime.fromisoformat(
                                         m["time_utc"].replace("Z", "+00:00")
                                     )
                                     delivery_date = event_time.strftime("%d-%m-%Y")
                                     delivery_dates[delivery_date] += 1
                                     delivery_time = event_time.strftime("%Y-%m-%d %H:%M:%S")
                     elif (
                         "tracking" in detail_for_tracking
                         and detail_for_tracking.get("tracking")
                         and "providers" in detail_for_tracking["tracking"]
                         and detail_for_tracking["tracking"]["providers"]
                     ):
                         events = detail_for_tracking["tracking"]["providers"][0].get("events", [])
                         provider = (
                             detail_for_tracking["tracking"]["providers"][0]
                             .get("provider", {})
                             .get("name")
                         )
                         # 按时间排序事件
                         sorted_events = sorted(events, key=lambda x: x.get("time_utc", ""))
                         # 记录已处理的状态,避免重复
                         launch_processed = False
                         delivery_processed = False
                         for event in sorted_events:
                             event_time_utc = event.get("time_utc")
                             if not event_time_utc:
                                 continue
                             event_time = datetime.fromisoformat(
                                 event_time_utc.replace("Z", "+00:00")
                             )
                             if provider == "UPS":
                                 if (
                                     not launch_processed
                                     and event["description"] == "Arrived at Facility"
                                 ):
                                     launch_date = event_time.strftime("%d-%m-%Y")
                                     launch_dates[launch_date] += 1
                                     launch_time = event_time.strftime("%Y-%m-%d %H:%M:%S")
                                     launch_processed = True
                                 elif (
                                     not delivery_processed
                                     and event["description"] == "Delivered"
                                 ):
                                     delivery_date = event_time.strftime("%d-%m-%Y")
                                     delivery_dates[delivery_date] += 1
                                     delivery_time = event_time.strftime("%Y-%m-%d %H:%M:%S")
                                     delivery_processed = True
                             elif provider == "FedEx":
                                 if (
                                     not launch_processed
                                     and event["description"] == "Picked up"
                                 ):
                                     launch_date = event_time.strftime("%d-%m-%Y")
                                     launch_dates[launch_date] += 1
                                     launch_time = event_time.strftime("%Y-%m-%d %H:%M:%S")
                                     launch_processed = True
                                 elif (
                                     not delivery_processed
                                     and event["description"] == "Delivered"
                                 ):
                                     delivery_date = event_time.strftime("%d-%m-%Y")
                                     delivery_dates[delivery_date] += 1
                                     delivery_time = event_time.strftime("%Y-%m-%d %H:%M:%S")
                                     delivery_processed = True
                             # 如果两个状态都已处理,则退出循环
                             if launch_processed and delivery_processed:
                                 break
                 
                 # 如果没有找到时间，则计数增加
                 if not launch_time:
                     not_launched += 1
                 if not delivery_time:
                     not_delivered += 1
                 
                 # 将明细添加到结果列表
                 detail_results.append(
                     {
                         "工作单号": work_num, # 这里是实际的 work_num
                         "快递单号": tracking_number,
                         "上线时间": launch_time if launch_time else "未上线",
                         "送达时间": delivery_time if delivery_time else "未送达",
                         "预计送达时间": eta if eta else "无预计时间",
                     }
                 )
            
            # --- 汇总逻辑 ---
            launch_details = "/".join(
                [f"{date}-{count}箱" for date, count in launch_dates.items()]
            )
            if not_launched > 0:
                launch_details += f"/未上线-{not_launched}箱" 
            delivery_details = "/".join(
                [f"{date}-{count}箱" for date, count in delivery_dates.items()]
            )
            if not_delivered > 0:
                delivery_details += f"/未送达-{not_delivered}箱"
            
            # 获取当前工作单号在本期账单中的快递单号数量
            current_tracking_nums_count = len(current_tracking_nums_for_work)
            # 获取master tracking number
            master_tracking = work_num_to_all_tracking[work_num][0] if work_num_to_all_tracking[work_num] else ""
            # 判断是否分票
            is_split = "Y" if len(launch_dates) > 1 or len(delivery_dates) > 1 else "N"
            summary_results.append(
                {
                    "工作单号": work_num,
                    "上线是否分批": "Y" if len(launch_dates) > 1 else "N",
                    "上线明细": launch_details,
                    "合计箱数": current_tracking_nums_count,
                    "派送是否分批": "Y" if len(delivery_dates) > 1 else "N",
                    "派送明细": delivery_details,
                    "master tracking": master_tracking,
                    "是否分票": is_split,
                    "业务系统总箱数": len(work_num_to_all_tracking[work_num]),
                    "不在本期账单箱数": len(work_num_to_all_tracking[work_num]) - current_tracking_nums_count
                }
            )
        
        # 最后处理那些在MySQL中未找到的快递单号，添加到明细中
        for tracking_num in not_found_tracking_nums:
             detail_results.append(
                {
                    "工作单号": "未找到", # 标记为未找到
                    "快递单号": tracking_num,
                    "上线时间": "未上线",
                    "送达时间": "未送达",
                    "预计送达时间": "无预计时间",
                }
            )
            # 这些单号不参与汇总
        # --- 修改逻辑结束 ---

        filename = f"finance_output_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
        file_path = os.path.join("./excel", filename)
        os.makedirs("./excel", exist_ok=True)
        with pd.ExcelWriter(file_path) as writer:
            pd.DataFrame(detail_results).to_excel(
                writer, sheet_name="明细", index=False
            )
            pd.DataFrame(summary_results).to_excel(
                writer, sheet_name="汇总", index=False
            )
        return FileResponse(
            file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=filename,
        )
    except Exception as e:
        # 假设 track17_logger 已定义
        # track17_logger.error(
        #     f"处理财务上传文件时出错: {str(e)}\n{traceback.format_exc()}"
        # )
        print(f"处理财务上传文件时出错: {str(e)}\n{traceback.format_exc()}") # 临时打印错误
        raise HTTPException(status_code=500, detail=f"文件处理失败: {str(e)}")

# 注意：原代码中 db.track17_tracking_data.find 的字段 "milestome" 可能是 "milestone" 的笔误。
# 如果是笔误，请将 detail.get("milestome", []) 改为 detail.get("milestone", []) 
