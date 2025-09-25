import os
from pathlib import Path
import math
from datetime import datetime
from typing import List
from typing import Optional
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile
)
from fastapi.responses import FileResponse, StreamingResponse
from loguru import logger


from app.utils import (
    MinioClient,
    output_custom_clear_history_log,
)

from app.db_mongo import get_session
from pydantic import BaseModel
class OutputSelectedLogRequest(BaseModel):
    id_list: List[str]
    start_time: Optional[str]
    end_time: Optional[str]

customer_clear_history_summary_router = APIRouter(tags=['清关历史记录'],prefix='/cumstom_clear_history_summary')
@customer_clear_history_summary_router.post(
    "/",
    summary="创建清关历史汇总记录"
)
async def create_summary(summary: dict, session: MongoClient = Depends(get_session)):
    db = session
    summary_dict = {k: v for k, v in summary.items() if k != "id"}
    # print(summary_dict)
    result = db.custom_clear_history_summary.insert_one(summary_dict)
    summary_dict["id"] = str(result.inserted_id)
    # logger.info(f"新增清理历史汇总成功: {summary_dict}")
    # 货值/重量
    money_per_kg = summary_dict["total_price_sum"] / summary_dict["gross_weight_kg"]
    port_or_packing = (
        summary_dict["port"] if summary_dict["port"] else summary_dict["packing_type"]
    )
    if summary_dict["estimated_tax_rate_cny_per_kg"] >= 1.2 or money_per_kg < 0.46:
        email_data = {
            "receiver_email": "caitlin.fang@hubs-scs.com",
            "subject": f"{summary_dict['user_id']}-{'-'.join(summary_dict['filename'].split('-')[1:-1]).replace('CI&PL','').strip()}-{round(money_per_kg,2)}-{summary_dict['estimated_tax_rate_cny_per_kg']} CNY/Kg-{port_or_packing}-税金{summary_dict['estimated_tax_amount']}-{summary_dict['gross_weight_kg']}Kg-货值{summary_dict['total_price_sum']}",
            "body": "",
            "status": 0,
            "create_time": datetime.now()
        }
        db.email_queue.insert_one(email_data)
    # for detail in summary_dict['details']:
    #     detail['summary_log_id'] = summary_dict['id']
    #     detail['generation_time'] = summary_dict['generation_time']
    #     db.custom_clear_history_detail.insert_one(detail)

    return summary_dict
@customer_clear_history_summary_router.get('/download_shuidan_file/{id}/{filename}', summary="下载税单文件")
async def download_shuidan_file(
    id: str,
    filename: str,
    session: MongoClient = Depends(get_session)
):
    try:
        db = session
        # 从MongoDB获取文件信息
        summary = db.custom_clear_history_summary.find_one({"_id": ObjectId(id)})
        if not summary or "shuidan" not in summary:
            raise HTTPException(status_code=404, detail="税单文件不存在")
            
        # 查找指定文件名的文件
        file_info = None
        for item in summary["shuidan"]:
            if item["filename"] == filename:
                file_info = item
                break
                
        if not file_info:
            raise HTTPException(status_code=404, detail=f"未找到文件名为 {filename} 的税单文件")
            
        # 先尝试从本地获取文件
        local_path = f"./file/shuidan/{id}/{filename}"
        if os.path.exists(local_path):
            return FileResponse(
                local_path,
                filename=filename,
                media_type='application/octet-stream'
            )
            
        # 本地不存在则从MinIO下载
        minio_client = MinioClient(
            os.getenv("MINIO_ENDPOINT"),
            os.getenv("MINIO_ACCESS_KEY"),
            os.getenv("MINIO_SECRET_KEY"),
            os.getenv("MINIO_BUCKET_NAME"),
            secure=False
        )
        minio_client.connect()
        
        # 确保本地目录存在
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # 从MinIO下载到本地
        minio_client.download_file(file_info['file_path'], local_path)
        
        return FileResponse(
            local_path,
            filename=filename,
            media_type='application/octet-stream'
        )
        
    except Exception as e:
        logger.error(f"下载税单文件失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
@customer_clear_history_summary_router.delete('/delete_shuidan_file/{id}/{filename}', summary="删除税单文件")
async def delete_shuidan_file(
    id: str,
    filename: str,
    session: MongoClient = Depends(get_session)
):
    try:
        db = session
        # 从MongoDB获取文件信息
        summary = db.custom_clear_history_summary.find_one({"_id": ObjectId(id)})
        if not summary or "shuidan" not in summary:
            raise HTTPException(status_code=404, detail="税单文件不存在")
            
        # 查找指定文件名的文件
        file_info = None
        file_index = -1
        for i, item in enumerate(summary["shuidan"]):
            if item["filename"] == filename:
                file_info = item
                file_index = i
                break
                
        if not file_info:
            raise HTTPException(status_code=404, detail=f"未找到文件名为 {filename} 的税单文件")
        
        # 删除本地文件(如果存在)
        local_path = f"./file/shuidan/{id}/{filename}"
        if os.path.exists(local_path):
            os.remove(local_path)
            
        # 从MinIO删除文件
        try:
            minio_client = MinioClient(
                os.getenv("MINIO_ENDPOINT"),
                os.getenv("MINIO_ACCESS_KEY"),
                os.getenv("MINIO_SECRET_KEY"),
                os.getenv("MINIO_BUCKET_NAME"),
                secure=False
            )
            minio_client.connect()
            
            # 从MinIO删除文件
            minio_client.client.remove_object(
                minio_client.bucket_name, 
                file_info['file_path']
            )
        except Exception as e:
            logger.warning(f"从MinIO删除文件失败: {str(e)}")
            # 继续执行，即使MinIO删除失败
        
        # 从MongoDB中移除文件记录
        shuidan_data = summary.get("shuidan", [])
        if file_index >= 0:
            shuidan_data.pop(file_index)
            
            # 更新MongoDB
            db.custom_clear_history_summary.update_one(
                {"_id": ObjectId(id)},
                {"$set": {"shuidan": shuidan_data}}
            )
            
        return {"message": "文件删除成功"}
        
    except Exception as e:
        logger.error(f"删除税单文件失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@customer_clear_history_summary_router.post('/upload_shuidan_file', summary="上传税单文件")
async def upload_shuidan_file(
    file: UploadFile = File(...),
    id: str = Form(...),
    file_type: str = Form(...),
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
        local_path = f"./file/shuidan/{id}/"
        if not os.path.exists(local_path):
            os.makedirs(local_path)
            
        minio_client = MinioClient(
            os.getenv("MINIO_ENDPOINT"),
            os.getenv("MINIO_ACCESS_KEY"), 
            os.getenv("MINIO_SECRET_KEY"),
            os.getenv("MINIO_BUCKET_NAME"),
            secure=False,
        )
        minio_client.connect()
        
        # 保存文件到本地
        file_path = os.path.join(local_path, file.filename)
        contents = await file.read()
        with open(file_path, "wb") as f:
            f.write(contents)
            
        # 上传到minio
        minio_path = f"shuidan/{id}/{Path(file_path).name}"
        minio_client.upload_file(file_path, minio_path)
        
        # 更新MongoDB中的shuidan字段
        # 先获取现有的shuidan数据
        summary = db.custom_clear_history_summary.find_one({"_id": ObjectId(id)})
        shuidan_data = summary.get("shuidan", []) if summary else []
        
        # 添加或更新文件信息
        new_file = {
            "type": file_type,
            "file_path": minio_path,
            "filename": file.filename
        }
        
        # 更新逻辑：
        # 1. 如果是abnormal类型，只有文件名完全相同才覆盖
        # 2. 其他类型，按照type覆盖
        updated = False
        for item in shuidan_data:
            if file_type == "abnormal":
                # abnormal类型只在文件名完全相同时才覆盖
                if item["filename"] == file.filename:
                    item.update(new_file)
                    updated = True
                    break
            else:
                # 其他类型按照type覆盖
                if item["type"] == file_type:
                    item.update(new_file)
                    updated = True
                    break
                
        # 如果不存在则添加新的
        if not updated:
            shuidan_data.append(new_file)
            
        # 更新MongoDB
        db.custom_clear_history_summary.update_one(
            {"_id": ObjectId(id)},
            {"$set": {"shuidan": shuidan_data}}
        )
            
        return {
            "message": "success",
            "uploaded_file": file.filename,
            "file_path": minio_path
        }
        
    except Exception as e:
        logger.error(f"上传税单文件失败: {str(e)}")
        # 删除本地文件
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))

@customer_clear_history_summary_router.post(
    "/update_cumstom_clear_history_summary_remarks/",
    summary="更新清关历史汇总备注/异常"
)
async def update_summary(
    request_body: dict,
    context_request: Request,
    session: MongoClient = Depends(get_session),
):
    db = session
    try:
        # 先查找该记录是否被锁定
        summary = db.custom_clear_history_summary.find_one(
            {"_id": ObjectId(request_body["id"])}
        )
        if not summary:
            return {"code": 500, "msg": "未找到该记录", "data": None}
        if summary.get("lock", False):
            return {"code": 500, "msg": "该记录已被锁定，不能修改", "data": None}
        user = context_request.state.user
        reviewer = user["sub"]
        update_data = {"latest_update_time": datetime.now(), "reviewer": reviewer}
        if "remarks" in request_body:
            update_data["remarks"] = request_body["remarks"]
        if "abnormal" in request_body:
            update_data["abnormal"] = request_body["abnormal"]
        db.custom_clear_history_summary.update_one(
            {"_id": ObjectId(request_body["id"])},
            {"$set": update_data},
        )
        result = db.custom_clear_history_summary.find_one(
            {"_id": ObjectId(request_body["id"])}
        )
        if result:
            result["id"] = str(result.pop("_id"))
        return {"code": 200, "msg": "更新成功", "data": result}
    except Exception as e:
        return {"code": 500, "msg": f"更新失败: {str(e)}", "data": None}


@customer_clear_history_summary_router.post(
    "/lock_cumstom_clear_history_summary_remarks/",
    summary="批量锁定/解锁清关历史汇总记录"
)
async def lock_summary(request_body: dict, session: MongoClient = Depends(get_session)):
    """
    批量锁定custom_clear_history_summary记录，summary_id为id列表
    参数通过json传入，格式为{"summary_id": [...], "lock": true/false}
    """
    db = session
    summary_id = request_body.get("summary_id", [])
    lock = request_body.get("lock", False)
    object_ids = [ObjectId(sid) for sid in summary_id]
    result = db.custom_clear_history_summary.update_many(
        {"_id": {"$in": object_ids}},
        {"$set": {"lock": lock}},
    )
    # 返回详细结果，包括修改数量和受影响的ID
    return {
        "modified_count": result.modified_count,
        "matched_count": result.matched_count,
        "locked": lock,
        "summary_ids": summary_id,
    }


@customer_clear_history_summary_router.get("/", summary="获取清关历史汇总列表")
def read_summaries(
    context_request: Request,
    enable_pagination: bool = Query(False, description="Enable pagination"),
    page: int = Query(1, description="Page number", ge=1),
    pageSize: int = Query(10, description="Number of items per page", ge=1, le=100),
    file_name: Optional[str] = Query(None, description="File name to filter by"),
    convey_type: Optional[str] = Query(None, description="convey_type to filter by"),
    remarks: Optional[str] = Query(None, description="remarks filter by"),
    abnormal: Optional[str] = Query(None, description="abnormal filter by"),
    abnormal_type: Optional[str] = Query(None, description="abnormal查询类型: equals/startswith/not_startswith"),
    port: Optional[str] = Query(None, description="port filter by"),
    start_time: datetime = Query(None, description="开始时间"),
    end_time: datetime = Query(None, description="结束时间"),
    generation_time_sort: Optional[str] = Query(
        None, description="生成时间排序 asc/desc"
    ),
    latest_update_time_sort: Optional[str] = Query(
        None, description="最后更新时间排序 asc/desc"
    ),
    user_id: Optional[str] = Query(None, description="user_id filter by"),
    reviewer: Optional[str] = Query(None, description="reviwer filter by"),
    lock: Optional[bool] = Query(None, description="lock filter by"),
    chinese_product_name: Optional[str] = Query(None, description="中文品名"),
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
        collection = db.custom_clear_history_summary

        query = {"$and": [{"$or": [{"remarks": {"$ne": "删除"}}, {"remarks": None}]}]}
        if chinese_product_name:
            query["$and"].append({
                "details": {
                    "$elemMatch": {
                        "chinese_name": {"$regex": f".*{chinese_product_name}.*", "$options": "i"}
                    }
                }
            })
            
        if file_name:
            # 将中文逗号替换为英文逗号
            file_name = file_name.replace('，', ',')
            file_names = file_name.split(',')
            file_name_conditions = [{"filename": {"$regex": f".*{name.strip()}.*", "$options": "i"}} for name in file_names]
            query["$and"].append({"$or": file_name_conditions})

        if remarks:
            query["remarks"] = {"$regex": f".*{remarks}.*", "$options": "i"}
        if abnormal:
            if abnormal_type == "equals":
                query["abnormal"] = abnormal
            elif abnormal_type == "startswith":
                query["abnormal"] = {"$regex": f"^{abnormal}", "$options": "i"}
            elif abnormal_type == "not_startswith":
                query["abnormal"] = {"$not": {"$regex": f"^{abnormal}", "$options": "i"}}
            else:
                query["abnormal"] = {"$regex": f".*{abnormal}.*", "$options": "i"}
        if port:
            # 将中文逗号替换为英文逗号
            port = port.replace('，', ',')
            ports = port.split(',')
            port_conditions = [{"port": port.strip()} for port in ports]
            query["$and"].append({"$or": port_conditions})
        if convey_type:
            # 如果运输方式为海运，则查询packing_type不为空的，如果为空运，则port不为空的
            if convey_type == "海运":
                query["packing_type"] = {"$ne": ""}
            elif convey_type == "空运":
                if port:
                    query["$and"].append({"$or": port_conditions})
                else:
                    query["port"] = {"$ne": ""}
            elif convey_type == "整柜":
                query["packing_type"] = {"$regex": "整柜"}
            elif convey_type == "拼箱":
                query["packing_type"] = {"$regex": "拼箱"}
        if start_time:
            query["generation_time"] = {"$gte": start_time, "$lte": end_time}

        # 设置排序
        sort_field = None
        sort_order = None

        if latest_update_time_sort:
            sort_field = "latest_update_time"
            sort_order = 1 if latest_update_time_sort == "asc" else -1
        elif generation_time_sort:
            sort_field = "generation_time"
            sort_order = 1 if generation_time_sort == "asc" else -1
        else:
            sort_field = "generation_time"
            sort_order = -1
        if user_id:
            if user_id == "admin":
                query["$and"].append(
                    {"$or": [
                        {"user_id": ""},
                        {"user_id": "admin"},
                        {"user_id": {"$exists": False}},
                    ]}
                )
            else:
                query["user_id"] = user_id
        if reviewer:
            query["reviewer"] = reviewer
        if lock is not None:
            if lock is False:
                query["$and"].append(
                    {"$or": [
                        {"lock": False},
                        {"lock": {"$exists": False}}
                    ]}
                )
            else:
                query["lock"] = lock

        user = context_request.state.user["sub"]
        if user != "admin":
            query["$and"].append(
                    {"$or": [
                        {"lock": False},
                        {"lock": {"$exists": False}}
                    ]}
                )

        # 如果排序字段是latest_update_time但记录中不存在该字段,则使用generation_time
        sort_conditions = (
            [(sort_field, sort_order), ("generation_time", sort_order)]
            if sort_field == "latest_update_time"
            else [(sort_field, sort_order)]
        )

        if enable_pagination:
            offset = (page - 1) * pageSize
            summaries = list(
                collection.find(query).sort(sort_conditions).skip(offset).limit(pageSize)
            )
            # Convert ObjectId to string
            for summary in summaries:
                summary["id"] = str(summary.pop("_id"))
                # 处理可能的无穷大值和NaN值
                for key, value in summary.items():
                    if isinstance(value, float):
                        if math.isinf(value) or math.isnan(value):
                            summary[key] = str(value)

            total = collection.count_documents(query)
            total_pages = (total + pageSize - 1) // pageSize

            return {"summaries": summaries, "total": total, "total_pages": total_pages}
        else:
            summaries = list(collection.find(query).sort(sort_conditions))
            # Convert ObjectId to string
            for summary in summaries:
                summary["id"] = str(summary.pop("_id"))
                # 处理可能的无穷大值和NaN值
                for key, value in summary.items():
                    if isinstance(value, float):
                        if math.isinf(value) or math.isnan(value):
                            summary[key] = str(value)

            return {"summaries": summaries, "total": len(summaries), "total_pages": 1}
    except Exception as e:
        return {"code": 500, "msg": f"查询失败: {str(e)}", "data": None}

@customer_clear_history_summary_router.get("/batch_hide_test_data", summary="批量隐藏测试数据")
def batch_hide_test_data(
    session: MongoClient = Depends(get_session),
):
    db = session
    # 查找filename中包含test的记录并更新remarks为"删除"(不区分大小写)
    db.custom_clear_history_summary.update_many(
        {"filename": {"$regex": r"-[Tt][Ee][Ss][Tt]"}},
        {"$set": {"remarks": "删除"}}
    )
    return {"message": "success"}



@customer_clear_history_summary_router.get("/output_cumtoms_clear_log/", summary="导出清关历史日志Excel")
async def output_log(
    start_time: str = Query(None, description="开始时间"),
    end_time: str = Query(None, description="结束时间"),
    file_name: Optional[str] = Query(None, description="File name to filter by"),
    convey_type: Optional[str] = Query(None, description="convey_type to filter by"),
    remarks: Optional[str] = Query(None, description="remarks filter by"),
    abnormal: Optional[str] = Query(None, description="abnormal filter by"),
    port: Optional[str] = Query(None, description="port filter by"),
):
    file_path = output_custom_clear_history_log(
        start_date=start_time,
        end_date=end_time,
        filename=file_name,
        convey_type=convey_type,
        remarks=remarks,
        abnormal=abnormal,
        port=port,
    )
    # 将文件路径转换为文件流
    file_stream = open(file_path, "rb")
    # 返回 Excel 文件
    return StreamingResponse(
        file_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=custom_clear_history_log.xlsx"
        },
    )

@customer_clear_history_summary_router.post("/output_selected_cumtoms_clear_log/", summary="导出选中清关历史日志Excel")
async def output_selected_log(
  request_body:OutputSelectedLogRequest,
):
    print(request_body)
    id_list = request_body.id_list
    start_time = request_body.start_time
    end_time = request_body.end_time
    file_path = output_custom_clear_history_log(
        id_list=id_list,
        start_date=start_time,
        end_date=end_time,
    )
    # 将文件路径转换为文件流
    file_stream = open(file_path, "rb")
    # 返回 Excel 文件
    return StreamingResponse(
        file_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=custom_clear_history_log.xlsx"
        },
    )
