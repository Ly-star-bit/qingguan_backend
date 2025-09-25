from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Query,
    Request
)


from app.db_mongo import get_session

custom_clear_history_origin_summary_router = APIRouter(tags=["清关历史数据（原始）汇总详情"],prefix="/cumstom_clear_history_original_summary")
@custom_clear_history_origin_summary_router.get(
    "/",
    summary="获取清关历史数据（原始）汇总详情"
)
async def read_original_summary(
    context_request: Request,
    type: str = Query(..., description="运输类型:空运|海运"),
    session: MongoClient = Depends(get_session)
):
    user = context_request.state.user
    db = session
    summary = list(db.custom_clear_history_original_summary.find(
        {"type": type, "user": user["sub"]}
    ).sort("created_at", -1))
    
    # 转换ObjectId为字符串
    for doc in summary:
        doc["_id"] = str(doc["_id"])
        
    return summary

@custom_clear_history_origin_summary_router.post(
    "/",
    summary="添加清关历史数据（原始）汇总详情"
)
async def create_original_summary(
    context_request: Request,
    type: str = Query(..., description="运输类型:空运|海运"),
    data: dict = Body(...),
    session: MongoClient = Depends(get_session),
   
):
    user = context_request.state.user
    db = session
    # 检查用户已有记录数
    count = db.custom_clear_history_original_summary.count_documents({
        "user": user["sub"],
        "type": type
    })
    
    if count >= 5:
        # 找到最早的记录并删除
        oldest_record = db.custom_clear_history_original_summary.find_one(
            {"user": user["sub"], "type": type},
            sort=[("created_at", 1)]
        )
        if oldest_record:
            db.custom_clear_history_original_summary.delete_one({"_id": oldest_record["_id"]})
    
    data["type"] = type
    data["user"] = user["sub"]
    data["created_at"] = datetime.now()
    result = db.custom_clear_history_original_summary.insert_one(data)
    return {"id": str(result.inserted_id)}

@custom_clear_history_origin_summary_router.delete(
    "/{summary_id}",
    summary="删除清关历史数据（原始）汇总详情"
)
async def delete_original_summary(
    context_request: Request,
    summary_id: str,
    session: MongoClient = Depends(get_session),
   
):
    user = context_request.state.user
    db = session
    if not ObjectId.is_valid(summary_id):
        raise HTTPException(status_code=400, detail="Invalid ID format")
        
    result = db.custom_clear_history_original_summary.delete_one({
        "_id": ObjectId(summary_id),
        "user": user["sub"]
    })
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Summary not found or unauthorized")
        
    return {"message": "Successfully deleted"}