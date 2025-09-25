
from typing import List, Optional
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    HTTPException
)
from loguru import logger


from pydantic import BaseModel,Field
from app.db_mongo import get_session


class HaiYunZiShui(BaseModel):
    id: str = Field(default=None, primary_key=True, description="自增主键")
    zishui_name: str = Field(default=None, max_length=100, description="自税名称")
    sender: str = Field(default=None, description="发货人")
    receiver: str = Field(default=None, description="收货人")
haiyunzishui_router = APIRouter(tags=["海运自税"],prefix='/haiyunzishui')
@haiyunzishui_router.post(
    "/",
    summary="创建海运自税记录"
)
async def create_haiyunzishui(
    haiyunzishui: HaiYunZiShui, session: MongoClient = Depends(get_session)
):
    try:
        db = session

        haiyunzishui_dict = haiyunzishui.model_dump()
        haiyunzishui_dict.pop("id", None)

        result = db.haiyunzishui.insert_one(haiyunzishui_dict)
        if result.inserted_id:
            haiyunzishui_dict["_id"] = str(result.inserted_id)
            return haiyunzishui_dict
        raise HTTPException(status_code=500, detail="Failed to create haiyunzishui")
    except Exception as e:
        logger.error(f"Error creating haiyunzishui: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@haiyunzishui_router.get("/", response_model=List[HaiYunZiShui], summary="获取海运自税列表")
async def read_haiyunzishuis(
    session: MongoClient = Depends(get_session),
    skip: int = 0,
    limit: Optional[int] = None,
):
    try:
        db = session
        query = db.haiyunzishui.find().skip(skip)
        if limit:
            query = query.limit(limit)
        haiyunzishuis = list(query)
        # print(haiyunzishuis)
        for item in haiyunzishuis:
            # 将_id转换为字符串并赋值给id字段
            item["id"] = str(item["_id"])
            # 删除原始的_id字段
            item.pop("_id", None)

        return haiyunzishuis
    except Exception as e:
        logger.error(f"Error reading haiyunzishuis: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@haiyunzishui_router.get(
    "/{haiyunzishui_id}",
    summary="获取海运自税详情"
)
async def read_haiyunzishui(
    haiyunzishui_id: str, session: MongoClient = Depends(get_session)
):
    try:
        db = session
        if not ObjectId.is_valid(haiyunzishui_id):
            raise HTTPException(status_code=400, detail="Invalid ID format")

        haiyunzishui = db.haiyunzishui.find_one({"_id": ObjectId(haiyunzishui_id)})
        if not haiyunzishui:
            raise HTTPException(status_code=404, detail="Haiyunzishui not found")
        haiyunzishui["_id"] = str(haiyunzishui["_id"])
        return haiyunzishui
    except Exception as e:
        logger.error(f"Error reading haiyunzishui: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@haiyunzishui_router.put(
    "/{haiyunzishui_id}",
    summary="更新海运自税"
)
async def update_haiyunzishui(
    haiyunzishui_id: str,
    updated_haiyunzishui: HaiYunZiShui,
    session: MongoClient = Depends(get_session),
):
    try:
        db = session
        if not ObjectId.is_valid(haiyunzishui_id):
            raise HTTPException(status_code=400, detail="Invalid ID format")

        existing_haiyunzishui = db.haiyunzishui.find_one(
            {"_id": ObjectId(haiyunzishui_id)}
        )
        if not existing_haiyunzishui:
            raise HTTPException(status_code=404, detail="Haiyunzishui not found")

        update_data = updated_haiyunzishui.model_dump(exclude_unset=True)
        db.haiyunzishui.update_one(
            {"_id": ObjectId(haiyunzishui_id)}, {"$set": update_data}
        )
        updated_haiyunzishui = db.haiyunzishui.find_one(
            {"_id": ObjectId(haiyunzishui_id)}
        )
        updated_haiyunzishui["_id"] = str(updated_haiyunzishui["_id"])
        return updated_haiyunzishui
    except Exception as e:
        logger.error(f"Error updating haiyunzishui: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@haiyunzishui_router.delete(
    "/{haiyunzishui_id}",
    summary="删除海运自税"
)
async def delete_haiyunzishui(
    haiyunzishui_id: str, session: MongoClient = Depends(get_session)
):
    try:
        db = session
        if not ObjectId.is_valid(haiyunzishui_id):
            raise HTTPException(status_code=400, detail="Invalid ID format")

        haiyunzishui = db.haiyunzishui.find_one({"_id": ObjectId(haiyunzishui_id)})
        if not haiyunzishui:
            raise HTTPException(status_code=404, detail="Haiyunzishui not found")

        db.haiyunzishui.delete_one({"_id": ObjectId(haiyunzishui_id)})
        haiyunzishui["_id"] = str(haiyunzishui["_id"])
        return haiyunzishui
    except Exception as e:
        logger.error(f"Error deleting haiyunzishui: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")