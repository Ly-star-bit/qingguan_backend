from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import APIKeyHeader
from bson import ObjectId
from app.db_mongo import db

api_key_router = APIRouter(tags=["API密钥管理"])
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

@api_key_router.post("/api_keys")
async def create_api_key(name: str):
    """创建API密钥"""
    api_key = {
        "name": name,
        "key": str(ObjectId()), # 使用ObjectId作为API密钥
        "status": 1  # 1表示启用
    }
    result = db.api_keys.insert_one(api_key)
    api_key["_id"] = str(result.inserted_id)
    return {"code": 200, "data": api_key}

@api_key_router.get("/api_keys")
async def get_api_keys():
    """获取所有API密钥"""
    api_keys = list(db.api_keys.find())
    for key in api_keys:
        key["_id"] = str(key["_id"])
    return {"code": 200, "data": api_keys}

@api_key_router.put("/api_keys/{key_id}")
async def update_api_key(key_id: str, name: str):
    """更新API密钥信息"""
    result = db.api_keys.update_one(
        {"_id": ObjectId(key_id)},
        {"$set": {"name": name}}
    )
    if result.modified_count:
        return {"code": 200, "message": "更新成功"}
    raise HTTPException(status_code=404, detail="API密钥不存在")

@api_key_router.delete("/api_keys/{key_id}")
async def delete_api_key(key_id: str):
    """删除API密钥"""
    result = db.api_keys.delete_one({"_id": ObjectId(key_id)})
    if result.deleted_count:
        return {"code": 200, "message": "删除成功"}
    raise HTTPException(status_code=404, detail="API密钥不存在")
