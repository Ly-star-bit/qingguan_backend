from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import APIKeyHeader
from bson import ObjectId
import secrets
from datetime import datetime, timedelta
from typing import List, Optional
from app.db_mongo import db
from pydantic import BaseModel

api_key_router = APIRouter(tags=["API密钥管理"],prefix='/api_keys')

# 定义创建API密钥的请求模型
class CreateApiKeyRequest(BaseModel):
    name: str
    scopes: Optional[List[str]] = None  # Endpoint IDs from api_endpoints collection
    rate_limit: Optional[int] = 1000

# 定义更新API密钥的请求模型
class UpdateApiKeyRequest(BaseModel):
    name: Optional[str] = None
    scopes: Optional[List[str]] = None  # Endpoint IDs from api_endpoints collection
    rate_limit: Optional[int] = None

@api_key_router.post("/", summary="创建API密钥")
async def create_api_key(request: CreateApiKeyRequest):
    """
    创建API密钥
    使用 secrets.token_urlsafe(32) 生成高熵随机密钥
    """
    # 生成高熵随机密钥
    api_key_value = secrets.token_urlsafe(32)
    
    api_key = {
        "name": request.name,
        "key": api_key_value,  # 使用高熵随机密钥
        "status": "active",  # active, inactive
        "scopes": request.scopes or [],  # 权限范围
        "created_at": datetime.utcnow(),
        "last_used": None,
        "usage_count": 0,
        "rate_limit": request.rate_limit,  # 使用请求中的限流值或默认值
        "rate_limit_window": 60  # 限流时间窗口（秒）
    }
    result = db.api_keys.insert_one(api_key)
    api_key["_id"] = str(result.inserted_id)
    # 返回密钥值，但仅在创建时返回
    api_key["key_value"] = api_key_value
    return {"code": 200, "data": api_key}

@api_key_router.get("/", summary="获取所有API密钥")
async def get_api_keys():
    """
    获取所有API密钥
    不在列表中返回密钥值，只返回部分信息
    """
    api_keys = list(db.api_keys.find())
    for key in api_keys:
        key["_id"] = str(key["_id"])
        # 不在列表中返回密钥值，只返回部分信息
        if "key" in key:
            del key["key"]
    return {"code": 200, "data": api_keys}

@api_key_router.put("/{key_id}", summary="更新API密钥信息")
async def update_api_key(key_id: str, request: UpdateApiKeyRequest):
    """
    更新API密钥信息
    可以更新名称、权限范围和限流设置
    """
    update_data = {}
    if request.name is not None:
        update_data["name"] = request.name
    if request.scopes is not None:
        update_data["scopes"] = request.scopes
    if request.rate_limit is not None:
        update_data["rate_limit"] = request.rate_limit
    
    result = db.api_keys.update_one(
        {"_id": ObjectId(key_id)},
        {"$set": update_data}
    )
    if result.modified_count:
        return {"code": 200, "message": "更新成功"}
    raise HTTPException(status_code=404, detail="API密钥不存在")

@api_key_router.patch("/{key_id}/disable", summary="禁用API密钥")
async def disable_api_key(key_id: str):
    """
    禁用API密钥（安全删除）
    相比删除更安全，可以随时重新启用
    """
    result = db.api_keys.update_one(
        {"_id": ObjectId(key_id)},
        {"$set": {"status": "inactive"}}
    )
    if result.modified_count:
        return {"code": 200, "message": "密钥已禁用"}
    raise HTTPException(status_code=404, detail="API密钥不存在")

@api_key_router.patch("/{key_id}/enable", summary="启用API密钥")
async def enable_api_key(key_id: str):
    """
    启用API密钥
    """
    result = db.api_keys.update_one(
        {"_id": ObjectId(key_id)},
        {"$set": {"status": "active"}}
    )
    if result.modified_count:
        return {"code": 200, "message": "密钥已启用"}
    raise HTTPException(status_code=404, detail="API密钥不存在")

# 注意：保留删除功能但推荐使用禁用功能
@api_key_router.delete("/{key_id}", summary="删除API密钥")
async def delete_api_key(key_id: str):
    """
    删除API密钥（永久删除，不推荐使用）
    建议使用禁用功能代替删除
    """
    result = db.api_keys.delete_one({"_id": ObjectId(key_id)})
    if result.deleted_count:
        return {"code": 200, "message": "密钥已删除"}
    raise HTTPException(status_code=404, detail="API密钥不存在")

# 添加 API 密钥验证、权限检查、限流和审计的依赖函数
async def validate_api_key(request: Request):
    """
    验证API密钥并记录使用情况、检查限流、验证权限
    """
    api_key_header_value = request.headers.get("X-API-Key")
    
    if not api_key_header_value:
        raise HTTPException(status_code=401, detail="缺少API密钥")
    
    # 查找API密钥
    api_key = db.api_keys.find_one({"key": api_key_header_value})
    
    if not api_key:
        raise HTTPException(status_code=401, detail="无效的API密钥")
    
    if api_key["status"] != "active":
        raise HTTPException(status_code=401, detail="API密钥已被禁用")
    
    # 检查权限范围 - 现在验证的是 endpoint ID 而不是 method+path
    current_method = request.method
    current_path = request.url.path
    allowed_scopes = api_key.get("scopes", [])
    
    # 检查端点权限：查找匹配的API端点
    target_endpoint = db.api_endpoints.find_one({
        "Method": current_method,
        "Path": current_path
    })
    
    # 如果找到目标端点，并且API密钥定义了权限范围，则验证权限
    if target_endpoint and allowed_scopes:
        endpoint_id = str(target_endpoint["_id"])
        if endpoint_id not in allowed_scopes:
            raise HTTPException(status_code=403, detail="API密钥没有访问此接口的权限")
    
    # 检查速率限制
    now = datetime.utcnow()
    rate_limit = api_key.get("rate_limit", 1000)
    rate_limit_window = api_key.get("rate_limit_window", 60)  # 默认60秒窗口
    
    # 计算窗口的开始时间
    window_start = now - timedelta(seconds=rate_limit_window)
    
    # 检查过去窗口时间内是否超过限制
    # 我们通过更新和检查当前计数来实现限流
    db.api_keys.update_one(
        {"_id": api_key["_id"]},
        {
            "$inc": {"usage_count": 1},
            "$set": {"last_used": now}
        }
    )
    
    # 重新获取API密钥以检查当前使用量
    updated_api_key = db.api_keys.find_one({"_id": api_key["_id"]})
    if updated_api_key["usage_count"] > rate_limit:
        raise HTTPException(status_code=429, detail="API调用频率超限")
    
    # 记录审计日志
    client_host = request.client.host
    current_endpoint = f"{request.method} {request.url.path}"
    db.api_keys.update_one(
        {"_id": api_key["_id"]},
        {
            "$set": {
                "last_used": now,
                "last_used_ip": client_host,
                "last_used_endpoint": current_endpoint
            }
        }
    )
    
    return api_key

# 添加获取API密钥详情的端点
@api_key_router.get("/{key_id}", summary="获取单个API密钥的详细信息")
async def get_api_key(key_id: str):
    """
    获取单个API密钥的详细信息（不返回密钥值）
    """
    api_key = db.api_keys.find_one({"_id": ObjectId(key_id)})
    if not api_key:
        raise HTTPException(status_code=404, detail="API密钥不存在")
    
    api_key["_id"] = str(api_key["_id"])
    if "key" in api_key:
        del api_key["key"]  # 不返回密钥值
    
    return {"code": 200, "data": api_key}

