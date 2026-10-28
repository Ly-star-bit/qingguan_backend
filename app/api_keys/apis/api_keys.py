from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import APIKeyHeader
from bson import ObjectId
from bson.errors import InvalidId
import secrets
from datetime import datetime, timedelta
from typing import List, Optional
from app.db_mongo import db
from app.permission_core import get_route_path, norm_path, parse_object_id
from casbin.util import key_match3
from pymongo import ReturnDocument
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
    if not request.scopes:
        # 没有指定接口的密钥原来等于不受限制（相当于管理员），现在一律拒绝访问，创建时必须选择
        raise HTTPException(status_code=400, detail="请至少选择一个可访问的接口")
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
    
    if request.scopes is not None and not request.scopes:
        raise HTTPException(status_code=400, detail="请至少选择一个可访问的接口")
    if not update_data:
        raise HTTPException(status_code=400, detail="没有需要更新的内容")
    result = db.api_keys.update_one(
        {"_id": parse_object_id(key_id, "密钥ID")},
        {"$set": update_data}
    )
    if result.matched_count:
        return {"code": 200, "message": "更新成功"}
    raise HTTPException(status_code=404, detail="API密钥不存在")

@api_key_router.patch("/{key_id}/disable", summary="禁用API密钥")
async def disable_api_key(key_id: str):
    """
    禁用API密钥（安全删除）
    相比删除更安全，可以随时重新启用
    """
    result = db.api_keys.update_one(
        {"_id": parse_object_id(key_id, "密钥ID")},
        {"$set": {"status": "inactive"}}
    )
    if result.matched_count:
        return {"code": 200, "message": "密钥已禁用"}
    raise HTTPException(status_code=404, detail="API密钥不存在")

@api_key_router.patch("/{key_id}/enable", summary="启用API密钥")
async def enable_api_key(key_id: str):
    """
    启用API密钥
    """
    result = db.api_keys.update_one(
        {"_id": parse_object_id(key_id, "密钥ID")},
        {"$set": {"status": "active"}}
    )
    if result.matched_count:
        return {"code": 200, "message": "密钥已启用"}
    raise HTTPException(status_code=404, detail="API密钥不存在")

# 注意：保留删除功能但推荐使用禁用功能
@api_key_router.delete("/{key_id}", summary="删除API密钥")
async def delete_api_key(key_id: str):
    """
    删除API密钥（永久删除，不推荐使用）
    建议使用禁用功能代替删除
    """
    result = db.api_keys.delete_one({"_id": parse_object_id(key_id, "密钥ID")})
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
    
    # 检查权限范围：密钥只能调用 scopes 里登记的接口，没有指定接口的密钥一律拒绝。
    # 原来按「带 /api 的实际路径」精确查找接口登记，而登记的路径不带 /api、且是 {参数} 模板，
    # 永远查不到，范围检查从未生效，任何密钥都能调用所有接口。
    current_method = request.method
    # 与路由匹配的路径保持一致（request.url.path 带着 root_path，可被 X-Forwarded-Prefix 请求头篡改）
    current_path = get_route_path(request.scope) or "/"
    if current_path.startswith("/api/"):
        current_path = current_path[4:]
    current_path = norm_path(current_path)
    scope_ids = []
    for scope in api_key.get("scopes") or []:
        try:
            scope_ids.append(ObjectId(scope))
        except (InvalidId, TypeError):
            continue
    allowed = any(
        key_match3(current_path, norm_path(e.get("Path")))
        for e in db.api_endpoints.find({"_id": {"$in": scope_ids}, "Method": current_method}, {"Path": 1})
    )
    if not allowed:
        raise HTTPException(status_code=403, detail="API密钥没有访问此接口的权限")
    
    # 检查速率限制（固定时间窗口）：
    # 原来比较的是累计调用次数 usage_count，永远不清零，累计超过 rate_limit 后这个密钥就永久 429。
    # 现在按 rate_limit_window 秒为一个窗口计数，窗口过期后重新计数；usage_count 仍保留为累计次数。
    now = datetime.utcnow()
    rate_limit = api_key.get("rate_limit") or 1000
    rate_limit_window = api_key.get("rate_limit_window") or 60
    audit = {
        "last_used": now,
        "last_used_ip": request.client.host if request.client else None,
        "last_used_endpoint": f"{request.method} {current_path}",
    }

    # 当前窗口已过期（或从未计数）：开启新窗口，计数置 1
    updated_api_key = db.api_keys.find_one_and_update(
        {
            "_id": api_key["_id"],
            "$or": [
                {"window_started_at": {"$exists": False}},
                {"window_started_at": {"$lte": now - timedelta(seconds=rate_limit_window)}},
            ],
        },
        {"$set": {"window_started_at": now, "window_count": 1, **audit}, "$inc": {"usage_count": 1}},
        return_document=ReturnDocument.AFTER,
    )
    if updated_api_key is None:
        # 仍在当前窗口内：计数加 1
        updated_api_key = db.api_keys.find_one_and_update(
            {"_id": api_key["_id"]},
            {"$set": audit, "$inc": {"usage_count": 1, "window_count": 1}},
            return_document=ReturnDocument.AFTER,
        )
    if updated_api_key and updated_api_key.get("window_count", 0) > rate_limit:
        raise HTTPException(status_code=429, detail="API调用频率超限")

    return api_key

# 添加获取API密钥详情的端点
@api_key_router.get("/{key_id}", summary="获取单个API密钥的详细信息")
async def get_api_key(key_id: str):
    """
    获取单个API密钥的详细信息（不返回密钥值）
    """
    api_key = db.api_keys.find_one({"_id": parse_object_id(key_id, "密钥ID")})
    if not api_key:
        raise HTTPException(status_code=404, detail="API密钥不存在")
    
    api_key["_id"] = str(api_key["_id"])
    if "key" in api_key:
        del api_key["key"]  # 不返回密钥值
    
    return {"code": 200, "data": api_key}

