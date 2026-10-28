from fastapi import APIRouter, Body, Depends, HTTPException, Request
from typing import Dict, List, Optional

from loguru import logger
from app.db_mongo import get_session
from bson import ObjectId
from app.permission_core import norm_path, parse_object_id
from pydantic import BaseModel
from app.db_mongo import enforcer
# 定义API端点模型
class ApiEndpoint(BaseModel):
    id: Optional[str] = None
    ApiGroup: str
    Method: str  
    Path: str
    Type: Optional[str] = None
    Description: str
    PermissionCode: Optional[str] = None    # 🔑 关键！关联 PermissionItem.code，如 "product:read"

api_router = APIRouter(tags=["api_endpoints"])

@api_router.post("/api_endpoints", summary="创建API端点")
async def create_api_endpoint(endpoint: ApiEndpoint, session = Depends(get_session)):
    """创建API端点"""
    db = session
    endpoint_dict = endpoint.dict(exclude_unset=True)
    if "id" in endpoint_dict:
        del endpoint_dict["id"]
        
    # 检查Method和Path组合是否已存在
    existing = db.api_endpoints.find_one({
        "ApiGroup": endpoint_dict["ApiGroup"],
        "Method": endpoint_dict["Method"],
        "Type": endpoint_dict.get("Type", "ACL"),  # 默认为ACL类型
        "Path": endpoint_dict["Path"]
    })
    if existing:
        raise HTTPException(status_code=400, detail="该Method和Path组合已存在")
        
    result = db.api_endpoints.insert_one(endpoint_dict)
    # 如果类型是RBAC，则添加Casbin分组策略
    if endpoint.Type == "RBAC":
        if not enforcer.has_grouping_policy(endpoint.Path, endpoint.ApiGroup):
            enforcer.add_grouping_policy(endpoint.Path, endpoint.ApiGroup)
        enforcer.load_policy()
    return {"id": str(result.inserted_id)}

@api_router.get("/api_endpoints", summary="获取所有API端点，按ApiGroup分组")
async def get_api_endpoints(session = Depends(get_session)):
    """获取所有API端点,按ApiGroup分组"""
    db = session
    
    # 分组顺序沿用 distinct 的顺序；端点一次查出后在内存中分组（原来每个分组查一次）
    api_groups = db.api_endpoints.distinct("ApiGroup")
    result = {group: [] for group in api_groups}
    for endpoint in db.api_endpoints.find({"ApiGroup": {"$in": api_groups}}):
        endpoint["id"] = str(endpoint["_id"])
        del endpoint["_id"]
        result[endpoint.get("ApiGroup")].append(endpoint)

    return result

@api_router.put("/api_endpoints/{endpoint_id}", summary="更新API端点")
async def update_api_endpoint(endpoint_id: str, endpoint: ApiEndpoint, session = Depends(get_session)):
    """更新API端点"""
    db = session
    endpoint_oid = parse_object_id(endpoint_id, "API端点ID")

    # 获取旧的端点信息
    old_endpoint_data = db.api_endpoints.find_one({"_id": endpoint_oid})
    if not old_endpoint_data:
        raise HTTPException(status_code=404, detail="API端点不存在")
    
    old_path = old_endpoint_data.get("Path")
    old_api_group = old_endpoint_data.get("ApiGroup")
    old_type = old_endpoint_data.get("Type")
    old_permission_code = old_endpoint_data.get("PermissionCode")

    endpoint_dict = endpoint.dict(exclude_unset=True)
    if "id" in endpoint_dict:
        del endpoint_dict["id"]
        
    # 检查Method和Path组合是否与其他记录冲突
    existing = db.api_endpoints.find_one({
        "_id": {"$ne": endpoint_oid},
        "ApiGroup": endpoint_dict["ApiGroup"],
        "Method": endpoint_dict["Method"],
        "Type": endpoint_dict.get("Type", "ACL"),  # 默认为ACL类型
        "Path": endpoint_dict["Path"]
    })
    if existing:
        raise HTTPException(status_code=400, detail="该Method和Path组合已存在")
        
    result = db.api_endpoints.update_one(
        {"_id": endpoint_oid},
        {"$set": endpoint_dict}
    )
    
    policy_changed = False
    # 如果旧类型是RBAC，并且(类型、路径或分组)已更改，则删除旧策略
    if old_type == "RBAC" and (endpoint.Type != "RBAC" or old_path != endpoint.Path or old_api_group != endpoint.ApiGroup):
        if enforcer.has_grouping_policy(old_path, old_api_group):
            enforcer.remove_grouping_policy(old_path, old_api_group)
            policy_changed = True
            
    # 如果新类型是RBAC，并且(类型、路径或分组)已更改，则添加新策略
    if endpoint.Type == "RBAC" and (old_type != "RBAC" or old_path != endpoint.Path or old_api_group != endpoint.ApiGroup):
        if not enforcer.has_grouping_policy(endpoint.Path, endpoint.ApiGroup):
            enforcer.add_grouping_policy(endpoint.Path, endpoint.ApiGroup)
        policy_changed = True
    if endpoint.Type == "RBAC":
        logger.info(f"endpoint.Path: {endpoint.Path}")
        logger.info(f"endpoint.ApiGroup: {endpoint.ApiGroup}")
        if not enforcer.has_grouping_policy(endpoint.Path, endpoint.ApiGroup):
            result1 = enforcer.add_grouping_policy(endpoint.Path, endpoint.ApiGroup)
            logger.info(f"result1: {result1}")
            policy_changed = True
    logger.info(f"policy_changed: {policy_changed}")
    if policy_changed:
        enforcer.load_policy()
    if result.modified_count == 0 and not policy_changed:
        return {"message": "未作修改"}
        
    return {"message": "更新成功"}

@api_router.delete("/api_endpoints/{endpoint_id}", summary="删除API端点")
async def delete_api_endpoint(endpoint_id: str, session = Depends(get_session)):
    """删除API端点"""
    db = session
    endpoint_oid = parse_object_id(endpoint_id, "API端点ID")

    # 获取端点信息以便删除Casbin策略
    endpoint_data = db.api_endpoints.find_one({"_id": endpoint_oid})
    if not endpoint_data:
        raise HTTPException(status_code=404, detail="API端点不存在")

    result = db.api_endpoints.delete_one({"_id": endpoint_oid})

    if result.deleted_count > 0:
        # 如果类型是RBAC，则删除Casbin策略
        if endpoint_data.get("Type") == "RBAC":
            path = endpoint_data.get("Path")
            api_group = endpoint_data.get("ApiGroup")
            if path and api_group and enforcer.has_grouping_policy(path, api_group):
                enforcer.remove_grouping_policy(path, api_group)
                enforcer.load_policy()

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="API端点不存在")
    return {"message": "删除成功"}

@api_router.post("/api_endpoints/sync_from_openapi", summary="从OpenAPI同步API端点")
async def sync_from_openapi(request: Request, session=Depends(get_session)):
    db = session
    openapi_schema = request.app.openapi()
    paths = openapi_schema.get("paths", {})
    created_count = 0
    # 已登记的接口按「路径（去掉末尾 /）+ 方法」判断，与分组、类型无关，避免同一接口被重复登记
    registered = {
        (norm_path(e.get("Path")), str(e.get("Method") or "").upper())
        for e in db.api_endpoints.find({}, {"Path": 1, "Method": 1})
    }

    for path, path_item in paths.items():
        # 中间件鉴权前会去掉请求路径的 /api 前缀（见 app/middleware.py），
        # 接口登记和 Casbin 规则里的路径都不带 /api；带上的话分配的权限永远匹配不上
        if path.startswith("/api/"):
            path = path[4:]
        # 跳过 OpenAPI 自身和管理接口
        if any(path.startswith(prefix) for prefix in ["/docs", "/redoc", "/openapi", "/api_endpoints", "/api/api_endpoints"]):
            continue

        for method, operation in path_item.items():
            if method.upper() not in {"GET", "POST", "PUT", "DELETE", "PATCH"}:
                continue

            tags = operation.get("tags", [])
            # 跳过无业务标签的接口（如健康检查）
            if not tags:
                continue

            api_group = tags[-1]
            # 可选：跳过特定管理 tag
            if api_group in {"api", "docs", "health"}:
                continue

            summary = operation.get("summary") or operation.get("description") or "No description"

            if (norm_path(path), method.upper()) not in registered:
                # 根据 HTTP 方法自动设置 PermissionCode
                action_map = {
                    "GET": "read",
                    "POST": "create", 
                    "PUT": "update",
                    "PATCH": "update",
                    "DELETE": "delete"
                }
                action = action_map.get(method.upper(), "access")
                permission_code = f"{api_group.lower()}_{summary}:{action}"
                
                db.api_endpoints.insert_one({
                    "ApiGroup": api_group,
                    "Method": method.upper(),
                    "Type": "ACL",
                    "Path": path,
                    "Description": summary,
                    "PermissionCode": permission_code,
                })
                registered.add((norm_path(path), method.upper()))
                created_count += 1

    return {"message": f"同步完成，新增 {created_count} 个业务API端点。"}


# 已删除 GET /user/get_user_api_permissions 和 PUT /user/update_user_api_permissions（前端没有调用）：
# 前者把用户 ID 当作 Casbin subject 查询（实际 subject 是用户名），查不到任何数据；
# 后者写入只有 4 个字段的 p 规则（模型要求 6 个字段），写入后所有非 admin 用户的鉴权都会报错。
