from fastapi import APIRouter, Body, Depends, HTTPException, Request
from typing import Dict, List, Optional

from loguru import logger
from app.db_mongo import get_session
from bson import ObjectId
from app.permission_core import parse_object_id
from pydantic import BaseModel
from app.db_mongo import enforcer
from app.schemas import UpdateUserApiPermissions
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

    for path, path_item in paths.items():
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

            existing = db.api_endpoints.find_one({
                "ApiGroup": api_group,
                "Method": method.upper(),
                "Type": "ACL",
                "Path": path
            })

            if not existing:
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
                created_count += 1

    return {"message": f"同步完成，新增 {created_count} 个业务API端点。"}

@api_router.get("/user/get_user_api_permissions", summary="获取用户API权限")
async def get_user_api_permissions(user_id: str, session = Depends(get_session)):
    """获取用户API权限"""
    db = session
    user = db.users.find_one({"_id": ObjectId(user_id)})
    
    # 获取用户在Casbin中的权限
    user_policies = enforcer.get_filtered_policy(0, user_id)
    permissions = [f"{policy[1]}:{policy[2]}" for policy in user_policies]  # resource:action format
    
    return permissions



@api_router.put("/user/update_user_api_permissions", summary="更新用户API权限")
async def update_user_api_permissions(update_user_api_permissions: UpdateUserApiPermissions, session = Depends(get_session)):
    """更新用户API权限"""
    db = session
    user_id = update_user_api_permissions.user_id
    api_ids = update_user_api_permissions.api_ids
    db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"api_ids": api_ids}}
    )
    # 获取当前用户的所有策略
    current_policies = enforcer.get_filtered_policy(0, user_id)
    
    # 获取当前用户已有的api_ids
    current_api_ids = {policy[1] for policy in current_policies}
    
    # 需要更新的api_ids集合
    update_api_ids = set(api_ids)
    
    # 需要删除的api_ids
    for policy in current_policies:
        if policy[1] not in update_api_ids:
            enforcer.remove_policy([user_id, policy[1], "access"])
    
    # 添加新策略
    for api_id in update_api_ids - current_api_ids:
        # 从数据库中获取API端点信息以获取权限码
        api_endpoint = db.api_endpoints.find_one({"_id": ObjectId(api_id)})
        if api_endpoint and api_endpoint.get("PermissionCode"):
            # 使用API端点的PermissionCode作为权限
            permission_code = api_endpoint["PermissionCode"]
            # 分割权限码以获取资源和操作
            parts = permission_code.split(':')
            if len(parts) == 2:
                resource, action = parts
                enforcer.add_policy(user_id, resource, action, "allow")
        else:
            # 如果没有权限码，使用api_id作为资源
            enforcer.add_policy(user_id, api_id, "access", "allow")
    
    enforcer.load_policy()
    
    return {"message": "更新成功"}