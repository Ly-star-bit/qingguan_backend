from fastapi import APIRouter, Body, Depends, HTTPException, Request
from typing import Dict, List, Optional

from loguru import logger
from app.db_mongo import get_session
from bson import ObjectId
from pydantic import BaseModel
from app.qingguan.apis.web_vba_mongo import enforcer
from app.schemas import UpdateUserApiPermissions
# 定义API端点模型
class ApiEndpoint(BaseModel):
    id: Optional[str] = None
    ApiGroup: str
    Method: str  
    Path: str
    Type: str
    Description: str

api_router = APIRouter(prefix="/api",tags=["api"])

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
        "Type": endpoint_dict["Type"],
        "Path": endpoint_dict["Path"]
    })
    if existing:
        raise HTTPException(status_code=400, detail="该Method和Path组合已存在")
        
    result = db.api_endpoints.insert_one(endpoint_dict)
    # 如果类型是RBAC，则添加Casbin分组策略
    if endpoint.Type == "RBAC":
        enforcer.add_grouping_policy(endpoint.Path, endpoint.ApiGroup)
        enforcer.save_policy()
        enforcer.load_policy()
    return {"id": str(result.inserted_id)}

@api_router.get("/api_endpoints", summary="获取所有API端点，按ApiGroup分组")
async def get_api_endpoints(session = Depends(get_session)):
    """获取所有API端点,按ApiGroup分组"""
    db = session
    
    # 获取所有不同的ApiGroup
    api_groups = db.api_endpoints.distinct("ApiGroup")
    
    # 按组构建返回数据
    result = {}
    for group in api_groups:
        endpoints = []
        for endpoint in db.api_endpoints.find({"ApiGroup": group}):
            endpoint["id"] = str(endpoint["_id"])
            del endpoint["_id"]
            endpoints.append(endpoint)
        result[group] = endpoints
        
    return result

@api_router.put("/api_endpoints/{endpoint_id}", summary="更新API端点")
async def update_api_endpoint(endpoint_id: str, endpoint: ApiEndpoint, session = Depends(get_session)):
    """更新API端点"""
    db = session

    # 获取旧的端点信息
    old_endpoint_data = db.api_endpoints.find_one({"_id": ObjectId(endpoint_id)})
    if not old_endpoint_data:
        raise HTTPException(status_code=404, detail="API端点不存在")
    
    old_path = old_endpoint_data.get("Path")
    old_api_group = old_endpoint_data.get("ApiGroup")
    old_type = old_endpoint_data.get("Type")

    endpoint_dict = endpoint.dict(exclude_unset=True)
    if "id" in endpoint_dict:
        del endpoint_dict["id"]
        
    # 检查Method和Path组合是否与其他记录冲突
    existing = db.api_endpoints.find_one({
        "_id": {"$ne": ObjectId(endpoint_id)},
        "ApiGroup": endpoint_dict["ApiGroup"],
        "Method": endpoint_dict["Method"],
        "Type": endpoint_dict["Type"],
        "Path": endpoint_dict["Path"]
    })
    if existing:
        raise HTTPException(status_code=400, detail="该Method和Path组合已存在")
        
    result = db.api_endpoints.update_one(
        {"_id": ObjectId(endpoint_id)},
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
        enforcer.add_grouping_policy(endpoint.Path, endpoint.ApiGroup)
        policy_changed = True
    if endpoint.Type == "RBAC":
            logger.info(f"endpoint.Path: {endpoint.Path}")
            logger.info(f"endpoint.ApiGroup: {endpoint.ApiGroup}")
            result1 = enforcer.add_grouping_policy(endpoint.Path, endpoint.ApiGroup)
            logger.info(f"result1: {result1}")
            policy_changed = True
    logger.info(f"policy_changed: {policy_changed}")
    if policy_changed:
        enforcer.save_policy()
        enforcer.load_policy()
    if result.modified_count == 0 and not policy_changed:
        return {"message": "未作修改"}
        
    return {"message": "更新成功"}

@api_router.delete("/api_endpoints/{endpoint_id}", summary="删除API端点")
async def delete_api_endpoint(endpoint_id: str, session = Depends(get_session)):
    """删除API端点"""
    db = session

    # 获取端点信息以便删除Casbin策略
    endpoint_data = db.api_endpoints.find_one({"_id": ObjectId(endpoint_id)})
    if not endpoint_data:
        raise HTTPException(status_code=404, detail="API端点不存在")

    result = db.api_endpoints.delete_one({"_id": ObjectId(endpoint_id)})

    if result.deleted_count > 0:
        # 如果类型是RBAC，则删除Casbin策略
        if endpoint_data.get("Type") == "RBAC":
            path = endpoint_data.get("Path")
            api_group = endpoint_data.get("ApiGroup")
            if path and api_group and enforcer.has_grouping_policy(path, api_group):
                enforcer.remove_grouping_policy(path, api_group)
                enforcer.save_policy()
                enforcer.load_policy()

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="API端点不存在")
    return {"message": "删除成功"}

@api_router.post("/api_endpoints/sync_from_openapi", summary="从OpenAPI同步API端点")
async def sync_from_openapi(request: Request, session=Depends(get_session)):
    """从OpenAPI规范自动同步API端点"""
    db = session
    openapi_schema = request.app.openapi()
    
    paths = openapi_schema.get("paths", {})
    created_count = 0
    
    for path, path_item in paths.items():
        for method, operation in path_item.items():
            # 常见的HTTP方法
            if method.upper() in ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"]:
                summary = operation.get("summary", "No description")
                tags = operation.get("tags", [])
                api_group = tags[0] if tags else "default"
                
                # 检查Method和Path组合是否已存在
                existing = db.api_endpoints.find_one({
                    "ApiGroup": api_group,
                    "Method": method.upper(),
                    "Type": "ACL",
                    "Path": path
                })
                
                if not existing:
                    endpoint_data = {
                        "ApiGroup": api_group,
                        "Method": method.upper(),
                        "Type": "ACL",
                        "Path": path,
                        "Description": summary,
                    }
                    db.api_endpoints.insert_one(endpoint_data)
                    created_count += 1
                    
    return {"message": f"同步完成，新增 {created_count} 个API端点。"}

@api_router.get("/user/get_user_api_permissions", summary="获取用户API权限")
async def get_user_api_permissions(user_id: str, session = Depends(get_session)):
    """获取用户API权限"""
    db = session
    user = db.users.find_one({"_id": ObjectId(user_id)})
    return user.get("api_ids", [])



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
    enforcer.update_filtered_policies    # 需要删除的api_ids
    for policy in current_policies:
        if policy[1] not in update_api_ids:
            enforcer.remove_policy(user_id, policy[1], policy[2])
            
    # 需要添加的api_ids
    for api_id in update_api_ids:
        if api_id not in current_api_ids:
            # 从数据库获取API信息
            api_info = db.api_endpoints.find_one({"_id": ObjectId(api_id)})
            if api_info:
                # 添加策略: user_id api_id method
                enforcer.add_policy(user_id, api_id, api_info["Method"])
    
    enforcer.load_policy()
    return {"message": "API权限更新成功"}