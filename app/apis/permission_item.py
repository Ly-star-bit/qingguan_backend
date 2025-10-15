from fastapi import APIRouter, Body, Depends, HTTPException, Request
from typing import Dict, List, Optional
from app.db_mongo import get_session, enforcer
from bson import ObjectId
from datetime import datetime
from pydantic import BaseModel


# 定义权限模型
class PermissionItem(BaseModel):
    id: Optional[str] = None
    code: str                    # e.g., "product:read"
    name: str                    # e.g., "产品查看"
    resource: str                # e.g., "product"
    action: str                  # e.g., "read"
    menu_id: Optional[str] = None # 可选：关联菜单用于前端分组（非必须）
    description: Optional[str] = None
    dynamic_params: Optional[Dict] = None  # 动态参数，用于处理动态权限


class CreatePermissionItem(BaseModel):
    code: str                    # e.g., "product:read"
    name: str                    # e.g., "产品查看"
    resource: str                # e.g., "product"
    action: str                  # e.g., "read"
    menu_id: Optional[str] = None # 可选：关联菜单用于前端分组（非必须）
    description: Optional[str] = None
    dynamic_params: Optional[Dict] = None  # 动态参数，用于处理动态权限


permission_item_router = APIRouter(tags=["permission_item"])

@permission_item_router.get("/permission_item", response_model=List[PermissionItem], summary="获取权限列表")
async def get_permission_list(session = Depends(get_session)):
    """获取权限列表"""
    db = session
    
    permissions = list(db.permissions.find({}))
    
    permission_list = []
    for permission in permissions:
        permission_id = str(permission["_id"])
        permission_dict = {
            "id": permission_id,
            "code": permission["code"],
            "name": permission["name"],
            "resource": permission["resource"],
            "action": permission["action"],
            "menu_id": permission.get("menu_id"),
            "description": permission.get("description"),
            "dynamic_params": permission.get("dynamic_params")
        }
        permission_list.append(PermissionItem(**permission_dict))
        
    return permission_list


@permission_item_router.post("/permission_item", summary="创建权限")
async def create_permission_item(permission_item: CreatePermissionItem, session = Depends(get_session)):
    """创建权限"""
    db = session
    
    # 构建查询条件，包含动态参数
    query = {"code": permission_item.code}
    
    # 如果有动态参数，也需要检查动态参数是否匹配
    if permission_item.dynamic_params:
        query["dynamic_params"] = permission_item.dynamic_params
    
    existing_permission = db.permissions.find_one(query)
    if existing_permission:
        raise HTTPException(status_code=400, detail="权限码已存在")
    
    permission_dict = permission_item.dict(exclude_unset=True)
        
    result = db.permissions.insert_one(permission_dict)
    return {"id": str(result.inserted_id)}


@permission_item_router.post("/permission_item/check_exists", summary="检查权限是否存在（含动态参数）")
async def check_permission_exists(code: str, dynamic_params: Optional[Dict] = None, session = Depends(get_session)):
    """检查权限是否已存在，支持动态参数匹配"""
    db = session
    
    # 构建查询条件，包含动态参数
    query = {"code": code}
    
    # 如果有动态参数，也需要检查动态参数是否匹配
    if dynamic_params:
        query["dynamic_params"] = dynamic_params
    
    existing_permission = db.permissions.find_one(query)
    if existing_permission:
        return {"exists": True, "id": str(existing_permission["_id"])}
    else:
        return {"exists": False}


@permission_item_router.put("/permission_item/{permission_id}", summary="更新权限")
async def update_permission_item(permission_id: str, permission_item: CreatePermissionItem, session = Depends(get_session)):
    """更新权限"""
    db = session
    
    permission_dict = permission_item.dict(exclude_unset=True)
        
    result = db.permissions.update_one(
        {"_id": ObjectId(permission_id)},
        {"$set": permission_dict}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="权限项未找到")
    return {"message": "权限项已更新"}


@permission_item_router.delete("/permission_item/{permission_id}", summary="删除权限")
async def delete_permission_item(permission_id: str, session = Depends(get_session)):
    """删除权限"""
    db = session
    
    # 检查权限是否被使用
    # 这里可以检查是否有用户或角色关联了这个权限
    # 示例：检查casbin策略中是否存在该权限
    try:
        # 简单检查，实际项目中可能需要更复杂的检查
        pass
    except Exception:
        pass
    
    result = db.permissions.delete_one({"_id": ObjectId(permission_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="权限项未找到")
    return {"message": "权限项已删除"}


@permission_item_router.get("/permission_item/{permission_id}", response_model=PermissionItem, summary="根据ID获取权限")
async def get_permission_by_id(permission_id: str, session = Depends(get_session)):
    """根据ID获取权限"""
    db = session
    
    permission = db.permissions.find_one({"_id": ObjectId(permission_id)})
    if not permission:
        raise HTTPException(status_code=404, detail="权限项未找到")
    
    permission_dict = {
        "id": str(permission["_id"]),
        "code": permission["code"],
        "name": permission["name"],
        "resource": permission["resource"],
        "action": permission["action"],
        "menu_id": permission.get("menu_id"),
        "description": permission.get("description"),
        "dynamic_params": permission.get("dynamic_params")
    }
    return PermissionItem(**permission_dict)


@permission_item_router.get("/permission_item/search", response_model=List[PermissionItem], summary="搜索权限")
async def search_permissions(resource: Optional[str] = None, action: Optional[str] = None, session = Depends(get_session)):
    """根据资源或操作搜索权限"""
    db = session
    
    query = {}
    if resource:
        query["resource"] = resource
    if action:
        query["action"] = action
    
    permissions = list(db.permissions.find(query))
    
    permission_list = []
    for permission in permissions:
        permission_dict = {
            "id": str(permission["_id"]),
            "code": permission["code"],
            "name": permission["name"],
            "resource": permission["resource"],
            "action": permission["action"],
            "menu_id": permission.get("menu_id"),
            "description": permission.get("description"),
            "dynamic_params": permission.get("dynamic_params")
        }
        permission_list.append(PermissionItem(**permission_dict))
        
    return permission_list


@permission_item_router.post("/permission_item/generate_test_data", summary="生成测试权限数据")
async def generate_test_permission_data(session = Depends(get_session)):
    """生成测试权限数据"""
    db = session
    
    # 清空现有权限数据
    db.permissions.delete_many({})
    
    # 创建测试权限数据
    test_permissions = [
        {
            "code": "user:read",
            "name": "用户查看",
            "resource": "user",
            "action": "read",
            "description": "查看用户信息的权限",
            "dynamic_params": None
        },
        {
            "code": "user:create",
            "name": "用户创建",
            "resource": "user",
            "action": "create",
            "description": "创建用户的权限",
            "dynamic_params": None
        },
        {
            "code": "user:update",
            "name": "用户更新",
            "resource": "user",
            "action": "update",
            "description": "更新用户信息的权限",
            "dynamic_params": None
        },
        {
            "code": "user:delete",
            "name": "用户删除",
            "resource": "user",
            "action": "delete",
            "description": "删除用户的权限",
            "dynamic_params": None
        },
        {
            "code": "product:read",
            "name": "产品查看",
            "resource": "product",
            "action": "read",
            "description": "查看产品的权限",
            "dynamic_params": None
        },
        {
            "code": "product:create",
            "name": "产品创建",
            "resource": "product",
            "action": "create",
            "description": "创建产品的权限",
            "dynamic_params": None
        },
        {
            "code": "order:read",
            "name": "订单查看",
            "resource": "order",
            "action": "read",
            "description": "查看订单的权限",
            "dynamic_params": None
        },
        {
            "code": "order:manage",
            "name": "订单管理",
            "resource": "order",
            "action": "manage",
            "description": "管理订单的权限",
            "dynamic_params": None
        }
    ]
    
    result = db.permissions.insert_many(test_permissions)
    
    return {"message": "测试权限数据已生成", "count": len(result.inserted_ids)}