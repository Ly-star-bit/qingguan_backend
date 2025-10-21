from fastapi import APIRouter, Body, Depends, HTTPException, Request
from typing import Dict, List, Optional
from app.db_mongo import get_session, enforcer
from bson import ObjectId
from datetime import datetime
from pydantic import BaseModel


# 定义权限模型
class PermissionItem(BaseModel):
    id: Optional[str] = None
    code: str                   
    name: str                   
    action: str                  # e.g., "read"
    menu_ids: Optional[List[str]] = []  # 可选：关联菜单用于前端分组（非必须）
    description: Optional[str] = None
    dynamic_params: Optional[Dict] = None  # 动态参数，用于处理动态权限


class CreatePermissionItem(BaseModel):
    code: str                   
    name: str                  
    action: str                  # e.g., "read"
    menu_ids: Optional[List[str]] = [] # 可选：关联菜单用于前端分组（非必须）
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
            # "resource": permission["resource"],
            "action": permission["action"],
            "menu_ids": permission.get("menu_ids"),
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
        # 检查 menu_ids 是否相同
        existing_menu_ids = set(existing_permission.get("menu_ids", []) or [])
        new_menu_ids = set(permission_item.menu_ids or [])
        
        if existing_menu_ids != new_menu_ids:
            # 如果 menu_ids 不同，合并两个集合
            merged_menu_ids = list(existing_menu_ids.union(new_menu_ids))
            
            # 更新现有权限的 menu_ids
            db.permissions.update_one(
                {"_id": existing_permission["_id"]},
                {"$set": {"menu_ids": merged_menu_ids}}
            )
            
            return {
                "id": str(existing_permission["_id"]), 
                "message": "权限已存在，menu_ids 已更新合并",
                "merged_menu_ids": merged_menu_ids
            }
        else:
            # menu_ids 相同，返回已存在错误
            raise HTTPException(status_code=400, detail="权限码已存在")
    else:
        # 权限不存在，创建新权限
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
        # "resource": permission["resource"],
        "action": permission["action"],
        "menu_id": permission.get("menu_id"),
        "description": permission.get("description"),
        "dynamic_params": permission.get("dynamic_params")
    }
    return PermissionItem(**permission_dict)




