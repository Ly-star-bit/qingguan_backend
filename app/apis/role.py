import os
import bcrypt
from fastapi import APIRouter, Body, Depends, Form, HTTPException, Request
from typing import List, Optional

import jwt
from pymongo import MongoClient
from app.db_mongo import get_session, enforcer
from bson import ObjectId
from pydantic import BaseModel
from passlib.context import CryptContext
from datetime import datetime


# 定义角色相关的请求模型
class RoleCreate(BaseModel):
    role_name: str
    description: Optional[str] = None
    permissions: Optional[List[str]] = []  # 


class RoleUpdate(BaseModel):
    role_name: Optional[str] = None
    description: Optional[str] = None
    permissions: Optional[List[str]] = None
    status: Optional[int] = None


# 定义角色模型
class Role(BaseModel):
    id: Optional[str] = None
    role_name: str
    description: Optional[str] = None
    permissions: Optional[List[str]] = []  # 权限列表，格式为 "object:action"
    status: Optional[int] = 1  # 1: active, 0: inactive
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RoleLogin(BaseModel):
    role_name: str
    password: str


role_router = APIRouter(tags=["role"])


@role_router.post("/roles/", response_model=Role, summary="创建新角色")
def create_role(role: RoleCreate, session: MongoClient = Depends(get_session)):
    """
    创建新角色
    """
    db = session
    if db.roles.find_one({"role_name": role.role_name}):
        raise HTTPException(status_code=400, detail="Role name already exists")

    new_role = {
        "role_name": role.role_name,
        "description": role.description,
        "permissions": role.permissions or [],
        "status": 1,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    result = db.roles.insert_one(new_role)
    new_role["id"] = str(result.inserted_id)

    return new_role


@role_router.get("/roles/", summary="获取角色列表，支持分页")
def read_roles(
    skip: int = 0,
    limit: int = 20,
    all_data: bool = False,
    session: MongoClient = Depends(get_session),
    context_request: Request = None,
):
    """
    获取角色列表，支持分页，并返回总数
    修复 ObjectId 不能被序列化的问题
    """
    db = session
    
    total = db.roles.count_documents({})
    roles = []
    
    # 如果all_data为True则不分页
    cursor = db.roles.find({})
    if not all_data:
        cursor = cursor.skip(skip).limit(limit)
        
    for role in cursor:
        role_dict = {
            "id": str(role.get("_id")),
            "role_name": role.get("role_name"), 
            "description": role.get("description"),
            "permissions": role.get("permissions", []),
            "status": role.get("status", 1),
            "created_at": role.get("created_at"),
            "updated_at": role.get("updated_at"),
        }
        roles.append(role_dict)
    return {"total": total, "roles": roles}


@role_router.get("/roles/{role_id}/", response_model=Role, summary="获取指定角色信息")
def read_role(role_id: str, session: MongoClient = Depends(get_session)):
    """
    获取指定角色信息
    """
    db = session
    role = db.roles.find_one({"_id": ObjectId(role_id)})
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    role["id"] = str(role["_id"])
    return role


@role_router.put("/roles/{role_id}/", response_model=Role, summary="更新指定角色信息")
def update_role(
    role_id: str,
    role_update: RoleUpdate,
    session: MongoClient = Depends(get_session),
):
    """
    更新指定角色信息
    """
    db = session
    # 先检查角色是否存在
    existing_role = db.roles.find_one({"_id": ObjectId(role_id)})
    if not existing_role:
        raise HTTPException(status_code=404, detail="Role not found")

    # 准备更新数据
    update_data = {}
    if role_update.role_name is not None:
        update_data["role_name"] = role_update.role_name
    if role_update.description is not None:
        update_data["description"] = role_update.description
    if role_update.permissions is not None:
        update_data["permissions"] = role_update.permissions
    if role_update.status is not None:
        update_data["status"] = role_update.status
    update_data["updated_at"] = datetime.now()

    # 更新角色
    result = db.roles.update_one(
        {"_id": ObjectId(role_id)}, 
        {"$set": update_data}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Role not found")

    # 返回更新后的角色信息
    updated_role = db.roles.find_one({"_id": ObjectId(role_id)})
    updated_role["id"] = str(updated_role["_id"])
    return updated_role


@role_router.delete("/roles/{role_id}/", summary="删除指定角色")
def delete_role(role_id: str, session: MongoClient = Depends(get_session)):
    """
    删除指定角色
    """
    db = session
    role = db.roles.find_one({"_id": ObjectId(role_id)})
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    # 删除角色相关的权限策略
    # 获取所有包含该角色名的策略并删除
    all_policies = enforcer.get_policy()
    for policy in all_policies:
        if policy[0] == role["role_name"]:  # role_name is in subject position
            enforcer.remove_policy(policy[0], policy[1], policy[2])
    
    enforcer.load_policy()
    
    # 从数据库删除角色
    result = db.roles.delete_one({"_id": ObjectId(role_id)})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Role not found")

    return {"message": "Role deleted successfully"}


@role_router.post("/roles/{role_id}/assign-permissions/", summary="为角色分配权限")
def assign_permissions_to_role(
    role_id: str,
    permissions: List[str] = Body(..., description="权限列表，格式为 'object:action'"),
    session: MongoClient = Depends(get_session),
):
    """
    为角色分配权限
    """
    db = session
    role = db.roles.find_one({"_id": ObjectId(role_id)})
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    # 更新角色的权限列表
    result = db.roles.update_one(
        {"_id": ObjectId(role_id)},
        {
            "$set": {
                "permissions": permissions,
                "updated_at": datetime.now()
            }
        }
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Role not found")

    # 清除该角色之前的所有策略
    # 获取所有包含该角色名的策略并删除
    all_policies = enforcer.get_policy()
    for policy in all_policies:
        if policy[0] == role["role_name"]:  # role_name is in subject position
            enforcer.remove_policy(policy[0], policy[1], policy[2])

    # 根据需要更新Casbin策略
    # 这里需要根据具体权限格式解析并添加到Casbin中
    for perm in permissions:
        if ':' in perm:
            obj, act = perm.split(':', 1)
            # 以角色名作为用户，添加策略
            if not enforcer.has_policy(role["role_name"], obj, act):
                enforcer.add_policy(role["role_name"], obj, act)
    
    enforcer.load_policy()

    return {"message": "Permissions assigned to role successfully", "permissions": permissions}


@role_router.get("/roles/{role_id}/permissions/", summary="获取角色权限")
def get_role_permissions(role_id: str, session: MongoClient = Depends(get_session)):
    """
    获取角色的权限列表
    """
    db = session
    role = db.roles.find_one({"_id": ObjectId(role_id)})
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    return {"permissions": role.get("permissions", [])}


@role_router.post("/roles/{role_id}/assign-to-user/", summary="将角色分配给用户")
def assign_role_to_user(
    role_id: str,
    username: str = Body(..., description="用户名"),
    session: MongoClient = Depends(get_session),
):
    """
    将角色分配给用户，实际上是将角色的权限赋予用户
    """
    db = session
    role = db.roles.find_one({"_id": ObjectId(role_id)})
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    user = db.users.find_one({"username": username})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # 获取角色的权限
    role_permissions = role.get("permissions", [])

    # 为用户添加角色拥有的所有权限
    for perm in role_permissions:
        if ':' in perm:
            obj, act = perm.split(':', 1)
            # 检查是否已存在该策略
            if not enforcer.has_policy(username, obj, act):
                enforcer.add_policy(username, obj, act)

    enforcer.load_policy()

    return {"message": f"Role {role['role_name']} assigned to user {username} successfully", "permissions_added": role_permissions}


@role_router.post("/roles/create-admin-role/", summary="创建默认管理员角色")
def create_admin_role(session: MongoClient = Depends(get_session)):
    """
    创建默认管理员角色，拥有所有权限
    """
    db = session
    # 检查是否已存在管理员角色
    if db.roles.find_one({"role_name": "admin"}):
        raise HTTPException(status_code=400, detail="Admin role already exists")

    # 创建管理员角色，拥有所有权限
    admin_role = {
        "role_name": "admin",
        "description": "系统管理员角色，拥有所有权限",
        "permissions": ["*:*"],  # 所有对象的所有操作
        "status": 1,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    result = db.roles.insert_one(admin_role)
    admin_role["id"] = str(result.inserted_id)

    return {"message": "Admin role created successfully", "role": admin_role}