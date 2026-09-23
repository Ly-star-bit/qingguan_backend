import os
import bcrypt
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from typing import List, Optional

import jwt
from pymongo import MongoClient
from app.db_mongo import get_session, enforcer
from app.casbin_subjects import ensure_role_name_available, rename_subject_in_casbin
from app.permission_core import parse_object_id, validate_name
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


def _remove_role_from_casbin(role_name: str) -> None:
    """删除角色在 Casbin 中的全部数据：该角色的 p 规则、成员关系、角色自身的继承关系"""
    enforcer.remove_filtered_policy(0, role_name)
    enforcer.remove_filtered_grouping_policy(1, role_name)
    enforcer.remove_filtered_grouping_policy(0, role_name)


@role_router.post("/roles/", response_model=Role, summary="创建新角色")
def create_role(role: RoleCreate, session: MongoClient = Depends(get_session)):
    """
    创建新角色
    """
    db = session
    validate_name(role.role_name, "角色名")
    if db.roles.find_one({"role_name": role.role_name}):
        raise HTTPException(status_code=400, detail="Role name already exists")
    ensure_role_name_available(db, role.role_name)

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
    role_oid = parse_object_id(role_id, "角色ID")
    role = db.roles.find_one({"_id": role_oid})
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
    role_oid = parse_object_id(role_id, "角色ID")
    # 先检查角色是否存在
    existing_role = db.roles.find_one({"_id": role_oid})
    if not existing_role:
        raise HTTPException(status_code=404, detail="Role not found")

    old_name = existing_role.get("role_name")
    renamed = role_update.role_name is not None and role_update.role_name != old_name
    if renamed:
        validate_name(role_update.role_name, "角色名")
        if db.roles.find_one({"role_name": role_update.role_name, "_id": {"$ne": role_oid}}):
            raise HTTPException(status_code=400, detail="Role name already exists")
        ensure_role_name_available(db, role_update.role_name)

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
        {"_id": role_oid}, 
        {"$set": update_data}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Role not found")

    if renamed and old_name:
        rename_subject_in_casbin(old_name, role_update.role_name)

    # 返回更新后的角色信息
    updated_role = db.roles.find_one({"_id": role_oid})
    updated_role["id"] = str(updated_role["_id"])
    return updated_role


@role_router.delete("/roles/{role_id}/", summary="删除指定角色")
def delete_role(role_id: str, session: MongoClient = Depends(get_session)):
    """
    删除指定角色
    """
    db = session
    role_oid = parse_object_id(role_id, "角色ID")
    role = db.roles.find_one({"_id": role_oid})
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    # 删除角色相关的 Casbin 数据（原来用 3 个字段删除 6 字段的规则，永远匹配不上）
    _remove_role_from_casbin(role["role_name"])
    enforcer.load_policy()

    # 从数据库删除角色
    result = db.roles.delete_one({"_id": role_oid})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Role not found")

    return {"message": "Role deleted successfully"}


@role_router.get("/roles/{role_id}/permissions/", summary="获取角色权限")
def get_role_permissions(role_id: str, session: MongoClient = Depends(get_session)):
    """
    获取角色的权限列表
    """
    db = session
    role_oid = parse_object_id(role_id, "角色ID")
    role = db.roles.find_one({"_id": role_oid})
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    return {"permissions": role.get("permissions", [])}


# 已删除三个旧接口（前端没有调用）：
# - assign-permissions / assign-to-user：写入只有 3 个字段的 p 规则（模型要求 6 个字段），
#   写入后 enforce 遍历到这条规则就抛 "invalid policy size"，所有非 admin 用户的请求都会失败；
# - create-admin-role：只在 roles 集合里建一条 "*:*" 的记录，不写 Casbin 规则，没有实际效果。
# 角色的接口权限和用户的角色分配统一通过 /casbin/policies/groups 维护。
