import os
import bcrypt
from fastapi import APIRouter, Body, Depends, Form, HTTPException, Request
from typing import List, Optional

import jwt
from openpyxl.descriptors import Bool
from pymongo import MongoClient
from app.db_mongo import get_session, enforcer
from app.permission_core import parse_object_id
from app.system_status import invalidate_system_status_cache
from pydantic import BaseModel
from passlib.context import CryptContext
from datetime import timedelta, datetime
from app.schemas import UserCreate, UserUpdate
from app.utils import (
    ACCESS_TOKEN_ALGORITHM,
    ACCESS_TOKEN_SECRET_KEY,
    create_access_token,
    create_refresh_token,
)

# 密码加密
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# 定义用户模型
class User(BaseModel):
    id: Optional[str] = None
    username: str
    password: str
    status: Optional[int] = 1
    last_login: Optional[datetime] = None
    last_ip: Optional[str] = None
    menu_ids: Optional[List[str]] = None


class UserOut(BaseModel):
    """返回给前端的用户信息（不含密码哈希）"""
    id: Optional[str] = None
    username: str
    status: Optional[int] = 1
    last_login: Optional[datetime] = None
    last_ip: Optional[str] = None
    menu_ids: Optional[List[str]] = None


def _user_out(user: dict) -> UserOut:
    return UserOut(
        id=str(user["_id"]) if "_id" in user else user.get("id"),
        username=user["username"],
        status=user.get("status", 1),
        last_login=user.get("last_login"),
        last_ip=user.get("last_ip"),
        menu_ids=user.get("menu_ids"),
    )


class UserLogin(BaseModel):
    username: str
    password: str


user_router = APIRouter(tags=["user"])


@user_router.post("/login", summary="用户登录，获取访问和刷新令牌")
def login_for_access_token(
    request: Request, user: UserLogin, session: MongoClient = Depends(get_session)
):
    db = session
    user_db = db.users.find_one({"username": user.username})
    if not user_db or not bcrypt.checkpw(
        user.password.encode("utf-8"), user_db["password"].encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    access_token_expires = timedelta(hours=3)
    access_token = create_access_token(
        data={
            "sub": user_db["username"],
            # 权限以服务端 Casbin 为准，不再把全部策略塞进 token；保留空数组兼容前端解析
            "permissions": [],
            "menu_ids": user_db.get("menu_ids", []),
        },
        expires_delta=access_token_expires,
    )

    refresh_token = create_refresh_token(data={"sub": user_db["username"]})
    # 更新用户登录时间以及ip
    db.users.update_one(
        {"username": user_db["username"]},
        {"$set": {"last_login": datetime.now(), "last_ip": request.client.host}},
    )
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }


@user_router.post("/refresh", summary="使用刷新令牌获取新的访问令牌")
def refresh_token(
    refresh_token: str = Body(..., embed=True),
    session: MongoClient = Depends(get_session),
):
    try:
        payload = jwt.decode(
            refresh_token, ACCESS_TOKEN_SECRET_KEY, algorithms=[ACCESS_TOKEN_ALGORITHM]
        )
        # 只接受 refresh token（access token 不能用来续期）。
        # 返回 "Refresh token expired" 让前端走重新登录流程（旧版本签发的 refresh token 没有 type）
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Refresh token expired")
        username = payload.get("sub")

        # 验证用户是否存在
        user_db = session.users.find_one({"username": username})
        if not user_db:
            raise HTTPException(status_code=401, detail="Invalid user")

        # 生成新的 access token
        access_token_expires = timedelta(hours=1)
        access_token = create_access_token(
            data={
                "sub": username,
                "permissions": [],
                "menu_ids": user_db.get("menu_ids", []),
            },
            expires_delta=access_token_expires,
        )

        return {"access_token": access_token}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Refresh token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")


@user_router.post("/users/", response_model=UserOut, summary="创建新用户")
def create_user(user: UserCreate, session: MongoClient = Depends(get_session)):
    db = session
    if db.users.find_one({"username": user.username}):
        raise HTTPException(status_code=400, detail="Username already exists")

    hashed_password = bcrypt.hashpw(user.password.encode("utf-8"), bcrypt.gensalt())
    new_user = {"username": user.username, "password": hashed_password.decode("utf-8")}
    result = db.users.insert_one(new_user)
    new_user["_id"] = result.inserted_id

    # 添加用户权限
    # for perm in user.permissions:
    #     obj, act = perm.split(':')
    #     enforcer.add_policy(user.username, obj, act,'allow')

    # enforcer.load_policy()
    return _user_out(new_user)


@user_router.put("/users/{user_id}/", response_model=UserOut, summary="更新指定用户信息")
def update_user(
    user_id: str,
    username: str = Form(...),
    password: str = Form(...),
    old_password: str = Form(...),
    session: MongoClient = Depends(get_session),
):
    db = session
    user_oid = parse_object_id(user_id, "用户ID")
    user_db = db.users.find_one({"_id": user_oid})
    if not user_db:
        raise HTTPException(status_code=404, detail="用户未找到")

    # 验证原始密码是否正确
    if not bcrypt.checkpw(
        old_password.encode("utf-8"), user_db["password"].encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="原始密码不正确")

    update_data = {}
    if username:
        update_data["username"] = username
    if password:
        hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
        update_data["password"] = hashed_password.decode("utf-8")

    if update_data:
        db.users.update_one({"_id": user_oid}, {"$set": update_data})

    return _user_out(db.users.find_one({"_id": user_oid}))


@user_router.get("/users/", summary="获取用户列表，支持分页")
def read_users(
    skip: int = 0,
    limit: int = 20,
    all_data: bool = False,
    session: MongoClient = Depends(get_session),
    context_request: Request = None,
):
    """
    获取用户列表，支持分页，并返回总数，排除admin用户
    修复 ObjectId 不能被序列化的问题
    """
    db = session
    current = getattr(context_request.state, "user", None) or {}
    user = current.get("sub")

    if user != "admin":
        query = {"username": {"$ne": "admin"}}
    else:
        query = {}
    total = db.users.count_documents(query)
    users = []
    
    # 如果all_data为True则不分页
    cursor = db.users.find(query)
    if not all_data:
        cursor = cursor.skip(skip).limit(limit)
        
    for user in cursor:
        user_dict = {
            "id": str(user.get("_id")),
            "username": user.get("username"), 
            "status": 1,
            "last_login": user.get("last_login"),
            "last_ip": user.get("last_ip"),
            # 只返回需要的字段，避免返回 ObjectId 或密码等敏感信息
        }
        users.append(user_dict)
    return {"total": total, "users": users}


@user_router.get("/users/{user_id}/", response_model=UserOut, summary="获取指定用户信息")
def read_user(user_id: str, session: MongoClient = Depends(get_session)):
    db = session
    user = db.users.find_one({"_id": parse_object_id(user_id, "用户ID")})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return _user_out(user)


@user_router.delete("/users/{user_id}/", summary="删除指定用户及其权限")
def delete_user(user_id: str, session: MongoClient = Depends(get_session)):
    db = session
    user_oid = parse_object_id(user_id, "用户ID")
    user = db.users.find_one({"_id": user_oid})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # 删除用户权限
    enforcer.delete_roles_for_user(user["username"])
    enforcer.delete_user(user["username"])

    db.users.delete_one({"_id": user_oid})

    return {"message": "User and associated permissions deleted successfully"}


@user_router.post("/users/reset-password/", summary="重置指定用户密码为默认值")
async def reset_password(user_id: str = Form(...), session=Depends(get_session)):
    """重置密码"""
    db = session
    default_password = "123456"
    hashed_password = pwd_context.hash(default_password)

    result = db.users.update_one(
        {"_id": parse_object_id(user_id, "用户ID")}, {"$set": {"password": hashed_password}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"message": "Password has been reset to default"}


@user_router.post("/system/forbidden/", summary="设置系统封禁或解封状态")
async def set_system_forbidden(
    forbidden: bool = Form(...),
    session: MongoClient = Depends(get_session),
    request: Request = None,
):
    """设置系统封禁状态"""
    # 检查是否为admin用户
    user = request.state.user
    if user.get("sub") != "admin":
        raise HTTPException(status_code=403, detail="只有管理员可以操作此接口")

    db = session
    # 更新系统状态表
    result = db.system_status.update_one(
        {"_id": "system_forbidden"},
        {"$set": {"forbidden": 1 if forbidden else 0}},
        upsert=True,
    )
    invalidate_system_status_cache()

    return {"message": "系统已封禁" if forbidden else "系统已解封", "status": "success"}


@user_router.get("/system/forbidden/status/", summary="获取系统封禁状态")
async def get_system_forbidden_status(session: MongoClient = Depends(get_session)):
    """获取系统封禁状态"""
    db = session
    status = db.system_status.find_one({"_id": "system_forbidden"})

    if not status:
        return {"forbidden": False}

    return {"forbidden": bool(status.get("forbidden", 0))}
