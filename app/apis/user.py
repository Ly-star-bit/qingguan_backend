import os
import secrets
import string
import bcrypt
from fastapi import APIRouter, Body, Depends, Form, HTTPException, Request
from typing import List, Optional

import jwt
from openpyxl.descriptors import Bool
from pymongo import MongoClient
from app.auth_utils import is_admin_user
from app.db_mongo import get_session, enforcer
from app.casbin_subjects import ensure_username_available, rename_subject_in_casbin
from app.permission_core import parse_object_id, validate_name
from app.session_guard import forget, session_claims, session_valid
from app.system_status import invalidate_system_status_cache
from pydantic import BaseModel
from datetime import timedelta, datetime
from app.schemas import UserCreate, UserUpdate
from app.utils import (
    ACCESS_TOKEN_ALGORITHM,
    ACCESS_TOKEN_SECRET_KEY,
    create_access_token,
    create_refresh_token,
)


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
    if user_db.get("status", 1) == 0:
        # 前端登录页对 403 会直接显示 detail
        raise HTTPException(status_code=403, detail="账号已被禁用，请联系管理员")

    access_token_expires = timedelta(hours=3)
    access_token = create_access_token(
        data={
            "sub": user_db["username"],
            # 权限以服务端 Casbin 为准，不再把全部策略塞进 token；保留空数组兼容前端解析
            "permissions": [],
            "menu_ids": user_db.get("menu_ids", []),
            **session_claims(user_db),
        },
        expires_delta=access_token_expires,
    )

    refresh_token = create_refresh_token(data={"sub": user_db["username"], **session_claims(user_db)})
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

        # 账号不存在、已禁用、已改名或令牌签发后改过密码：用前端识别的 detail，让它直接跳回登录页
        forget(username)
        if not session_valid(payload):
            raise HTTPException(status_code=401, detail="Refresh token expired")
        user_db = session.users.find_one({"username": username})

        # 生成新的 access token
        access_token_expires = timedelta(hours=1)
        access_token = create_access_token(
            data={
                "sub": username,
                "permissions": [],
                "menu_ids": user_db.get("menu_ids", []),
                **session_claims(user_db),
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
    validate_name(user.username, "用户名")
    if db.users.find_one({"username": user.username}):
        raise HTTPException(status_code=400, detail="Username already exists")
    ensure_username_available(db, user.username)

    if user.status not in (0, 1):
        raise HTTPException(status_code=400, detail="status 只能是 0 或 1")
    hashed_password = bcrypt.hashpw(user.password.encode("utf-8"), bcrypt.gensalt())
    new_user = {"username": user.username, "password": hashed_password.decode("utf-8"), "status": user.status}
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
    request: Request,
    user_id: str,
    username: Optional[str] = Form(None),
    # 前端不改密码时不传 password（原来是必填，不改密码保存会直接 422）
    password: Optional[str] = Form(None),
    # 修改自己的账号必须填原始密码；管理员修改其他用户可以不填
    old_password: Optional[str] = Form(None),
    status: Optional[int] = Form(None),
    session: MongoClient = Depends(get_session),
):
    db = session
    user_oid = parse_object_id(user_id, "用户ID")
    user_db = db.users.find_one({"_id": user_oid})
    if not user_db:
        raise HTTPException(status_code=404, detail="用户未找到")

    old_name = user_db["username"]
    caller = (getattr(request.state, "user", None) or {}).get("sub")
    # admin 账号在中间件里跳过所有权限检查，修改它始终要原始密码，
    # 否则拥有 admin 角色的其他用户可以直接改掉它的密码并接管
    admin_editing_other = caller != old_name and old_name != "admin" and is_admin_user(caller)
    if not old_password and not admin_editing_other:
        raise HTTPException(status_code=400, detail="请输入原始密码（只有管理员修改其他用户时可以不填）")
    # 填了原始密码就照常校验（包括管理员）
    if old_password and not bcrypt.checkpw(
        old_password.encode("utf-8"), user_db["password"].encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="原始密码不正确")

    renamed = bool(username) and username != old_name
    if renamed:
        # 用户名是 Casbin 的 subject：admin 账号在中间件里直接放行，不允许改名；
        # 新名字不能和其他用户、角色重名，否则会继承对方的权限
        if old_name == "admin":
            raise HTTPException(status_code=400, detail="admin 账号不能改名")
        validate_name(username, "用户名")
        if db.users.find_one({"username": username, "_id": {"$ne": user_oid}}, {"_id": 1}):
            raise HTTPException(status_code=400, detail="Username already exists")
        ensure_username_available(db, username)

    if status is not None and status not in (0, 1):
        raise HTTPException(status_code=400, detail="status 只能是 0 或 1")
    if status == 0 and old_name == "admin":
        raise HTTPException(status_code=400, detail="admin 账号不能禁用")

    update_data = {}
    if renamed:
        update_data["username"] = username
    if password:
        hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
        update_data["password"] = hashed_password.decode("utf-8")
    if status is not None:
        update_data["status"] = status

    if update_data:
        change = {"$set": update_data}
        # 改密码、禁用、改名后，该账号已签发的令牌全部作废（自己改密码也要重新登录）
        if password or status == 0 or renamed:
            change["$inc"] = {"token_version": 1}
        db.users.update_one({"_id": user_oid}, change)
        forget(old_name, username or old_name)
    if renamed:
        # 角色分配和直接授予的规则跟着迁移到新用户名，旧用户名下不留规则
        rename_subject_in_casbin(old_name, username)

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
            "status": user.get("status", 1),
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


def _ensure_can_manage(request: Request, target: dict, action: str) -> None:
    """
    重置密码、删除账号的保护：admin 账号在中间件里跳过所有权限检查，不允许被这两个操作接管或删除；
    拥有 admin 角色的账号只能由管理员操作；不能删除自己
    """
    caller = (getattr(request.state, "user", None) or {}).get("sub")
    name = target["username"]
    if name == "admin":
        raise HTTPException(status_code=403, detail=f"admin 账号不能{action}")
    if action == "删除" and caller == name:
        raise HTTPException(status_code=400, detail="不能删除自己的账号")
    if is_admin_user(name) and not is_admin_user(caller):
        raise HTTPException(status_code=403, detail=f"只有管理员可以对管理员账号{action}")


@user_router.delete("/users/{user_id}/", summary="删除指定用户及其权限")
def delete_user(request: Request, user_id: str, session: MongoClient = Depends(get_session)):
    db = session
    user_oid = parse_object_id(user_id, "用户ID")
    user = db.users.find_one({"_id": user_oid})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    _ensure_can_manage(request, user, "删除")

    # 删除用户权限
    enforcer.delete_roles_for_user(user["username"])
    enforcer.delete_user(user["username"])

    db.users.delete_one({"_id": user_oid})
    forget(user["username"])

    return {"message": "User and associated permissions deleted successfully"}


@user_router.post("/users/reset-password/", summary="重置指定用户密码为随机临时密码")
def reset_password(request: Request, user_id: str = Form(...), session=Depends(get_session)):
    """重置为随机临时密码并返回（只返回这一次），该账号已登录的会话全部失效"""
    db = session
    user_oid = parse_object_id(user_id, "用户ID")
    user = db.users.find_one({"_id": user_oid})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    _ensure_can_manage(request, user, "重置密码")

    # 原来固定重置为 123456：重置后、本人改密前，任何人都能用它登录
    # 去掉容易看错的 0 O o 1 l I，方便管理员口头或手抄转告
    alphabet = "".join(c for c in string.ascii_letters + string.digits if c not in "0Oo1lI")
    temp_password = "".join(secrets.choice(alphabet) for _ in range(10))
    db.users.update_one(
        {"_id": user_oid},
        {
            "$set": {"password": bcrypt.hashpw(temp_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")},
            "$inc": {"token_version": 1},
        },
    )
    forget(user["username"])
    return {"message": "密码已重置", "password": temp_password}


@user_router.post("/system/forbidden/", summary="设置系统封禁或解封状态")
async def set_system_forbidden(
    forbidden: bool = Form(...),
    session: MongoClient = Depends(get_session),
    request: Request = None,
):
    """设置系统封禁状态"""
    # 检查是否为admin用户
    user = request.state.user
    if not is_admin_user(user.get("sub")):
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
