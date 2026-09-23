from fastapi import HTTPException, Request

from app.db_mongo import enforcer


def is_admin_user(username: str) -> bool:
    """admin 账号，或拥有 admin 角色（含继承）的用户"""
    if not username:
        return False
    return username == "admin" or "admin" in enforcer.get_implicit_roles_for_user(username)


def ensure_self_or_admin(request: Request, username: str) -> None:
    """
    只允许查询自己的数据（admin 除外）。
    用于中间件 AUTH_ONLY_PATHS 中的接口：这些接口只校验登录、不走 Casbin，
    需要在接口内确认调用者就是被查询的用户。
    API Key 调用已由中间件校验过，直接放行。
    """
    user = getattr(request.state, "user", None)
    if user is None:
        if request.headers.get("X-API-Key"):
            return
        raise HTTPException(status_code=401, detail="未提供认证令牌")

    current = user.get("sub")
    if current == username or is_admin_user(current):
        return
    raise HTTPException(status_code=403, detail="没有权限")
