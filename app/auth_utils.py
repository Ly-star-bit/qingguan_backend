from fastapi import HTTPException, Request

from app.db_mongo import enforcer
from app.permission_core import ADMIN_ROLE


def is_admin_user(username: str) -> bool:
    """admin 账号，或拥有管理员角色 admin_role（含继承，被禁用的角色不算）的用户"""
    if not username:
        return False
    return username == "admin" or ADMIN_ROLE in enforcer.get_implicit_roles_for_user(username)


def grants_admin(role: str) -> bool:
    """
    角色是管理员角色，或继承了管理员角色（中间角色被禁用也算：重新启用后就会生效）。
    分配、移除、禁用、删除这样的角色都会改变谁是管理员，只能由管理员操作
    """
    seen, level = set(), [role]
    while level:
        if ADMIN_ROLE in level:
            return True
        seen.update(level)
        level = [g[1] for name in level for g in enforcer.get_filtered_named_grouping_policy("g", 0, name)
                 if len(g) > 1 and g[1] not in seen]
    return False


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
