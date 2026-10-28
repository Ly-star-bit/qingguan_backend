"""
登录会话校验：令牌签发后账号被禁用、删除、改名、改密码或重置密码时，让已签发的令牌失效。

令牌里带 uid（用户 _id）和 ver（签发时用户的 token_version）。中间件每个请求核对一次
（按用户名缓存几秒，避免每个请求都查库），不一致时返回「令牌已过期」：前端会去刷新令牌，
刷新接口同样核对，失败后前端跳回登录页。
旧版本签发的令牌没有 uid/ver：ver 按 0 处理、不核对 uid，账号被禁用或改密码后同样失效。
"""
import time
from typing import Any, Dict, Optional

from app.db_mongo import db

# 多进程部署时，其他进程最多在这么多秒后感知到变化
_CACHE_TTL_SECONDS = 5
_cache: Dict[str, tuple] = {}


def _user_state(username: str) -> Optional[dict]:
    now = time.monotonic()
    hit = _cache.get(username)
    if hit and hit[1] > now:
        return hit[0]
    doc = db.users.find_one({"username": username}, {"_id": 1, "status": 1, "token_version": 1})
    _cache[username] = (doc, now + _CACHE_TTL_SECONDS)
    return doc


def session_claims(user: dict) -> Dict[str, Any]:
    """签发令牌时写入的会话字段"""
    return {"uid": str(user["_id"]), "ver": int(user.get("token_version") or 0)}


def session_valid(payload: dict) -> bool:
    """令牌对应的账号仍然存在、未禁用，且令牌签发后没有被注销过"""
    username = payload.get("sub")
    user = _user_state(username) if username else None
    if not user or user.get("status", 1) == 0:
        return False
    uid = payload.get("uid")
    if uid and uid != str(user["_id"]):
        # 原账号已改名或删除，同名的是另一个账号
        return False
    return int(payload.get("ver") or 0) == int(user.get("token_version") or 0)


def forget(*usernames: str) -> None:
    """账号状态变化后清掉本进程的缓存，让下一个请求立即读到新状态"""
    for name in usernames:
        _cache.pop(name, None)
