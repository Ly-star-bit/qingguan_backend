"""
系统封禁状态（system_status 集合）带短时缓存的读取。
中间件对每个非 admin 请求都要检查一次，原来每次都查库。
"""
import time

from app.db_mongo import db

_CACHE_TTL_SECONDS = 5
_cache = {"value": None, "expires_at": 0.0}


def is_system_forbidden() -> bool:
    now = time.monotonic()
    if _cache["value"] is None or now >= _cache["expires_at"]:
        doc = db.system_status.find_one({"_id": "system_forbidden"})
        _cache["value"] = bool(doc and doc.get("forbidden") == 1)
        _cache["expires_at"] = now + _CACHE_TTL_SECONDS
    return _cache["value"]


def invalidate_system_status_cache() -> None:
    """修改封禁状态后调用，让下一次请求立即读到新状态"""
    _cache["value"] = None
