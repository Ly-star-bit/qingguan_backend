"""
统一认证登录（Hubs ONE 统一认证中心：企业微信扫码 / 邮箱账号）。

员工在统一认证中心登录后，带着授权码回到前端 /login/callback，前端把授权码交给这里：
后端用授权码（+ PKCE）换取身份令牌并校验，按绑定表找到本系统账号，再和账号密码登录一样签发本系统的令牌。
所以登录之后的会话、刷新、Casbin 权限都不变（令牌的 sub 仍是本系统用户名），权限仍在本系统里管理。
首次用统一认证登录时，员工输一次本系统账号密码完成绑定，以后扫码直接进入。

启用：在统一认证中心登记应用（「谁能使用」选「允许登录的用户都能用」）和网页（机密）客户端，
回调地址填 https://<本系统前端域名>/login/callback，再配置 .env 里的 OIDC_*（见 .env.example）。
"""
import base64
import hashlib
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlencode

import httpx
import jwt
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Request
from jwt.exceptions import InvalidTokenError, PyJWKClientConnectionError, PyJWKClientError
from loguru import logger
from pydantic import BaseModel
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.apis.user import check_password, issue_login_tokens
from app.db_mongo import db

load_dotenv()


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes")


OIDC_ENABLED = _env_flag("OIDC_ENABLED")
OIDC_AUTHORITY = os.getenv("OIDC_AUTHORITY", "").strip()
OIDC_CLIENT_ID = os.getenv("OIDC_CLIENT_ID", "").strip()
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET", "").strip()
OIDC_REDIRECT_URI = os.getenv("OIDC_REDIRECT_URI", "").strip()
# 内置 admin 账号默认不能用统一认证登录：共用的管理员账号不应只凭某一个人扫码就能进入
OIDC_ALLOW_ADMIN_BINDING = _env_flag("OIDC_ALLOW_ADMIN_BINDING")

# 只允许员工登录（统一认证中心的客户账号不能进本系统）
STAFF_USER_TYPE = "staff"
BUILTIN_ADMIN = "admin"
# 跳去统一认证中心到回来、首次绑定输密码，都要在这个时间内完成
PENDING_LIFETIME = timedelta(minutes=10)
# 每次绑定最多输错几次密码，超过后要重新扫码
MAX_BIND_ATTEMPTS = 5
METADATA_TTL_SECONDS = 3600

# 绑定表：统一认证中心的用户（签发方 + sub）对应本系统哪个账号。按 _id 关联，账号改名后绑定仍有效
BINDINGS = "user_oidc"
# 进行中的登录（state:…）和绑定（bind:…），过期后由 MongoDB 自动清理；多进程部署时各进程共用
PENDING = "oidc_pending"

try:
    db[BINDINGS].create_index([("issuer", 1), ("subject", 1)], unique=True)
    db[BINDINGS].create_index("user_id", unique=True)
    db[PENDING].create_index("expires_at", expireAfterSeconds=0)
except Exception:
    logger.exception("统一认证登录索引创建失败")

oidc_router = APIRouter(prefix="/oidc", tags=["统一认证登录"])


class OidcLoginIn(BaseModel):
    code: str
    state: str


class OidcBindIn(BaseModel):
    bind_ticket: str
    username: str
    password: str


# ---- 统一认证中心的发现文档和签名公钥 ----

_metadata: dict = {}
_metadata_expires = 0.0
# 遇到不认识的签名密钥（统一认证中心换了证书）时会自动重新拉取公钥
_jwks_client: Optional[jwt.PyJWKClient] = None


def _get_metadata() -> dict:
    global _metadata, _metadata_expires, _jwks_client
    if _metadata and time.monotonic() < _metadata_expires:
        return _metadata
    url = OIDC_AUTHORITY.rstrip("/") + "/.well-known/openid-configuration"
    try:
        response = httpx.get(url, timeout=15)
        response.raise_for_status()
        metadata = response.json()
    except Exception:
        logger.exception(f"获取统一认证中心配置失败：{url}")
        raise HTTPException(status_code=502, detail="统一认证中心暂时无法访问，请稍后重试")
    if _jwks_client is None or _jwks_client.uri != metadata["jwks_uri"]:
        _jwks_client = jwt.PyJWKClient(metadata["jwks_uri"], lifespan=METADATA_TTL_SECONDS)
    _metadata, _metadata_expires = metadata, time.monotonic() + METADATA_TTL_SECONDS
    return metadata


# ---- 进行中的登录和绑定 ----

def _put_pending(key: str, data: dict) -> None:
    db[PENDING].insert_one({"_id": key, **data, "expires_at": datetime.now(timezone.utc) + PENDING_LIFETIME})


def _alive(doc: Optional[dict]) -> bool:
    # MongoDB 每分钟才清理一次过期数据，读取时自己再核对
    return bool(doc) and doc["expires_at"].replace(tzinfo=timezone.utc) > datetime.now(timezone.utc)


def _ensure_enabled() -> None:
    if not OIDC_ENABLED:
        raise HTTPException(status_code=400, detail="统一认证登录未启用")


def _ensure_can_login(user_db: dict) -> None:
    """与账号密码登录相同的检查，另外内置 admin 账号默认不能用统一认证登录"""
    if user_db.get("status", 1) == 0:
        raise HTTPException(status_code=403, detail="账号已被禁用，请联系管理员")
    if user_db["username"] == BUILTIN_ADMIN and not OIDC_ALLOW_ADMIN_BINDING:
        raise HTTPException(status_code=403, detail="admin 账号请用账号密码登录")


def _redeem(code: str, state: str) -> dict:
    """用授权码换令牌，校验身份令牌，返回统一认证身份"""
    # state 只能用一次：取出即删除
    pending = db[PENDING].find_one_and_delete({"_id": f"state:{state}"})
    if not _alive(pending):
        raise HTTPException(status_code=400, detail="登录请求已过期或无效，请重新登录")

    metadata = _get_metadata()
    try:
        response = httpx.post(
            metadata["token_endpoint"],
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": OIDC_REDIRECT_URI,
                "code_verifier": pending["verifier"],
                "client_id": OIDC_CLIENT_ID,
                "client_secret": OIDC_CLIENT_SECRET,
            },
            timeout=15,
        )
        body = response.json()
    except Exception:
        logger.exception("统一认证换取令牌失败")
        raise HTTPException(status_code=502, detail="统一认证中心暂时无法访问，请稍后重试")
    if response.status_code != 200:
        logger.warning(f"统一认证换取令牌失败：{response.status_code} {body}")
        description = body.get("error_description") or "授权码无效或已过期"
        raise HTTPException(status_code=400, detail=f"统一认证登录失败：{description}")

    id_token = body.get("id_token")
    if not id_token:
        raise HTTPException(status_code=502, detail="统一认证没有返回身份令牌")
    try:
        key = _jwks_client.get_signing_key_from_jwt(id_token)
        claims = jwt.decode(
            id_token,
            key.key,
            algorithms=["RS256"],
            audience=OIDC_CLIENT_ID,
            issuer=metadata["issuer"],
            leeway=60,
            options={"require": ["exp", "iat", "iss", "aud", "sub"]},
        )
    except PyJWKClientConnectionError:
        logger.exception("获取统一认证中心签名公钥失败")
        raise HTTPException(status_code=502, detail="统一认证中心暂时无法访问，请稍后重试")
    except (PyJWKClientError, InvalidTokenError):
        logger.exception("统一认证身份令牌校验失败")
        raise HTTPException(status_code=400, detail="统一认证返回的身份令牌无效，请重新登录")

    if not secrets.compare_digest(str(claims.get("nonce", "")), pending["nonce"]):
        raise HTTPException(status_code=400, detail="统一认证返回的身份令牌无效，请重新登录")
    if claims.get("user_type") != STAFF_USER_TYPE:
        raise HTTPException(status_code=403, detail="只有员工账号可以登录本系统")
    return {"issuer": metadata["issuer"], "subject": claims["sub"], "display_name": claims.get("name")}


def _login_result(request: Request, user_db: dict) -> dict:
    return {**issue_login_tokens(db, user_db, request), "username": user_db["username"]}


# ---- 接口 ----

@oidc_router.get("/options", summary="统一认证登录是否启用")
def get_options():
    return {"enabled": OIDC_ENABLED}


@oidc_router.get("/authorize", summary="获取统一认证中心登录地址")
def get_authorize_url():
    _ensure_enabled()
    metadata = _get_metadata()
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    verifier = secrets.token_urlsafe(32)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")
    _put_pending(f"state:{state}", {"verifier": verifier, "nonce": nonce})
    query = urlencode({
        "client_id": OIDC_CLIENT_ID,
        "redirect_uri": OIDC_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid profile",
        "state": state,
        "nonce": nonce,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    })
    return {"url": f"{metadata['authorization_endpoint']}?{query}", "state": state}


@oidc_router.post("/login", summary="统一认证回调：用授权码登录")
def oidc_login(request: Request, body: OidcLoginIn):
    _ensure_enabled()
    identity = _redeem(body.code, body.state)

    binding = db[BINDINGS].find_one({"issuer": identity["issuer"], "subject": identity["subject"]})
    if not binding:
        ticket = secrets.token_urlsafe(24)
        _put_pending(f"bind:{ticket}", {"identity": identity, "attempts": 0})
        return {"bind_required": True, "bind_ticket": ticket, "display_name": identity["display_name"]}

    user_db = db.users.find_one({"_id": binding["user_id"]})
    if not user_db:
        raise HTTPException(status_code=403, detail="绑定的本系统账号已不存在，请联系管理员处理")
    _ensure_can_login(user_db)
    db[BINDINGS].update_one(
        {"_id": binding["_id"]},
        {"$set": {"username": user_db["username"], "display_name": identity["display_name"], "last_login": datetime.now()}},
    )
    return _login_result(request, user_db)


@oidc_router.post("/bind", summary="首次统一认证登录：绑定本系统账号")
def oidc_bind(request: Request, body: OidcBindIn):
    _ensure_enabled()
    key = f"bind:{body.bind_ticket}"
    pending = db[PENDING].find_one({"_id": key})
    if not _alive(pending):
        raise HTTPException(status_code=400, detail="绑定已过期，请重新用统一认证登录")
    identity = pending["identity"]

    user_db = db.users.find_one({"username": body.username})
    if not check_password(user_db, body.password):
        counted = db[PENDING].find_one_and_update({"_id": key}, {"$inc": {"attempts": 1}}, return_document=ReturnDocument.AFTER)
        if not counted or counted["attempts"] >= MAX_BIND_ATTEMPTS:
            db[PENDING].delete_one({"_id": key})
            raise HTTPException(status_code=400, detail="密码错误次数过多，请重新用统一认证登录")
        raise HTTPException(status_code=401, detail="账号或密码错误")
    _ensure_can_login(user_db)

    try:
        db[BINDINGS].insert_one({
            "issuer": identity["issuer"],
            "subject": identity["subject"],
            "user_id": user_db["_id"],
            "username": user_db["username"],
            "display_name": identity["display_name"],
            "created_at": datetime.now(),
            "last_login": datetime.now(),
        })
    except DuplicateKeyError:
        existing = db[BINDINGS].find_one({"issuer": identity["issuer"], "subject": identity["subject"]})
        # 同一次绑定重复提交（如连点两下）：已经绑好了，直接登录
        if not (existing and existing["user_id"] == user_db["_id"]):
            if existing:
                raise HTTPException(status_code=409, detail="该统一认证账号已绑定其他账号，请重新用统一认证登录")
            raise HTTPException(status_code=409, detail="该账号已绑定其他统一认证用户，请联系管理员")
    db[PENDING].delete_one({"_id": key})
    logger.warning(f"账号 {user_db['username']} 已绑定统一认证用户 {identity['subject']}（{identity['display_name']}）")
    return _login_result(request, user_db)
