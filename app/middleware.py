import base64
import binascii
from fnmatch import fnmatch
import traceback
from loguru import logger
from starlette.authentication import AuthenticationBackend, AuthenticationError, SimpleUser, AuthCredentials
from starlette.middleware.authentication import AuthenticationMiddleware
from fastapi import Request
from fastapi_authz import CasbinMiddleware
from starlette.responses import JSONResponse

from fastapi import  HTTPException, status

import jwt  # PyJWT
from jwt.exceptions import InvalidTokenError, ExpiredSignatureError
from starlette.middleware.base import BaseHTTPMiddleware
from dotenv import load_dotenv
import os
import hmac

from app.api_keys.apis.api_keys import validate_api_key
from app.db_mongo import enforcer
from app.permission_core import get_route_path
from app.system_status import is_system_forbidden

load_dotenv()


class BasicAuth(AuthenticationBackend):
    async def authenticate(self, request):
        if request.url.path == "/ip_white_list/":
            return AuthCredentials(["authenticated"]), SimpleUser("anonymous")

        if "Authorization" not in request.headers:
            logger.info("没有Authorization")
            return None

        auth = request.headers["Authorization"]
        logger.info(auth)
        try:
            scheme, credentials = auth.split()
            decoded = base64.b64decode(credentials).decode("ascii")
        except (ValueError, UnicodeDecodeError, binascii.Error):
            raise AuthenticationError("Invalid basic auth credentials")

        username, _, password = decoded.partition(":")
        return AuthCredentials(["authenticated"]), SimpleUser(username)
class ForwardedPrefixMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        prefix = request.headers.get("x-forwarded-prefix", "")
        request.scope["root_path"] = prefix.rstrip("/")
        return await call_next(request)

#
# JWT 配置
ACCESS_TOKEN_SECRET_KEY = os.getenv("ACCESS_TOKEN_SECRET_KEY")
ACCESS_TOKEN_ALGORITHM = os.getenv("ACCESS_TOKEN_ALGORITHM")
def is_excluded(path: str) -> bool:
    """检查路径是否被排除"""
    for excluded in EXCLUDED_PATHS:
        if fnmatch(path, excluded):
            return True
    return False
EXCLUDED_PATHS = {
    "/api/login/",
    "/api/docs*",           # 匹配 /docs, /docs/, /docsomething
    "/api/openapi.json*",
    "/api/redoc/",
    "/api/qingguan/ip_white_list/",
    "/api/qingguan/update-md/latest/",
    "/api/excel-preview/",
    "/api/luckysheet-preview/",
    "/api/upload-excel-luckysheet/",
    "/api/process_excel_usp_data/",
    "/api/refresh/",
    "/api/17track/notify/",
}

# 只校验登录、不走 Casbin 的接口：所有登录用户都需要调用，
# 接口内部通过 app.auth_utils.ensure_self_or_admin 限制只能查询自己（admin 除外）
AUTH_ONLY_PATHS = {
    "/api/menu/user/get_user_menu_permissions/",
    "/api/casbin/policies/filter/",
}

# skudetail 外部调用使用的固定令牌，从环境变量读取；未配置时不启用
SKUDETAIL_STATIC_TOKEN = os.getenv("SKUDETAIL_STATIC_TOKEN")

# 前缀排除（用于 /static/xxx, /tiles/xxx 等）
EXCLUDED_PREFIXES = ("/static/", "/tiles", )
class AccessTokenAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 必须和路由实际匹配的路径一致：路由会先去掉 root_path（来自 X-Forwarded-Prefix 请求头）再匹配。
        # 原来用 request.url.path（不去 root_path）：请求 /static/api/users/ 并带上
        # X-Forwarded-Prefix: /static，这里按 /static/ 前缀放行，路由却执行了 /api/users/，等于绕过登录。
        path = get_route_path(request.scope) or "/"
        method = request.method
        if len(path) > 1 and not path.endswith('/'):
                path = path + '/'
        logger.debug(f"request.url.path: {path}")
        # 1. 完全匹配的路径（无需鉴权）
        if is_excluded(path):
            return await call_next(request)

        # 3. 前缀匹配的路径（如 /static/...）
        if any(path.startswith(prefix) for prefix in EXCLUDED_PREFIXES):
            return await call_next(request)
        # logger.info(f"request.headers:{request.headers}") 
        # 从请求头获取 token
        api_key_header_value = request.headers.get("X-API-Key")
        if api_key_header_value:
            # 中间件里抛出的 HTTPException 不会被 FastAPI 处理（会变成 500），这里转成对应的响应
            try:
                await validate_api_key(request)
            except HTTPException as e:
                return JSONResponse(status_code=e.status_code, content={"detail": e.detail})
            return await call_next(request)
        auth_header = request.headers.get("Authorization")
        
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "未提供认证令牌"}
            )
        
        token = auth_header.split(" ")[1]
        if (
            SKUDETAIL_STATIC_TOKEN
            and path.startswith("/api/skudetail/")
            and hmac.compare_digest(token.encode(), SKUDETAIL_STATIC_TOKEN.encode())
        ):
            return await call_next(request)
        # logger.info(f"request.url.path:{request.url.path}")
        try:
            # 验证并解码 token（jwt.decode 会校验 exp，过期时抛 ExpiredSignatureError）
            payload = jwt.decode(token, ACCESS_TOKEN_SECRET_KEY, algorithms=[ACCESS_TOKEN_ALGORITHM])
            if payload.get("exp") is None:
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={"detail": "令牌没有过期时间"}
                )
            # refresh token 只能用于 /refresh 换取 access token
            if payload.get("type") == "refresh":
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={"detail": "无效的令牌"}
                )

            # 将解码后的 payload 存储在请求状态中
            request.state.user = payload 

            subject = payload.get("sub")
            obj = path
            # /api 前缀只在路由层使用，Casbin 策略里存的路径不带 /api
            if obj.startswith("/api/"):
                obj = obj[4:]
            action = request.method
            logger.debug(f"{subject}-{obj}-{action}")

        except ExpiredSignatureError:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "令牌已过期"}
            )
        except InvalidTokenError:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "无效的令牌"}
            )
        except Exception as e:
            logger.error(traceback.format_exc())
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "无法验证令牌"}
            )

        if subject != "admin" and path not in AUTH_ONLY_PATHS:
            if is_system_forbidden():
                return JSONResponse(
                    status_code=status.HTTP_403_FORBIDDEN,
                    content={"detail": "系统已封禁"}
                )

            try:
                allowed = enforcer.enforce(subject, obj, action, {})
            except Exception:
                # 策略数据有问题（例如某条规则字段数不对）时每个请求都会报错。
                # 单独返回 500 并打印堆栈，不要伪装成“令牌无效”让前端以为是登录问题。
                logger.exception(f"Casbin 鉴权异常: {subject}-{obj}-{action}")
                return JSONResponse(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    content={"detail": "权限配置异常，请联系管理员"}
                )
            if not allowed:
                logger.info(f"没有权限: {subject}-{obj}-{action}")
                return JSONResponse(
                    status_code=status.HTTP_403_FORBIDDEN,
                    content={"detail": "没有权限"}
                )

        return await call_next(request)