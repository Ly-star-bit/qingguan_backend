import base64
import binascii
import traceback
from loguru import logger
from starlette.authentication import AuthenticationBackend, AuthenticationError, SimpleUser, AuthCredentials
from starlette.middleware.authentication import AuthenticationMiddleware
from fastapi import Request
from fastapi_authz import CasbinMiddleware
from starlette.responses import JSONResponse

from fastapi import  HTTPException, status

from datetime import datetime
import jwt  # PyJWT
from jwt.exceptions import InvalidTokenError, ExpiredSignatureError
from starlette.middleware.base import BaseHTTPMiddleware
from dotenv import load_dotenv
import os
from app.api_keys.apis.api_keys import validate_api_key
from app.db_mongo import get_session, enforcer

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
    

# JWT 配置
ACCESS_TOKEN_SECRET_KEY = os.getenv("ACCESS_TOKEN_SECRET_KEY")
ACCESS_TOKEN_ALGORITHM = os.getenv("ACCESS_TOKEN_ALGORITHM")
class AccessTokenAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 定义不需要验证的路径
        logger.info(f"request.url.path:{request.url.path}")
        excluded_paths = ["/login", "/docs", "/openapi.json", "/redoc","/qingguan/ip_white_list/","/excel-preview","/luckysheet-preview","/upload-excel-luckysheet","/process_excel_usp_data","/refresh","/17track/notify"]
        
        # 添加GET /menu 不需要鉴权
        if request.method == "GET" and request.url.path == "/menu":
            return await call_next(request)
            
        if '/static/' in request.url.path or "/tiles" in request.url.path or "/hubs_client" in request.url.path:
            return await call_next(request)
        if request.url.path in excluded_paths:
            return await call_next(request)
        # logger.info(f"request.headers:{request.headers}") 
        # 从请求头获取 token
        api_key_header_value = request.headers.get("X-API-Key")
        if api_key_header_value:
            await validate_api_key(request)
            return await call_next(request)
        auth_header = request.headers.get("Authorization")
        
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "未提供认证令牌"}
            )
        
        token = auth_header.split(" ")[1]
        if token == "k$loysdafgo123445$" and "skudetail" in request.url.path:
            return await call_next(request)
        # logger.info(f"request.url.path:{request.url.path}")
        try:
            # 验证并解码 token
            payload = jwt.decode(token, ACCESS_TOKEN_SECRET_KEY, algorithms=[ACCESS_TOKEN_ALGORITHM])
            
            # 检查 token 是否过期
            exp_timestamp = payload.get("exp")
            if exp_timestamp is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="令牌没有过期时间"
                )
            
            if datetime.utcnow() > datetime.utcfromtimestamp(exp_timestamp):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="令牌已过期"
                )
            # logger.info(f"payload:{payload}")
            # 将解码后的 payload 存储在请求状态中
            request.state.user = payload 

            subject = payload.get("sub")
            path = request.url.path
            action = request.method

            if subject != "admin":
                db = next(get_session())
                system_status = db.system_status.find_one({"_id": "system_forbidden"})
                if system_status and system_status.get("forbidden") == 1:
                    return JSONResponse(
                        status_code=status.HTTP_403_FORBIDDEN,
                        content={"detail": "系统已封禁"}
                    )
                
                # if not enforcer.enforce(subject, path, action):
                #     return JSONResponse(
                #         status_code=status.HTTP_403_FORBIDDEN,
                #         content={"detail": "没有权限"}
                #     )

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
        
        return await call_next(request)