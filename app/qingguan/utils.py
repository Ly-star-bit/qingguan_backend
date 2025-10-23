
from fastapi import (
    HTTPException,
    Query,
    Request,
    status,
)

from app.db_mongo import enforcer

async def require_products_permission(
    request: Request,
    startland: str = Query("China", description="起运地"),
    destination: str = Query("America", description="目的地"),
):
    """
    路由级鉴权依赖：
    - 从 middleware 写入的 request.state.user 里取 subject(sub)
    - 使用请求路径作为 obj，HTTP 方法作为 act
    - 把 startland/destination 作为 ABAC 属性传入 attrs
    """
    user_state = getattr(request.state, "user", None)
    subject = (user_state or {}).get("sub") if isinstance(user_state, dict) else None
    if not subject:
        # middleware 没有写入 sub 或者未登录
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthenticated: missing sub",
        )

    obj = request.url.path
    act = request.method
    env = {"startland": startland, "destination": destination}
    # 根据你的模型：(sub, obj, act, attrs) 进行 ABAC 鉴权
    allowed = enforcer.enforce(subject, obj, act, env)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="startland没有权限"
        )
