from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Dict, List, Optional
from app.auth_utils import ensure_self_or_admin
from app.db_mongo import get_session, enforcer
from pydantic import BaseModel

from app.permission_core import (
    build_endpoint_index,
    collect_allow_policies,
    endpoint_path_candidates,
    match_permission_attrs,
    parse_attrs_list,
    parse_object_id,
    permission_code_key,
    serialize_permission,
)


# 定义权限模型
class PermissionItem(BaseModel):
    id: Optional[str] = None
    code: str
    name: str
    action: str                  # e.g., "read"
    menu_ids: Optional[List[str]] = []  # 可选：关联菜单用于前端分组（非必须）
    description: Optional[str] = None
    dynamic_params: Optional[Dict] = None  # 动态参数，用于处理动态权限


class CreatePermissionItem(BaseModel):
    code: str
    name: str
    action: str                  # e.g., "read"
    menu_ids: Optional[List[str]] = [] # 可选：关联菜单用于前端分组（非必须）
    description: Optional[str] = None
    dynamic_params: Optional[Dict] = None  # 动态参数，用于处理动态权限


# 清关文件制作菜单（/permission_item/qingguan_file/user/{username} 专用）
QINGGUAN_FILE_MENU_ID = "68f2fe1b06800ece17e30da8"
# 清关文件制作可选的全部起运国/目的国（拥有基础读取权限或 admin 时返回）
QINGGUAN_ALL_COUNTRY = {
    "startland": ["Vietnam", "China"],
    "destination": ["America"]
}

permission_item_router = APIRouter(tags=["permission_item"])


def resolve_user_permissions(db, username: str, menu_id: Optional[str] = None) -> List[dict]:
    """
    计算用户实际拥有的权限项（permissions 原始文档，已去重，保持原有顺序）：
    1) 取 Casbin 隐式权限（含角色继承）中的 allow 规则
    2) (path, method) → api_endpoints._id
    3) endpoint._id → permissions.code，再按策略 attrs 与 dynamic_params 匹配
    menu_id 不为空时，只保留关联了该菜单的权限项。
    """
    raw = enforcer.get_implicit_permissions_for_user(username)
    policies = collect_allow_policies(raw, parse_attrs_list)
    if not policies:
        return []

    eps = db.api_endpoints.find(
        {"Path": {"$in": endpoint_path_candidates(policies)}},
        {"_id": 1, "Path": 1, "Method": 1}
    )
    ep_index = build_endpoint_index(eps)

    # 所有策略涉及的 endpoint 一次性查出对应权限项（原来是每条策略查一次）
    all_endpoint_ids = {eid for ids in ep_index.values() for eid in ids}
    if not all_endpoint_ids:
        return []
    oid_list = list(all_endpoint_ids)
    str_list = [str(x) for x in oid_list]
    candidates = list(db.permissions.find({"code": {"$in": oid_list + str_list}}))

    # code → [(自然顺序下标, 权限项)]，用于按原来“每条策略单独查询”的顺序取回
    perms_by_code: Dict[str, List[tuple]] = {}
    for order, perm in enumerate(candidates):
        perms_by_code.setdefault(permission_code_key(perm.get("code")), []).append((order, perm))

    seen_ids = set()
    result: List[dict] = []
    for path, method, attrs in policies:
        endpoint_ids = ep_index.get(f"{path}::{method}", [])
        if not endpoint_ids:
            continue
        matched = []
        for eid in endpoint_ids:
            matched.extend(perms_by_code.get(str(eid), []))
        matched.sort(key=lambda item: item[0])

        for _, perm in matched:
            if menu_id is not None and menu_id not in (perm.get("menu_ids") or []):
                continue
            if not match_permission_attrs(perm.get("dynamic_params"), attrs):
                continue
            perm_id = str(perm["_id"])
            if perm_id not in seen_ids:
                seen_ids.add(perm_id)
                result.append(perm)
    return result


@permission_item_router.get("/permission_item", response_model=List[PermissionItem], summary="获取权限列表")
async def get_permission_list(session = Depends(get_session)):
    """获取权限列表"""
    db = session
    return [PermissionItem(**serialize_permission(p)) for p in db.permissions.find({})]


@permission_item_router.post("/permission_item", summary="创建权限")
async def create_permission_item(permission_item: CreatePermissionItem, session = Depends(get_session)):
    """创建权限"""
    db = session

    # 构建查询条件，包含动态参数
    query = {"code": permission_item.code}

    # 如果有动态参数，也需要检查动态参数是否匹配
    if permission_item.dynamic_params:
        query["dynamic_params"] = permission_item.dynamic_params

    existing_permission = db.permissions.find_one(query)

    if existing_permission:
        # 检查 menu_ids 是否相同
        existing_menu_ids = set(existing_permission.get("menu_ids", []) or [])
        new_menu_ids = set(permission_item.menu_ids or [])

        if existing_menu_ids != new_menu_ids:
            # 如果 menu_ids 不同，合并两个集合
            merged_menu_ids = list(existing_menu_ids.union(new_menu_ids))

            # 更新现有权限的 menu_ids
            db.permissions.update_one(
                {"_id": existing_permission["_id"]},
                {"$set": {"menu_ids": merged_menu_ids}}
            )

            return {
                "id": str(existing_permission["_id"]),
                "message": "权限已存在，menu_ids 已更新合并",
                "merged_menu_ids": merged_menu_ids
            }
        else:
            # menu_ids 相同，返回已存在错误
            raise HTTPException(status_code=400, detail="权限码已存在")
    else:
        # 权限不存在，创建新权限
        permission_dict = permission_item.dict(exclude_unset=True)
        result = db.permissions.insert_one(permission_dict)
        return {"id": str(result.inserted_id)}


@permission_item_router.post("/permission_item/check_permission", summary="检查当前用户是否有指定权限")
async def check_user_permission(
    username: str,
    request: Request,
    permission_name: Optional[str] = None,
    session = Depends(get_session)
):
    """检查用户是否有指定权限（只能查自己，admin 除外）"""
    ensure_self_or_admin(request, username)
    db = session

    # 获取用户信息
    user = db.users.find_one({"username": username})
    if not user:
        raise HTTPException(status_code=404, detail="用户未找到")

    # 必须提供permission_name
    if not permission_name:
        raise HTTPException(status_code=400, detail="必须提供permission_name")

    # 查询权限项
    permission = db.permissions.find_one({"name": permission_name})
    if not permission:
        return {"has_permission": False, "reason": "权限项不存在"}

    permission_id = str(permission["_id"])

    # 检查指定的权限项是否在用户权限列表中
    for perm in resolve_user_permissions(db, username):
        if str(perm["_id"]) == permission_id:
            return {"has_permission": True, "permission": PermissionItem(**serialize_permission(perm))}

    return {"has_permission": False, "reason": "用户没有此权限项"}


@permission_item_router.put("/permission_item/{permission_id}", summary="更新权限")
async def update_permission_item(permission_id: str, permission_item: CreatePermissionItem, session = Depends(get_session)):
    """更新权限"""
    db = session

    permission_dict = permission_item.dict(exclude_unset=True)

    result = db.permissions.update_one(
        {"_id": parse_object_id(permission_id, "权限项ID")},
        {"$set": permission_dict}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="权限项未找到")
    return {"message": "权限项已更新"}


@permission_item_router.delete("/permission_item/{permission_id}", summary="删除权限")
async def delete_permission_item(permission_id: str, session = Depends(get_session)):
    """删除权限"""
    db = session
    result = db.permissions.delete_one({"_id": parse_object_id(permission_id, "权限项ID")})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="权限项未找到")
    return {"message": "权限项已删除"}


@permission_item_router.get("/permission_item/{permission_id}", response_model=PermissionItem, summary="根据ID获取权限")
async def get_permission_by_id(permission_id: str, session = Depends(get_session)):
    """根据ID获取权限"""
    db = session

    permission = db.permissions.find_one({"_id": parse_object_id(permission_id, "权限项ID")})
    if not permission:
        raise HTTPException(status_code=404, detail="权限项未找到")
    return PermissionItem(**serialize_permission(permission))


@permission_item_router.get("/permission_item/user/{username}", response_model=List[PermissionItem], summary="获取用户的所有权限项")
async def get_user_permissions(username: str, session = Depends(get_session)):
    """获取指定用户的所有权限项"""
    db = session
    if not db.users.find_one({"username": username}, {"_id": 1}):
        raise HTTPException(status_code=404, detail="用户未找到")

    return [PermissionItem(**serialize_permission(p)) for p in resolve_user_permissions(db, username)]


@permission_item_router.get("/permission_item/user/{username}/menu/{menu_id}", response_model=List[PermissionItem], summary="获取用户指定菜单的所有权限项")
async def get_user_menu_permissions(username: str, menu_id: str, session = Depends(get_session)):
    """获取指定用户下某个菜单的所有权限项"""
    db = session
    if not db.users.find_one({"username": username}, {"_id": 1}):
        raise HTTPException(status_code=404, detail="用户未找到")

    if not db.menu.find_one({"_id": parse_object_id(menu_id, "菜单ID")}, {"_id": 1}):
        raise HTTPException(status_code=404, detail="菜单未找到")

    permissions = resolve_user_permissions(db, username, menu_id=menu_id)
    return [PermissionItem(**serialize_permission(p, default_menu_ids=[])) for p in permissions]


@permission_item_router.get("/permission_item/qingguan_file/user/{username}", response_model=List[Dict], summary="获取指定用户在固定菜单下的权限项（特殊处理）")
async def get_user_menu_permissions_fixed(username: str, request: Request, session = Depends(get_session)):
    """
    获取指定用户在固定菜单下的权限项，按特殊规则处理返回数据。
    清关文件页面给所有业务用户用，只能查自己（admin 除外），否则能看到别人的航线权限。
    """
    ensure_self_or_admin(request, username)
    db = session
    menu_id = QINGGUAN_FILE_MENU_ID
    all_country = QINGGUAN_ALL_COUNTRY

    if not db.users.find_one({"username": username}, {"_id": 1}):
        raise HTTPException(status_code=404, detail="用户未找到")

    # 如果是admin用户，直接返回全部权限
    if username == "admin":
        return [
            {"category": "空运", "routes": all_country},
            {"category": "海运", "routes": all_country}
        ]

    if not db.menu.find_one({"_id": parse_object_id(menu_id, "菜单ID")}, {"_id": 1}):
        raise HTTPException(status_code=404, detail="菜单未找到")

    permissions = resolve_user_permissions(db, username, menu_id=menu_id)

    # 按特殊规则处理返回数据
    result = []
    has_read_air_permission = False
    has_read_sea_permission = False
    air_routes = []
    sea_routes = []

    for permission in permissions:
        name = permission.get("name", "")
        dynamic_params = permission.get("dynamic_params") or {}

        # 检查是否有基础的"读取空运产品"或"读取海运产品"权限
        if "读取空运产品" in name and not dynamic_params:
            has_read_air_permission = True
        elif "读取海运产品" in name and not dynamic_params:
            has_read_sea_permission = True

        # 收集具体航线权限
        if dynamic_params and "startland" in dynamic_params and "destination" in dynamic_params:
            route_type = dynamic_params.get("type", "")
            route_info = {
                "startland": dynamic_params.get("startland"),
                "destination": dynamic_params.get("destination"),
                "permission_id": str(permission["_id"])
            }
            if route_type == "air" or "空运" in name:
                air_routes.append(route_info)
            elif route_type == "sea" or "海运" in name:
                sea_routes.append(route_info)

    # 构建返回结果
    if has_read_air_permission or air_routes:
        if has_read_air_permission:
            result.append({
                "category": "空运",
                "routes":all_country
            })
        else:
            # 合并所有航线的startland和destination为list
            startlands = list(set(r["startland"] for r in air_routes if r.get("startland")))
            destinations = list(set(r["destination"] for r in air_routes if r.get("destination")))
            result.append({
                "category": "空运",
                "routes": {
                    "startland": startlands,
                    "destination": destinations
                }
            })

    if has_read_sea_permission or sea_routes:
        if has_read_sea_permission:
            result.append({
                "category": "海运",
                "routes": all_country
            })
        else:
            # 合并所有航线的startland和destination为list
            startlands = list(set(r["startland"] for r in sea_routes if r.get("startland")))
            destinations = list(set(r["destination"] for r in sea_routes if r.get("destination")))
            result.append({
                "category": "海运",
                "routes": {
                    "startland": startlands,
                    "destination": destinations
                }
            })

    # 如果没有特殊权限，直接返回权限列表
    if not result:
        result = [serialize_permission(p, default_menu_ids=[]) for p in permissions]

    return result
