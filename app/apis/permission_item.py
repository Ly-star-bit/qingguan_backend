from fastapi import APIRouter, Body, Depends, HTTPException, Request
from typing import Dict, List, Optional
from app.db_mongo import get_session, enforcer
from bson import ObjectId
from datetime import datetime
from pydantic import BaseModel


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


permission_item_router = APIRouter(tags=["permission_item"])

@permission_item_router.get("/permission_item", response_model=List[PermissionItem], summary="获取权限列表")
async def get_permission_list(session = Depends(get_session)):
    """获取权限列表"""
    db = session
    
    permissions = list(db.permissions.find({}))
    
    permission_list = []
    for permission in permissions:
        permission_id = str(permission["_id"])
        permission_dict = {
            "id": permission_id,
            "code": permission["code"],
            "name": permission["name"],
            # "resource": permission["resource"],
            "action": permission["action"],
            "menu_ids": permission.get("menu_ids"),
            "description": permission.get("description"),
            "dynamic_params": permission.get("dynamic_params")
        }
        permission_list.append(PermissionItem(**permission_dict))
        
    return permission_list


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
    permission_name: Optional[str] = None,
    session = Depends(get_session)
):
    """检查用户是否有指定权限"""
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

    # 使用get_user_permissions的逻辑，获取用户所有权限项
    user_permissions = await get_user_permissions(username, session)

    # 检查指定的权限项是否在用户权限列表中
    for perm in user_permissions:
        if perm.id == permission_id:
            return {"has_permission": True, "permission": perm}

    return {"has_permission": False, "reason": "用户没有此权限项"}


@permission_item_router.put("/permission_item/{permission_id}", summary="更新权限")
async def update_permission_item(permission_id: str, permission_item: CreatePermissionItem, session = Depends(get_session)):
    """更新权限"""
    db = session
    
    permission_dict = permission_item.dict(exclude_unset=True)
        
    result = db.permissions.update_one(
        {"_id": ObjectId(permission_id)},
        {"$set": permission_dict}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="权限项未找到")
    return {"message": "权限项已更新"}


@permission_item_router.delete("/permission_item/{permission_id}", summary="删除权限")
async def delete_permission_item(permission_id: str, session = Depends(get_session)):
    """删除权限"""
    db = session
    
    # 检查权限是否被使用
    # 这里可以检查是否有用户或角色关联了这个权限
    # 示例：检查casbin策略中是否存在该权限
    try:
        # 简单检查，实际项目中可能需要更复杂的检查
        pass
    except Exception:
        pass
    
    result = db.permissions.delete_one({"_id": ObjectId(permission_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="权限项未找到")
    return {"message": "权限项已删除"}


@permission_item_router.get("/permission_item/{permission_id}", response_model=PermissionItem, summary="根据ID获取权限")
async def get_permission_by_id(permission_id: str, session = Depends(get_session)):
    """根据ID获取权限"""
    db = session

    permission = db.permissions.find_one({"_id": ObjectId(permission_id)})
    if not permission:
        raise HTTPException(status_code=404, detail="权限项未找到")

    permission_dict = {
        "id": str(permission["_id"]),
        "code": permission["code"],
        "name": permission["name"],
        # "resource": permission["resource"],
        "action": permission["action"],
        "menu_id": permission.get("menu_id"),
        "description": permission.get("description"),
        "dynamic_params": permission.get("dynamic_params")
    }
    return PermissionItem(**permission_dict)


@permission_item_router.get("/permission_item/user/{username}", response_model=List[PermissionItem], summary="获取用户的所有权限项")
async def get_user_permissions(username: str, session = Depends(get_session)):
    """获取指定用户的所有权限项"""
    import json
    db = session

    # 获取用户信息
    user = db.users.find_one({"username": username})
    if not user:
        raise HTTPException(status_code=404, detail="用户未找到")

    # 从Casbin中获取用户的隐式权限
    raw = enforcer.get_implicit_permissions_for_user(username)

    # 解析策略并提取path、method和attrs_json
    policies = []
    # print(f'raw:{raw}')
    for row in raw:
        # row格式: [sub, path, method, attrs_json, eft, desc]
        if len(row) < 5:
            continue
        sub, path, method, attrs_json, eft, *_ = row
        if str(eft).lower() != "allow":
            continue
        if not path or not method:
            continue
        norm_path = (path or "/").rstrip("/") or "/"
        norm_method = (method or "").upper()

        # 解析attrs_json
        try:
            attrs = json.loads(attrs_json) if attrs_json else []
        except (json.JSONDecodeError, TypeError):
            attrs = []

        policies.append((norm_path, norm_method, attrs))

    if not policies:
        return []

    # 通过path和method查找api_endpoints
    path_set = {p[0] for p in policies}
    for i in list(path_set):
        if not i.endswith("/"):
            path_set.add(f"{i}/")

    eps = list(db.api_endpoints.find(
        {"Path": {"$in": list(path_set)}},
        {"_id": 1, "Path": 1, "Method": 1}
    ))

    # 建立索引：path::method → endpoint_ids
    ep_index = {}
    for ep in eps:
        k = f'{(ep.get("Path") or "/").rstrip("/") or "/"}::{str(ep.get("Method") or "").upper()}'
        ep_index.setdefault(k, []).append(ep["_id"])

    # 收集用户实际拥有的权限（考虑attrs）
    user_permissions = []

    for path, method, attrs in policies:
        k = f"{path}::{method}"
        endpoint_ids = ep_index.get(k, [])

        if not endpoint_ids:
            continue

        oid_list = list(endpoint_ids)
        str_list = [str(x) for x in oid_list]

        # 查询此endpoint对应的权限
        perms = list(db.permissions.find({
            "code": {"$in": oid_list + str_list}
        }))

        for perm in perms:
            perm_dynamic = perm.get("dynamic_params") or {}

            # 如果attrs为空数组，则匹配没有dynamic_params或dynamic_params为空的权限（基础权限）
            if not attrs or attrs == [{}]:
                if not perm_dynamic:
                    user_permissions.append(perm)
            else:
                # attrs是一个数组，需要检查每个条件
                for attr in attrs:
                    if not attr:
                        if not perm_dynamic:
                            user_permissions.append(perm)
                            break
                    else:
                        # 检查permission的dynamic_params是否匹配policy的attrs
                        match = True
                        for key, value in attr.items():
                            perm_value = perm_dynamic.get(key)
                            if value == "*":
                                continue
                            if isinstance(value, list):
                                if perm_value not in value:
                                    match = False
                                    break
                            elif str(perm_value).upper() != str(value).upper():
                                match = False
                                break

                        if match:
                            user_permissions.append(perm)
                            break

    # 去重
    seen_ids = set()
    unique_permissions = []
    for perm in user_permissions:
        perm_id = str(perm["_id"])
        if perm_id not in seen_ids:
            seen_ids.add(perm_id)
            unique_permissions.append(perm)

    permissions_list = []
    for permission in unique_permissions:
        permission_id = str(permission["_id"])
        permission_dict = {
            "id": permission_id,
            "code": permission["code"],
            "name": permission["name"],
            "action": permission["action"],
            "menu_ids": permission.get("menu_ids"),
            "description": permission.get("description"),
            "dynamic_params": permission.get("dynamic_params")
        }
        permissions_list.append(PermissionItem(**permission_dict))
    # print(f"permission:{permissions_list}")
    return permissions_list


@permission_item_router.get("/permission_item/user/{username}/menu/{menu_id}", response_model=List[PermissionItem], summary="获取用户指定菜单的所有权限项")
async def get_user_menu_permissions(username: str, menu_id: str, session = Depends(get_session)):
    """获取指定用户下某个菜单的所有权限项"""
    import json
    db = session

    # 获取用户信息
    user = db.users.find_one({"username": username})
    if not user:
        raise HTTPException(status_code=404, detail="用户未找到")

    # 检查菜单是否存在
    menu = db.menu.find_one({"_id": ObjectId(menu_id)})
    if not menu:
        raise HTTPException(status_code=404, detail="菜单未找到")

    # 从Casbin中获取用户的隐式权限
    raw = enforcer.get_implicit_permissions_for_user(username)

    # 解析策略并提取path、method和attrs_json
    policies = []
    for row in raw:
        # row格式: [sub, path, method, attrs_json, eft, desc]
        if len(row) < 5:
            continue
        _, path, method, attrs_json, eft, *_ = row
        if str(eft).lower() != "allow":
            continue
        if not path or not method:
            continue
        norm_path = (path or "/").rstrip("/") or "/"
        norm_method = (method or "").upper()

        # 解析attrs_json
        try:
            attrs = json.loads(attrs_json) if attrs_json else []
        except (json.JSONDecodeError, TypeError):
            attrs = []

        policies.append((norm_path, norm_method, attrs))

    if not policies:
        return []

    # 通过path和method查找api_endpoints
    path_set = {p[0] for p in policies}
    for i in list(path_set):
        if not i.endswith("/"):
            path_set.add(f"{i}/")

    eps = list(db.api_endpoints.find(
        {"Path": {"$in": list(path_set)}},
        {"_id": 1, "Path": 1, "Method": 1}
    ))

    # 建立索引：path::method → endpoint_ids
    ep_index = {}
    for ep in eps:
        k = f'{(ep.get("Path") or "/").rstrip("/") or "/"}::{str(ep.get("Method") or "").upper()}'
        ep_index.setdefault(k, []).append(ep["_id"])

    # 收集用户实际拥有的权限（考虑attrs）
    user_permissions = []

    for path, method, attrs in policies:
        k = f"{path}::{method}"
        endpoint_ids = ep_index.get(k, [])

        if not endpoint_ids:
            continue

        oid_list = list(endpoint_ids)
        str_list = [str(x) for x in oid_list]

        # 查询此endpoint对应的权限
        perms = list(db.permissions.find({
            "code": {"$in": oid_list + str_list}
        }))

        # 过滤出属于指定菜单的权限
        perms = [p for p in perms if menu_id in (p.get("menu_ids") or [])]

        for perm in perms:
            perm_dynamic = perm.get("dynamic_params") or {}

            # 如果attrs为空数组，则匹配没有dynamic_params或dynamic_params为空的权限（基础权限）
            if not attrs or attrs == [{}]:
                if not perm_dynamic:
                    user_permissions.append(perm)
            else:
                # attrs是一个数组，需要检查每个条件
                for attr in attrs:
                    if not attr:
                        if not perm_dynamic:
                            user_permissions.append(perm)
                            break
                    else:
                        # 检查permission的dynamic_params是否匹配policy的attrs
                        match = True
                        for key, value in attr.items():
                            perm_value = perm_dynamic.get(key)
                            if value == "*":
                                continue
                            if isinstance(value, list):
                                if perm_value not in value:
                                    match = False
                                    break
                            elif str(perm_value).upper() != str(value).upper():
                                match = False
                                break

                        if match:
                            user_permissions.append(perm)
                            break

    # 去重
    seen_ids = set()
    unique_permissions = []
    for perm in user_permissions:
        perm_id = str(perm["_id"])
        if perm_id not in seen_ids:
            seen_ids.add(perm_id)
            unique_permissions.append(perm)

    permissions_list = []
    for permission in unique_permissions:
        permission_id = str(permission["_id"])
        permission_dict = {
            "id": permission_id,
            "code": permission["code"],
            "name": permission["name"],
            "action": permission["action"],
            "menu_ids": permission.get("menu_ids") or [],
            "description": permission.get("description"),
            "dynamic_params": permission.get("dynamic_params")
        }
        permissions_list.append(PermissionItem(**permission_dict))

    return permissions_list


@permission_item_router.get("/permission_item/qingguan_file/user/{username}", response_model=List[Dict], summary="获取指定用户在固定菜单下的权限项（特殊处理）")
async def get_user_menu_permissions_fixed(username: str, session = Depends(get_session)):
    """获取指定用户在固定菜单下的权限项，按特殊规则处理返回数据"""
    import json
    db = session
    menu_id = "68f2fe1b06800ece17e30da8"

    # 获取用户信息
    user = db.users.find_one({"username": username})
    if not user:
        raise HTTPException(status_code=404, detail="用户未找到")

    # 定义全部国家
    all_country = {
        "startland": ["Vietnam", "China"],
        "destination": ["America"]
    }

    # 如果是admin用户，直接返回全部权限
    if username == "admin":
        return [
            {"category": "空运", "routes": all_country},
            {"category": "海运", "routes": all_country}
        ]

    # 检查菜单是否存在
    menu = db.menu.find_one({"_id": ObjectId(menu_id)})
    if not menu:
        raise HTTPException(status_code=404, detail="菜单未找到")

    # 从Casbin中获取用户的隐式权限
    raw = enforcer.get_implicit_permissions_for_user(username)

    # 解析策略并提取path、method和attrs_json
    policies = []
    for row in raw:
        # row格式: [sub, path, method, attrs_json, eft, desc]
        if len(row) < 5:
            continue
        _, path, method, attrs_json, eft, *_ = row
        if str(eft).lower() != "allow":
            continue
        if not path or not method:
            continue
        norm_path = (path or "/").rstrip("/") or "/"
        norm_method = (method or "").upper()

        # 解析attrs_json
        try:
            attrs = json.loads(attrs_json) if attrs_json else []
        except (json.JSONDecodeError, TypeError):
            attrs = []

        policies.append((norm_path, norm_method, attrs))

    if not policies:
        return []

    # 通过path和method查找api_endpoints
    path_set = {p[0] for p in policies}
    for i in list(path_set):
        if not i.endswith("/"):
            path_set.add(f"{i}/")

    eps = list(db.api_endpoints.find(
        {"Path": {"$in": list(path_set)}},
        {"_id": 1, "Path": 1, "Method": 1}
    ))

    # 建立索引：path::method → endpoint_ids
    ep_index = {}
    for ep in eps:
        k = f'{(ep.get("Path") or "/").rstrip("/") or "/"}::{str(ep.get("Method") or "").upper()}'
        ep_index.setdefault(k, []).append(ep["_id"])

    # 收集用户实际拥有的权限（考虑attrs）
    user_permissions = []

    for path, method, attrs in policies:
        k = f"{path}::{method}"
        endpoint_ids = ep_index.get(k, [])

        if not endpoint_ids:
            continue

        oid_list = list(endpoint_ids)
        str_list = [str(x) for x in oid_list]

        # 查询此endpoint对应的权限
        perms = list(db.permissions.find({
            "code": {"$in": oid_list + str_list}
        }))

        # 过滤出属于指定菜单的权限
        perms = [p for p in perms if menu_id in (p.get("menu_ids") or [])]

        for perm in perms:
            perm_dynamic = perm.get("dynamic_params") or {}

            # 如果attrs为空数组，则匹配没有dynamic_params或dynamic_params为空的权限（基础权限）
            if not attrs or attrs == [{}]:
                if not perm_dynamic:
                    user_permissions.append(perm)
            else:
                # attrs是一个数组，需要检查每个条件
                for attr in attrs:
                    if not attr:
                        # 空attr匹配无dynamic_params的权限
                        if not perm_dynamic:
                            user_permissions.append(perm)
                            break
                    else:
                        # 检查permission的dynamic_params是否匹配policy的attrs
                        match = True
                        for key, value in attr.items():
                            perm_value = perm_dynamic.get(key)
                            # 通配符匹配
                            if value == "*":
                                continue
                            # 列表匹配
                            if isinstance(value, list):
                                if perm_value not in value:
                                    match = False
                                    break
                            # 精确匹配（忽略大小写）
                            elif str(perm_value).upper() != str(value).upper():
                                match = False
                                break

                        if match:
                            user_permissions.append(perm)
                            break

    # 去重
    seen_ids = set()
    unique_permissions = []
    for perm in user_permissions:
        perm_id = str(perm["_id"])
        if perm_id not in seen_ids:
            seen_ids.add(perm_id)
            unique_permissions.append(perm)

    permissions = unique_permissions

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
        for permission in permissions:
            permission_id = str(permission["_id"])
            permission_dict = {
                "id": permission_id,
                "code": permission["code"],
                "name": permission["name"],
                "action": permission["action"],
                "menu_ids": permission.get("menu_ids", []),
                "description": permission.get("description"),
                "dynamic_params": permission.get("dynamic_params")
            }
            result.append(permission_dict)

    return result




