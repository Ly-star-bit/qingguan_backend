from collections import defaultdict
from fastapi import APIRouter, Body, Depends, HTTPException, Request
from typing import Dict, List, Optional

from loguru import logger
from app.db_mongo import get_session, enforcer
from bson import ObjectId
from datetime import datetime
from pydantic import BaseModel

from app.schemas import UpdateUserMenuPermissions
from typing import List, Dict, Any
from bson import ObjectId
import json

# 定义菜单模型
class MenuItem(BaseModel):
    id: Optional[str] = None
    name: str
    parent_id: Optional[str] = None
    children: Optional[List['MenuItem']] = None
    path: Optional[str] = None
    api_endpoint_ids: Optional[List[str]] = None
    sort_order:Optional[int] = None
menu_router = APIRouter(tags=["menu"])

@menu_router.get("/menu", response_model=List[MenuItem], summary="获取菜单树")
async def get_menu_tree(session=Depends(get_session)):
    """获取菜单树"""
    db = session

    def get_children(parent_id):
        """递归获取子菜单"""
        children = list(db.menu.find({"parent_id": parent_id}))
        # 排序逻辑：sort_order为空的放最后
        children.sort(key=lambda x: (x.get("sort_order") is None, x.get("sort_order") or 0))

        
        children_items = []
        for child in children:
            child_id = str(child["_id"])
            child_dict = {
                "id": child_id,
                "name": child["name"],
                "parent_id": child["parent_id"],
                "path": child.get("path", ""),
                "api_endpoint_ids": child.get("api_endpoint_ids", []),
                "sort_order": child.get("sort_order"),
                "children": get_children(child_id)
            }
            children_items.append(MenuItem(**child_dict))
        return children_items

    # 获取所有一级菜单
    root_menus = list(db.menu.find({"parent_id": None}))
    # 排序逻辑：sort_order为空的放最后
    # root_menus.sort(key=lambda x: (x.get("sort_order") is None, x.get("sort_order", 0)))
    root_menus.sort(key=lambda x: (x.get("sort_order") is None, x.get("sort_order") or 0))

    menu_tree = []
    for root in root_menus:
        root_id = str(root["_id"])
        menu_item = MenuItem(
            id=root_id,
            name=root["name"],
            parent_id=root.get("parent_id"),
            path=root.get("path", ""),
            api_endpoint_ids=root.get("api_endpoint_ids", []),
            sort_order=root.get("sort_order"),
            children=get_children(root_id)
        )
        menu_tree.append(menu_item)

    return menu_tree

@menu_router.post("/menu", summary="创建菜单项")
async def create_menu_item(menu_item: MenuItem, session = Depends(get_session)):
    """创建菜单项"""
    db = session
    menu_dict = menu_item.dict(exclude_unset=True)
    if "id" in menu_dict:
        del menu_dict["id"]
    if "children" in menu_dict:
        del menu_dict["children"]
        
    result = db.menu.insert_one(menu_dict)
    return {"id": str(result.inserted_id)}

@menu_router.put("/menu/{menu_id}", summary="更新菜单项")
async def update_menu_item(menu_id: str, menu_item: MenuItem, session = Depends(get_session)):
    """更新菜单项"""
    db = session
    menu_dict = menu_item.dict(exclude_unset=True)
    print(menu_dict)
    if "id" in menu_dict:
        del menu_dict["id"]
    if "children" in menu_dict:
        del menu_dict["children"]
        
    result = db.menu.update_one(
        {"_id": ObjectId(menu_id)},
        {"$set": menu_dict}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Menu item not found")
    return {"message": "Menu item updated"}

@menu_router.delete("/menu/{menu_id}", summary="删除菜单项")
async def delete_menu_item(menu_id: str, session = Depends(get_session)):
    """删除菜单项"""
    db = session
    # 检查是否有子菜单
    if db.menu.find_one({"parent_id": menu_id}):
        raise HTTPException(status_code=400, detail="Cannot delete menu item with children")
        
    result = db.menu.delete_one({"_id": ObjectId(menu_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Menu item not found")
    return {"message": "Menu item deleted"}



@menu_router.get(
    "/menu/user/get_user_menu_permissions",
    response_model=List[MenuItem],
    summary="获取用户菜单权限",
)
async def get_user_menu_permissions(username: str, session=Depends(get_session)):
    """
    1) 获取用户隐式权限（含角色继承），只保留 allow 的 p 规则
    2) (Path, Method) → 批量反查 api_endpoints，拿到 endpoint._id
    3) 用 endpoint._id 反查 permissions（code），并用动态参数做子集匹配
    4) 依据命中的 permissions.menu_ids 构建“只包含授权分支”的菜单树（含祖先、排序）
    """
    db = session

    # --- admin 直接返回完整菜单树 ---
    is_admin = (username == "admin") or ("admin" in enforcer.get_implicit_roles_for_user(username))
    if is_admin:
        return _build_full_menu_tree(db)

    # --- 1) 取 Casbin 隐式权限，仅保留 allow 的 p 规则；解析 attrs ---
    raw = enforcer.get_implicit_permissions_for_user(username)
    # 你贴的 row 结构: [sub, path, method, attrs_json, eft, desc]
    def parse_attrs(raw_attrs: Any) -> Dict[str, Any]:
        if raw_attrs in (None, "", "[]", "{}", []):
            return {}
        if isinstance(raw_attrs, dict):
            return raw_attrs
        try:
            data = json.loads(raw_attrs)
            if isinstance(data, list):
                merged = {}
                for it in data:
                    if isinstance(it, dict):
                        merged.update(it)
                return merged
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return {}

    policies: List[tuple[str, str, Dict[str, Any]]] = []
    for row in raw:
        # 容错：长度不足时跳过
        if len(row) < 5:
            continue
        sub, path, method, attrs_json, eft, *_ = row
        if str(eft).lower() != "allow":
            continue
        if not path or not method:
            continue
        norm_path = (path or "/").rstrip("/") or "/"
        norm_method = (method or "").upper()
        attrs = parse_attrs(attrs_json)
        policies.append((norm_path, norm_method, attrs))

    if not policies:
        return []

    # 去重（path+method+attrs）
    seen = set()
    uniq = []
    for p in policies:
        k = f'{p[0]}::{p[1]}::{json.dumps(p[2], sort_keys=True, ensure_ascii=False)}'
  
        if k not in seen:
          
            seen.add(k)
            uniq.append(p)
    policies = uniq
    # --- 2) 批量反查 endpoint（建议建立 {Path:1, Method:1} 联合索引）---
    path_set = {p[0] for p in policies}
    for i in list(path_set):  # 转换为列表
        if not i.endswith("/"):
            path_set.add(f"{i}/")
    # print(f"path_set:{path_set}")
    eps = list(db.api_endpoints.find(
        {"Path": {"$in": list(path_set)}},
        {"_id": 1, "Path": 1, "Method": 1}
    ))
    # print(f"eps:{eps}")
    # Path::Method → [endpoint_id]
    ep_index: Dict[str, List[ObjectId]] = {}
    for ep in eps:
        k = f'{(ep.get("Path") or "/").rstrip("/") or "/"}::{str(ep.get("Method") or "").upper()}'
        ep_index.setdefault(k, []).append(ep["_id"])
    endpoint_ids: set[ObjectId] = set()
    # endpoint_id → 多个 casbin attrs（同一接口可能多条策略）
    policy_map: Dict[str, List[Dict[str, Any]]] = {}
    for path, method, attrs in policies:
        k = f"{path}::{method}"
        for eid in ep_index.get(k, []):
            endpoint_ids.add(eid)
            policy_map.setdefault(str(eid), []).append(attrs)

    if not endpoint_ids:
        return []

    # --- 3) 反查 permissions（code）+ 动态参数子集匹配 ---
    oid_list = list(endpoint_ids)
    str_list = [str(x) for x in oid_list]

    candidates = list(db.permissions.find({
        "$or": [
            {"code": {"$in": oid_list}},   # code 为 ObjectId
            {"code": {"$in": str_list}},   # code 为字符串
        ]
    }))

    def dyn_subset(perm_dyn: Dict[str, Any] | None, casbin_dyn: Dict[str, Any]) -> bool:
        """判断 permission.dynamic_params ⊆ casbin_attrs"""
        perm_dyn = perm_dyn or {}
        if not perm_dyn:
            # perm 无动态参数：仅当 casbin 也无动态参数时匹配“无参版本”
            return not casbin_dyn
        for k, v in perm_dyn.items():
            if k not in casbin_dyn:
                return False
            if str(casbin_dyn[k]) != str(v):
                return False
        return True

    authorized_menu_ids: set[str] = set()
    matched_permissions: List[dict] = []
    # print(f"candidates:{candidates}")
    for perm in candidates:
        code = perm.get("code")
        # 统一用字符串 key 查询 policy_map
        key1 = None
        if isinstance(code, ObjectId):
            key1 = str(code)
        else:
            # 尝试把字符串转成 ObjectId 字符串，不可转就当原字符串
            try:
                key1 = str(ObjectId(str(code)))
            except Exception:
                key1 = str(code)

        casbin_attrs_list = policy_map.get(key1, [])
        if not casbin_attrs_list:
            # logger.info(f"{perm.get('name')} continue")
            continue

        perm_dyn = perm.get("dynamic_params") or {}
        # logger.info(f"casbin_attrs_list:{casbin_attrs_list}")
        # 子集匹配（任一条 casbin 策略覆盖即可）
        ok = any(dyn_subset(perm_dyn, cattrs) for cattrs in casbin_attrs_list)
        if not ok:
            # logger.info(f"{perm.get('name')} continue")
            continue

        # scope 规则（与 casbin attrs 中包含 scope 时对齐；若 casbin 未给 scope，则不强制剔除）
        scope_from_perm = str(perm_dyn.get("scope", "")).strip().lower()
        casbin_scopes = {str(ca.get("scope", "")).strip().lower() for ca in casbin_attrs_list if "scope" in ca}
        if casbin_scopes:  # 只有当 casbin 明确给 scope 时，才按 scope 过滤 permission
            if scope_from_perm and scope_from_perm not in casbin_scopes:
                logger.info(f"{perm.get('name')} continue")
                continue

        matched_permissions.append(perm)
        for mid in perm.get("menu_ids") or []:
            if mid:
                authorized_menu_ids.add(mid)

    if not authorized_menu_ids:
        return []
    # print(f"authorized_menu_ids:{authorized_menu_ids}")
    # --- 4) 祖先补齐 + 只保留授权分支的树 ---
    # 祖先补齐
    def add_parent_menus(menu_id: str, authorized_set: set[str]):
        m = db.menu.find_one({"_id": ObjectId(menu_id)})
        if m and m.get("parent_id"):
            pid = m["parent_id"]
            if pid not in authorized_set:
                authorized_set.add(pid)
                add_parent_menus(pid, authorized_set)

    for mid in list(authorized_menu_ids):
        add_parent_menus(mid, authorized_menu_ids)

    # 构建剪枝树
    return _build_pruned_menu_tree(db, authorized_menu_ids)


# ========== 辅助：构建完整树 ==========
def _build_full_menu_tree(db) -> List[MenuItem]:
    def sort_key(m): return (m.get("sort_order") is None, m.get("sort_order") or 0)

    def children_of(pid):
        cs = list(db.menu.find({"parent_id": pid}))
        cs.sort(key=sort_key)
        out = []
        for ch in cs:
            out.append(MenuItem(
                id=str(ch["_id"]),
                name=ch["name"],
                parent_id=ch.get("parent_id"),
                path=ch.get("path", ""),
                api_endpoint_ids=ch.get("api_endpoint_ids", []),
                sort_order=ch.get("sort_order"),
                children=children_of(str(ch["_id"]))
            ))
        return out

    roots = list(db.menu.find({"parent_id": None}))
    roots.sort(key=sort_key)
    tree = []
    for r in roots:
        tree.append(MenuItem(
            id=str(r["_id"]),
            name=r["name"],
            parent_id=r.get("parent_id"),
            path=r.get("path", ""),
            api_endpoint_ids=r.get("api_endpoint_ids", []),
            sort_order=r.get("sort_order"),
            children=children_of(str(r["_id"]))
        ))
    return tree


# ========== 辅助：按授权集合剪枝 ==========
def _build_pruned_menu_tree(db, allowed_ids: set[str]) -> List[MenuItem]:
    def sort_key(m): return (m.get("sort_order") is None, m.get("sort_order") or 0)

    all_menus = list(db.menu.find({}))
    by_id = {str(m["_id"]): m for m in all_menus}
    children: Dict[str, List[dict]] = {}
    roots = []
    for m in all_menus:
        pid = m.get("parent_id")
        if pid is None:
            roots.append(m)
        else:
            children.setdefault(pid, []).append(m)

    # 只保留 need_ids（授权节点 + 祖先）
    need_ids = set(allowed_ids)

    def build(node: dict) -> MenuItem | None:
        mid = str(node["_id"])
        if mid not in need_ids:
            return None
        raw_children = sorted(children.get(mid, []), key=sort_key)
        built_children: List[MenuItem] = []
        for ch in raw_children:
            b = build(ch)
            if b is not None:
                built_children.append(b)
        # 既不是授权节点也没有授权子节点 → 丢弃
        if mid not in allowed_ids and not built_children:
            return None
        return MenuItem(
            id=mid,
            name=node["name"],
            parent_id=node.get("parent_id"),
            path=node.get("path", ""),
            api_endpoint_ids=node.get("api_endpoint_ids", []),
            sort_order=node.get("sort_order"),
            children=built_children
        )

    roots_sorted = sorted(roots, key=sort_key)
    out: List[MenuItem] = []
    for r in roots_sorted:
        b = build(r)
        if b is not None:
            out.append(b)
    return out


