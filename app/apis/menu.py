from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Any, Dict, List, Optional

from loguru import logger
from app.db_mongo import get_session, enforcer
from pydantic import BaseModel
from bson import ObjectId

from app.auth_utils import ensure_self_or_admin, is_admin_user
from app.permission_core import (
    build_endpoint_index,
    collect_allow_policies,
    endpoint_path_candidates,
    menu_dyn_subset,
    parse_attrs_merged,
    parse_object_id,
)
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


# ========== 菜单树（一次查询全部菜单，在内存中建树） ==========
def _menu_sort_key(m: dict):
    """排序逻辑：sort_order 为空的放最后"""
    return (m.get("sort_order") is None, m.get("sort_order") or 0)


class _MenuIndex:
    """menu 集合的内存索引"""

    def __init__(self, db):
        self.all = list(db.menu.find({}))
        self.by_id = {str(m["_id"]): m for m in self.all}
        self.roots: List[dict] = []
        self.children: Dict[Any, List[dict]] = {}
        for m in self.all:
            pid = m.get("parent_id")
            if pid is None:
                self.roots.append(m)
            else:
                self.children.setdefault(pid, []).append(m)

    def with_ancestors(self, menu_ids: set) -> set:
        """补齐所有祖先菜单（原来每个祖先查一次库）"""
        result = set(menu_ids)
        for mid in list(menu_ids):
            node = self.by_id.get(str(mid))
            while node and node.get("parent_id"):
                pid = node["parent_id"]
                if pid in result:
                    break
                result.add(pid)
                node = self.by_id.get(str(pid))
        return result

    def build_tree(self, allowed_ids: Optional[set] = None) -> List[MenuItem]:
        """allowed_ids 为 None 返回完整树；否则只保留授权节点及其祖先"""

        def build(node: dict) -> Optional[MenuItem]:
            mid = str(node["_id"])
            if allowed_ids is not None and mid not in allowed_ids:
                return None
            built_children: List[MenuItem] = []
            for ch in sorted(self.children.get(mid, []), key=_menu_sort_key):
                b = build(ch)
                if b is not None:
                    built_children.append(b)
            return MenuItem(
                id=mid,
                name=node["name"],
                parent_id=node.get("parent_id"),
                path=node.get("path", ""),
                api_endpoint_ids=node.get("api_endpoint_ids", []),
                sort_order=node.get("sort_order"),
                children=built_children
            )

        out: List[MenuItem] = []
        for root in sorted(self.roots, key=_menu_sort_key):
            b = build(root)
            if b is not None:
                out.append(b)
        return out


@menu_router.get("/menu", response_model=List[MenuItem], summary="获取菜单树")
async def get_menu_tree(session=Depends(get_session)):
    """获取菜单树"""
    return _MenuIndex(session).build_tree()

def _check_parent(db, parent_id: Optional[str], menu_id: Optional[str] = None) -> None:
    """父级菜单必须存在，且不能是菜单自己或它的子孙（否则这一枝菜单在树里整个消失）"""
    if parent_id is None:
        return
    index = _MenuIndex(db).by_id
    node = index.get(parent_id)
    if node is None:
        raise HTTPException(status_code=400, detail="父级菜单不存在")
    seen = set()
    while node is not None:
        nid = str(node["_id"])
        if nid == menu_id:
            raise HTTPException(status_code=400, detail="不能把菜单移到它自己或它的子菜单下")
        if nid in seen:
            break
        seen.add(nid)
        node = index.get(node.get("parent_id")) if node.get("parent_id") else None


@menu_router.post("/menu", summary="创建菜单项")
async def create_menu_item(menu_item: MenuItem, session = Depends(get_session)):
    """创建菜单项"""
    db = session
    menu_dict = menu_item.dict(exclude_unset=True)
    if "id" in menu_dict:
        del menu_dict["id"]
    if "children" in menu_dict:
        del menu_dict["children"]
    if "parent_id" in menu_dict:
        menu_dict["parent_id"] = menu_dict["parent_id"] or None
        _check_parent(db, menu_dict["parent_id"])

    result = db.menu.insert_one(menu_dict)
    return {"id": str(result.inserted_id)}

@menu_router.put("/menu/{menu_id}", summary="更新菜单项")
async def update_menu_item(menu_id: str, menu_item: MenuItem, session = Depends(get_session)):
    """更新菜单项"""
    db = session
    menu_dict = menu_item.dict(exclude_unset=True)
    if "id" in menu_dict:
        del menu_dict["id"]
    if "children" in menu_dict:
        del menu_dict["children"]
    if "parent_id" in menu_dict:
        # 前端清空父级时传 null：设为顶级菜单（_MenuIndex 把 parent_id 为空的当作根）
        menu_dict["parent_id"] = menu_dict["parent_id"] or None
        _check_parent(db, menu_dict["parent_id"], menu_id)

    result = db.menu.update_one(
        {"_id": parse_object_id(menu_id, "菜单ID")},
        {"$set": menu_dict}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Menu item not found")
    return {"message": "Menu item updated"}

@menu_router.delete("/menu/{menu_id}", summary="删除菜单项")
async def delete_menu_item(menu_id: str, session = Depends(get_session)):
    """删除菜单项"""
    db = session
    menu_oid = parse_object_id(menu_id, "菜单ID")
    # 检查是否有子菜单
    if db.menu.find_one({"parent_id": menu_id}):
        raise HTTPException(status_code=400, detail="该菜单下还有子菜单，请先删除或移走子菜单")

    result = db.menu.delete_one({"_id": menu_oid})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Menu item not found")
    return {"message": "Menu item deleted"}



@menu_router.get(
    "/menu/user/get_user_menu_permissions",
    response_model=List[MenuItem],
    summary="获取用户菜单权限",
)
async def get_user_menu_permissions(username: str, request: Request, session=Depends(get_session)):
    """
    1) 获取用户隐式权限（含角色继承），只保留 allow 的 p 规则
    2) (Path, Method) → 批量反查 api_endpoints，拿到 endpoint._id
    3) 用 endpoint._id 反查 permissions（code），并用动态参数做子集匹配
    4) 依据命中的 permissions.menu_ids 构建“只包含授权分支”的菜单树（含祖先、排序）
    """
    # 该接口只校验登录不走 Casbin（见 middleware.AUTH_ONLY_PATHS），只能查自己（admin 除外）
    ensure_self_or_admin(request, username)
    db = session

    # --- admin 直接返回完整菜单树 ---
    if is_admin_user(username):
        return _MenuIndex(db).build_tree()

    # --- 1) 取 Casbin 隐式权限，仅保留 allow 的 p 规则；解析 attrs ---
    raw = enforcer.get_implicit_permissions_for_user(username)
    policies = collect_allow_policies(raw, parse_attrs_merged)
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
    eps = db.api_endpoints.find(
        {"Path": {"$in": endpoint_path_candidates(policies)}},
        {"_id": 1, "Path": 1, "Method": 1}
    )
    ep_index = build_endpoint_index(eps)
    endpoint_ids: set[ObjectId] = set()
    # endpoint_id → 多个 casbin attrs（同一接口可能多条策略）
    policy_map: Dict[str, List[Dict[str, Any]]] = {}
    for path, method, attrs in policies:
        for eid in ep_index.get(f"{path}::{method}", []):
            endpoint_ids.add(eid)
            policy_map.setdefault(str(eid), []).append(attrs)

    if not endpoint_ids:
        return []

    # --- 3) 反查 permissions（code）+ 动态参数子集匹配 ---
    oid_list = list(endpoint_ids)
    str_list = [str(x) for x in oid_list]

    candidates = db.permissions.find({
        "$or": [
            {"code": {"$in": oid_list}},   # code 为 ObjectId
            {"code": {"$in": str_list}},   # code 为字符串
        ]
    })

    authorized_menu_ids: set[str] = set()
    for perm in candidates:
        code = perm.get("code")
        # 统一用字符串 key 查询 policy_map
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
            continue

        perm_dyn = perm.get("dynamic_params") or {}
        # 子集匹配（任一条 casbin 策略覆盖即可）
        if not any(menu_dyn_subset(perm_dyn, cattrs) for cattrs in casbin_attrs_list):
            continue

        # scope 规则（与 casbin attrs 中包含 scope 时对齐；若 casbin 未给 scope，则不强制剔除）
        scope_from_perm = str(perm_dyn.get("scope", "")).strip().lower()
        casbin_scopes = {str(ca.get("scope", "")).strip().lower() for ca in casbin_attrs_list if "scope" in ca}
        if casbin_scopes:  # 只有当 casbin 明确给 scope 时，才按 scope 过滤 permission
            if scope_from_perm and scope_from_perm not in casbin_scopes:
                logger.debug(f"{perm.get('name')} scope 不匹配，跳过")
                continue

        for mid in perm.get("menu_ids") or []:
            if mid:
                authorized_menu_ids.add(mid)

    if not authorized_menu_ids:
        return []

    # --- 4) 祖先补齐 + 只保留授权分支的树 ---
    menus = _MenuIndex(db)
    return menus.build_tree(menus.with_ancestors(authorized_menu_ids))
