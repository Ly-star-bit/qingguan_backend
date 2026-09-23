"""
权限相关的纯逻辑（不依赖数据库连接），供 casbin_policy / permission_item / menu 等接口复用。

Casbin p 规则槽位约定：[sub, obj, act, attrs(JSON), eft, desc]
"""
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException


# -----------------------------
# 通用
# -----------------------------
def parse_object_id(value: str, label: str = "ID") -> ObjectId:
    """把路径参数转换为 ObjectId，格式不合法时返回 400 而不是 500"""
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=400, detail=f"无效的{label}: {value}")


def normalize_obj(obj: str) -> str:
    """
    策略 obj 统一以 / 结尾：中间件会给请求路径补 /，keyMatch3 是整串匹配，
    不带 / 的策略永远匹配不上。
    """
    if obj and len(obj) > 1 and not obj.endswith("/"):
        return obj + "/"
    return obj


def norm_path(path: Optional[str]) -> str:
    """去掉末尾 /，用于 path 比较（根路径保留 /）"""
    return (path or "/").rstrip("/") or "/"


# -----------------------------
# attrs（策略属性 JSON）
# -----------------------------
def dump_attrs(attrs: Optional[dict]) -> str:
    if not attrs:
        return "[]"
    return json.dumps([attrs], ensure_ascii=False, separators=(",", ":"))


def load_attrs(s: Optional[str]) -> dict:
    """策略 attrs → 单个 dict（取列表第一项；兼容直接存 dict 的旧格式）"""
    if not s:
        return {}
    try:
        parsed = json.loads(s)
    except Exception:
        return {}
    if isinstance(parsed, list) and parsed:
        return parsed[0] if isinstance(parsed[0], dict) else {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def parse_attrs_list(attrs_json: Any) -> List[dict]:
    """策略 attrs → dict 列表（权限项匹配用；旧格式的单个 dict 视为一项）"""
    if not attrs_json:
        return []
    try:
        data = json.loads(attrs_json) if isinstance(attrs_json, str) else attrs_json
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    return []


def parse_attrs_merged(raw_attrs: Any) -> Dict[str, Any]:
    """策略 attrs → 合并后的单个 dict（菜单匹配用）"""
    if raw_attrs in (None, "", "[]", "{}", []):
        return {}
    if isinstance(raw_attrs, dict):
        return raw_attrs
    try:
        data = json.loads(raw_attrs)
    except Exception:
        return {}
    if isinstance(data, list):
        merged: Dict[str, Any] = {}
        for it in data:
            if isinstance(it, dict):
                merged.update(it)
        return merged
    if isinstance(data, dict):
        return data
    return {}


def policy_row_to_dict(p: List[str]) -> Dict[str, Any]:
    """Casbin p 规则 → 接口返回结构"""
    return {
        "ptype": "p",
        "sub": p[0] if len(p) > 0 else "",
        "obj": p[1] if len(p) > 1 else "",
        "act": p[2] if len(p) > 2 else "",
        "attrs": load_attrs(p[3] if len(p) > 3 else "{}"),
        "eft": p[4] if len(p) > 4 and p[4] else "allow",
        "description": p[5] if len(p) > 5 else "",
    }


# -----------------------------
# 策略 → api_endpoints → permissions
# -----------------------------
def collect_allow_policies(raw: Iterable[List[str]], parse_attrs) -> List[Tuple[str, str, Any]]:
    """从 Casbin 隐式权限中取出 allow 规则：[(规范化 path, 大写 method, 解析后的 attrs)]"""
    policies = []
    for row in raw:
        if len(row) < 5:
            continue
        _, path, method, attrs_json, eft, *_ = row
        if str(eft).lower() != "allow":
            continue
        if not path or not method:
            continue
        policies.append((norm_path(path), (method or "").upper(), parse_attrs(attrs_json)))
    return policies


def endpoint_path_candidates(policies: Iterable[Tuple[str, str, Any]]) -> List[str]:
    """查询 api_endpoints 用的 Path 集合：同时包含带 / 和不带 / 两种写法"""
    paths = {p[0] for p in policies}
    for path in list(paths):
        if not path.endswith("/"):
            paths.add(f"{path}/")
    return list(paths)


def build_endpoint_index(endpoints: Iterable[dict]) -> Dict[str, List[ObjectId]]:
    """api_endpoints → {"path::METHOD": [endpoint_id, ...]}"""
    index: Dict[str, List[ObjectId]] = {}
    for ep in endpoints:
        key = f'{norm_path(ep.get("Path"))}::{str(ep.get("Method") or "").upper()}'
        index.setdefault(key, []).append(ep["_id"])
    return index


def permission_code_key(code: Any) -> str:
    """permissions.code 可能是 ObjectId 也可能是字符串，统一成字符串比较"""
    return str(code)


def match_permission_attrs(perm_dynamic: Optional[dict], attrs: List[dict]) -> bool:
    """
    权限项匹配规则（permission_item 接口使用）：
    - 策略 attrs 为空：只匹配没有 dynamic_params 的基础权限
    - 否则任一 attr 满足即可：attr 为空匹配基础权限；
      attr 的每个键：值为 "*" 通配，列表表示取值范围，其余忽略大小写相等
    """
    perm_dynamic = perm_dynamic or {}
    if not attrs or attrs == [{}]:
        return not perm_dynamic
    for attr in attrs:
        if not attr:
            if not perm_dynamic:
                return True
            continue
        if not isinstance(attr, dict):
            continue
        matched = True
        for key, value in attr.items():
            perm_value = perm_dynamic.get(key)
            if value == "*":
                continue
            if isinstance(value, list):
                if perm_value not in value:
                    matched = False
                    break
            elif str(perm_value).upper() != str(value).upper():
                matched = False
                break
        if matched:
            return True
    return False


def menu_dyn_subset(perm_dyn: Optional[dict], casbin_dyn: Dict[str, Any]) -> bool:
    """
    菜单可见性匹配规则（menu 接口使用，与权限项规则不同，保持原有行为）：
    permission.dynamic_params 必须是策略 attrs 的子集，且逐键字符串精确相等；
    无动态参数的权限只匹配无 attrs 的策略。
    """
    perm_dyn = perm_dyn or {}
    if not perm_dyn:
        return not casbin_dyn
    for k, v in perm_dyn.items():
        if k not in casbin_dyn:
            return False
        if str(casbin_dyn[k]) != str(v):
            return False
    return True


def serialize_permission(permission: dict, default_menu_ids: Any = None) -> Dict[str, Any]:
    """permissions 文档 → 接口返回结构"""
    menu_ids = permission.get("menu_ids")
    return {
        "id": str(permission["_id"]),
        # code 可能存成 ObjectId，返回模型要求字符串（原来遇到 ObjectId 会直接 500）
        "code": str(permission["code"]),
        "name": permission["name"],
        "action": permission["action"],
        "menu_ids": menu_ids if menu_ids is not None else default_menu_ids,
        "description": permission.get("description"),
        "dynamic_params": permission.get("dynamic_params"),
    }
