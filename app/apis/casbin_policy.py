from fastapi import APIRouter, HTTPException, Query, Request
from typing import Any, List, Optional, Dict

from loguru import logger
from pydantic import BaseModel, Field
from app.db_mongo import db, enforcer, filter_service  # 确保已注册 satisfies 函数

from app.auth_utils import is_admin_user
from app.permission_core import (
    dump_attrs,
    ensure_rule_loadable,
    load_attrs,
    normalize_obj,
    policy_row_to_dict,
    sanitize_description,
    validate_name,
)
from app.schemas import Policy, UpdatePolicy, Group, GroupWithPolicies
import json

from app.casbin_new_func import  FilterCondition

# dump_attrs / load_attrs 已移到 app.permission_core，这里保留导出以兼容旧的引用方式
__all__ = ["policy_router", "dump_attrs", "load_attrs"]

policy_router = APIRouter(prefix="/casbin", tags=["casbin"])


def _implicit_policies(name: str) -> List[Dict]:
    """某用户/角色的所有（含继承）p 规则"""
    return [policy_row_to_dict(p) for p in enforcer.get_implicit_permissions_for_user(name)]


def _existing_rule(ptype: str, rule: List[str]) -> List[str]:
    """
    定位已存在的规则：先按原样匹配；找不到时再按 obj 补 / 后匹配
    （兼容 obj 未补 / 时写入的旧数据，也兼容前端传入未补 / 的 obj）
    """
    if enforcer.has_named_policy(ptype, rule):
        return rule
    normalized = [rule[0], normalize_obj(rule[1]), *rule[2:]]
    if normalized != rule and enforcer.has_named_policy(ptype, normalized):
        return normalized
    return rule


@policy_router.get("/policies/get_role_policies")
async def get_role_policies(role: str):
    """
    获取某“角色/用户”的所有（含隐式）权限策略。
    约定：p 规则槽位为 [sub, obj, act, attrs(JSON), eft, desc]
    """
    try:
        return _implicit_policies(role)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取策略失败: {e}")

@policy_router.post("/policies/reload")
async def reload_policies():
    enforcer.load_policy()
    return {"message": "策略重新加载成功"}

@policy_router.get("/policies")
async def get_policies(policy_type: Optional[str] = Query(None, description="p 或 g")):
    """
    获取策略：
    - p 规则字段映射：v0=sub, v1=obj, v2=act, v3=attrs(JSON), v4=eft, v5=desc
    """
    result = {}

    if not policy_type or policy_type == "p":
        result["p_policies"] = [policy_row_to_dict(p) for p in enforcer.get_named_policy("p")]

    if not policy_type or policy_type == "g":
        g_policies = enforcer.get_named_grouping_policy("g")
        g_results = [{
            "ptype": "g",
            "user": g[0] if len(g) > 0 else "",
            "role": g[1] if len(g) > 1 else "",
            "description": g[2] if len(g) > 2 else ""
        } for g in g_policies]
        result["g_policies"] = g_results

    return result

@policy_router.post("/policies")
async def add_policy(policy: Policy):
    """添加 p 策略（含 attrs JSON）"""
    rule = [policy.sub, normalize_obj(policy.obj), policy.act, dump_attrs(policy.attrs), policy.eft, sanitize_description(policy.description)]
    ensure_rule_loadable(policy.ptype, rule)
    success = enforcer.add_named_policy(policy.ptype, rule)
    if not success:
        raise HTTPException(status_code=400, detail="策略已存在或无法添加")
    enforcer.load_policy()
    return {"message": "策略添加成功"}

@policy_router.delete("/policies")
async def remove_policy(policy: Policy):
    """删除 p 策略"""
    rule = [policy.sub, policy.obj, policy.act, dump_attrs(policy.attrs), policy.eft, policy.description or ""]
    success = enforcer.remove_named_policy(policy.ptype, _existing_rule(policy.ptype, rule))
    if not success:
        # 精确匹配不到（描述被改过、attrs 存储格式或键顺序不同）时，按「路径 + 方法 + 效果 + attrs」找同一条规则
        target = _rule_identity(policy.obj, policy.act, policy.eft, policy.attrs)
        for r in [list(r) for r in enforcer.get_filtered_named_policy(policy.ptype, 0, policy.sub)]:
            if len(r) > 4 and _rule_identity(r[1], r[2], r[4], load_attrs(r[3])) == target:
                success = enforcer.remove_named_policy(policy.ptype, r) or success
    if not success:
        raise HTTPException(status_code=404, detail="策略不存在或无法删除")
    enforcer.load_policy()
    return {"message": "策略删除成功"}

@policy_router.put("/policies")
async def update_policies(update_policies: List[UpdatePolicy]):
    """
    批量更新 p 策略（支持跨 ptype）
    槽位：sub,obj,act,attrs,eft,desc
    """
    if not update_policies:
        raise HTTPException(status_code=400, detail="更新策略列表不能为空")

    for up in update_policies:
        ensure_rule_loadable(up.new_ptype, [
            up.new_sub, normalize_obj(up.new_obj), up.new_act,
            dump_attrs(up.new_attrs), up.new_eft, sanitize_description(up.new_description)
        ])

    rollback_log = []
    try:
        for i, up in enumerate(update_policies):
            old_rule = _existing_rule(up.old_ptype, [
                up.old_sub, up.old_obj, up.old_act,
                dump_attrs(up.old_attrs), up.old_eft, up.old_description or ""
            ])
            # 新规则与添加时保持一致：obj 统一补 /
            new_rule = [
                up.new_sub, normalize_obj(up.new_obj), up.new_act,
                dump_attrs(up.new_attrs), up.new_eft, sanitize_description(up.new_description)
            ]

            if up.old_ptype != up.new_ptype:
                removed = enforcer.remove_named_policy(up.old_ptype, old_rule)
                if not removed:
                    raise HTTPException(status_code=404, detail=f"第 {i+1} 条：旧策略不存在，无法更新")
                rollback_log.append(("add", up.old_ptype, old_rule))

                added = enforcer.add_named_policy(up.new_ptype, new_rule)
                if not added:
                    raise HTTPException(status_code=400, detail=f"第 {i+1} 条：新策略已存在或无法添加")
                rollback_log.append(("remove", up.new_ptype, new_rule))
            else:
                updated = enforcer.update_named_policy(up.old_ptype, old_rule, new_rule)
                if not updated:
                    raise HTTPException(status_code=404, detail=f"第 {i+1} 条：策略不存在或无法更新")
                rollback_log.append(("update", up.old_ptype, new_rule, old_rule))

        return {"message": f"成功更新 {len(update_policies)} 条策略"}

    except HTTPException:
        _rollback_policies(enforcer, rollback_log)
        raise
    except Exception as e:
        _rollback_policies(enforcer, rollback_log)
        raise HTTPException(status_code=500, detail=f"批量更新失败: {str(e)}")

def _rollback_policies(enforcer, rollback_log: List[tuple]):
    for op in reversed(rollback_log):
        try:
            if op[0] == "add":
                enforcer.add_named_policy(op[1], op[2])
            elif op[0] == "remove":
                enforcer.remove_named_policy(op[1], op[2])
            elif op[0] == "update":
                enforcer.update_named_policy(op[1], op[2], op[3])
        except Exception:
            logger.exception(f"回滚策略失败: {op}")

@policy_router.get("/policies/check")
async def check_permission(subject: str, object: str, action: str, env: Optional[str] = Query(None, description="JSON 字符串")):
    """
    检查权限（支持 env JSON）：
    - env 传 JSON 字符串，如 {"start":"CN","dest":"US","type":"sea"}
    - 对不需要属性的策略，env 可为空或 {}
    """
    env_dict = {}
    if env:
        try:
            env_dict = json.loads(env)
        except Exception:
            raise HTTPException(status_code=400, detail="env 不是合法 JSON")
    has_permission = enforcer.enforce(subject, object, action, env_dict)
    return {"has_permission": has_permission}

@policy_router.get("/policies/get_user_policies")
async def get_user_policies(username: str):
    """获取用户的所有（含角色继承）策略"""
    return _implicit_policies(username)


@policy_router.post("/policies/groups")
async def add_group(group: Group, request: Request):
    """添加用户组 g(user, role, desc)"""
    validate_name(group.user, "用户名")
    validate_name(group.group, "角色名")
    if group.user == group.group:
        raise HTTPException(status_code=400, detail="用户名和角色名不能相同")
    # 原来不校验：可以把用户挂到另一个用户名下（继承对方直接授予的全部权限），
    # 或挂到 admin 角色上（is_admin_user 视为管理员）
    if group.group == "admin":
        caller = (getattr(request.state, "user", None) or {}).get("sub")
        if not is_admin_user(caller):
            raise HTTPException(status_code=403, detail="只有管理员可以分配 admin 角色")
    elif not db.roles.find_one({"role_name": group.group}, {"_id": 1}):
        raise HTTPException(status_code=400, detail=f"角色 {group.group} 不存在，请先在「角色管理」中创建")
    if not (db.users.find_one({"username": group.user}, {"_id": 1})
            or db.roles.find_one({"role_name": group.user}, {"_id": 1})):
        raise HTTPException(status_code=400, detail=f"用户或角色 {group.user} 不存在")
    policy = [group.user, group.group, sanitize_description(group.description or f"用户 {group.user} 具有 {group.group} 角色")]
    ensure_rule_loadable("g", policy)
    if enforcer.add_named_grouping_policy("g", policy):
        enforcer.load_policy()
        return {"message": "组添加成功"}
    else:
        raise HTTPException(status_code=400, detail="组已存在或无法添加")

@policy_router.delete("/policies/groups")
async def remove_group(group: Group):
    """删除用户组：按 (user, role) 删除，不要求描述一致（添加时会自动生成描述）"""
    if enforcer.remove_filtered_named_grouping_policy("g", 0, group.user, group.group):
        enforcer.load_policy()
        return {"message": "组删除成功"}
    else:
        raise HTTPException(status_code=400, detail="组不存在或无法删除")

def _has_attrs(attrs_json: str) -> bool:
    return bool(load_attrs(attrs_json))


def _rule_identity(obj: str, act: str, eft: str, attrs: Optional[dict]) -> tuple:
    """按前端能看到的内容判断是否同一条规则：obj（补 /）、方法、效果、attrs（前端拿到的是 load_attrs 的结果）"""
    return (normalize_obj(obj), act, str(eft).lower(), json.dumps(attrs or {}, sort_keys=True, ensure_ascii=False))


@policy_router.put("/policies/groups")
async def update_group(group_with_policies: GroupWithPolicies):
    """
    覆盖写某角色的接口策略（前端「保存角色接口权限」）。

    - 按差异逐条落库：casbin_pymongo_adapter 没有实现 update_filtered_policies，
      原来的 update_filtered_named_policies 只改了内存，随后 load_policy() 又从库里重新加载，
      结果这个接口一直不生效。
    - 保护：
      1) 前端只传 allow 规则，该角色已有的 deny 规则全部保留；
      2) 传来的规则与已有规则内容相同（obj/方法/效果/attrs 一致）时，原样保留已有规则，
         不按前端数据重写（前端拿到的 attrs 只是规则里的第一项，重写可能丢掉其余项）；
      3) 旧版「编辑接口」弹窗会把 attrs 置空：某接口(obj+act)传来的 attrs 为空，
         而原来有带 attrs 的 allow 规则时，沿用原来的规则（不放宽航线限制）。
    """
    role = validate_name(group_with_policies.group, "角色名")
    # 这里按角色整体覆盖规则；传入用户名就会改写该用户直接授予的权限
    if db.users.find_one({"username": role}, {"_id": 1}):
        raise HTTPException(status_code=400, detail=f"{role} 是用户名，不是角色")
    existing = [list(r) for r in enforcer.get_filtered_named_policy("p", 0, role)]

    same: Dict[tuple, List[List[str]]] = {}
    for r in existing:
        if len(r) > 4:
            same.setdefault(_rule_identity(r[1], r[2], r[4], load_attrs(r[3])), []).append(r)

    # 原有带 attrs 的 allow 规则，按 (obj, act) 分组
    restricted: Dict[tuple, List[List[str]]] = {}
    for r in existing:
        if len(r) > 4 and str(r[4]).lower() == "allow" and _has_attrs(r[3]):
            restricted.setdefault((normalize_obj(r[1]), r[2]), []).append(r)

    new_rules: List[List[str]] = []
    for policy in group_with_policies.policies:
        identity = _rule_identity(policy.obj, policy.act, policy.eft, policy.attrs)
        if identity in same:
            new_rules.extend(same[identity])
            continue
        rule = [
            role,                            # sub = 角色
            normalize_obj(policy.obj),       # obj（与添加策略时一致，统一补 /）
            policy.act,                      # act
            dump_attrs(policy.attrs),        # attrs JSON
            policy.eft,                      # eft
            sanitize_description(policy.description)  # desc（英文逗号等会让规则加载失败）
        ]
        ensure_rule_loadable("p", rule)
        key = (rule[1], rule[2])
        if str(policy.eft).lower() == "allow" and not policy.attrs and key in restricted:
            new_rules.extend(restricted[key])
        else:
            new_rules.append(rule)

    # 保留已有的 deny 规则
    new_rules.extend(r for r in existing if len(r) > 4 and str(r[4]).lower() != "allow")

    # 去重（保持顺序）
    seen = set()
    target: List[List[str]] = []
    for r in new_rules:
        k = tuple(r)
        if k not in seen:
            seen.add(k)
            target.append(r)

    existing_keys = {tuple(r) for r in existing}
    for r in existing:
        if tuple(r) not in seen:
            enforcer.remove_named_policy("p", r)
    for r in target:
        if tuple(r) not in existing_keys:
            enforcer.add_named_policy("p", r)

    enforcer.load_policy()
    return {"message": "组及策略更新成功"}

@policy_router.get("/policies/groups")
async def get_groups():
    return enforcer.get_all_roles()



class FilterItem(BaseModel):
    field: str
    value: Any
    operator: str

class FilterPoliciesRequest(BaseModel):
    filters: List[FilterItem]
    skip: int = Field(default=0, ge=0)
    limit: int = Field(default=100, ge=1, le=1000)


# 字段和操作符白名单：field 会直接成为 MongoDB 查询的键，value 会直接成为查询值，
# 不限制的话可以传 "$where"、{"$ne": ...} 之类的查询操作符
_FILTER_FIELDS = {"v0", "v1", "v2", "v3", "v4", "v5"}
_SELF_OPERATORS = {"eq", "contains", "in"}
_ADMIN_OPERATORS = _SELF_OPERATORS | {"regex", "gt", "lt"}
_SCALAR_TYPES = (str, int, float, bool)


def _check_filter_request(request: FilterPoliciesRequest, http_request: Request) -> None:
    """
    该接口只校验登录不走 Casbin（见 middleware.AUTH_ONLY_PATHS）：
    - 非 admin 只能查自己：v0 条件必须有且只有一个，并且是 eq 自己的用户名。
      （原来只检查“第一个 eq 的 v0”，而过滤服务用的是“第一个 v0”，
       先放一个 contains 别人的 v0、再放一个 eq 自己的 v0，就能查到别人的角色策略）
    - 所有人：字段、操作符、取值类型都要在白名单内。
    """
    user = getattr(http_request.state, "user", None)
    if user is None:
        # API Key 调用已由中间件校验并按 scopes 限制，这里视同管理员
        if not http_request.headers.get("X-API-Key"):
            raise HTTPException(status_code=401, detail="未提供认证令牌")
        is_admin = True
    else:
        is_admin = is_admin_user(user.get("sub"))

    allowed_operators = _ADMIN_OPERATORS if is_admin else _SELF_OPERATORS
    for f in request.filters:
        if f.field not in _FILTER_FIELDS:
            raise HTTPException(status_code=400, detail=f"不支持的过滤字段: {f.field}")
        if f.operator not in allowed_operators:
            raise HTTPException(status_code=400, detail=f"不支持的过滤操作: {f.operator}")
        if f.operator == "in":
            valid = isinstance(f.value, list) and all(isinstance(v, _SCALAR_TYPES) for v in f.value)
        elif f.operator in ("contains", "regex"):
            valid = isinstance(f.value, str)
        else:
            valid = isinstance(f.value, _SCALAR_TYPES)
        if not valid:
            raise HTTPException(status_code=400, detail=f"过滤值类型不正确: {f.field}")

    if not is_admin:
        v0_filters = [f for f in request.filters if f.field == "v0"]
        if (
            len(v0_filters) != 1
            or v0_filters[0].operator != "eq"
            or str(v0_filters[0].value) != user.get("sub")
        ):
            raise HTTPException(status_code=403, detail="没有权限")

@policy_router.post("/policies/filter")
async def filter_policies_post(request: FilterPoliciesRequest, http_request: Request):
    """
    POST /policies/filter
    json:
    {
        "filters": [
            {"field": "v0", "value": "user123", "operator": "eq"},
            {"field": "v1", "value": "api", "operator": "contains"},
            {"field": "v3", "value": "read", "operator": "contains"}
        ],
        "skip": 0,
        "limit": 10
    }
    该接口只校验登录不走 Casbin（见 middleware.AUTH_ONLY_PATHS）：
    非 admin 只能按 v0=自己 查询。
    """
    _check_filter_request(request, http_request)

    try:
        conditions = [FilterCondition(
            field=f.field,
            value=f.value,
            operator=f.operator
        ) for f in request.filters]

        return filter_service.filter_policies_advanced(
            conditions=conditions,
            skip=request.skip,
            limit=request.limit,
            include_inheritance=True
        )
    except Exception as e:
        logger.exception(f"Error filtering policies: {e}")
        raise HTTPException(status_code=500, detail=f"过滤策略失败: {e}")
