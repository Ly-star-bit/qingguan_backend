from fastapi import APIRouter, Depends, HTTPException, Body, Query
from typing import Any, List, Optional, Dict

from loguru import logger
from pydantic import BaseModel, Field
from app.db_mongo import get_session, enforcer,filter_service  # 确保已注册 satisfies 函数

from app.schemas import Policy, UpdatePolicy, Group, GroupWithPolicies
import json

from app.casbin_new_func import  FilterCondition

def dump_attrs(attrs: dict | None) -> str:
    if not attrs:
        return "[]"
    return json.dumps([attrs], ensure_ascii=False, separators=(",", ":"))

def load_attrs(s: str | None) -> dict:
    if not s:
        return {}
    try:
        parsed = json.loads(s)
        # 如果解析结果是列表且非空，取第一个元素作为 dict
        if isinstance(parsed, list) and parsed:
            return parsed[0] if isinstance(parsed[0], dict) else {}
        # 如果直接是 dict（兼容旧格式），也接受
        elif isinstance(parsed, dict):
            return parsed
        else:
            return {}
    except Exception:
        return {}
policy_router = APIRouter(prefix="/casbin", tags=["casbin"])
@policy_router.get("/policies/get_role_policies")
async def get_role_policies(role: str):
    """
    获取某“角色/用户”的所有（含隐式）权限策略。
    约定：p 规则槽位为 [sub, obj, act, attrs(JSON), eft, desc]
    """
    try:
        # 含继承关系的策略（推荐）
        rules = enforcer.get_implicit_permissions_for_user(role)
        # 如果你只想看该 role 直挂的 p 规则，可改为：
        # rules = enforcer.get_filtered_named_policy("p", 0, role)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取策略失败: {e}")

    results: List[Dict] = []
    for p in rules:
        # 有的 casbin 返回里不带 ptype，这里强制标为 "p"
        sub = p[0] if len(p) > 0 else ""
        obj = p[1] if len(p) > 1 else ""
        act = p[2] if len(p) > 2 else ""
        attrs = load_attrs(p[3] if len(p) > 3 else "{}")
        eft = p[4] if len(p) > 4 and p[4] else "allow"
        desc = p[5] if len(p) > 5 else ""
   
        results.append({
            "ptype": "p",
            "sub": sub,
            "obj": obj,
            "act": act,
            "attrs": attrs,     # ★ 新增返回 attrs，便于前端展示/编辑
            "eft": eft,
            "description": desc
        })

    return results
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
        p_policies = enforcer.get_named_policy("p")
        p_results = [{
            "ptype": "p",
            "sub": p[0] if len(p) > 0 else "",
            "obj": p[1] if len(p) > 1 else "",
            "act": p[2] if len(p) > 2 else "",
            "attrs": load_attrs(p[3] if len(p) > 3 else "{}"),
            "eft": p[4] if len(p) > 4 and p[4] else "allow",
            "description": p[5] if len(p) > 5 else ""
        } for p in p_policies]
        result["p_policies"] = p_results

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
    obj = policy.obj
    if len(obj) > 1 and not obj.endswith('/'):
                obj = obj + '/'
    rule = [policy.sub, obj, policy.act, dump_attrs(policy.attrs), policy.eft, policy.description or ""]
    print(rule)
    success = enforcer.add_named_policy(policy.ptype, rule)
    if not success:
        raise HTTPException(status_code=400, detail="策略已存在或无法添加")
    enforcer.load_policy()
    return {"message": "策略添加成功"}

@policy_router.delete("/policies")
async def remove_policy(policy: Policy):
    """删除 p 策略"""
    rule = [policy.sub, policy.obj, policy.act, dump_attrs(policy.attrs), policy.eft, policy.description or ""]
    success = enforcer.remove_named_policy(policy.ptype, rule)
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

    rollback_log = []
    try:
        for i, up in enumerate(update_policies):
            old_rule = [
                up.old_sub, up.old_obj, up.old_act,
                dump_attrs(up.old_attrs), up.old_eft, up.old_description or ""
            ]
            new_rule = [
                up.new_sub, up.new_obj, up.new_act,
                dump_attrs(up.new_attrs), up.new_eft, up.new_description or ""
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
            pass

@policy_router.get("/policies/check")
async def check_permission(subject: str, object: str, action: str, env: Optional[str] = Query(None, description="JSON 字符串")):
    """
    检查权限（支持 env JSON）：
    - env 传 JSON 字符串，如 {"start":"CN","dest":"US","type":"sea"}
    - 对不需要属性的策略，env 可为空或 {}
    """
    import json
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
    """获取用户的策略（按 sub 过滤）
    
    如果是 admin user 则返回所有的策略
    """
    # 检查是否为 admin 用户
    is_admin = (username == "admin") or ("admin" in enforcer.get_implicit_roles_for_user(username))
    if is_admin:
        policies = enforcer.get_policy()   
    else:

        # Step 1: 获取用户的所有权限(包括通过角色继承的)
        policies = enforcer.get_implicit_permissions_for_user(username)
    
    return [{
        "ptype": "p",
        "sub": p[0],
        "obj": p[1],
        "act": p[2],
        "attrs": load_attrs(p[3] if len(p) > 3 else "{}"),
        "eft": p[4] if len(p) > 4 and p[4] else "allow",
        "description": p[5] if len(p) > 5 else ""
    } for p in policies]


@policy_router.post("/policies/groups")
async def add_group(group: Group):
    """添加用户组 g(user, role, desc)"""
    policy = [group.user, group.group, group.description or f"用户 {group.user} 具有 {group.group} 角色"]
    if enforcer.add_named_grouping_policy("g", policy):
        enforcer.load_policy()
        return {"message": "组添加成功"}
    else:
        raise HTTPException(status_code=400, detail="组已存在或无法添加")

@policy_router.delete("/policies/groups")
async def remove_group(group: Group):
    """删除用户组"""
    if enforcer.remove_named_grouping_policy("g", [group.user, group.group, group.description or ""]):
        enforcer.load_policy()
        return {"message": "组删除成功"}
    else:
        raise HTTPException(status_code=400, detail="组不存在或无法删除")

@policy_router.put("/policies/groups")
async def update_group(group_with_policies: GroupWithPolicies):
    """
    更新某 role 的全部 p 策略（覆盖写）：
    - 用 update_filtered_named_policies("p", rules, 0, role) 按 sub=role 过滤替换
    """
    new_rules = []
    for policy in group_with_policies.policies:
        rule = [
            group_with_policies.group,       # sub = 角色
            policy.obj,                      # obj
            policy.act,                      # act
            dump_attrs(policy.attrs),        # attrs JSON
            policy.eft,                      # eft
            policy.description or ""         # desc
        ]
        new_rules.append(rule)

    enforcer.update_filtered_named_policies("p", new_rules, 0, group_with_policies.group)
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

@policy_router.post("/policies/filter")
async def filter_policies_post(request: FilterPoliciesRequest):
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
    """
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
        logger.error(f"Error filtering policies: {e}")
        return {"error": str(e)}
