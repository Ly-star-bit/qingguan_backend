from fastapi import APIRouter, Depends, HTTPException
from typing import List
from app.db_mongo import get_session, enforcer
from pydantic import BaseModel
from app.schemas import Policy, UpdatePolicy, Group, GroupUpdate, GroupWithPolicies

policy_router = APIRouter(prefix="/casbin",tags=["casbin"])
@policy_router.post("/policies/reload")
async def reload_policies():
    enforcer.load_policy()  # 重新加载最新策略
    return {"message": "策略重新加载成功"}

@policy_router.get("/policies")
async def get_policies(policy_type: str = None):
    """获取所有策略,可以通过policy_type参数过滤只获取p或g策略"""
    result = {}
    
    # 如果未指定类型或指定为p,获取p策略
    if not policy_type or policy_type == "p":
        p_policies = enforcer.get_named_policy("p")
        p_results = [{
            "ptype": "p", 
            "sub": p[0],
            "obj": p[1],
            "act": p[2],
            "eft": p[3] if len(p) > 3 else "allow",
            "description": p[5] if len(p) > 5 else ""
        } for p in p_policies]
        result["p_policies"] = p_results
    
    # 如果未指定类型或指定为g,获取g策略
    if not policy_type or policy_type == "g":
        g_policies = enforcer.get_named_grouping_policy("g")
        g_results = [{
            "ptype": "g",
            "user": g[0],
            "role": g[1], 
            "description": g[2] if len(g) > 2 else ""
        } for g in g_policies]
        result["g_policies"] = g_results
    
    return result

@policy_router.post("/policies")
async def add_policy(policy: Policy):
    """添加策略"""
    # 兼容旧结构：sub,obj,act,eft,"",description
    success = enforcer.add_named_policy(policy.ptype, [policy.sub, policy.obj, policy.act, policy.eft, "", policy.description])
    if not success:
        raise HTTPException(status_code=400, detail="策略已存在")
    # autosave 已开启，无需手动保存
    enforcer.load_policy()
    return {"message": "策略添加成功"}

@policy_router.delete("/policies")
async def remove_policy(policy: Policy):
    """删除策略"""
    success = enforcer.remove_named_policy(policy.ptype, [policy.sub, policy.obj, policy.act, policy.eft, "", policy.description])
    if not success:
        raise HTTPException(status_code=404, detail="策略不存在")
    # autosave 已开启，无需手动保存
    enforcer.load_policy()
    return {"message": "策略删除成功"}

@policy_router.put("/policies")
async def update_policies(update_policies: List[UpdatePolicy]):
    """
    批量更新策略
    - 支持跨 ptype 更新（删除旧 + 添加新）
    - 支持同 ptype 更新（直接替换）
    - 任一策略失败则整体回滚（尽力而为）
    """
    print(update_policies)
    if not update_policies:
        raise HTTPException(status_code=400, detail="更新策略列表不能为空")

    # 记录已成功操作的策略，用于回滚
    rollback_log = []

    try:
        for i, update_policy in enumerate(update_policies):
            old_policy = [
                update_policy.old_sub,
                update_policy.old_obj,
                update_policy.old_act,
                update_policy.old_eft,
                "",  # v4（通常为空）
                update_policy.old_description,
            ]
            new_policy = [
                update_policy.new_sub,
                update_policy.new_obj,
                update_policy.new_act,
                update_policy.new_eft,
                "",  # v4
                update_policy.new_description,
            ]

            if update_policy.old_ptype != update_policy.new_ptype:
                # 跨 ptype：先删后加
                removed = enforcer.remove_named_policy(update_policy.old_ptype, old_policy)
                if not removed:
                    raise HTTPException(
                        status_code=404,
                        detail=f"第 {i+1} 项：旧策略不存在，无法更新"
                    )
                rollback_log.append(("add", update_policy.old_ptype, old_policy))  # 回滚时重新添加旧策略

                added = enforcer.add_named_policy(update_policy.new_ptype, new_policy)
                if not added:
                    raise HTTPException(
                        status_code=400,
                        detail=f"第 {i+1} 项：新策略已存在或无法添加"
                    )
                rollback_log.append(("remove", update_policy.new_ptype, new_policy))  # 回滚时删除新策略

            else:
                # 同 ptype：直接更新
                updated = enforcer.update_named_policy(
                    update_policy.old_ptype, old_policy, new_policy
                )
                if not updated:
                    raise HTTPException(
                        status_code=404,
                        detail=f"第 {i+1} 项：策略不存在或无法更新"
                    )
                # 回滚：用新策略换回旧策略
                rollback_log.append((
                    "update", 
                    update_policy.old_ptype, 
                    new_policy, 
                    old_policy
                ))

        # 所有策略更新成功，持久化到存储（如数据库/文件）
        # enforcer.save_policy()  # 注意：不是 load_policy！
        return {"message": f"成功更新 {len(update_policies)} 条策略"}

    except HTTPException:
        # 发生业务错误，尝试回滚
        _rollback_policies(enforcer, rollback_log)
        raise  # 重新抛出原始异常
    except Exception as e:
        # 发生未知错误，尝试回滚
        _rollback_policies(enforcer, rollback_log)
        raise HTTPException(status_code=500, detail=f"批量更新失败: {str(e)}")


def _rollback_policies(enforcer, rollback_log: List[tuple]):
    """尽力回滚已执行的操作（不保证100%成功）"""
    for op in reversed(rollback_log):
        try:
            if op[0] == "add":
                enforcer.add_named_policy(op[1], op[2])
            elif op[0] == "remove":
                enforcer.remove_named_policy(op[1], op[2])
            elif op[0] == "update":
                enforcer.update_named_policy(op[1], op[2], op[3])  # new -> old
        except Exception:
            # 回滚失败也继续，避免掩盖原始错误
            pass

@policy_router.get("/policies/check")
async def check_permission(subject: str, object: str, action: str):
    """检查权限"""
    has_permission = enforcer.enforce(subject, object, action)
    return {"has_permission": has_permission}

@policy_router.get("/policies/get_user_policies")
async def get_user_policies(username: str):
    """获取用户的所有权限"""
    policies = enforcer.get_filtered_named_policy("p", 0, username)
    return [{
        "ptype": "p",
        "sub": p[0],
        "obj": p[1],
        "act": p[2],
        "eft": p[3] if len(p) > 3 else "allow",
        "description": p[5] if len(p) > 5 else ""
    } for p in policies]

@policy_router.post("/policies/groups")
async def add_group(group: Group):
    """添加用户组"""
    policy = [
        group.user,
        group.group,
        group.description or f"用户 {group.user} 具有 {group.group} 角色"
    ]
    if enforcer.add_named_grouping_policy("g", policy):
        enforcer.load_policy()
        return {"message": "组添加成功"}
    else:
        raise HTTPException(status_code=400, detail="组已存在或无法添加")

@policy_router.delete("/policies/groups")
async def remove_group(group: Group):
    """删除用户组"""
    
    if enforcer.remove_named_grouping_policy("g", [group.user, group.group, group.description]):
        enforcer.load_policy()
        return {"message": "组删除成功"}
    else:
        raise HTTPException(status_code=400, detail="组不存在或无法删除")

@policy_router.put("/policies/groups")
async def update_group(group_with_policies: GroupWithPolicies):
    """更新用户组及其对应的策略权限"""
    # 组装新的策略列表
    new_rules = []
    for policy in group_with_policies.policies:
        policy_data = [group_with_policies.group, policy.obj, policy.act, policy.eft, "", policy.description]
        new_rules.append(policy_data)

    # 使用 casbin 的批量更新：按 subject=group 过滤，替换为 new_rules
    enforcer.update_filtered_named_policies("p", new_rules, 0, group_with_policies.group)

    enforcer.load_policy()
    return {"message": "组及策略更新成功"}

@policy_router.get("/policies/groups")
async def get_groups():
    """获取所有用户组"""
    print(enforcer.get_all_roles())
    return enforcer.get_all_roles()

@policy_router.get("/policies/get_role_policies")
async def get_role_policies(role: str):
    """获取角色的所有权限策略"""
    # policies = enforcer.get_filtered_named_policy("p", 0, role)
    policies = enforcer.get_implicit_permissions_for_user(role)
    return [{
        "ptype": "p",
        "sub": p[0],
        "obj": p[1],
        "act": p[2],
        "eft": p[3] if len(p) > 3 else "allow",
        "description": p[5] if len(p) > 5 else ""
    } for p in policies]
