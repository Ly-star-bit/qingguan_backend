from fastapi import APIRouter, Depends, HTTPException
from typing import List
from app.db_mongo import get_session, enforcer
from pydantic import BaseModel

# 定义策略模型
class Policy(BaseModel):
    subject: str  # 用户
    object: str   # 资源
    action: str   # 操作

policy_router = APIRouter(tags=["policy"])

@policy_router.get("/policies")
async def get_policies():
    """获取所有策略"""
    policies = enforcer.get_policy()
    return [{"subject": p[0], "object": p[1], "action": p[2]} for p in policies]

@policy_router.post("/policy")
async def add_policy(policy: Policy):
    """添加策略"""
    success = enforcer.add_policy(policy.subject, policy.object, policy.action)
    if not success:
        raise HTTPException(status_code=400, detail="Policy already exists")
    enforcer.save_policy()
    return {"message": "Policy added successfully"}

@policy_router.delete("/policy")
async def remove_policy(policy: Policy):
    """删除策略"""
    success = enforcer.remove_policy(policy.subject, policy.object, policy.action)
    if not success:
        raise HTTPException(status_code=404, detail="Policy not found")
    enforcer.save_policy()
    return {"message": "Policy removed successfully"}

@policy_router.get("/policy/check")
async def check_permission(subject: str, object: str, action: str):
    """检查权限"""
    has_permission = enforcer.enforce(subject, object, action)
    return {"has_permission": has_permission}

@policy_router.get("/policy/user/{username}")
async def get_user_policies(username: str):
    """获取用户的所有权限"""
    policies = enforcer.get_filtered_policy(0, username)
    return [{"subject": p[0], "object": p[1], "action": p[2]} for p in policies]
