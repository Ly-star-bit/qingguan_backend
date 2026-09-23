"""
用户名和角色名在 Casbin 里都是 subject（p 规则的 v0、g 规则的 v0/v1），共用同一个命名空间：
- 用户和角色同名时，g(r.sub, p.sub) 对同名直接成立，该用户会拥有这个角色的全部规则；
- 改名时必须同步迁移规则，否则权限留在旧名字下，以后谁用了旧名字谁就继承这些权限。
"""
from fastapi import HTTPException

from app.db_mongo import enforcer


def ensure_username_available(db, username: str) -> None:
    """新用户名不能和已有角色重名"""
    if db.roles.find_one({"role_name": username}, {"_id": 1}) or username in enforcer.get_all_roles():
        raise HTTPException(status_code=400, detail=f"用户名 {username} 与已有角色重名")


def ensure_role_name_available(db, role_name: str) -> None:
    """新角色名不能和已有用户重名"""
    if db.users.find_one({"username": role_name}, {"_id": 1}):
        raise HTTPException(status_code=400, detail=f"角色名 {role_name} 与已有用户重名")


def rename_subject_in_casbin(old_name: str, new_name: str) -> None:
    """
    用户或角色改名时迁移 Casbin 数据：以它为 subject 的 p 规则、它的成员关系和它自身的继承关系。
    注意：casbin_pymongo_adapter 没有实现 add_policies / update_filtered_policies，
    批量接口在 auto_save 下会静默失败（或只改内存），这里只能逐条操作。
    """
    for rule in [list(r) for r in enforcer.get_filtered_named_policy("p", 0, old_name)]:
        enforcer.update_named_policy("p", rule, [new_name, *rule[1:]])

    # 成员关系 g(x, old) 与自身的继承关系 g(old, parent)
    for g in [list(r) for r in enforcer.get_filtered_named_grouping_policy("g", 1, old_name)]:
        enforcer.remove_named_grouping_policy("g", g)
        enforcer.add_named_grouping_policy("g", [g[0], new_name, *g[2:]])
    for g in [list(r) for r in enforcer.get_filtered_named_grouping_policy("g", 0, old_name)]:
        enforcer.remove_named_grouping_policy("g", g)
        enforcer.add_named_grouping_policy("g", [new_name, *g[1:]])

    enforcer.load_policy()
