"""创建查验工作台的 3 个系统角色，并把旧版「人员与权限」里的绑定迁移成角色分配。

查验工作台的业务角色来自系统角色（见 inspection.SYSTEM_ROLES）：
  inspection_monitor  登记与跟进：4 个接口
  inspection_analyst  处理与分析：4 个接口
  inspection_viewer   在查查询（只读）：读取数据、下载附件 2 个接口
之后给员工开通只需在「用户」里分配角色，菜单、接口权限和业务角色一次生效。

用法（在 qingguan_backend 目录下，使用与后端相同的 .env）：
    python -m app.inspection.setup_roles --dry-run
    python -m app.inspection.setup_roles
可重复执行，已存在的角色、规则、分配不会重复创建。执行后需要重启后端（或调用 /api/casbin/policies/reload）。
"""
import argparse
import sys
from datetime import datetime

from app.db_mongo import db, enforcer
from app.inspection.apis.inspection import MEMBERS, SYSTEM_ROLES
from app.permission_core import dump_attrs, norm_path, normalize_obj, sanitize_description

READ = [("GET", "/inspection/workspace"), ("GET", "/inspection/file")]
WRITE = [("POST", "/inspection/workspace"), ("POST", "/inspection/upload")]
ROLE_SPECS = {
    "inspection_monitor": ("查验工作台：登记与跟进", READ + WRITE),
    "inspection_analyst": ("查验工作台：处理与分析", READ + WRITE),
    "inspection_viewer": ("查验工作台：在查查询（只读）", READ),
}
assert set(ROLE_SPECS) == set(SYSTEM_ROLES)
BUSINESS_TO_SYSTEM = {v: k for k, v in SYSTEM_ROLES.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="创建查验工作台系统角色并迁移旧的人员绑定")
    parser.add_argument("--dry-run", action="store_true", help="只显示将要做的修改，不写入")
    args = parser.parse_args()
    write = not args.dry_run

    # 1. 4 个接口必须已登记（不带 /api 的路径）
    endpoints = {}
    for e in db.api_endpoints.find():
        endpoints.setdefault((str(e.get("Method") or "").upper(), norm_path(e.get("Path"))), e)
    missing = [f"{m} {p}" for m, p in READ + WRITE if (m, p) not in endpoints]
    if missing:
        sys.exit(f"接口管理中缺少：{'、'.join(missing)}。请先在「接口管理」点「从 OpenAPI 同步」后再执行")

    # 2. 每个接口对应的权限项（没有就创建，并挂到查验工作台菜单下）
    menu = db.menu.find_one({"path": "/business/inspection"})
    items = {}
    for key in READ + WRITE:
        e = endpoints[key]
        perms = list(db.permissions.find({"code": str(e["_id"])}))
        item = next((p for p in perms if not p.get("dynamic_params")), None)
        if item:
            items[key] = str(item["_id"])
            continue
        print(f"创建权限项：{e.get('Description')}（{key[0]} {key[1]}）")
        doc = {
            "code": str(e["_id"]),
            "name": e.get("Description") or f"{key[0]} {key[1]}",
            "action": {"GET": "read", "POST": "create"}[key[0]],
            "description": e.get("Description") or "",
            "menu_ids": [str(menu["_id"])] if menu else [],
        }
        items[key] = str(db.permissions.insert_one(doc).inserted_id) if write else "(dry-run)"

    # 3. 角色及其接口规则
    for role_name, (description, keys) in ROLE_SPECS.items():
        if db.users.find_one({"username": role_name}, {"_id": 1}):
            sys.exit(f"角色名 {role_name} 与已有用户重名，已停止")
        role = db.roles.find_one({"role_name": role_name})
        wanted = [items[k] for k in keys]
        if not role:
            print(f"创建角色：{role_name}（{description}）")
            if write:
                now = datetime.now()
                db.roles.insert_one({
                    "role_name": role_name, "description": description, "permissions": wanted,
                    "status": 1, "created_at": now, "updated_at": now,
                })
        else:
            add = [i for i in wanted if i not in (role.get("permissions") or [])]
            if add:
                print(f"角色 {role_name} 补充权限项 {len(add)} 个")
                if write:
                    db.roles.update_one({"_id": role["_id"]}, {"$push": {"permissions": {"$each": add}}, "$set": {"updated_at": datetime.now()}})
        for method, path in keys:
            obj = normalize_obj(path)
            if enforcer.get_filtered_named_policy("p", 0, role_name, obj, method):
                continue
            print(f"  规则：{role_name} {method} {obj}")
            if write:
                desc = sanitize_description(endpoints[(method, path)].get("Description") or f"{role_name} - {path}")
                enforcer.add_named_policy("p", [role_name, obj, method, dump_attrs({}), "allow", desc])

    # 4. 旧版「人员与权限」的绑定 → 角色分配
    for m in db[MEMBERS].find():
        username, system_role = m["_id"], BUSINESS_TO_SYSTEM.get(m.get("role"))
        if not system_role:
            print(f"跳过 {username}：未知业务角色 {m.get('role')}")
            continue
        if not db.users.find_one({"username": username}, {"_id": 1}):
            print(f"跳过 {username}：系统中不存在该账号")
            continue
        # g 规则带第三个字段（描述），按前两个字段判断是否已分配
        if enforcer.get_filtered_named_grouping_policy("g", 0, username, system_role):
            continue
        print(f"分配角色：{username} -> {system_role}")
        if write:
            enforcer.add_named_grouping_policy("g", [username, system_role, f"用户 {username} 具有 {system_role} 角色"])

    if args.dry_run:
        print("dry-run：未写入任何数据")
    else:
        print("完成。请重启后端（或调用 /api/casbin/policies/reload），然后在「用户」里给员工分配查验角色。")


if __name__ == "__main__":
    main()
