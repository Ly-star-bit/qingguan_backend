"""修复「从 OpenAPI 同步」写入的带 /api 前缀的接口路径。

中间件鉴权时会去掉请求路径的 /api 前缀再交给 Casbin（见 app/middleware.py），
所以 api_endpoints.Path 和 casbin 规则里的路径都应该不带 /api。
同步接口会把 OpenAPI 里带 /api 的路径原样写入，导致：
  - 已有接口被重复登记一遍（/api/xxx 与 /xxx 并存）；
  - 用这些接口分配的权限永远匹配不上，用户能看到菜单但打开页面提示「没有权限」。

处理方式：
  - 带 /api 的接口：有权限项引用的，改成不带 /api 的路径（已存在同名接口时把权限项改指向它）；没有引用的直接删除；
  - 带 /api 的 Casbin p 规则：改成不带 /api 的路径。

用法（在 qingguan_backend 目录下，使用与后端相同的 .env）：
    python -m app.inspection.fix_api_prefix_paths --dry-run
    python -m app.inspection.fix_api_prefix_paths
执行后需要重启后端（或调用 /api/casbin/policies/reload），让运行中的服务重新加载权限。
"""
import argparse
from collections import defaultdict

from app.db_mongo import db, enforcer
from app.permission_core import norm_path


def strip_api(path: str) -> str:
    return path[4:] if path.startswith("/api/") else path


def main() -> None:
    parser = argparse.ArgumentParser(description="修复带 /api 前缀的接口路径与权限规则")
    parser.add_argument("--dry-run", action="store_true", help="只显示将要做的修改，不写入")
    args = parser.parse_args()

    endpoints = list(db.api_endpoints.find())
    clean = {(norm_path(e["Path"]), e["Method"]): e for e in endpoints if not e["Path"].startswith("/api/")}
    referenced = defaultdict(list)
    for perm in db.permissions.find({}, {"code": 1, "name": 1}):
        referenced[str(perm.get("code"))].append(perm)

    to_delete, to_rename, to_repoint = [], [], []
    for e in endpoints:
        if not e["Path"].startswith("/api/"):
            continue
        perms = referenced.get(str(e["_id"]), [])
        twin = clean.get((norm_path(strip_api(e["Path"])), e["Method"]))
        if not perms:
            to_delete.append(e)
        elif twin:
            to_repoint.append((e, twin, perms))
        else:
            to_rename.append((e, perms))
            clean[(norm_path(strip_api(e["Path"])), e["Method"])] = e

    print(f"带 /api 前缀的接口：{len(to_delete) + len(to_rename) + len(to_repoint)} 个")
    print(f"  无权限项引用，删除：{len(to_delete)} 个")
    for e, perms in to_rename:
        print(f"  改路径：{e['Method']} {e['Path']} -> {strip_api(e['Path'])}（权限项：{'、'.join(p.get('name', '') for p in perms)}）")
    for e, twin, perms in to_repoint:
        print(f"  已有 {twin['Method']} {twin['Path']}，权限项改指向它并删除 {e['Path']}（权限项：{'、'.join(p.get('name', '') for p in perms)}）")

    rules = [r for r in enforcer.get_named_policy("p") if len(r) > 1 and r[1].startswith("/api/")]
    print(f"带 /api 前缀的权限规则：{len(rules)} 条")
    for r in rules:
        print(f"  {r[0]} {r[2]} {r[1]} -> {strip_api(r[1])}")

    if args.dry_run:
        print("dry-run：未写入任何数据")
        return

    for e, twin, perms in to_repoint:
        db.permissions.update_many({"_id": {"$in": [p["_id"] for p in perms]}}, {"$set": {"code": str(twin["_id"])}})
        to_delete.append(e)
    for e, _ in to_rename:
        db.api_endpoints.update_one({"_id": e["_id"]}, {"$set": {"Path": strip_api(e["Path"])}})
    if to_delete:
        db.api_endpoints.delete_many({"_id": {"$in": [e["_id"] for e in to_delete]}})
    for r in rules:
        new = [r[0], strip_api(r[1]), *r[2:]]
        if enforcer.has_named_policy("p", new):
            enforcer.remove_named_policy("p", r)
        else:
            enforcer.update_named_policy("p", r, new)
    print("完成。请重启后端（或调用 /api/casbin/policies/reload）让运行中的服务重新加载权限。")


if __name__ == "__main__":
    main()
