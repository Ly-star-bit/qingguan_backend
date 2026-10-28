"""一次性清理「取消勾选」留下的 deny 规则。

以前在「用户」「角色管理」里取消勾选一个权限项，会把对应的 allow 规则改成 deny。
deny 优先于所有 allow：同一接口通过其他角色或直接授予的权限也会一起被禁掉，而且界面上看不到。
带参数（attrs）的 deny 在中间件鉴权时不比对参数，会把这个接口的所有请求都拦下。
现在取消勾选改为直接删除授权，这个脚本把已有的 deny 规则全部删除。

注意：会删除全部 deny 规则，只在这次改造时执行一次。以后如果在「策略管理」里有意添加了 deny 规则，不要再执行。

用法（在 qingguan_backend 目录下，使用与后端相同的 .env）：
    python -m app.cleanup_deny_rules --dry-run
    python -m app.cleanup_deny_rules
执行后需要重启后端（或调用 /api/casbin/policies/reload），让运行中的服务重新加载权限。
"""
import argparse

from app.db_mongo import enforcer


def main() -> None:
    parser = argparse.ArgumentParser(description="删除全部 deny 规则（取消勾选遗留）")
    parser.add_argument("--dry-run", action="store_true", help="只列出，不删除")
    args = parser.parse_args()

    denies = [list(r) for r in enforcer.get_named_policy("p") if len(r) > 4 and str(r[4]).lower() == "deny"]
    print(f"deny 规则：{len(denies)} 条")
    for r in denies:
        print(f"  {r[0]}  {r[2]} {r[1]}  参数 {r[3]}  描述 {r[5] if len(r) > 5 else ''}")

    if args.dry_run:
        print("dry-run：未删除任何规则")
        return
    removed = sum(1 for r in denies if enforcer.remove_named_policy("p", r))
    print(f"已删除 {removed} 条。请重启后端（或调用 /api/casbin/policies/reload）。")


if __name__ == "__main__":
    main()
