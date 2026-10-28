"""把 HUBS-Inspection 交付包里的数据快照导入查验工作台（inspection_* 集合）。

用法（在 qingguan_backend 目录下，使用与后端相同的 .env）：
    python -m app.inspection.import_hubs_snapshot <解压后的 HUBS-Inspection 目录> [--dry-run]

只插入尚不存在的记录（按原 ID 判断），已存在的不会覆盖，可重复执行。
HUBS 的成员按邮箱授权，与本系统的用户名对不上，因此不导入，只列出来，请在页面「人员与权限」中重新绑定。
"""
import argparse
import json
import shutil
import sqlite3
import sys
from pathlib import Path

from app.db_mongo import db
from app.inspection.apis.inspection import AUDIT, CARGO, FILE_DIR, FILES, ROUNDS, SHIPMENTS, blob_path


def main() -> None:
    parser = argparse.ArgumentParser(description="导入 HUBS 查验工作台数据快照")
    parser.add_argument("hubs_dir", type=Path, help="解压后的 HUBS-Inspection 目录（含 data/inspection.sqlite）")
    parser.add_argument("--dry-run", action="store_true", help="只统计，不写入")
    args = parser.parse_args()

    sqlite_path = (args.hubs_dir / "data" / "inspection.sqlite").resolve()
    attachments = args.hubs_dir / "data" / "attachments"
    if not sqlite_path.is_file():
        sys.exit(f"找不到 {sqlite_path}")
    # immutable=1：只读打开，不生成 -wal/-shm，也不改动交付包
    src = sqlite3.connect(f"file:{sqlite_path}?immutable=1", uri=True)
    src.row_factory = sqlite3.Row

    def rows(table: str):
        return [dict(r) for r in src.execute(f"SELECT * FROM {table}")]

    shipments = []
    for r in rows("shipments"):
        shipments.append({
            "_id": r["id"], "mawb": r["mawb"], "revision": r["revision"],
            "updated": r["updated"], "data": json.loads(r["body"]),
        })
    rounds = [
        {"_id": r["id"], "shipment_id": r["shipment_id"], "created": r["created"], **json.loads(r["body"])}
        for r in rows("rounds")
    ]
    cargo = [{"_id": r.pop("id"), **r} for r in rows("cargo")]
    files = [{"_id": r.pop("id"), **r} for r in rows("files")]
    audit = [{"_id": r.pop("id"), **r} for r in rows("audit")]

    missing = [f["_id"] for f in files if not (attachments / blob_path(f["_id"]).name).is_file()]
    if missing:
        sys.exit(f"交付包缺少 {len(missing)} 个附件文件，例如 {missing[0]}，已停止导入")

    existing_mawbs = {s["mawb"]: s["_id"] for s in db[SHIPMENTS].find({}, {"mawb": 1})}
    conflicts = [s["mawb"] for s in shipments if existing_mawbs.get(s["mawb"], s["_id"]) != s["_id"]]
    if conflicts:
        sys.exit(f"以下主单号已在系统中以其他记录登记，请人工核对后再导入：{', '.join(conflicts)}")

    for name, docs in ((SHIPMENTS, shipments), (ROUNDS, rounds), (CARGO, cargo), (FILES, files), (AUDIT, audit)):
        have = {d["_id"] for d in db[name].find({"_id": {"$in": [d["_id"] for d in docs]}}, {"_id": 1})}
        new = [d for d in docs if d["_id"] not in have]
        print(f"{name}: 快照 {len(docs)} 条，已存在 {len(have)} 条，新增 {len(new)} 条")
        if new and not args.dry_run:
            db[name].insert_many(new)

    copied = 0
    if not args.dry_run:
        FILE_DIR.mkdir(parents=True, exist_ok=True)
    for f in files:
        target = blob_path(f["_id"])
        if target.exists():
            continue
        copied += 1
        if not args.dry_run:
            shutil.copyfile(attachments / target.name, target)
    print(f"附件：{len(files)} 个，新复制 {copied} 个到 {FILE_DIR.resolve()}")

    members = rows("members")
    if members:
        print("HUBS 原有成员（未导入，请在「人员与权限」中按本系统用户名重新绑定）：")
        for m in members:
            print(f"  {m['name']} <{m['email']}> 角色 {m['role']}")
    if args.dry_run:
        print("dry-run：未写入任何数据")


if __name__ == "__main__":
    main()
