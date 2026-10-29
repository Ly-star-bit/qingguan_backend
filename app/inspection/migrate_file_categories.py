"""调整「对应文件」的附件类别（2026-10-29 需求）。

「对应文件」改为：主单、分单、仓单、Segregation、发票、其它。已上传的附件按下表换到新类别，
文件本身不动，每票在「操作记录」里留一条说明：
  整理完的出口资料       → 发票
  更新后的发票及补充文件 → 其它
  分单及仓单             → 其它（类别已去掉）
  出口资料汇总           → 其它（类别已去掉）

用法（在 qingguan_backend 目录下，使用与后端相同的 .env）：
    python -m app.inspection.migrate_file_categories --dry-run
    python -m app.inspection.migrate_file_categories
可重复执行，已经换过的不会再处理。新版后端只认新类别，部署后端时执行一次。
"""
import argparse
import uuid
from collections import Counter

from app.db_mongo import db
from app.inspection.apis.inspection import AUDIT, FILES, SHIPMENTS, _now

RENAMES = {
    "整理完的出口资料": "发票",
    "更新后的发票及补充文件": "其它",
    "分单及仓单": "其它",
    "出口资料汇总": "其它",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="调整查验附件的对应文件类别")
    parser.add_argument("--dry-run", action="store_true", help="只显示将要做的修改，不写入")
    args = parser.parse_args()

    files = list(db[FILES].find({"category": {"$in": list(RENAMES)}}, {"shipment_id": 1, "category": 1}))
    if not files:
        print("没有需要调整的附件")
        return
    mawbs = {s["_id"]: s["mawb"] for s in db[SHIPMENTS].find({"_id": {"$in": list({f["shipment_id"] for f in files})}}, {"mawb": 1})}
    per_ship = Counter((f["shipment_id"], f["category"]) for f in files)
    for (shipment_id, old), n in sorted(per_ship.items(), key=lambda x: mawbs.get(x[0][0], "")):
        print(f"{mawbs.get(shipment_id, shipment_id)}：{old} → {RENAMES[old]}，{n} 个")
    for old, n in Counter(f["category"] for f in files).items():
        print(f"合计 {old} → {RENAMES[old]}：{n} 个")

    if args.dry_run:
        print("dry-run：未写入任何数据")
        return
    for old, new in RENAMES.items():
        db[FILES].update_many({"category": old}, {"$set": {"category": new}})
    db[AUDIT].insert_many([
        {
            "_id": str(uuid.uuid4()),
            "shipment_id": shipment_id,
            "actor": "系统",
            "action": f"附件类别调整：{old} → {RENAMES[old]}（{n} 个）",
            "created": _now(),
        }
        for (shipment_id, old), n in per_ship.items()
    ])
    print(f"已调整 {len(files)} 个附件")


if __name__ == "__main__":
    main()
