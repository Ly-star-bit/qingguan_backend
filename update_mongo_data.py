import os
from datetime import datetime

import pandas as pd
from pymongo import MongoClient, ReplaceOne, UpdateOne
from dotenv import load_dotenv
from bson import ObjectId

load_dotenv()

# MongoDB 连接配置
MONGO_CONFIG = {
    'host': os.getenv("MONGO_HOST"),
    'port': int(os.getenv("MONGO_PORT")),
    'username': os.getenv("MONGO_USER"),
    'password': os.getenv("MONGO_PASS"),
    'database': os.getenv("MONGO_DB")
}
uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
mongo_client = MongoClient(uri)
mongo_db = mongo_client[MONGO_CONFIG['database']]


# ==================== 安全批量写入基础设施 ====================
# 所有批量写库都必须经过 safe_bulk_write / safe_update_many：
# 先整表备份，再试跑少量样本并打印前后对比，人工确认后才落全量。


class _Missing:
    """字段在文档中不存在时的占位值，便于在前后对比中直观显示。"""

    def __repr__(self):
        return '(无)'


_MISSING = _Missing()

# 本次运行中已备份过的集合，同一集合只备份一次
_backed_up_collections = {}


def backup_collection(collection_name):
    """
    将集合完整复制到 {collection_name}_backup_{时间戳}，并校验条数一致。

    同一次运行内对同一集合重复调用只会产生一份备份，返回备份集合名。
    空集合视为异常（通常意味着连错库或集合名写错），直接抛错阻断后续写入。
    """
    if collection_name in _backed_up_collections:
        return _backed_up_collections[collection_name]

    source_count = mongo_db[collection_name].count_documents({})
    if source_count == 0:
        raise RuntimeError(f"集合 {collection_name} 为空，拒绝在其上执行批量写入")

    backup_name = f"{collection_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    mongo_db[collection_name].aggregate([{"$match": {}}, {"$out": backup_name}])

    backup_count = mongo_db[backup_name].count_documents({})
    if backup_count != source_count:
        raise RuntimeError(f"备份校验失败: 源 {source_count} 条，备份 {backup_count} 条")

    _backed_up_collections[collection_name] = backup_name
    print(f"📦 已备份 {collection_name} -> {backup_name}（{backup_count} 条）")
    return backup_name


def restore_from_backup(backup_name, target_name, ids=None):
    """
    用备份集合中的文档整体覆盖线上文档。

    Args:
        backup_name (str): 备份集合名。
        target_name (str): 目标线上集合名。
        ids (list | None): 只回滚这些 _id；为空时回滚整个集合。
    """
    query = {"_id": {"$in": list(ids)}} if ids else {}
    operations = [
        ReplaceOne({"_id": document["_id"]}, document, upsert=True)
        for document in mongo_db[backup_name].find(query)
    ]
    if not operations:
        print(f"备份 {backup_name} 中没有可回滚的文档")
        return

    result = mongo_db[target_name].bulk_write(operations)
    restored = result.modified_count + len(result.upserted_ids or {})
    print(f"↩️  已从 {backup_name} 回滚 {restored} 条到 {target_name}")


def _resolve_path(document, path):
    """按点分路径（如 "加征.加征_301"）取值，路径不存在时返回 _MISSING。"""
    value = document
    for key in path.split('.'):
        if not isinstance(value, dict) or key not in value:
            return _MISSING
        value = value[key]
    return value


def _touched_paths(update):
    """从更新文档中提取所有被写入的字段路径，用于打印前后对比。"""
    paths = []
    for operator, fields in update.items():
        if operator.startswith('$') and isinstance(fields, dict):
            paths.extend(fields.keys())
    return paths


def _preview_and_apply(collection, sample):
    """
    逐条执行样本操作，并打印每个被改字段的前后取值。

    Returns:
        list: 实际被改动的文档 _id，用于用户否决时精确回滚。
    """
    print(f"--- 试跑 {len(sample)} 条 ---")
    sample_ids = []

    for filter_doc, update_doc in sample:
        before = collection.find_one(filter_doc)
        if before is None:
            print(f"  ⚠️  未匹配到文档: {filter_doc}")
            continue

        paths = _touched_paths(update_doc)
        old_values = {path: _resolve_path(before, path) for path in paths}

        collection.update_one({"_id": before["_id"]}, update_doc)
        sample_ids.append(before["_id"])

        after = collection.find_one({"_id": before["_id"]})
        for path in paths:
            print(f"  _id {before['_id']} | {path}: "
                  f"{old_values[path]!r} -> {_resolve_path(after, path)!r}")

    print("--- 试跑结束 ---")
    return sample_ids


def safe_bulk_write(collection_name, operations, sample_size=3):
    """
    带备份与试跑的批量写入，是本文件唯一允许的批量写库入口。

    流程：整表备份 -> 试跑前 sample_size 条并打印前后对比 -> 人工确认 -> 执行剩余全量。
    用户否决时，用备份把试跑改动的文档精确还原。

    Args:
        collection_name (str): 目标集合名。
        operations (list): (filter_doc, update_doc) 二元组列表。
        sample_size (int): 试跑条数。

    Returns:
        int: 实际写入的文档数。
    """
    if not operations:
        print(f"集合 {collection_name}: 没有待执行的操作")
        return 0

    collection = mongo_db[collection_name]
    print(f"\n集合 {collection_name}: 待执行 {len(operations)} 条操作")

    backup_name = backup_collection(collection_name)
    sample_ids = _preview_and_apply(collection, operations[:sample_size])

    remaining = operations[sample_size:]
    if not remaining:
        print(f"✅ 全部 {len(sample_ids)} 条已在试跑中完成")
        return len(sample_ids)

    answer = input(f"剩余 {len(remaining)} 条，确认执行全量? [y/N]: ").strip().lower()
    if answer != 'y':
        restore_from_backup(backup_name, collection_name, sample_ids)
        print("已取消全量更新")
        return 0

    result = collection.bulk_write(
        [UpdateOne(filter_doc, update_doc) for filter_doc, update_doc in remaining]
    )
    total = len(sample_ids) + result.modified_count
    print(f"✅ 完成，共写入 {total} 条（试跑 {len(sample_ids)} + 全量 {result.modified_count}）")
    return total


def safe_update_many(collection_name, query, update, sample_size=3):
    """
    对满足 query 的全部文档施加同一份 update，等价于 update_many 但走安全流程。

    Args:
        collection_name (str): 目标集合名。
        query (dict): 匹配条件。
        update (dict): 更新文档，如 {"$set": {...}}。
        sample_size (int): 试跑条数。

    Returns:
        int: 实际写入的文档数。
    """
    operations = [
        ({"_id": document["_id"]}, update)
        for document in mongo_db[collection_name].find(query, {"_id": 1})
    ]
    return safe_bulk_write(collection_name, operations, sample_size)


# ==================== 业务迁移与修复脚本 ====================


def migrate_menu_id_to_menu_ids():
    """将 permissions 集合中的 menu_id 字段迁移为 menu_ids 数组，并删除旧字段。"""
    permissions_collection = mongo_db['permissions']
    operations = []
    skipped = 0

    for permission in permissions_collection.find({"menu_id": {"$exists": True}}):
        if 'menu_ids' in permission:
            print(f"⚠️  权限 {permission.get('code')} 已存在 menu_ids 字段，跳过")
            skipped += 1
            continue

        menu_id = permission.get('menu_id')
        if isinstance(menu_id, list):
            menu_ids = menu_id
        elif isinstance(menu_id, str) and menu_id != "":
            menu_ids = [menu_id]
        else:
            menu_ids = []

        operations.append((
            {"_id": permission["_id"]},
            {"$set": {"menu_ids": menu_ids}, "$unset": {"menu_id": ""}}
        ))

    print(f"待迁移 {len(operations)} 个权限，已跳过 {skipped} 个")
    safe_bulk_write('permissions', operations)


def add_additional_tax_data():
    """为 products_sea 中 country 为 China 的商品写入加征_0405 与加征_0409。"""
    products_collection = mongo_db['products_sea']
    operations = []

    for document in products_collection.find({"country": "China"}, {"加征": 1}):
        if "加征" in document:
            update = {"$set": {"加征.加征_0405": "0.1", "加征.加征_0409": "1.15"}}
        else:
            update = {"$set": {"加征": {"加征_0405": "0.1", "加征_0409": "1.15"}}}
        operations.append(({"_id": document["_id"]}, update))

    safe_bulk_write('products_sea', operations)


def update_estimated_tax_rate():
    """
    按 estimated_tax_amount / gross_weight_kg * 7.3 重算 estimated_tax_rate_cny_per_kg。

    毛重缺失或为 0 的文档无法计算，直接跳过。
    """
    summary_collection = mongo_db['custom_clear_history_summary']
    operations = []
    skipped = 0

    for document in summary_collection.find():
        gross_weight_kg = document.get('gross_weight_kg') or 0
        if gross_weight_kg == 0:
            skipped += 1
            continue

        rate = round(document.get('estimated_tax_amount', 0) / gross_weight_kg * 7.3, 2)
        operations.append((
            {"_id": document["_id"]},
            {"$set": {"estimated_tax_rate_cny_per_kg": rate}}
        ))

    print(f"待更新 {len(operations)} 条，毛重为 0 跳过 {skipped} 条")
    safe_bulk_write('custom_clear_history_summary', operations)


def update_products_from_excel(excel_file):
    """
    从 Excel 的"处理过"工作表按 id 更新 products 集合的中文品名、单价与件箱。

    Args:
        excel_file (str): Excel 文件路径。
    """
    dataframe = pd.read_excel(excel_file, sheet_name="处理过")

    required_columns = ['id', '中文品名', '单价更新', '件/箱更新']
    missing_columns = [column for column in required_columns if column not in dataframe.columns]
    if missing_columns:
        raise ValueError(f"Excel 文件缺少必要的列: {missing_columns}")

    operations = [
        (
            {"_id": ObjectId(row['id'])},
            {"$set": {
                '中文品名': row['中文品名'],
                '单价': row['单价更新'],
                '件箱': row['件/箱更新'],
            }}
        )
        for _, row in dataframe.iterrows()
    ]

    safe_bulk_write('products', operations)


def update_additional_tax_fields():
    """
    为同时存在"加征_芬太尼"和"加征_对等"的商品写入 加征_Section122 = 0.125。

    加征字段缺失或非对象的文档先初始化为空对象。
    加征_301 是各商品独立的原始税率，不在此处统一赋值；
    误覆盖后用 restore_tax_301_from_old() 从备份集合回填。
    """
    for collection_name in ['products_sea', 'products']:
        print(f"\n===== 初始化 {collection_name} 的加征字段 =====")
        safe_update_many(
            collection_name,
            {"$or": [{"加征": {"$exists": False}}, {"加征": ""}, {"加征": None}]},
            {"$set": {"加征": {}}}
        )

        print(f"\n===== 写入 {collection_name} 的加征_Section122 =====")
        safe_update_many(
            collection_name,
            {"加征.加征_芬太尼": {"$exists": True}, "加征.加征_对等": {"$exists": True}},
            {"$set": {"加征.加征_Section122": "0.125"}}
        )


def reset_leftover_tax_301():
    """
    处理备份中没有 加征_301、无法回填的新增商品。

    这些文档的 301 仍停留在误刷值 0.125，统一改为 0.0，并把 加征_Section122 设为 0.125。
    """
    wrong_value = '0.125'

    for backup_name, target_name in [('products_old', 'products'),
                                     ('products_sea_old', 'products_sea')]:
        backup_ids = set(
            document["_id"]
            for document in mongo_db[backup_name].find(
                {"加征.加征_301": {"$exists": True}}, {"_id": 1}
            )
        )

        operations = [
            (
                {"_id": document["_id"]},
                {"$set": {"加征.加征_301": "0.0", "加征.加征_Section122": "0.125"}}
            )
            for document in mongo_db[target_name].find(
                {"加征.加征_301": wrong_value}, {"_id": 1}
            )
            if document["_id"] not in backup_ids
        ]

        print(f"\n集合 {target_name}: 待重置 {len(operations)} 个")
        safe_bulk_write(target_name, operations)


def restore_tax_301_from_old():
    """
    从备份集合按 _id 把 加征.加征_301 的原值回填到线上集合。

    products_old -> products，products_sea_old -> products_sea。
    仅回填线上现值等于误刷值 0.125 的文档：其余现值说明该条未被误刷
    （可能是备份之后的人工修改），回填反而会覆盖掉正确数据。
    备份中无该字段的文档无法回填，单独列出待人工确认。
    """
    wrong_value = '0.125'

    for backup_name, target_name in [('products_old', 'products'),
                                     ('products_sea_old', 'products_sea')]:
        backup_values = {
            document["_id"]: document["加征"]["加征_301"]
            for document in mongo_db[backup_name].find(
                {"加征.加征_301": {"$exists": True}}, {"加征.加征_301": 1}
            )
        }

        operations = []
        no_backup = []
        untouched = []
        for target_document in mongo_db[target_name].find(
            {"加征.加征_301": {"$exists": True}}, {"加征.加征_301": 1}
        ):
            current_value = target_document["加征"]["加征_301"]
            if current_value != wrong_value:
                untouched.append((target_document["_id"], current_value))
                continue

            if target_document["_id"] not in backup_values:
                no_backup.append(target_document["_id"])
                continue

            original_value = backup_values[target_document["_id"]]
            if original_value == current_value:
                continue

            operations.append((
                {"_id": target_document["_id"]},
                {"$set": {"加征.加征_301": original_value}}
            ))

        print(f"\n集合 {target_name}: 待回填 {len(operations)} 个")
        print(f"  现值非{wrong_value}、保持不动的 {len(untouched)} 个: "
              f"{[(str(doc_id), value) for doc_id, value in untouched]}")
        print(f"  备份中无301、无法回填的 {len(no_backup)} 个（保持{wrong_value}，需人工确认）: "
              f"{[str(doc_id) for doc_id in no_backup]}")

        safe_bulk_write(target_name, operations)


def check_tax_301_all_zero():
    """
    检查 products_old 和 products_sea_old 集合中 加征.加征_301 是否全部为 0。

    字段可能是数字或字符串，统一转成 float 后比较；无法转换的视为异常值。
    """
    for collection_name in ['products_old', 'products_sea_old']:
        collection = mongo_db[collection_name]

        total = collection.count_documents({"加征.加征_301": {"$exists": True}})
        missing = collection.count_documents({"加征.加征_301": {"$exists": False}})

        non_zero = []
        invalid = []
        for document in collection.find({"加征.加征_301": {"$exists": True}}, {"加征.加征_301": 1}):
            value = document["加征"]["加征_301"]
            try:
                if float(value) != 0:
                    non_zero.append((document["_id"], value))
            except (TypeError, ValueError):
                invalid.append((document["_id"], value))

        print(f"\n集合 {collection_name}: 含加征_301字段 {total} 个，缺失字段 {missing} 个")
        if not non_zero and not invalid:
            print("  ✅ 加征_301 全部为 0")
        else:
            print(f"  ❌ 非0值 {len(non_zero)} 个，非法值 {len(invalid)} 个")
            for doc_id, value in (non_zero + invalid)[:20]:
                print(f"    _id: {doc_id} | 加征_301: {value!r}")


if __name__ == '__main__':
    # 所有批量写库都会先整表备份，再试跑 3 条打印前后对比，确认后才执行全量。
    # 出问题时用 restore_from_backup('xxx_backup_时间戳', 'xxx') 整表回滚。
    update_additional_tax_fields()
