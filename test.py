import os
import pandas as pd
from pymongo import MongoClient
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
dadan_db = mongo_client['dadan']
qingguan_latest_db = mongo_client['qingguan-latest']

# 复制 custom_clear_history_summary collection 到 dadan_db 和 qingguan_latest_db
custom_clear_history_summary = mongo_db['custom_clear_history_summary']
custom_clear_history_summary_data = list(custom_clear_history_summary.find())

if custom_clear_history_summary_data:
    # 复制到 dadan_db
    # dadan_custom_clear_history_summary = dadan_db['custom_clear_history_summary']
    # dadan_custom_clear_history_summary.delete_many({})  # 清空集合
    # dadan_custom_clear_history_summary.insert_many(custom_clear_history_summary_data)
    # print("custom_clear_history_summary 复制到 dadan_db 完成")

    # 复制到 qingguan_latest_db
    qingguan_latest_custom_clear_history_summary = qingguan_latest_db['custom_clear_history_summary']
    qingguan_latest_custom_clear_history_summary.delete_many({})  # 清空集合
    qingguan_latest_custom_clear_history_summary.insert_many(custom_clear_history_summary_data)
    print("custom_clear_history_summary 复制到 qingguan_latest_db 完成")

# 复制products collection到dadan数据库的products_copy中
# products_collection = mongo_db['products']
# products_copy_collection = dadan_db['products_copy']

# # 清空products_copy集合
# products_copy_collection.delete_many({})

# # 复制所有文档
# products = list(products_collection.find({}))
# if len(products) > 0:
#     products_copy_collection.insert_many(products)

# # 更新products_copy collection中的加征字段
# products_copy = products_copy_collection.find({})

# for product in products_copy:
#     if '加征' in product and isinstance(product['加征'], str):
#         # 获取原始加征值和0204加征值
#         origin_levy = product['加征']
#         levy_0204 = product.get('加征0204', '')
        
#         # 构建新的加征字典
#         new_levy = {
#             '加征_origin': origin_levy,
#             '加征0204': levy_0204
#         }
        
#         # 更新文档
#         products_copy_collection.update_one(
#             {'_id': product['_id']},
#             {'$set': {'加征': new_levy}}
#         )

# 复制dadan数据库的所有collection到qingguan-latest数据库
# dadan_collections = dadan_db.list_collection_names()
# for collection_name in dadan_collections:
#     print(f"开始复制集合: {collection_name} 从 dadan 到 qingguan-latest")
    
#     # 获取dadan集合中的所有文档
#     dadan_collection = dadan_db[collection_name]
#     documents = list(dadan_collection.find())
#     doc_count = len(documents)
    
#     # 获取qingguan-latest集合
#     qingguan_latest_collection = qingguan_latest_db[collection_name]
    
#     # 清空qingguan-latest集合中的现有数据
#     qingguan_latest_collection.delete_many({})
    
#     # 批量插入文档到qingguan-latest集合
#     if doc_count > 0:
#         qingguan_latest_collection.insert_many(documents)
        
#     print(f"集合 {collection_name} 复制完成，复制文档数: {doc_count}")

# print("所有dadan数据库中的集合已复制到 qingguan-latest 数据库")