import os
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# 本地 MongoDB 连接配置
LOCAL_MONGO_CONFIG = {
    'host': '192.168.20.111',
    'port': 27018,
    'username': 'luoyu',
    'password': 'luoyu123456',
    'database': 'qingguan'  # 替换为本地数据库名
}

# 线上 MongoDB 连接配置
REMOTE_MONGO_CONFIG = {
    'host': os.getenv("MONGO_HOST"),
    'port': int(os.getenv("MONGO_PORT")), 
    'username': os.getenv("MONGO_USER"),
    'password': os.getenv("MONGO_PASS"),
    'database': os.getenv("MONGO_DB")
}

def sync_collections():
    """
    将本地MongoDB数据库中的集合同步到线上MongoDB
    """
    try:
        # 连接本地MongoDB
        local_client = MongoClient(
            host=LOCAL_MONGO_CONFIG['host'],
            port=LOCAL_MONGO_CONFIG['port'],
            username=LOCAL_MONGO_CONFIG['username'],
            password=LOCAL_MONGO_CONFIG['password']
        )
        local_db = local_client[LOCAL_MONGO_CONFIG['database']]
        
        # 连接远程MongoDB
        remote_uri = f"mongodb://{REMOTE_MONGO_CONFIG['username']}:{REMOTE_MONGO_CONFIG['password']}@{REMOTE_MONGO_CONFIG['host']}:{REMOTE_MONGO_CONFIG['port']}"
        remote_client = MongoClient(remote_uri)
        remote_db = remote_client[REMOTE_MONGO_CONFIG['database']]
        
        # 获取本地数据库中的所有集合
        collections = local_db.list_collection_names()
        
        for collection_name in collections:
            print(f"开始同步集合: {collection_name}")
            
            # 获取本地集合中的所有文档
            local_collection = local_db[collection_name]
            documents = list(local_collection.find())
            doc_count = len(documents)
            
            # 获取远程集合
            remote_collection = remote_db[collection_name]
            
            # 清空远程集合中的现有数据
            remote_collection.delete_many({})
            
            # 批量插入文档到远程集合
            if doc_count > 0:
                remote_collection.insert_many(documents)
                
            print(f"集合 {collection_name} 同步完成，同步文档数: {doc_count}")
            
        print("所有集合同步完成")
        
    except Exception as e:
        print(f"同步过程中发生错误: {str(e)}")
        
    finally:
        # 关闭数据库连接
        local_client.close()
        remote_client.close()

def sync_collections_from_remote():
    """
    将线上MongoDB数据库中的集合同步到本地MongoDB
    """
    try:
        # 连接远程MongoDB
        remote_uri = f"mongodb://{REMOTE_MONGO_CONFIG['username']}:{REMOTE_MONGO_CONFIG['password']}@{REMOTE_MONGO_CONFIG['host']}:{REMOTE_MONGO_CONFIG['port']}"
        remote_client = MongoClient(remote_uri)
        remote_db = remote_client[REMOTE_MONGO_CONFIG['database']]

        # 连接本地MongoDB
        local_client = MongoClient(
            host=LOCAL_MONGO_CONFIG['host'],
            port=LOCAL_MONGO_CONFIG['port'],
            username=LOCAL_MONGO_CONFIG['username'],
            password=LOCAL_MONGO_CONFIG['password']
        )
        local_db = local_client[LOCAL_MONGO_CONFIG['database']]
        
        # 获取远程数据库中的所有集合
        collections = remote_db.list_collection_names()
        
        for collection_name in collections:
            print(f"开始从远程同步集合: {collection_name}")
            
            # 获取远程集合中的所有文档
            remote_collection = remote_db[collection_name]
            documents = list(remote_collection.find())
            doc_count = len(documents)
            
            # 获取本地集合
            local_collection = local_db[collection_name]
            
            # 清空本地集合中的现有数据
            local_collection.delete_many({})
            
            # 批量插入文档到本地集合
            if doc_count > 0:
                local_collection.insert_many(documents)
                
            print(f"集合 {collection_name} 同步完成，同步文档数: {doc_count}")
            
        print("所有集合从远程同步完成")
        
    except Exception as e:
        print(f"同步过程中发生错误: {str(e)}")
        
    finally:
        # 关闭数据库连接
        local_client.close()
        remote_client.close()

if __name__ == "__main__":
    sync_collections_from_remote()