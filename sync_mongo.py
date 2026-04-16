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


def sync_products_to_local_new():
    """
    将线上MongoDB数据库中的products和products_sea集合同步到本地MongoDB，
    添加'new'后缀，不删除本地原有数据
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

        # 需要同步的集合列表
        collections_to_sync = ['products', 'products_sea']

        for collection_name in collections_to_sync:
            print(f"开始从远程同步集合: {collection_name}")

            # 检查远程集合是否存在
            if collection_name not in remote_db.list_collection_names():
                print(f"警告: 远程集合 {collection_name} 不存在，跳过同步")
                continue

            # 获取远程集合中的所有文档
            remote_collection = remote_db[collection_name]
            documents = list(remote_collection.find())
            doc_count = len(documents)

            # 创建带'new'后缀的本地集合名称
            new_collection_name = f"{collection_name}_new"
            local_new_collection = local_db[new_collection_name]

            # 清空本地new集合中的现有数据（保留本地原始数据）
            local_new_collection.delete_many({})

            # 批量插入文档到本地new集合
            if doc_count > 0:
                local_new_collection.insert_many(documents)

            print(f"集合 {collection_name} 同步完成，同步到 {new_collection_name}，文档数: {doc_count}")

        print("products和products_sea集合同步完成，保存为products_new和products_sea_new")

    except Exception as e:
        print(f"同步过程中发生错误: {str(e)}")

    finally:
        # 关闭数据库连接
        local_client.close()
        remote_client.close()


def update_new_collections_with_route():
    """
    更新products_new和products_sea_new集合中的文档，
    根据country字段添加startland和destination字段
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

        # 需要更新的集合列表
        collections_to_update = ['products_new', 'products_sea_new']

        for collection_name in collections_to_update:
            print(f"开始更新集合: {collection_name}")

            # 检查集合是否存在
            if collection_name not in local_db.list_collection_names():
                print(f"警告: 本地集合 {collection_name} 不存在，跳过更新")
                continue

            # 获取集合
            collection = local_db[collection_name]

            # 获取所有文档
            documents = list(collection.find())
            updated_count = 0

            for doc in documents:
                # 获取country字段值
                country = doc.get('country', '')

                # 根据country设置startland和destination
                if country == 'China' or country == 'Vietnam':
                    # China或Vietnam：startland为China，destination为America
                    startland = country
                    destination = 'America'
                elif country == 'Australia' or country == 'Canada':
                    # Australia或Canada：startland为China，destination为对应国家
                    startland = 'China'
                    destination = country
                else:
                    # 其他情况：startland为China，destination为America
                    startland = 'China'
                    destination = 'America'

                # 更新文档，添加startland和destination字段
                collection.update_one(
                    {'_id': doc['_id']},
                    {
                        '$set': {
                            'startland': startland,
                            'destination': destination
                        }
                    }
                )
                updated_count += 1

            print(f"集合 {collection_name} 更新完成，更新文档数: {updated_count}")

        print("所有new集合更新完成")

    except Exception as e:
        print(f"更新过程中发生错误: {str(e)}")

    finally:
        # 关闭数据库连接
        local_client.close()


def sync_users_password_from_remote():
    """
    将线上MongoDB数据库中users集合的username和password同步到本地，
    当username相同时，覆盖本地用户的密码
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

        # 检查远程users集合是否存在
        if 'users' not in remote_db.list_collection_names():
            print("警告: 远程users集合不存在，跳过同步")
            return

        print("开始从远程同步users集合密码")

        # 获取远程users集合中的所有用户
        remote_users_collection = remote_db['users']
        remote_users = list(remote_users_collection.find())

        # 获取本地users集合
        local_users_collection = local_db['users']

        updated_count = 0
        not_found_count = 0

        for remote_user in remote_users:
            # 获取远程用户的username和password
            username = remote_user.get('username')
            password = remote_user.get('password')

            if not username or not password:
                print("警告: 远程用户缺少username或password字段，跳过")
                continue

            # 在本地users集合中查找相同username的用户
            local_user = local_users_collection.find_one({'username': username})

            if local_user:
                # 更新本地用户的密码
                local_users_collection.update_one(
                    {'username': username},
                    {'$set': {'password': password}}
                )
                updated_count += 1
                print(f"已更新用户 {username} 的密码")
            else:
                not_found_count += 1
                print(f"警告: 本地未找到用户 {username}，跳过密码更新")

        print("users集合密码同步完成")
        print(f"更新密码的用户数: {updated_count}")
        print(f"本地未找到的用户数: {not_found_count}")

    except Exception as e:
        print(f"同步用户密码过程中发生错误: {str(e)}")

    finally:
        # 关闭数据库连接
        local_client.close()
        remote_client.close()


def copy_local_to_remote_new_database():
    """
    将本地MongoDB数据库的全部内容复制到线上新数据库 'qingguan2-latest'
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

        # 创建新的远程数据库配置
        new_remote_config = REMOTE_MONGO_CONFIG.copy()
        new_remote_config['database'] = 'qingguan2-latest'

        # 连接远程MongoDB到新数据库
        remote_uri = f"mongodb://{new_remote_config['username']}:{new_remote_config['password']}@{new_remote_config['host']}:{new_remote_config['port']}"
        remote_client = MongoClient(remote_uri)
        remote_db = remote_client[new_remote_config['database']]

        print(f"开始将本地数据库 {LOCAL_MONGO_CONFIG['database']} 复制到远程数据库 {new_remote_config['database']}")

        # 获取本地数据库中的所有集合
        collections = local_db.list_collection_names()

        if not collections:
            print("本地数据库中没有找到任何集合")
            return

        total_docs_copied = 0

        for collection_name in collections:
            print(f"开始复制集合: {collection_name}")

            # 获取本地集合中的所有文档
            local_collection = local_db[collection_name]
            documents = list(local_collection.find())
            doc_count = len(documents)

            # 获取远程集合（如果不存在会自动创建）
            remote_collection = remote_db[collection_name]

            # 清空远程集合中的现有数据（防止重复）
            remote_collection.delete_many({})

            # 批量插入文档到远程集合
            if doc_count > 0:
                remote_collection.insert_many(documents)
                total_docs_copied += doc_count

            print(f"集合 {collection_name} 复制完成，复制文档数: {doc_count}")

        print("\n✅ 所有集合复制完成!")
        print(f"复制到远程数据库: {new_remote_config['database']}")
        print(f"总共复制文档数: {total_docs_copied}")

    except Exception as e:
        print(f"复制数据库过程中发生错误: {str(e)}")

    finally:
        # 关闭数据库连接
        local_client.close()
        remote_client.close()


def copy_remote_collections_between_databases():
    """
    将线上远程数据库 qingguan 中的两个集合复制到远程数据库 qingguan2-latest
    集合：custom_clear_history_original_summary 和 custom_clear_history_summary
    """
    try:
        # 连接主远程数据库 (qingguan)
        main_remote_uri = f"mongodb://{REMOTE_MONGO_CONFIG['username']}:{REMOTE_MONGO_CONFIG['password']}@{REMOTE_MONGO_CONFIG['host']}:{REMOTE_MONGO_CONFIG['port']}"
        main_remote_client = MongoClient(main_remote_uri)
        main_remote_db = main_remote_client[REMOTE_MONGO_CONFIG['database']]

        # 创建目标远程数据库配置 (qingguan2-latest)
        target_remote_config = REMOTE_MONGO_CONFIG.copy()
        target_remote_config['database'] = 'qingguan2-latest'

        # 连接目标远程数据库 (qingguan2-latest)
        target_remote_uri = f"mongodb://{target_remote_config['username']}:{target_remote_config['password']}@{target_remote_config['host']}:{target_remote_config['port']}"
        target_remote_client = MongoClient(target_remote_uri)
        target_remote_db = target_remote_client[target_remote_config['database']]

        # 需要复制的集合列表
        collections_to_copy = [
            'custom_clear_history_original_summary',
            'custom_clear_history_summary'
        ]

        print(f"开始从远程数据库 {REMOTE_MONGO_CONFIG['database']} 复制集合到远程数据库 {target_remote_config['database']}")

        total_docs_copied = 0

        for collection_name in collections_to_copy:
            print(f"\n开始复制集合: {collection_name}")

            # 检查源集合是否存在
            if collection_name not in main_remote_db.list_collection_names():
                print(f"警告: 源数据库中集合 {collection_name} 不存在，跳过复制")
                continue

            # 获取源集合中的所有文档
            main_collection = main_remote_db[collection_name]
            documents = list(main_collection.find())
            doc_count = len(documents)

            # 获取目标集合
            target_collection = target_remote_db[collection_name]

            # 清空目标集合中的现有数据
            target_collection.delete_many({})

            # 批量插入文档到目标集合
            if doc_count > 0:
                target_collection.insert_many(documents)
                total_docs_copied += doc_count

            print(f"集合 {collection_name} 复制完成，复制文档数: {doc_count}")

        print("\n✅ 集合复制完成!")
        print(f"源数据库: {REMOTE_MONGO_CONFIG['database']}")
        print(f"目标数据库: {target_remote_config['database']}")
        print(f"总共复制文档数: {total_docs_copied}")

    except Exception as e:
        print(f"复制集合过程中发生错误: {str(e)}")

    finally:
        # 关闭数据库连接
        main_remote_client.close()
        target_remote_client.close()

def backup_and_update_products_price():
    """
    备份products集合到products_old，并将每个文档的'单价'字段乘以1.1
    操作远程MongoDB
    """
    try:
        # 连接远程MongoDB
        remote_uri = f"mongodb://{REMOTE_MONGO_CONFIG['username']}:{REMOTE_MONGO_CONFIG['password']}@{REMOTE_MONGO_CONFIG['host']}:{REMOTE_MONGO_CONFIG['port']}"
        remote_client = MongoClient(remote_uri)
        remote_db = remote_client[REMOTE_MONGO_CONFIG['database']]

        products_collection = remote_db['products']
        products_old_collection = remote_db['products_old']

        # 读取所有文档
        documents = list(products_collection.find())
        doc_count = len(documents)
        
        print(f"读取到 {doc_count} 条products文档")

        # 备份到products_old（先清空再插入）
        products_old_collection.delete_many({})
        if documents:
            products_old_collection.insert_many(documents)
        print(f"已备份 {doc_count} 条文档到products_old")

        # 更新单价字段 * 1.1
        updated_count = 0
        for doc in documents:
            单价 = doc.get('单价')
            if 单价 is not None:
                try:
                    # 尝试转换为浮点数并乘以1.1
                    new_price = float(单价) * 1.1
                    products_collection.update_one(
                        {'_id': doc['_id']},
                        {'$set': {'单价': str(new_price)}}
                    )
                    updated_count += 1
                except (ValueError, TypeError) as e:
                    print(f"警告: 文档 {doc.get('_id')} 的单价 '{单价}' 转换失败: {e}")

        print(f"已更新 {updated_count} 条文档的单价字段 (*1.1)")
        print("操作完成")

    except Exception as e:
        print(f"操作过程中发生错误: {str(e)}")

    finally:
        remote_client.close()


def restore_products_price():
    """
    恢复products集合的单价，将每个文档的'单价'字段除以1.1，并四舍五入保存2位小数
    操作远程MongoDB
    """
    try:
        # 连接远程MongoDB
        remote_uri = f"mongodb://{REMOTE_MONGO_CONFIG['username']}:{REMOTE_MONGO_CONFIG['password']}@{REMOTE_MONGO_CONFIG['host']}:{REMOTE_MONGO_CONFIG['port']}"
        remote_client = MongoClient(remote_uri)
        remote_db = remote_client[REMOTE_MONGO_CONFIG['database']]

        products_collection = remote_db['products']

        # 读取所有文档
        documents = list(products_collection.find())
        doc_count = len(documents)
        
        print(f"读取到 {doc_count} 条products文档")

        # 恢复单价字段 / 1.1 并四舍五入保留2位小数
        restored_count = 0
        for doc in documents:
            单价 = doc.get('单价')
            if 单价 is not None:
                try:
                    # 尝试转换为浮点数并除以1.1，然后四舍五入保留2位小数
                    new_price = round(float(单价) / 1.1, 2)
                    print(f"文档 {doc.get('_id')}: 原单价={单价}, 恢复后={new_price}")
                    products_collection.update_one(
                        {'_id': doc['_id']},
                        {'$set': {'单价': str(new_price)}}
                    )
                    restored_count += 1
                    # if restored_count >= 1:
                    #     break
                except (ValueError, TypeError) as e:
                    print(f"警告: 文档 {doc.get('_id')} 的单价 '{单价}' 转换失败: {e}")

        print(f"已恢复 {restored_count} 条文档的单价字段 (/1.1, 四舍五入保留2位小数)")
        print("操作完成")

    except Exception as e:
        print(f"操作过程中发生错误: {str(e)}")

    finally:
        remote_client.close()


if __name__ == "__main__":
    # 选择要执行的同步函数：
    # sync_collections() - 将本地数据同步到线上
    # sync_collections_from_remote() - 将线上所有集合同步到本地
    # sync_products_to_local_new() - 将线上的products和products_sea集合同步到本地，添加new后缀
    # update_new_collections_with_route() - 更新products_new和products_sea_new集合，根据country字段添加startland和destination字段
    # sync_users_password_from_remote() - 将线上users集合的密码同步到本地，根据username覆盖
    # copy_local_to_remote_new_database() - 将本地数据库全部复制到线上新数据库qingguan2-latest
    # copy_remote_collections_between_databases() - 将qingguan数据库的两个集合复制到qingguan2-latest数据库
    # backup_and_update_products_price() - 备份products到products_old，并将单价*1.1
    # restore_products_price() - 恢复products的单价，将单价/1.1并四舍五入保留2位小数

    # 调用新函数
    restore_products_price()