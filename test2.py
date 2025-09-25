import os
import pymysql
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv  
load_dotenv()

# 创建同步记录集合
SYNC_LOG_COLLECTION = 'sync_logs'

# 从环境变量获取MySQL配置
DATABASE_CONFIG = {
    'user': os.getenv("MYSQL_USER", "root"),  # 默认用户为root
    'password': os.getenv("MYSQL_PASS", "password"),  # 默认密码为password
    'host': os.getenv("MYSQL_HOST", "localhost"),  # 默认主机为localhost
    'database': os.getenv("MYSQL_DB", "your_database"),  # 默认数据库
    "port": int(os.getenv("MYSQL_PORT", 3306)),  # 默认端口3306
    "charset": "utf8mb4"  # 添加字符集配置
}

# 使用统一的配置
mysql_config = DATABASE_CONFIG

# MongoDB 连接配置
MONGO_CONFIG = {
    'host': os.getenv("MONGO_HOST"),
    'port': int(os.getenv("MONGO_PORT")),
    'username': os.getenv("MONGO_USER"),
    'password': os.getenv("MONGO_PASS"),
    'database': os.getenv("MONGO_DB")
}
uri=f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
mongo_client = MongoClient(uri)
mongo_db = mongo_client[MONGO_CONFIG['database']]

def add_fields_to_products():
    """
    给products集合中的每个文档添加 "加征0204" 和 "加征代码" 字段（如果不存在）。
    """
    products_collection = mongo_db['products']
    
    # 使用 update_many 和 $set 操作符来添加字段
    result = products_collection.update_many(
        {
            "$or": [
                {"加征0204": {"$exists": False}},
                {"加征代码": {"$exists": False}}
            ]
        },
        {
            "$set": {
                "加征0204": "0.1",
                "加征代码": "9903.01.20"
            }
        }
    )
    
    print(f"Updated {result.modified_count} documents in products collection.")



def check_if_synced(table_name):
    """
    检查表是否已经同步过
    :param table_name: 表名
    :return: 是否同步过
    """
    sync_log = mongo_db[SYNC_LOG_COLLECTION].find_one({'table_name': table_name})
    return sync_log is not None

def record_sync(table_name, row_count):
    """
    记录同步信息
    :param table_name: 表名
    :param row_count: 同步行数
    """
    mongo_db[SYNC_LOG_COLLECTION].update_one(
        {'table_name': table_name},
        {'$set': {
            'last_sync_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'row_count': row_count
        }},
        upsert=True
    )

def sync_table(mysql_table, mongo_collection, primary_key='id'):
    """
    同步单个表数据
    :param mysql_table: MySQL表名
    :param mongo_collection: MongoDB集合名
    :param primary_key: 主键字段名
    """
    # 检查是否已经同步过
    if check_if_synced(mysql_table):
        print(f'表 {mysql_table} 已经同步过，跳过')
        return

    # 连接MySQL
    mysql_conn = pymysql.connect(**mysql_config)
    cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取MongoDB集合
    collection = mongo_db[mongo_collection]
    
    try:
        # 查询MySQL数据
        cursor.execute(f'SELECT * FROM `{mysql_table}`')
        rows = cursor.fetchall()
        
        # 同步数据到MongoDB
        for row in rows:
            # 转换datetime对象为字符串
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            
            # 过滤掉id字段
            if 'id' in row:
                del row['id']
            
            # 如果是product3表，处理特殊字段
            if mysql_table == 'product3':
                field_mapping = {
                    '加征%': '加征',
                    'Duty(%)': 'Duty',
                    '件/箱': '件箱',
                    '认证？': '认证',
                    '豁免截止日期/说明':'豁免截止日期说明'
                }
                for old_field, new_field in field_mapping.items():
                    if old_field in row:
                        row[new_field] = row.pop(old_field)
            
            # 更新或插入数据
            collection.insert_one(row)
        
        # 记录同步信息
        record_sync(mysql_table, len(rows))
        print(f'成功同步表 {mysql_table} -> {mongo_collection}, 共 {len(rows)} 条记录')
        
    except Exception as e:
        print(f'同步表 {mysql_table} 失败: {str(e)}')
    finally:
        cursor.close()
        mysql_conn.close()

def sync_all_tables():
    """
    同步所有表数据
    """
    # 配置需要同步的表
    tables_to_sync = {
        'port': 'ports',
        'product3': 'products',
        '收发货人': 'consignees',
        
        '工厂数据':'factories',
        '海运自税':"haiyunzishui",
        'casbin_rule':'casbin_rule',
        'ip_white_list':'ip_white_list',
        'shipmentlog':'shipment_logs',
        'dalei':'dalei',
        'user':'users',
    }
    
    for mysql_table, mongo_collection in tables_to_sync.items():
        sync_table(mysql_table, mongo_collection)

if __name__ == '__main__':
    # sync_all_tables()
    # 在初始化之后调用该函数
    add_fields_to_products()
