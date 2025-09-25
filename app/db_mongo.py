import os
import casbin
from dotenv import load_dotenv
from pymongo import MongoClient
from contextlib import contextmanager
from casbin_pymongo_adapter import Adapter

# 加载环境变量
load_dotenv()

# MongoDB 配置
MONGO_CONFIG = {
    'host': os.getenv("MONGO_HOST"),
    'port': int(os.getenv("MONGO_PORT")),
    'username': os.getenv("MONGO_USER"),
    'password': os.getenv("MONGO_PASS"),
    'database': os.getenv("MONGO_DB")
}
uri=f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"

# 创建 MongoDB 客户端
client = MongoClient(
    uri
)

# 获取数据库
db = client[MONGO_CONFIG['database']]

# 创建 Casbin 适配器
adapter = Adapter(uri,MONGO_CONFIG['database'])
enforcer = casbin.Enforcer('model.conf', adapter)
# 显式开启自动保存，避免手动 save_policy 造成重复写入
enforcer.enable_auto_save(True)
enforcer.load_policy()

# 为 casbin_rule 集合添加唯一索引，防止完全相同策略重复插入
try:
    db.casbin_rule.create_index(
        [("ptype", 1), ("v0", 1), ("v1", 1), ("v2", 1), ("v3", 1), ("v4", 1), ("v5", 1)],
        unique=True
    )
except Exception:
    # 索引已存在或当前适配器集合名不同时忽略
    pass

@contextmanager
def get_db():
    try:
        yield db
    finally:
        pass  # MongoDB 连接由客户端管理，不需要手动关闭

def get_session():
    with get_db() as session:
        yield session

