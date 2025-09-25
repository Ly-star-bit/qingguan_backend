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

@contextmanager
def get_db():
    try:
        yield db
    finally:
        pass  # MongoDB 连接由客户端管理，不需要手动关闭

def get_session():
    with get_db() as session:
        yield session

