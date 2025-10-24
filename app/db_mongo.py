import json
import os
import casbin
from dotenv import load_dotenv
from pymongo import MongoClient
from contextlib import contextmanager
from casbin_pymongo_adapter import Adapter

from app.casbin_new_func import CasbinPolicyFilter

# 加载环境变量
load_dotenv()
def norm_key(k): return k.strip()
def norm_val(k, v):
    if v is None: return None
    if k in ("start", "dest", "startLand", "destination"): return str(v).upper()
    if k in ("type",): return str(v).lower()
    return v

def _as_list(x):
    if x is None: return []
    if isinstance(x, (list, tuple, set)): return list(x)
    return [x]

def satisfies(attrs_json: str, env: dict) -> bool:
    """
    attrs_json: 策略里的 JSON 字符串
    env:        运行时传入的属性 dict（e.g. {"start":"CN","dest":"US","type":"sea"}）
    支持：
      - 直接取值/列表： {"start":["CN","VN"],"dest":"US"}
      - 通配 * ： {"start":"*"}
      - 比较操作：{"type":{"eq":"sea"}}, {"start":{"neq":"RU"}},
                  {"country":{"in":["US","JP"]}}, {"dest":{"nin":["RU","IR"]}}
    约束之间 AND 关系；未出现的键视为“不满足”除非策略为 "*"
    """
    try:
        attrs = json.loads(attrs_json)[0] if attrs_json else {}

    except Exception:
        return False
    #环境变量是空的也不限制
  
    if not env:
        return True
    # 空对象：不限制
    if not attrs:
        return True

    # 规范化 env
    env_norm = {norm_key(k): norm_val(k, v) for k, v in (env or {}).items()}

    for raw_k, rule in attrs.items():
        k = norm_key(raw_k)

        # 通配
        if rule == "*" or rule is None:
            continue

        ev = env_norm.get(k)
        # 列表或标量直接匹配（等价于 "in"）
        if isinstance(rule, (list, tuple, set)) or not isinstance(rule, dict):
            allowed = set(norm_val(k, v) for v in _as_list(rule))
            if ev is None:
                return False
            if allowed and ev not in allowed:
                return False
            continue

        # 高级操作
        # 支持 eq/neq/in/nin （可按需扩展 regex, gte, lte 等）
        if "eq" in rule:
            target = norm_val(k, rule["eq"])
            if ev != target:
                return False
        if "neq" in rule:
            target = norm_val(k, rule["neq"])
            if ev == target:
                return False
        if "in" in rule:
            allowed = set(norm_val(k, v) for v in _as_list(rule["in"]))
            if ev not in allowed:
                return False
        if "nin" in rule:
            denied = set(norm_val(k, v) for v in _as_list(rule["nin"]))
            if ev in denied:
                return False
    return True

# MongoDB 配置
MONGO_CONFIG = {
    'host': os.getenv("MONGO_HOST"),
    'port': int(os.getenv("MONGO_PORT")),
    'username': os.getenv("MONGO_USER"),
    'password': os.getenv("MONGO_PASS"),
    'database': os.getenv("MONGO_DB")
}
uri=f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
filter_service = CasbinPolicyFilter(mongo_uri=uri,database_name=MONGO_CONFIG['database'])

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
enforcer.add_function("satisfies", satisfies)
# enforcer.add_function("keyMatch4", key_match4)
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

