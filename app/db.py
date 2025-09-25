import os
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import QueuePool, create_engine
from sqlmodel import SQLModel, Session

from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv()

DATABASE_CONFIG = {
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASS"),
    'host': os.getenv("MYSQL_HOST"),
    'database': "fbatms",
    "port": int(os.getenv("MYSQL_PORT"))
}
DATABASE_URL = f"mysql+mysqlconnector://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

engine = create_engine(f'mysql+pymysql://{DATABASE_CONFIG["user"]}:{DATABASE_CONFIG["password"]}@{DATABASE_CONFIG["host"]}:{DATABASE_CONFIG["port"]}/{DATABASE_CONFIG["database"]}')

pool_engine = create_engine(
    DATABASE_URL,
    pool_size=10,           # 连接池大小
    max_overflow=20,        # 连接池溢出大小
    pool_timeout=30,        # 连接池超时时间
    pool_recycle=1800,      # 连接池回收时间
    poolclass=QueuePool,     # 使用QueuePool连接池
    pool_pre_ping=True,  # 新增

)
def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def get_session():
    try:
        return Session(pool_engine)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise