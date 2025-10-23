import io
import os
from dotenv import load_dotenv
from fastapi import APIRouter, File, HTTPException, UploadFile, Form
from fastapi.responses import StreamingResponse
import pandas as pd
from app.hubs_new_morelink.upload import dahuo_upload, exec_generated_code
from pathlib import Path
import uuid
from app.hubs_new_morelink.hubs_client import HubsClient
from app.hubs_new_morelink.schemas import DahuoUploadResponse,DahuoUploadSuccessItem
from feapder.db.mysqldb import MysqlDB
from loguru import logger
import traceback

load_dotenv()



DATABASE_CONFIG = {
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASS"),
    'host': os.getenv("MYSQL_HOST"),
    'database':  os.getenv("MYSQL_DB"),
    "port": int(os.getenv("MYSQL_PORT"))
}
mysql_client = MysqlDB(
    ip=DATABASE_CONFIG['host'],
    port=DATABASE_CONFIG['port'],
    db=DATABASE_CONFIG['database'],
    user_name=DATABASE_CONFIG['user'],
    user_pass=DATABASE_CONFIG['password']
)


file_path = r"C:\Users\a1337\Downloads\发货数据.xlsx"
client_name = "XMHX-厦门和新-JX"
# 1. 从数据库获取 success_code
client_data = mysql_client.find(
    f"SELECT success_code FROM client WHERE client_name='{client_name}'",
    to_json=True,
    limit=1
)
logger.info(client_data)
if not client_data:
    raise HTTPException(status_code=404, detail=f"客户 {client_name} 不存在")

success_code = client_data["success_code"]

# 3. 调用处理逻辑
dataframe, error_msg = exec_generated_code(success_code, file_path)
if dataframe is None:
    raise HTTPException(status_code=400, detail=f"数据处理失败: {error_msg}")

data = dataframe.to_dict(orient="records")
# hubs_client = HubsClient()
success_data, fail_data = dahuo_upload(data)  
#