import os
import subprocess
from fastapi import FastAPI
import jpype
from loguru import logger
from sqlmodel import SQLModel, select
from app.apis.web_vba import web_vba_router,IPWhitelistMiddleware
from app.api_keys.apis.api_keys import api_key_router
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi.staticfiles import StaticFiles

from app.db import create_db_and_tables,get_session
from app.dadan.models import ShipmentLog
from app.utils import create_email_handler, shenzhen_customes_pdf_gennerate
from morelink_api import MoreLinkClient
from contextlib import asynccontextmanager

from rpa_tools.email_tools import send_email
from rpa_tools import find_playwright_node_path
import traceback
# "yu.luo@hubs-scs.com,op_sea@hubs-scs.com"
receiver_emial = "yu.luo@hubs-scs.com"
logger.level("ALERT", no=35, color="<red>")
task_logger = logger.bind(task="task")

logger.add(create_email_handler(receiver_emial), level="ALERT")
task_logger.add(create_email_handler(receiver_emial), level="ALERT")

# 配置日志文件保存
logger.add("log/app.log",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           level="INFO",
           rotation="1 MB",
           retention="10 days",
           compression="zip")

task_logger.add("log/task.log",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           level="INFO",
           rotation="1 MB",
           retention="10 days",
           compression="zip")

def start_nextjs():
    # 获取当前工作目录
    cwd = os.getcwd()
    # 进入到 Next.js 项目的目录
    logger.info("正在启动服务next")
   
    if not os.path.isdir(cwd):
        raise ValueError(f"Next.js directory does not exist: {cwd}")
    # 启动 Next.js 应用
    process = subprocess.Popen(['next', 'start', '-H', '0.0.0.0'], cwd=r"D:\react_python\vba_front", shell=True)
    return process
@asynccontextmanager
async def lifespan(app: FastAPI):
    # start_nextjs()
    create_db_and_tables()
    scheduler.start()
    yield
    if jpype.isJVMStarted():
        jpype.shutdownJVM()

    scheduler.shutdown()
# 定义FastAPI应用
app = FastAPI(lifespan=lifespan)
app.include_router(web_vba_router)
app.include_router(api_key_router, prefix="/api")  # API密钥相关路由

# 添加静态文件服务
app.mount("/static", StaticFiles(directory="static"), name="static")

# 设置CORS
origins = [
    "http://localhost",
    "http://192.168.20.143:8088",
    "http://localhost:3000",
        "http://192.168.20.87:3000",

    "http://47.103.138.130:3000",
    "http://47.103.138.130:3001"

    # 你可以在这里添加其他允许的源
]
app.add_middleware(IPWhitelistMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 创建一个scheduler实例
scheduler = AsyncIOScheduler()
 
# 每分钟执行的定时任务
@scheduler.scheduled_job('interval', minutes=1)
async def cron_job():
   with get_session() as session:
           # 获取 ShipmentLog 数据
        query = select(ShipmentLog).where(ShipmentLog.status == 0)
        
        result = session.exec(query)
        request_data_list = result.all()
        if not request_data_list:
            task_logger.info("没有需要处理的任务")
            return
        node_path  = find_playwright_node_path()
        morelink_client = MoreLinkClient(node_path)
        data = morelink_client.zongdan_api_httpx()
        for request_data in request_data_list:
          
            try:

                filter_data =        [
                        row for row in data
                        if row.get('billno') == request_data.master_bill_no
                    ]

                
                if not filter_data:
                    task_logger.log("ALERT", f"海运清关提单pdf-总单列表提单号搜索不到：{request_data.master_bill_no}")
                    request_data.status = -1
                    session.add(request_data)
                    session.commit()
                    session.refresh(request_data)
                    continue
                
                pdf_file = shenzhen_customes_pdf_gennerate(request_data.model_dump(), filter_data[0])
                task_logger.info(f"已生成pdf文件->{pdf_file}")
                send_email(receiver_email=receiver_emial,subject="海运清关提单pdf",body="",attachments=[pdf_file])
                # 更新 ShipmentLog 的状态
                request_data.status = 1
                session.add(request_data)
                session.commit()
                session.refresh(request_data)
            except Exception as e:
                task_logger.error(f"错误为:{traceback.format_exc()}")
                continue
               # 更新 ShipmentLog 的状态为失败
                
            

# # # 在启动 FastAPI 应用之前启动 Next.js
# @app.on_event("startup")
# async def startup_event():
#     create_db_and_tables()
#     scheduler.start()
# @app.on_event("shutdown")
# def shutdown_event():
#     # 关闭 JVM
#     if jpype.isJVMStarted():
#         jpype.shutdownJVM()

#     scheduler.shutdown()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app=app,host="0.0.0.0",port=8084)