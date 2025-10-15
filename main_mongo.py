import os
import subprocess
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from tracemalloc import start

import jpype
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_authz import CasbinMiddleware
from loguru import logger
from sqlmodel import SQLModel, select

from app.apis.api import api_router
from app.apis.casbin_policy import policy_router as casbin_policy_router
from app.apis.department import department_router
from app.apis.excel_preview import mount_static_files
from app.apis.excel_preview import router as excel_preview_router
from app.apis.menu import menu_router
from app.apis.user import user_router
from app.apis.role import role_router
from app.cargo_tracking_data.apis.cargo_tracking import cargo_tracking_router
from app.dadan.apis.order import order_router
from app.dadan.models import ShipmentLog
from app.db_mongo import enforcer, get_session
from app.middleware import (
    AccessTokenAuthMiddleware,
    AuthenticationMiddleware,
    BasicAuth,
)
from app.price_card.apis.price_card import price_card_router
from app.qingguan.apis.web_vba_mongo import IPWhitelistMiddleware, web_vba_router
from app.qingguan.apis.qingguan_all_router import qingguan_router
from app.fentan.apis.all_fentan import fentan_router
from app.route_17track.apis.route_17track import router_17track
from app.skudetail.apis.skudetail import skudetail_router
from app.api_keys.apis.api_keys import api_key_router
from app.apis.permission_item import permission_item_router
from app.hubs_new_morelink.apis.hubs_client_router import hubs_router
from app.utils import create_email_handler, output_custom_clear_history_log
from email_ip_auto import main as email_ip_auto_main
from morelink_api import MoreLinkClient
from morelink_output_excel_client import main as morelink_output_excel_main
from morelink_output_excel_client import morelink_get_operNo
from rpa_tools import find_playwright_node_path
from rpa_tools.email_tools import send_email
from fastapi.openapi.utils import get_openapi
# "yu.luo@hubs-scs.com,op_sea@hubs-scs.com"
receiver_emial = "op_sea@hubs-scs.com"
logger.level("ALERT", no=35, color="<red>")
task_logger = logger.bind(task="task")
task_logger = task_logger.patch(lambda record: record.update(task="task"))

logger.add(create_email_handler(receiver_emial), level="ALERT")
task_logger.add(create_email_handler(receiver_emial), level="ALERT")

# 配置日志文件保存
logger.add(
    "log/app.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="10 MB",
    retention="10 days",
    compression="zip",
)

task_logger.add(
    "log/task.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="10 MB",
    retention="10 days",
    compression="zip",
)


def start_nextjs():
    # 获取当前工作目录
    cwd = os.getcwd()
    # 进入到 Next.js 项目的目录
    logger.info("正在启动服务next")

    if not os.path.isdir(cwd):
        raise ValueError(f"Next.js directory does not exist: {cwd}")
    # 启动 Next.js 应用
    process = subprocess.Popen(
        ["next", "start", "-H", "0.0.0.0"],
        cwd=r"D:\react_python\dadan\front",
        shell=True,
    )
    return process


@asynccontextmanager
async def lifespan(app: FastAPI):
    # start_nextjs()
    # scheduler.start()
    yield
    # if jpype.isJVMStarted():
    #     jpype.shutdownJVM()

    # scheduler.shutdown()


# 定义FastAPI应用
app = FastAPI(lifespan=lifespan)
app.include_router(user_router)
app.include_router(role_router)

# app.include_router(web_vba_router)
app.include_router(qingguan_router)
app.include_router(menu_router)
app.include_router(order_router)
app.include_router(api_router)
app.include_router(department_router)
app.include_router(excel_preview_router)
app.include_router(cargo_tracking_router)
app.include_router(price_card_router)
app.include_router(router_17track)
app.include_router(skudetail_router)
app.include_router(casbin_policy_router)
app.include_router(api_key_router)
app.include_router(fentan_router)
app.include_router(permission_item_router)
app.include_router(hubs_router)
# 挂载静态文件
mount_static_files(app)

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="My API",
        version="1.0.0",
        routes=app.routes,
    )

    # 定义两个安全方案
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "Enter token as: Bearer <your-token>"
        },
        "ApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "Enter your API key"
        }
    }

    # 关键：使用数组表示“或”关系
    # 每个对象代表一种认证方式，满足任意一个即可
    openapi_schema["security"] = [
        {"BearerAuth": []},
        {"ApiKeyAuth": []}
    ]

    app.openapi_schema = openapi_schema
    return openapi_schema

app.openapi = custom_openapi
# 设置CORS
origins = [
    "http://localhost",
    "http://192.168.20.143:8088",
    "http://localhost:3000",
    "http://192.168.20.87:3000",
    "http://47.103.138.130:3000",
    "http://47.103.138.130:3001",
    # 你可以在这里添加其他允许的源
]
# app.add_middleware(CasbinMiddleware, enforcer=enforcer)

# app.add_middleware(AuthenticationMiddleware, backend=BasicAuth())
app.add_middleware(IPWhitelistMiddleware)
app.add_middleware(AccessTokenAuthMiddleware)


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# 创建一个scheduler实例
scheduler = AsyncIOScheduler()

# 每分钟执行的定时任务
# @scheduler.scheduled_job('interval', minutes=1)
# def cron_job():
#     session = next(get_session())
#     db = session
#     email_queue = list(db.email_queue.find({"status": 0}))
#     for email_data in email_queue:
#         try:
#             send_email(
#                 receiver_email=email_data["receiver_email"],
#                 subject=email_data["subject"],
#                 body=email_data["body"],
#             )
#             db.email_queue.update_one(
#                 {"_id": email_data["_id"]},
#                 {"$set": {"status": 1}}
#             )
#             task_logger.info(f"邮件发送成功->{email_data['subject']}")
#         except Exception as e:
#             task_logger.error(f"错误为:{traceback.format_exc()}")
#             continue
#     # 获取 ShipmentLog 数据
#     request_data_list = list(db.shipment_logs.find({"status": 0}))

#     if not request_data_list:
#         task_logger.info("没有需要处理的任务")
#         return

#     node_path = find_playwright_node_path()
#     morelink_client = MoreLinkClient(node_path)
#     data = morelink_client.zongdan_api_httpx()

#     for request_data in request_data_list:
#         try:
#             filter_data = [
#                 row for row in data
#                 if row.get('billno') == request_data['master_bill_no'].strip()
#             ]

#             if not filter_data:
#                 task_logger.log("ALERT", f"海运清关提单pdf-总单列表提单号搜索不到：{request_data['master_bill_no']}")
#                 db.shipment_logs.update_one(
#                     {"_id": request_data["_id"]},
#                     {"$set": {"status": -1}}
#                 )
#                 continue

#             pdf_file = shenzhen_customes_pdf_gennerate(request_data, filter_data[0])
#             task_logger.info(f"已生成pdf文件->{pdf_file}")
#             send_email(receiver_email=receiver_emial, subject="海运清关提单pdf", body="", attachments=[pdf_file])

#             # 更新 ShipmentLog 的状态
#             db.shipment_logs.update_one(
#                 {"_id": request_data["_id"]},
#                 {"$set": {"status": 1}}
#             )

#         except Exception as e:
#             task_logger.error(f"错误为:{traceback.format_exc()}")
#             continue


# @scheduler.scheduled_job('interval', minutes=5)
# def email_ip_auto_white_list():
#     email_ip_auto_main()


@scheduler.scheduled_job("cron", hour="23,12")
def morelink_output_excel():
    morelink_output_excel_main()


@scheduler.scheduled_job("cron", hour="10")
def morelink_get_operNo_cron():
    morelink_get_operNo()


@scheduler.scheduled_job("cron", hour="20")
def send_email_qingguan_history():
    session = next(get_session())
    db = session
    start_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_time = datetime.now().strftime("%Y-%m-%d")

    # 获取空运和海运的id_list
    # 获取空运和海运的id_list
    air_id_list = [
        str(doc["_id"])
        for doc in db.custom_clear_history_summary.find(
            {
                "port": {"$ne": ""},
                "$or": [{"lock": False}, {"lock": {"$exists": False}}],
                "remarks": {"$ne": "删除"},
            },
            {"_id": 1},
        )
    ]
    sea_id_list = [
        str(doc["_id"])
        for doc in db.custom_clear_history_summary.find(
            {
                "packing_type": {"$ne": ""},
                "$or": [{"lock": False}, {"lock": {"$exists": False}}],
                "remarks": {"$ne": "删除"},
            },
            {"_id": 1},
        )
    ]
    attachments = []

    # 生成空运文件
    if air_id_list:
        air_file_path = output_custom_clear_history_log(
            id_list=air_id_list,
            start_date=start_time,
            end_date=end_time,
        )
        # 获取当前时间并格式化为文件名
        current_time = datetime.now().strftime("%Y%m%d_%H") + "空"
        new_air_file_path = os.path.join(
            os.path.dirname(air_file_path), f"{current_time}.xlsx"
        )
        os.rename(air_file_path, new_air_file_path)
        attachments.append(new_air_file_path)

    # 生成海运文件
    if sea_id_list:
        sea_file_path = output_custom_clear_history_log(
            id_list=sea_id_list,
            start_date=start_time,
            end_date=end_time,
        )
        current_time = datetime.now().strftime("%Y%m%d_%H") + "海"
        new_sea_file_path = os.path.join(
            os.path.dirname(sea_file_path), f"{current_time}.xlsx"
        )
        os.rename(sea_file_path, new_sea_file_path)
        attachments.append(new_sea_file_path)

    # 只有当有文件时才发送邮件
    if attachments:
        send_email(
            receiver_email="yu.luo@hubs-scs.com",
            subject="海运和空运清关历史未锁定记录",
            body="",
            attachments=attachments,
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app=app, host="0.0.0.0", port=8085)
    # send_email_qingguan_history()
    #
