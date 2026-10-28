import json
import os
import subprocess
import sys
import asyncio
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from tracemalloc import start

from app.tools_auto.utils.hubs.hubs_client import HubsClient

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import httpx
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
    ForwardedPrefixMiddleware,
)
from app.price_card.apis.price_card import price_card_router
from app.qingguan.apis.web_vba_mongo import IPWhitelistMiddleware, web_vba_router
from app.qingguan.apis.qingguan_all_router import qingguan_router
from app.fentan.apis.all_fentan import fentan_router
from app.inspection.apis.inspection import inspection_router
from app.route_17track.apis.route_17track import router_17track
from app.skudetail.apis.skudetail import skudetail_router
from app.api_keys.apis.api_keys import api_key_router
from app.apis.permission_item import permission_item_router
from app.tools_auto.apis.tool_auto_route import tool_auto_router
from app.utils import create_email_handler, output_custom_clear_history_log
from app.utils_aspose import shenzhen_customes_pdf_gennerate
from email_ip_auto import main as email_ip_auto_main
from morelink_api import MoreLinkClient
from morelink_output_excel_client import main as morelink_output_excel_main
from morelink_output_excel_client import morelink_get_operNo
from rpa_tools import find_playwright_node_path
from rpa_tools.email_tools import send_email
from fastapi.openapi.utils import get_openapi
from loguru import logger

# "yu.luo@hubs-scs.com,op_sea@hubs-scs.com"
receiver_emial = "op_sea@hubs-scs.com"
try:
    logger.level("ALERT", no=35, color="<red>")
except TypeError:
    # 级别已存在
    pass


# 定义过滤器函数，用于分离不同logger的日志
def task_filter(record):
    """只允许task_logger的日志通过"""
    return record["extra"].get("logger_name") == "task_logger"


def app_filter(record):
    """只允许普通logger的日志通过（排除task_logger）"""
    return record["extra"].get("logger_name") != "task_logger"


# 配置logger的邮件告警（只发送非task的告警）
logger.add(create_email_handler(receiver_emial), level="ALERT", filter=app_filter)
# 配置logger的文件保存（只保存非task的日志）
logger.add(
    "log/app.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="10 MB",
    retention="10 days",
    compression="zip",
    filter=app_filter,
)

# 创建独立的task_logger实例（带有特定标记）
task_logger = logger.bind(logger_name="task_logger")

# 配置task_logger的邮件告警（只发送task的告警）
task_logger.add(create_email_handler(receiver_emial), level="ALERT", filter=task_filter)
# 配置task_logger的文件保存（只保存task的日志）
task_logger.add(
    "log/task.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level}  | {message}",
    level="INFO",
    rotation="10 MB",
    retention="10 days",
    compression="zip",
    filter=task_filter,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # scheduler.start()
    yield
    # if jpype.isJVMStarted():
    #     jpype.shutdownJVM()

    # scheduler.shutdown()


# 定义FastAPI应用
app = FastAPI(lifespan=lifespan, redirect_slashes=True)
app.include_router(user_router, prefix="/api")
app.include_router(role_router, prefix="/api")

# app.include_router(web_vba_router)
app.include_router(qingguan_router, prefix="/api")
app.include_router(menu_router, prefix="/api")
app.include_router(order_router, prefix="/api")
app.include_router(api_router, prefix="/api")
app.include_router(department_router, prefix="/api")
app.include_router(excel_preview_router, prefix="/api")
app.include_router(cargo_tracking_router, prefix="/api")
app.include_router(price_card_router, prefix="/api")
app.include_router(router_17track, prefix="/api")
app.include_router(skudetail_router, prefix="/api")
app.include_router(casbin_policy_router, prefix="/api")
app.include_router(api_key_router, prefix="/api")
app.include_router(fentan_router, prefix="/api")
app.include_router(permission_item_router, prefix="/api")
app.include_router(tool_auto_router, prefix="/api")
app.include_router(inspection_router, prefix="/api")
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
            "description": "Enter token as: Bearer <your-token>",
        },
        "ApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "Enter your API key",
        },
    }

    # 关键：使用数组表示“或”关系
    # 每个对象代表一种认证方式，满足任意一个即可
    openapi_schema["security"] = [{"BearerAuth": []}, {"ApiKeyAuth": []}]

    app.openapi_schema = openapi_schema
    return openapi_schema


app.openapi = custom_openapi
# 设置CORS
origins = [
    "http://localhost",
    "http://192.168.20.143:8088",
    "http://localhost:3000",
    "http://192.168.20.87:3000",
    "http://192.168.20.87:3003",
    "http://47.103.138.130:3000",
    "http://47.103.138.130:3001",
    # 你可以在这里添加其他允许的源
]
# app.add_middleware(CasbinMiddleware, enforcer=enforcer)

# app.add_middleware(AuthenticationMiddleware, backend=BasicAuth())
# app.add_middleware(IPWhitelistMiddleware)
app.add_middleware(AccessTokenAuthMiddleware)
# app.add_middleware(SlashMiddleware)
app.add_middleware(ForwardedPrefixMiddleware)

#
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
@scheduler.scheduled_job("interval", minutes=1)
def cron_job():
    session = next(get_session())
    db = session
    email_queue = list(db.email_queue.find({"status": 0}))
    for email_data in email_queue:
        try:
            send_email(
                receiver_email=email_data["receiver_email"],
                subject=email_data["subject"],
                body=email_data["body"],
            )
            db.email_queue.update_one(
                {"_id": email_data["_id"]}, {"$set": {"status": 1}}
            )
            task_logger.info(f"邮件发送成功->{email_data['subject']}")
        except Exception as e:
            task_logger.error(f"错误为:{traceback.format_exc()}")
            continue
    # 获取 ShipmentLog 数据
    request_data_list = list(db.shipment_logs.find({"status": 0}))

    if not request_data_list:
        task_logger.info("没有需要处理的任务")
        return

    # 初始化 Hubs 客户端和端口映射
    hubs_client = HubsClient()
    today = datetime.now()
    one_month_ago = today - timedelta(days=90)
    one_month_later = today + timedelta(days=30)
    s_date = one_month_ago.strftime("%Y-%m-%d")
    e_date = one_month_later.strftime("%Y-%m-%d")
    hubs_payload = {"pageindex":1,"pagesize":200,"open_search":False,"sorts":[],"s_date":s_date,"e_date":e_date,"transport_type":"海运","sel_type":"operno","ids":[],"searchlist":[]}
    hubs_zongdan_data_res = hubs_client.httpx_client.post("https://admin.hubs-scs.com/api/v1/oms/oms_zginfo/getlist",json=hubs_payload)
    hubs_zongdan_data = hubs_zongdan_data_res.json()
    
    if not hubs_zongdan_data.get("code") == 200 or not hubs_zongdan_data.get("data"):
        hubs_data_list = []
    else:
        hubs_data_list = hubs_zongdan_data['data']['list']
    
    # 获取端口映射
    port_payload = {"keyword":"","pageindex":1,"pagesize":100,"transport_type":"海运"}
    hubs_port_res = hubs_client.httpx_client.post("https://admin.hubs-scs.com/api/v1/pom/base_port/getcomponentlist",json=port_payload)
    hubs_port_data = hubs_port_res.json()
    port_code_full_english_name_mapping = dict()
    if hubs_port_data.get("data") and hubs_port_data["data"].get("list"):
        for port in hubs_port_data['data']['list']:
            port_code_full_english_name_mapping[port['port_code']] = port['port_name_yw']
    
    # 初始化 Morelink 客户端
    # node_path = find_playwright_node_path()
    # morelink_client = MoreLinkClient(node_path)
    # morelink_data = morelink_client.zongdan_api_httpx()

    hubs_field_mapping = {
        "hbl_no": "hblno",
        "pol": "startland",
        "pod2": "destination",
        "vsl_voy": "flight",
        "cabinet_type": "cabinettype",
        "seal_no": "sealno",
        "atd": "ATD",
        "cntr_no":"cabinetNo"
    }
    
    morelink_field_mapping = {
        "billno": "hblno",
        "startland": "startland",
        "destination": "destination",
        "flight": "flight",
        "cabinetNo": "cabinetNo",
        "sealno": "sealno",
        "ATD": "ATD",
    }
    
    for request_data in request_data_list:
        try:
            master_bill_no = request_data["master_bill_no"].strip()
            filter_data = []
            source_system = None
            
            # 先从 Hubs 新系统查询
            filter_data = [i for i in hubs_data_list if i['bl_no'] == master_bill_no]
            if filter_data:
                source_system = "hubs"
                field_mapping = hubs_field_mapping
            # else:
            #     # 从 Morelink 旧系统查询
            #     filter_data = [row for row in morelink_data if row.get("billno") == master_bill_no]
            #     if filter_data:
            #         source_system = "morelink"
            #         field_mapping = morelink_field_mapping
            
            mapped_data = []
            for item in filter_data:
                mapped_item = {field_mapping.get(k, k): v for k, v in item.items()}
                if source_system == "hubs":
                    mapped_item['startland'] = port_code_full_english_name_mapping.get(mapped_item.get('startland'), mapped_item.get('startland'))
                    mapped_item['destination'] = port_code_full_english_name_mapping.get(mapped_item.get('destination'), mapped_item.get('destination'))
                mapped_data.append(mapped_item)
            
            if not filter_data:
                task_logger.log(
                    "ALERT",
                    f"海运清关提单pdf-总单列表提单号搜索不到：{master_bill_no}",
                )
                db.shipment_logs.update_one(
                    {"_id": request_data["_id"]}, {"$set": {"status": -1}}
                )
                continue
            
            pdf_file = shenzhen_customes_pdf_gennerate(request_data, mapped_data[0])
            task_logger.info(f"已生成pdf文件->{pdf_file}")
            send_email(
                receiver_email=receiver_emial,
                subject="海运清关提单pdf",
                body="",
                attachments=[pdf_file],
            )

            # 更新 ShipmentLog 的状态
            db.shipment_logs.update_one(
                {"_id": request_data["_id"]}, {"$set": {"status": 1}}
            )

        except Exception as e:
            task_logger.error(f"错误为:{traceback.format_exc()}")
            continue


# @scheduler.scheduled_job('interval', minutes=5)
# def email_ip_auto_white_list():
#     email_ip_auto_main()


@scheduler.scheduled_job("cron", hour="23,12")
def morelink_output_excel():
    morelink_output_excel_main()


@scheduler.scheduled_job("cron", hour="10")
def morelink_get_operNo_cron():
    morelink_get_operNo()


@scheduler.scheduled_job("interval", minutes=20)
def work_num_backfilling_job():
    """工作单号回填定时任务"""
    from app.tools_auto.utils.dadan.work_num_dadan import execute_backfilling

    try:
        execute_backfilling()
    except Exception as e:
        task_logger.error(f"工作单号回填任务执行失败: {traceback.format_exc()}")


@scheduler.scheduled_job("interval", days=3)
def get_latest_usd_cny_rate():
    """
    获取最新的USD/CNY汇率
    """
    url = "https://wise.com/rates/history+live?source=USD&target=CNY&length=30&resolution=hourly&unit=day"

    try:
        # 发送GET请求
        with httpx.Client() as client:
            response = client.get(url)
            response.raise_for_status()  # 检查HTTP错误

            # 解析JSON响应
            data = response.json()

            if not data:
                task_logger.info("❌ API返回空数据")
                return None
            session = next(get_session())
            db = session
            # 按时间戳排序，获取最新的汇率
            latest_rate = max(data, key=lambda x: x["time"])

            # 转换时间戳为可读格式
            timestamp = latest_rate["time"] / 1000  # 转换为秒
            readable_time = datetime.fromtimestamp(timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            # 四舍五入保留2位小数
            rate_value = round(latest_rate["value"], 4)

            task_logger.info("📈 最新的USD/CNY汇率信息:")
            task_logger.info(f"   汇率: {rate_value}")
            task_logger.info(f"   时间: {readable_time}")
            task_logger.info(f"   源货币: {latest_rate['source']}")
            task_logger.info(f"   目标货币: {latest_rate['target']}")
            task_logger.info(f"   时间戳: {latest_rate['time']}")

            # 更新数据库中的汇率信息
            db.exchange_rates.update_one(
                {"type": "美金人民币汇率"},
                {"$set": {"rate": rate_value, "update_time": readable_time}},
                upsert=True,
            )

            task_logger.info("✅ 汇率信息已更新到数据库")
            return latest_rate

    except httpx.RequestError as e:
        task_logger.info(f"❌ 请求错误: {e}")
        return None
    except httpx.HTTPStatusError as e:
        task_logger.info(f"❌ HTTP错误: {e}")
        return None
    except json.JSONDecodeError as e:
        task_logger.info(f"❌ JSON解析错误: {e}")
        return None
    except Exception as e:
        task_logger.info(f"❌ 未知错误: {e}")
        return None


# @scheduler.scheduled_job("cron", hour="20")
# def send_email_qingguan_history():
#     session = next(get_session())
#     db = session
#     start_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
#     end_time = datetime.now().strftime("%Y-%m-%d")

#     # 获取空运和海运的id_list
#     # 获取空运和海运的id_list
#     air_id_list = [
#         str(doc["_id"])
#         for doc in db.custom_clear_history_summary.find(
#             {
#                 "port": {"$ne": ""},
#                 "$or": [{"lock": False}, {"lock": {"$exists": False}}],
#                 "remarks": {"$ne": "删除"},
#             },
#             {"_id": 1},
#         )
#     ]
#     sea_id_list = [
#         str(doc["_id"])
#         for doc in db.custom_clear_history_summary.find(
#             {
#                 "packing_type": {"$ne": ""},
#                 "$or": [{"lock": False}, {"lock": {"$exists": False}}],
#                 "remarks": {"$ne": "删除"},
#             },
#             {"_id": 1},
#         )
#     ]
#     attachments = []

#     # 生成空运文件
#     if air_id_list:
#         air_file_path = output_custom_clear_history_log(
#             id_list=air_id_list,
#             start_date=start_time,
#             end_date=end_time,
#         )
#         # 获取当前时间并格式化为文件名
#         current_time = datetime.now().strftime("%Y%m%d_%H") + "空"
#         new_air_file_path = os.path.join(
#             os.path.dirname(air_file_path), f"{current_time}.xlsx"
#         )
#         os.rename(air_file_path, new_air_file_path)
#         attachments.append(new_air_file_path)

#     # 生成海运文件
#     if sea_id_list:
#         sea_file_path = output_custom_clear_history_log(
#             id_list=sea_id_list,
#             start_date=start_time,
#             end_date=end_time,
#         )
#         current_time = datetime.now().strftime("%Y%m%d_%H") + "海"
#         new_sea_file_path = os.path.join(
#             os.path.dirname(sea_file_path), f"{current_time}.xlsx"
#         )
#         os.rename(sea_file_path, new_sea_file_path)
#         attachments.append(new_sea_file_path)

#     # 只有当有文件时才发送邮件
#     if attachments:
#         send_email(
#             receiver_email="yu.luo@hubs-scs.com",
#             subject="海运和空运清关历史未锁定记录",
#             body="",
#             attachments=attachments,
#         )


if __name__ == "__main__":
    import uvicorn

    # uvicorn.run(app=app, host="0.0.0.0", port=8085)
    # get_latest_usd_cny_rate()
    cron_job()
    # send_email_qingguan_history()
    #
    # cron_job()

    # node_path = find_playwright_node_path()
    # morelink_client = MoreLinkClient(node_path)
    # data = morelink_client.zongdan_api_httpx()

    # filter_data = [
    #     row for row in data
    #     if row.get('billno') =="MATS1637457000 ".strip()
    # ]
