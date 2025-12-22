from datetime import datetime
import json
import httpx
import sys
import asyncio
import traceback

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from loguru import logger

from app.db_mongo import get_session
from app.utils import create_email_handler
from app.utils_aspose import shenzhen_customes_pdf_gennerate
from morelink_api import MoreLinkClient
from rpa_tools import find_playwright_node_path
from rpa_tools.email_tools import send_email

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
        except Exception:
            task_logger.error(f"错误为:{traceback.format_exc()}")
            continue
    # 获取 ShipmentLog 数据
    request_data_list = list(db.shipment_logs.find({"status": 0}))

    if not request_data_list:
        task_logger.info("没有需要处理的任务")
        return
    node_path = find_playwright_node_path()
    morelink_client = MoreLinkClient(node_path)
    data = morelink_client.zongdan_api_httpx()

    for request_data in request_data_list:
        try:
            filter_data = [
                row
                for row in data
                if row.get("billno") == request_data["master_bill_no"].strip()
            ]

            if not filter_data:
                task_logger.log(
                    "ALERT",
                    f"海运清关提单pdf-总单列表提单号搜索不到：{request_data['master_bill_no']}",
                )
                db.shipment_logs.update_one(
                    {"_id": request_data["_id"]}, {"$set": {"status": -1}}
                )
                continue

            pdf_file = shenzhen_customes_pdf_gennerate(request_data, filter_data[0])
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

        except Exception:
            task_logger.error(f"错误为:{traceback.format_exc()}")
            continue


if __name__ == "__main__":
    # cron_job()
    get_latest_usd_cny_rate()
