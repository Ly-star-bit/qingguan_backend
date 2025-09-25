import re
from datetime import datetime
import time
from rpa_tools import read_email_by_subject,send_email
# from feapder.db.mysqldb import MysqlDB
from loguru import logger
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
task_logger = logger.bind(task="email_ip_auto_white_list")

# 配置日志文件保存
logger.add("log/email_ip_auto_white_list.log",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           level="INFO",
           rotation="1 MB",
           retention="10 days",
           compression="zip")
MONGO_CONFIG = {
    'host': os.getenv("MONGO_HOST"),
    'port': int(os.getenv("MONGO_PORT")),
    'username': os.getenv("MONGO_USER"),
    'password': os.getenv("MONGO_PASS"),
    'database': os.getenv("MONGO_DB")
}
uri=f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
mongo_client = MongoClient(uri)
mongo_db = mongo_client[MONGO_CONFIG['database']]  # 替换为你的数据库名
ip_whitelist_collection = mongo_db['ip_white_list']  # 替换为你的集合名
# mongo_db_qingguan_latest = mongo_client['qingguan-latest']
# ip_whitelist_collection_qingguan_latest = mongo_db_qingguan_latest['ip_white_list']  # 替换为你的集合名
def extract_ip_from_email(email_body):
    # 使用正则表达式提取IP地址
    ip_pattern = re.compile(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b')
    match = ip_pattern.search(email_body)
    if match:
        return match.group()
    return None

def main():
    email_data = read_email_by_subject(subject_input="ip",seen=True,email_num=3)
    if not email_data:
        logger.info("No email data found.")
        return
    # email_data = [
    #     {"from":"yu.luo@hubs-scs.com",'body':'999.23.43.23'}
    # ]
    # mysql_client = MysqlDB(
    #     ip="localhost",
    #     port=3306,
    #     db="lowcoder_app",
    #     user_name='hubs',
    #     user_pass='E9Vz12QbKh1o4PyddRMS'
    # )

    # mysql_client = MysqlDB(
    #     ip="192.168.20.97",
    #     port=3360,
    #     db="lowcoder_app",
    #     user_name='luoyu',
    #     user_pass='luoyu123456'
    # )
    for email in email_data:
        try:
            email_body = email.get('body').strip()
            email_sender: str = email.get('from')
            if not email_sender.endswith("@hubs-scs.com"):
                logger.warning(f"{email_sender}->不是hubs邮箱")
                send_email(receiver_email=f"{email_sender}",subject="ip添加结果",body=f'{email_sender}->不是hubs邮箱')

                continue
            if not email_body:
                logger.warning(f"Email from {email.get('from')} has no body.")
                continue

            ip = extract_ip_from_email(email_body)
            if not ip:
                print(f"No IP address found in the email from {email.get('from')}.")
                continue

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # mysql_client.add_smart("ip_white_list", {"ip": ip, "remarks": f"{email_sender}\n{current_time}"})
            result = ip_whitelist_collection.insert_one({"ip": ip, "remarks": f"{email_sender}\n{current_time}"})
            # result_qingguan_latest = ip_whitelist_collection_qingguan_latest.insert_one({"ip": ip, "remarks": f"{email_sender}\n{current_time}"})
            if result.acknowledged:
                logger.info(f"IP address {ip} added to whitelist successfully.")
            else:
                 logger.error(f"Failed to add IP address {ip} to whitelist.")


            send_email(receiver_email=f"{email_sender}",subject="ip添加结果",body=f'{ip}->成功添加到whitelist')
        except Exception as e:
            logger.exception(f"Error processing email from {email_sender}: {e}")

            send_email(receiver_email=f"{email_sender}",subject="ip添加结果",body=f'{ip}->添加白名单失败')

            pass

if __name__ == "__main__":
    import schedule
    
    schedule.every(60).seconds.do(main)

    while True:
        schedule.run_pending()
        time.sleep(5)  # 等待1秒以避免CPU占用过高
    # main()
