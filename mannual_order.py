import os

from loguru import logger
from feapder.db.mysqldb import MysqlDB

from app.tools_auto.utils.hubs.hubs_client import HubsClient
from morelink_api import MoreLinkClient
from rpa_tools import find_playwright_node_path

logger.info("开始获取数据")
db = MysqlDB(
    ip=os.getenv("MYSQL_HOST"),
    port=int(os.getenv("MYSQL_PORT")),
    db="fbatms",
    user_name=os.getenv("MYSQL_USER"),
    user_pass=os.getenv("MYSQL_PASS")
)
logger.info("数据库连接成功")

# 获取原始数据
raw_data = db.find("select * from tb_fbatracking where sono='' or customerId=0", to_json=True)
if not raw_data:
    logger.info("没有需要更新的数据")
   

logger.info(f"获取到{len(raw_data)}条需要更新的数据")
customers_data = db.find("select id,customerName from tb_customers", to_json=True)
customers_data_dict = {item["customerName"]:item["id"] for item in customers_data}

# 按fbaShipmentBoxId的U分隔前面字符串分组
grouped_data = {}
for item in raw_data:
    if item.get('fbaShipmentBoxId'):
        # 以U分隔,取前面部分作为分组key
        group_key = item['fbaShipmentBoxId'].split('U')[0] if 'U' in item['fbaShipmentBoxId'] else item['fbaShipmentBoxId']
        if group_key not in grouped_data:
            grouped_data[group_key] = []
        grouped_data[group_key].append(item)

logger.info("开始获取MoreLink数据")

hubs_client = HubsClient()
hubs_dahuo_data= hubs_client.get_bigwaybill_orders_list(pagesize=500)
# morelink_client = MoreLinkClient(node_path=find_playwright_node_path())
# molink_dahuo_data = morelink_client.dahuodingdan_all_data()
# logger.info(f"获取到{len(molink_dahuo_data)}条MoreLink数据")
logger.info(f"获取到{len(hubs_dahuo_data)}条Hubs数据")

data = grouped_data
update_params = []  # 收集批量更新的参数
BATCH_SIZE = 100  # 批量更新阈值

def do_batch_update():
    """执行批量更新"""
    if not update_params:
        return

    logger.info(f"开始批量更新 {len(update_params)} 条记录")
    update_sql = "UPDATE tb_fbatracking SET sono = %s, customerId = %s, operNo = %s WHERE trackingId = %s OR fbaShipmentBoxId LIKE %s"

    conn, cursor = db.get_connection()
    try:
        affect_count = cursor.executemany(update_sql, update_params)
        conn.commit()
        logger.info(f"批量更新完成，成功更新 {affect_count} 条记录")
    except Exception as e:
        conn.rollback()
        logger.error(f"批量更新失败：{e}")
        raise
    finally:
        db.close_connection(conn, cursor)

    update_params.clear()  # 清空已处理的参数

for key, value in data.items():
    group_tracking_number = value[0]['trackingId']
    customerId = None

    for hubs_dahuo_item in hubs_dahuo_data:
        if key in str(hubs_dahuo_item["fbx_no"]):
            sono = hubs_dahuo_item["sono"]
            operNo = hubs_dahuo_item["operno"]
            customername = hubs_dahuo_item["abbreviation"]
            customerId = customers_data_dict.get(customername, None)
            # 如果直接匹配失败，尝试模糊匹配（customername 可能是完整名称的前缀）
            if customerId is None:
                for full_name, cid in customers_data_dict.items():
                    if customername in full_name:
                        customerId = cid
                        logger.info(f"模糊匹配客户：'{customername}' -> '{full_name}'")
                        break
            break

    if customerId:
        update_params.append((sono, customerId, operNo, group_tracking_number, f"{key}%"))

        # 达到阈值，执行批量更新
        if len(update_params) >= BATCH_SIZE:
            do_batch_update()

# 处理剩余的数据
do_batch_update()
            