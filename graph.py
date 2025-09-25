from morelink_api import MoreLinkClient
from feapder.db.mysqldb import MysqlDB
from dotenv import load_dotenv
import os
from loguru import logger
load_dotenv()

def get_data():
    try:
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
        raw_data = db.find("select * from tb_fbatracking where sono is null or customerId is null or type = 2", to_json=True)
        if not raw_data:
            logger.info("没有需要更新的数据")
            return {
                "code": 200,
                "message": "没有需要更新的数据",
                "data": None
            }
        
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
        morelink_client = MoreLinkClient()
        molink_dahuo_data = morelink_client.dahuodingdan_all_data()
        logger.info(f"获取到{len(molink_dahuo_data)}条MoreLink数据")

        data = grouped_data
        update_count = 0
        for key, value in data.items():
            try:
                group_tracking_number = value[0]['trackingId']
                customerId = None
                for molink_dahuo_item in molink_dahuo_data:
                    if group_tracking_number in str(molink_dahuo_item["CourierNumber"]):
                        sono = molink_dahuo_item["sono"]
                        operNo = molink_dahuo_item["operNo"]
                        customername = molink_dahuo_item["customername"]
                        customerId = customers_data_dict[customername]
                        break

                if customerId:
                    db.update_smart("tb_fbatracking", {"sono": sono, "customerId": customerId,"operatorName": operNo}, f"trackingId = '{group_tracking_number}' or fbaShipmentBoxId like '{key}%'")
                    update_count += 1
                logger.debug(f"处理分组 {key}: {value}")
            except Exception as e:
                logger.error(f"处理分组{key}时出错: {str(e)}")
                continue
                
        logger.info(f"数据更新完成,共更新{update_count}条数据")
        return {
            "code": 200,
            "message": "获取数据成功",
            "data": data
        }
    except Exception as e:
        logger.error(f"获取数据出错: {str(e)}")
        return {
            "code": 500,
            "message": f"获取数据失败: {str(e)}",
            "data": None
        }

if __name__ == "__main__":
    data = get_data()
    print(data)
