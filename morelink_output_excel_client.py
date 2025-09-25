import os
import traceback
import urllib
import time
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from morelink_api import MoreLinkClient
import pandas as pd
import httpx
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger
from feapder.db.mysqldb import MysqlDB
logger.add("log/tracking_data_log.log",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           level="INFO",
           rotation="1 MB",
           retention="10 days",
           compression="zip")
class QiPaiDataProcessor:
    def __init__(self):
        load_dotenv()
        self._init_mongo_connection()
        self.morelink_client = MoreLinkClient()
        
    def _init_mongo_connection(self):
        """初始化MongoDB连接"""
        try:
            MONGO_CONFIG = {
                'host': os.getenv("MONGO_HOST"),
                'port': int(os.getenv("MONGO_PORT")), 
                'username': os.getenv("MONGO_USER"),
                'password': os.getenv("MONGO_PASS"),
                'database': os.getenv("MONGO_DB")
            }
            uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
            self.mongo_client = MongoClient(uri)
            self.mongo_db = self.mongo_client[MONGO_CONFIG['database']]
            self.qipai_collection = self.mongo_db['cargo_tracking_data']
            logger.info(f"MongoDB连接成功! 连接到数据库: {MONGO_CONFIG['database']}")
        except Exception as e:
            logger.error(f"MongoDB连接失败! 错误信息: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
           retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException, ConnectionError)))
    def get_morelink_routeinfo(self, sono):
        """获取MoreLink路由信息，使用重试机制"""
        try:
            sono = sono.split("-")[0]
            url = "https://morelink56.com/Common/IDATA?type=fun&proc=bll.RouteInfo.getRouInfo"
            payload = {"sono": sono}
            headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
            encoded_data = urllib.parse.urlencode(payload)

            response = self.morelink_client.httpx_client.post(url, content=encoded_data, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()['rows']
            logger.warning(f"获取路由信息失败，状态码: {response.status_code}, sono: {sono}")
            return None
        except Exception as e:
            logger.error(f"获取路由信息出错: {str(e)}, sono: {sono}")
            raise

    def _create_base_dict(self, row):
        """创建基础数据字典"""
        oper_no = row.get("operNo", "")
        return {
            '客户名称': row['customername'],
            "提货时间": row.get("khCreateTime", "").split(" ")[0] if row.get("khCreateTime") else "",
            "开船/起飞": row.get("outtime", ""),
            "主单号": row.get("billno", ""),
            "A/S单号": oper_no,
            "收货地": row.get("d_code", ""),
            "件数": row.get("GoodsNum", ""),
            "FBA号": row.get("fbano", ""),
            "客户内部号": row.get("extendoperno", ""),
            "预计到港时间": row.get("eta", ""),
            "派送方式": row.get("pstype", ""),
            "机场提货/港口提柜": "",
            "计划派送时间": "",
            "实际送达": "",
            "卡车追踪码/快递单号": row.get("CourierNumber", ""),
            "时效（按15天/22天计算）": "",
            "POD": "",
            "上架情况": "",
            "当前状态": "",

            "sono": row['sono']
        }

    def _update_route_info(self, order, route_info):
        """更新路由信息"""
        updates = {}
        is_ship = order['A/S单号'].startswith('S')
        if route_info:
            updates['当前状态'] = route_info[0]['stauts']
        for info in route_info:
            if is_ship:
                if info['title'] == "货物已抵达转运中心":
                    updates['机场提货/港口提柜'] = info['routedate']
            else:
                if info['title'] == "货物抵达目的地机场":
                    updates['机场提货/港口提柜'] = info['routedate']
                    
            if info['title'] == "计划交仓时间":
                updates['计划派送时间'] = info['routedate']
            elif info['title'] == "货物已签收":
                updates['实际送达'] = info['routedate']
            elif info['title'] == "卡车追踪码":
                updates['卡车追踪码/快递单号'] = info['content']
        
        return updates, is_ship

    def _bulk_update(self, operations):
        """批量更新数据"""
        if operations:
            try:
                result = self.qipai_collection.bulk_write(operations)
                logger.info(f"批量更新成功: {result.modified_count} 条记录已更新")
                return True
            except Exception as e:
                logger.error(f"批量更新失败: {str(e)}")
                return False

    def process_data(self):
        """处理七派数据的主方法"""
        try:
            # 获取大货订单数据
            logger.info("开始获取大货订单数据...")
            dahuo_data = self.morelink_client.dahuodingdan_all_data()
            logger.info(f"成功获取到 {len(dahuo_data)} 条大货订单数据")
            
            # 处理七派数据
            qipai_data_ship = []
            qipai_data_air = []
            
            # 准备批量更新操作
            base_update_operations = []
            
            # 更新MongoDB基础数据
            logger.info("开始更新基础数据...")
            counter = 0
            for row in dahuo_data:
                if row["customername"] == "FSQP-佛山七派-SZ":
                    base_dict = self._create_base_dict(row)
                    operation = UpdateOne(
                        {"A/S单号": base_dict["A/S单号"]},
                        {"$set": base_dict},
                        upsert=True
                    )
                    base_update_operations.append(operation)
                    counter += 1
                    
                    # 每100条执行一次批量更新
                    if len(base_update_operations) >= 100:
                        if not self._bulk_update(base_update_operations):
                            # 如果更新失败，尝试重试
                            time.sleep(2)
                            self._bulk_update(base_update_operations)
                        base_update_operations = []
            
            logger.info(f"找到 {counter} 条七派数据")
            
            # 更新剩余的基础数据
            if base_update_operations:
                self._bulk_update(base_update_operations)
            
            # 更新路由信息
            logger.info("开始更新路由信息...")
            all_orders = list(self.qipai_collection.find({
                "客户名称": "FSQP-佛山七派-SZ",
                "当前状态": {"$ne": "已签收"}
            }))
            logger.info(f"从数据库获取到 {len(all_orders)} 条七派订单")
            
            route_update_operations = []
            updated_orders = []
            
            for idx, order in enumerate(all_orders):
                try:
                    # 每处理100条数据打印一次进度
                    if idx > 0 and idx % 100 == 0:
                        logger.info(f"已处理 {idx}/{len(all_orders)} 条订单")
                    
                    route_info = self.get_morelink_routeinfo(order['sono'])
                    if route_info:
                        updates, is_ship = self._update_route_info(order, route_info)
                        if updates:
                            operation = UpdateOne(
                                {"A/S单号": order['A/S单号']},
                                {"$set": updates}
                            )
                            route_update_operations.append(operation)
                            order.update(updates)
                            updated_orders.append(order)
                    
                    # 每100条执行一次批量更新
                    if len(route_update_operations) >= 100:
                        if not self._bulk_update(route_update_operations):
                            # 如果更新失败，尝试重试
                            time.sleep(2)
                            self._bulk_update(route_update_operations)
                        route_update_operations = []
                        
                        # 分类处理后的订单
                        for updated_order in updated_orders:
                            if updated_order['A/S单号'].startswith('S'):
                                qipai_data_ship.append(updated_order)
                            else:
                                qipai_data_air.append(updated_order)
                        updated_orders = []
                except Exception as e:
                    logger.error(f"处理订单 {order.get('A/S单号', '')} 时出错: {traceback.format_exc()}")
                    continue
            
            # 更新剩余的路由信息
            if route_update_operations:
                self._bulk_update(route_update_operations)
                
                # 处理剩余的更新订单
                for updated_order in updated_orders:
                    if updated_order['A/S单号'].startswith('S'):
                        qipai_data_ship.append(updated_order)
                    else:
                        qipai_data_air.append(updated_order)
            
            logger.info(f"七派数据处理完成，共 {len(qipai_data_ship)} 条海运数据，{len(qipai_data_air)} 条空运数据")
            return qipai_data_ship, qipai_data_air
        
        except Exception as e:
            logger.error(f"处理七派数据时发生错误: {str(e)}")
            return [], []

class HKLMTDataProcessor:
    """香港兰玛特
    """
    def __init__(self):
        load_dotenv()
        self._init_mongo_connection()
        self.morelink_client = MoreLinkClient()
        
    def _init_mongo_connection(self):
        """初始化MongoDB连接"""
        try:
            MONGO_CONFIG = {
                'host': os.getenv("MONGO_HOST"),
                'port': int(os.getenv("MONGO_PORT")), 
                'username': os.getenv("MONGO_USER"),
                'password': os.getenv("MONGO_PASS"),
                'database': os.getenv("MONGO_DB")
            }
            uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
            self.mongo_client = MongoClient(uri)
            self.mongo_db = self.mongo_client[MONGO_CONFIG['database']]
            self.qipai_collection = self.mongo_db['cargo_tracking_data']
            logger.info(f"MongoDB连接成功! 连接到数据库: {MONGO_CONFIG['database']}")
        except Exception as e:
            logger.error(f"MongoDB连接失败! 错误信息: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
           retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException, ConnectionError)))
    def get_morelink_routeinfo(self, sono):
        """获取MoreLink路由信息，使用重试机制"""
        try:
            url = "https://morelink56.com/Common/IDATA?type=fun&proc=bll.RouteInfo.getRouInfo"
            payload = {"sono": sono}
            headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
            encoded_data = urllib.parse.urlencode(payload)

            response = self.morelink_client.httpx_client.post(url, content=encoded_data, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()['rows']
            logger.warning(f"获取路由信息失败，状态码: {response.status_code}, sono: {sono}")
            return None
        except Exception as e:
            logger.error(f"获取路由信息出错: {str(e)}, sono: {sono}")
            raise
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), 
           retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException, ConnectionError)))
    def get_morelink_zongdan_data(self):
        """获取MoreLink总单数据,默认获取最近2个月数据"""
 
        
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - relativedelta(months=2)).strftime("%Y-%m-%d")
        zongdan_data = self.morelink_client.zongdan_api_httpx(start_date,end_date)
        return zongdan_data
        
    def _create_base_dict(self, row):
        """创建基础数据字典"""
        oper_no = row.get("operNo", "")
     
        CourierNumber = row.get("CourierNumber", "")
        sub_courier_number = ""
        if CourierNumber:
            CourierNumber_list = CourierNumber.split(",")
            if len(CourierNumber_list) > 1:
                CourierNumber = CourierNumber_list[-1]
                sub_courier_number = ",".join(CourierNumber_list[:-1])
            else:
                CourierNumber = CourierNumber_list[0]
        
        ps_type = row.get("pstype", "")
        if ps_type == "TRUCK":
            CourierNumber, sub_courier_number = "卡车","卡车"
     
            
        return {
            '客户名称': row['customername'],
            "月份":"",
            "收货时间": row.get("khCreateTime", "").split(" ")[0] if row.get("khCreateTime") else "",
            "备货单号":row.get("extendoperno",""),
            "起运地": row.get("startland", ""),
            "目的港": row.get("destination", ""),
            "提单号": row.get("billno", ""),
            "A/S单号": oper_no,
            "派送方式": row.get("pstype", ""),
            "箱数": row.get("GoodsNum", ""),
            "快递单号": CourierNumber,
            "子单号": sub_courier_number,
            "FBA号": row.get("fbano", ""),
            "收货地": row.get("d_code", ""),
            "是否国内查验":"",
            "报关放行时间":"",
            "上航班时间": f"预计{row.get('b_etd', '')}" if row.get('b_etd') else "",
            "航班抵达时间": f"预计{row.get('b_eta', '')}" if row.get('b_eta') else "",
            "清关放行时间": "",
            "当地提取时间": "",
            "当前状态": "",
            "签收时间": "",
            "时效": "",
            "是否进口查验": "",
            "异常备注": "",
            "航班号": "",
            "sono": row['sono'],
            "orderno":row['orderno']
        }

    def _update_route_info(self, order, route_info):
        """更新路由信息"""
        updates = {}
        is_ship = order['A/S单号'].startswith('S')
        if route_info:
            updates['当前状态'] = route_info[0]['stauts']
        for info in route_info:
           
            if info['title'] == "货物报关已放行":
                    updates['报关放行时间'] = info['routedate']

            
            elif info['title'] == "货物已起飞":
                    updates['上航班时间'] = info['routedate']
                    
            elif info['title'] == "货物抵达目的地机场":
                updates['航班抵达时间'] = info['routedate']
            elif info['title'] == "货物已清关":
                updates['清关放行时间'] = info['routedate']
            elif info['title'] == '货物已抵达转运中心':
                updates['当地提取时间'] = info['routedate']
            
        
            elif info['title'] == "货物已签收":
                updates['签收时间'] = info['routedate']
        
        return updates, is_ship
    def _update_zongdan_info(self, order, zongdan_data):
        """更新总单信息"""
        updates = {}
        if  not order.get('orderno'):
            logger.warning(f"订单 {order.get('A/S单号', '')} 没有订单号")
            return updates

        for zongdan in zongdan_data:
            if zongdan['orderno'] == order['orderno']:
                updates['航班号'] = zongdan['flightno']
                break
        return updates
    def _bulk_update(self, operations):
        """批量更新数据"""
        if operations:
            try:
                result = self.qipai_collection.bulk_write(operations)
                logger.info(f"批量更新成功: {result.modified_count} 条记录已更新")
                return True
            except Exception as e:
                logger.error(f"批量更新失败: {str(e)}")
                return False

    def process_data(self):
        """处理香港兰玛特数据的主方法"""
        try:
            # 获取大货订单数据
            logger.info("开始获取大货订单数据...")
            dahuo_data = self.morelink_client.dahuodingdan_all_data()
            logger.info(f"成功获取到 {len(dahuo_data)} 条大货订单数据")
            
            # 处理兰玛特数据
            data_ship = []
            data_air = []
            
            # 准备批量更新操作
            base_update_operations = []
            
            # 更新MongoDB基础数据
            logger.info("开始更新基础数据...")
            counter = 0
            for row in dahuo_data:
                if row["customername"] == "HKLMT-香港兰玛特-SZ" :
                    base_dict = self._create_base_dict(row)
                    operation = UpdateOne(
                        {"A/S单号": base_dict["A/S单号"]},
                        {"$set": base_dict},
                        upsert=True
                    )
                    base_update_operations.append(operation)
                    counter += 1
                    
                    # 每100条执行一次批量更新
                    if len(base_update_operations) >= 100:
                        if not self._bulk_update(base_update_operations):
                            # 如果更新失败，尝试重试
                            time.sleep(2)
                            self._bulk_update(base_update_operations)
                        base_update_operations = []
            
            logger.info(f"找到 {counter} 条兰玛特数据")
            
            # 更新剩余的基础数据
            if base_update_operations:
                self._bulk_update(base_update_operations)
            
            # 获取总单数据
            logger.info("开始获取总单数据...")
            zongdan_data = self.get_morelink_zongdan_data()
            # 更新路由信息
            logger.info("开始更新路由信息...")
            all_orders = list(self.qipai_collection.find({
                "客户名称": "HKLMT-香港兰玛特-SZ",
                "当前状态": {"$ne": "已签收"}
            }))
            logger.info(f"从数据库获取到 {len(all_orders)} 条兰玛特订单")
            
            route_update_operations = []
            updated_orders = []
            
            for idx, order in enumerate(all_orders):
                try:
                  
                    # 每处理100条数据打印一次进度
                    if idx > 0 and idx % 100 == 0:
                        logger.info(f"已处理 {idx}/{len(all_orders)} 条订单")
                    
                    route_info = self.get_morelink_routeinfo(order['sono'])
                    if not route_info:
                        continue
                        
                    updates, is_ship = self._update_route_info(order, route_info)
                    if not updates:
                        continue
                        
                    zongdan_updates = self._update_zongdan_info(order, zongdan_data)
                    if zongdan_updates:
                        updates.update(zongdan_updates)
                        
                    operation = UpdateOne(
                        {"A/S单号": order['A/S单号']},
                        {"$set": updates}
                    )
                    route_update_operations.append(operation)
                    order.update(updates)
                    updated_orders.append(order)
                    
                    # 每100条执行一次批量更新
                    if len(route_update_operations) >= 100:
                        if not self._bulk_update(route_update_operations):
                            # 如果更新失败，尝试重试
                            time.sleep(2)
                            self._bulk_update(route_update_operations)
                        route_update_operations = []
                        
                        # 分类处理后的订单
                        for updated_order in updated_orders:
                            if updated_order['A/S单号'].startswith('S'):
                                data_ship.append(updated_order)
                            else:
                                data_air.append(updated_order)
                        updated_orders = []
                except Exception as e:
                    logger.error(f"处理订单 {order.get('A/S单号', '')} 时出错: {traceback.format_exc()}")
                    continue
            
            # 更新剩余的路由信息
            if route_update_operations:
                self._bulk_update(route_update_operations)
                
                # 处理剩余的更新订单
                for updated_order in updated_orders:
                    if updated_order['A/S单号'].startswith('S'):
                        data_ship.append(updated_order)
                    else:
                        data_air.append(updated_order)
            
            logger.info(f"兰玛特数据处理完成，共 {len(data_ship)} 条海运数据，{len(data_air)} 条空运数据")
            return data_ship, data_air
        
        except Exception as e:
            logger.error(f"处理兰玛特数据时发生错误: {str(e)}")
            return [], []

def export_to_excel(data, filename):
    """将数据导出到Excel文件"""
    try:
        if not data:
            logger.warning(f"没有数据可导出到文件: {filename}")
            return False
            
        df = pd.DataFrame(data)
        # 删除MongoDB的_id字段
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
            
        df.to_excel(filename, index=False)
        logger.info(f"成功导出数据到文件: {filename}")
        return True
    except Exception as e:
        logger.error(f"导出Excel失败: {str(e)}")
        return False

def main():
    try:
        #处理七派数据
        # logger.info("开始处理七派数据...")
        # processor_qipai = QiPaiDataProcessor()
        # qipai_data_ship, qipai_data_air = processor_qipai.process_data()
        
        # # 导出七派数据到Excel
        # export_to_excel(qipai_data_ship, "七派海运数据.xlsx")
        # export_to_excel(qipai_data_air, "七派空运数据.xlsx")
        
        # 处理兰玛特数据
        logger.info("开始处理兰玛特数据...")
        processor_hklmt = HKLMTDataProcessor()
        hklmt_data_ship, hklmt_data_air = processor_hklmt.process_data()
        
        # # 导出兰玛特数据到Excel
        # export_to_excel(hklmt_data_ship, "兰玛特海运数据.xlsx")
        # export_to_excel(hklmt_data_air, "兰玛特空运数据.xlsx")
        
        logger.info("所有数据处理完成")
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")

def morelink_get_operNo():
    """获取MoreLink的operNo"""
    load_dotenv()
    mysql_db = MysqlDB(
        ip = os.getenv("MYSQL_HOST"), 
        port = int(os.getenv("MYSQL_PORT")),
        db = "fbatms",
        user_name = os.getenv("MYSQL_USER"),
        user_pass = os.getenv("MYSQL_PASS")
    )
    sql = "select sono from tb_fbatracking where operNo is null"
    operNo = mysql_db.find(sql,to_json=True)
    so_str = ",".join([item['sono'] for item in operNo if not item['sono'].startswith("A")])
    morelink_client = MoreLinkClient()

    operNo = morelink_client.dahuodingdan_worknum_search_httpx(so_str,"SO")
    all_data = [{'operNo': item['operNo'],'sono': item['sono']} for item in operNo]
    if all_data:
    # 批量更新SQL
        update_sql = """
            UPDATE tb_fbatracking 
            SET operNo = CASE sono 
            {}
            END
            WHERE sono IN ({})
        """.format(
            '\n'.join([f"WHEN '{item['sono']}' THEN '{item['operNo']}'" for item in all_data]),
            ','.join([f"'{item['sono']}'" for item in all_data])
        )
        mysql_db.update(update_sql)
    
    
    return all_data
if __name__ == "__main__":
    main()
    # morelink_get_operNo()
