import os
import time
import uuid
import pandas as pd
from datetime import datetime
from loguru import logger
import httpx
from pymongo import MongoClient
from dotenv import load_dotenv
import random

from app.utils import fedex_process_excel_with_zip_codes, ups_process_excel_with_zip_codes
from rpa_tools.email_tools import read_email_by_subject, send_email
logger.add("logs/address_get.log")

class AddressProcessor:
    def __init__(self):
        load_dotenv()
        # MongoDB 连接配置
        self.email_sender = None
        self.MONGO_CONFIG = {
            'host': os.getenv("MONGO_HOST"),
            'port': int(os.getenv("MONGO_PORT")),  # pyright: ignore[reportArgumentType]
            'username': os.getenv("MONGO_USER"),
            'password': os.getenv("MONGO_PASS"),
            'database': os.getenv("MONGO_DB")
        }
        uri = f"mongodb://{self.MONGO_CONFIG['username']}:{self.MONGO_CONFIG['password']}@{self.MONGO_CONFIG['host']}:{self.MONGO_CONFIG['port']}"
        self.mongo_client = MongoClient(uri)
        self.db = self.mongo_client[self.MONGO_CONFIG['database']]

        # 分开存储地址缓存和业务数据
        self.address_cache_collection = self.db['address_cache']  # 存储从Smarty API获取的地址信息缓存
        self.customer_info_collection = self.db['customer_info']  # 存储客户的业务数据（remote_info, address_info, price等）

        # 定义 property 中文映射
        self.property_chinese_mapping = {
            "FEDEX": {
                "Contiguous U.S.": "普通偏远",
                "Contiguous U.S.: Extended": "超偏远", 
                "Contiguous U.S.: Remote": "超级偏远",
                "Alaska": "阿拉斯加偏远",
                "Hawaii": "夏威夷偏远",
                "Intra-Hawaii": "夏威夷内部偏远",
            },
            "UPS": {
                "US 48 Zip": "普通偏远",
                "US 48 Zip DAS Extended": "超偏远",
                "Remote HI Zip": "夏威夷偏远", 
                "Remote AK Zip": "阿拉斯加偏远",
                "Remote US 48 Zip": "超级偏远",
            },
        }

    def save_and_rename_excel(self, excel_path: str) -> str:
        """
        保存并重命名Excel文件
        
        Args:
            excel_path: 原始Excel文件路径
            
        Returns:
            str: 新的Excel文件路径
        """
        # 生成唯一文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        file_dir = os.path.dirname(excel_path)
        file_name = os.path.basename(excel_path)
        name_without_ext = os.path.splitext(file_name)[0]
        new_file_name = f"{name_without_ext}_{timestamp}_{unique_id}.xlsx"
        new_file_path = os.path.join(file_dir, new_file_name)
        
        # 读取并保存Excel文件
        df = pd.read_excel(excel_path)
        df.to_excel(new_file_path, index=False)
        
        return new_file_path

    def process_remote_info(self, excel_path: str) -> dict:
        """
        批量处理偏远信息
        
        Args:
            excel_path: Excel文件路径
            
        Returns:
            dict: 客户ID对应的偏远信息
        """
        # 读取Excel文件
        df = pd.read_excel(excel_path)
        
        # 获取收件人邮编和客户ID列
        df['收件人邮编'] = df['收件人邮编'].astype(str)
        df['客户'] = df['客户'].astype(str)
        
        pdf_path = os.path.join(
            os.getcwd(),
            "file", 
            "remoteaddresscheck",
            "DAS_Contiguous_Extended_Remote_Alaska_Hawaii_2025.pdf",
        )

        # 检查PDF文件是否存在
        if not os.path.exists(pdf_path):
            logger.error("未找到Delivery Area Surcharge.pdf文件")
            return {}

        # 存储每个客户的偏远信息
        remote_info = {}
        
        # 遍历每行数据处理偏远信息
        for _, row in df.iterrows():
            client_id = row['客户']
            zip_code = row['收件人邮编']
            
            fedex_result = fedex_process_excel_with_zip_codes(zip_code, pdf_path)
            ups_result = ups_process_excel_with_zip_codes(zip_code)
            
            remote_type = []
            
            # 处理 FedEx 结果
            for item in fedex_result:
                if item["property"] != "邮编错误,不足五位" and item["property"] != "Unknown":
                    carrier_type = item["type"].upper()
                    english_property = item["property"]
                    if carrier_type in self.property_chinese_mapping and english_property in self.property_chinese_mapping[carrier_type]:
                        remote_type.append(f'FedEx:{self.property_chinese_mapping[carrier_type][english_property]}')
                        
            # 处理 UPS 结果
            for item in ups_result:
                if item["property"] != "邮编错误,不足五位" and item["property"] != "Unknown":
                    carrier_type = item["type"].upper()
                    english_property = item["property"]
                    if carrier_type in self.property_chinese_mapping and english_property in self.property_chinese_mapping[carrier_type]:
                        remote_type.append(f'UPS:{self.property_chinese_mapping[carrier_type][english_property]}')
            
            remote_info[client_id] = ' | '.join(remote_type) if remote_type else '非偏远'

        return remote_info

    def get_address_from_cache(self, street: str, secondary: str, city: str, state: str, zipcode: str):
        """
        从缓存中获取地址信息
        
        Returns:
            str: 地址类型信息，如果不存在返回None
        """
        query = {
            'street': street,
            'secondary': secondary,
            'city': city,
            'state': state,
            'zipcode': zipcode
        }
        result = self.address_cache_collection.find_one(query)
        return result['address_type'] if result else None

    def save_address_to_cache(self, street: str, secondary: str, city: str, state: str, zipcode: str, address_type: str):
        """
        保存地址信息到缓存
        """
        doc = {
            'street': street,
            'secondary': secondary,
            'city': city,
            'state': state,
            'zipcode': zipcode,
            'address_type': address_type,
            'created_at': datetime.now()
        }
        
        # 使用地址字段作为唯一标识
        self.address_cache_collection.update_one(
            {
                'street': street,
                'secondary': secondary,
                'city': city,
                'state': state,
                'zipcode': zipcode
            },
            {'$set': doc},
            upsert=True
        )

    def get_proxy_list(self):
        """
        获取代理列表
        
        Returns:
            list: 代理列表，每个代理包含ip和port
        """
        url = "https://api.lumiproxy.com/web_v1/free-proxy/list"
        params = {
            "page_size": 60,
            "page": 1,
            "protocol": 1,
            "language": "zh-hans"
        }
        return [
            {
                "ip": "127.0.0.1",
                "port": "7897",
                "protocol": "http"
            }
        ]

        try:
            with httpx.Client() as client:
                response = client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data["code"] == 200 and data["data"]["list"]:
                    proxies = []
                    for proxy in data["data"]["list"]:
                        if proxy["status"] == 1:  # 只使用状态为1的代理
                            proxies.append({
                                "ip": proxy["ip"],
                                "port": proxy["port"],
                                "protocol": proxy["protocol"]
                            })
                    return proxies
                return []
        except Exception as e:
            logger.error(f"获取代理列表时发生错误: {str(e)}")
            return []

    def get_smarty_address(self, street: str, secondary: str, city: str, state: str, zipcode: str):
        """
        使用Smarty API查询地址信息，优先从缓存获取
        
        Returns:
            str: 地址类型 (Residential/Commercial)，如果出错则返回None
        """
        # 先从缓存中查询
        cached_result = self.get_address_from_cache(street, secondary, city, state, zipcode)
        if cached_result:
            return cached_result
            
        # 如果缓存中没有，则调用API
        url = "https://us-street.api.smarty.com/street-address"
        
        params = {
            "key": "21102174564513388",
            "agent": "smarty+(website:demo/single-address@latest)", 
            "match": "enhanced",
            "candidates": "5",
            "geocode": "true",
            "license": "us-rooftop-geocoding-cloud",
            "street": street,
            "secondary": secondary,
            "city": city,
            "state": state,
            "zipcode": zipcode,
            "isAutocompleteClosed": "true"
        }

        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd", 
            "accept-language": "zh-CN,zh;q=0.9",
            "origin": "https://www.smarty.com",
            "priority": "u=1, i",
            "referer": "https://www.smarty.com/",
            "sec-ch-ua": '"Chromium";v="136", "Brave";v="136", "Not.A/Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors", 
            "sec-fetch-site": "same-site",
            "sec-gpc": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        }

        # 获取代理列表
        proxies = self.get_proxy_list()
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                if not proxies:
                    # 如果没有可用代理，直接请求
                    with httpx.Client() as client:
                        response = client.get(url, params=params, headers=headers)
                        response.raise_for_status()
                        json_data = response.json()
                else:
                    # 随机选择一个代理
                    proxy = random.choice(proxies)
                    proxy_url = f"http://{proxy['ip']}:{proxy['port']}"

                    with httpx.Client(proxy=proxy_url,timeout=10) as client:
                        response = client.get(url, params=params, headers=headers)
                        response.raise_for_status()
                        json_data = response.json()
                
                if json_data and len(json_data) > 0:
                    address_type = json_data[0]['metadata'].get('rdi')
                    if not address_type:
                        address_type = "未查询到rdi"
                    
                    self.save_address_to_cache(street, secondary, city, state, zipcode, address_type)
                    return address_type
                return None
                
            except Exception as e:
                logger.error(f"使用代理请求时发生错误: {str(e)}")
                retry_count += 1
                if proxies:
                    # 如果使用代理失败，从列表中移除该代理
                    proxies.remove(proxy)
                time.sleep(1)  # 请求失败后等待1秒再重试
                
        logger.error("所有重试都失败")
        return None

    def process_address_info(self, excel_path: str, remote_info: dict):
        """
        处理地址信息并保存到数据库
        
        Args:
            excel_path: Excel文件路径
            remote_info: 已处理的偏远信息字典（key为客户ID）
        """
        # 读取Excel文件
        df = pd.read_excel(excel_path)
        df.fillna(value="",inplace=True)
        excel_name = os.path.basename(excel_path)
        # 遍历每一行数据
        for _, row in df.iterrows():
            client_id = str(row['客户'])
            street = str(row['收件人地址1'])
            secondary = str(row['收件人地址2'])
            city = str(row['收件人城市'])
            state = str(row['收件人省洲'])
            zipcode = str(row['收件人邮编'])
            
            # 获取偏远信息
            remote_type = remote_info.get(client_id, '未知')
            
            # 准备文档数据
            doc = {
                'client_id': client_id,
                'street': street,
                'secondary': secondary,
                'city': city,
                'state': state,
                'zipcode': zipcode,
                'remote_info': remote_type,
                'address_info': None,  # 初始为None，后续更新
                'price': None,  # 初始为None，后续更新
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                'excel_name': excel_name,
                'email_sender': self.email_sender if self.email_sender else "yu.luo@hubs-scs.com",
                'status': 0
            }
            
            # 使用client_id作为唯一标识，如果存在则更新，不存在则插入
            self.customer_info_collection.update_one(
                {'client_id': client_id},
                {'$set': doc},
                upsert=True
            )

    def update_address_info(self, client_id: str, address_info: str):
        """
        更新地址信息
        """
        self.customer_info_collection.update_one(
            {'client_id': client_id,'status':0},
            {
                '$set': {
                    'address_info': address_info,
                    'updated_at': datetime.now()
                }
            }
        )

    def update_price(self, client_id: str):
        """
        更新价格信息
        """
        # 获取文档
        doc = self.customer_info_collection.find_one({'client_id': client_id,'status':0})
        if not doc:
            return
        
        address_type = doc.get('address_info')
        remote_info = doc.get('remote_info', '')
        
        if not address_type or not remote_info:
            logger.warning(f"客户 {client_id} 缺少地址类型或偏远信息，无法更新价格")
            return
            
        price = None
        
        # Residential地址处理
        if address_type == 'Residential':
            if '非偏远' in remote_info:
                price = 'CNY40/箱'
            elif '普通偏远' in remote_info:
                price = 'CNY80/箱'
            elif '超偏远' in remote_info:
                price = 'CNY90/箱'
            elif remote_info and '非偏远' not in remote_info and '普通偏远' not in remote_info and '超偏远' not in remote_info:
                price = 'CNY130/箱'
                
        # Commercial地址处理
        elif address_type == 'Commercial':
            if '普通偏远' in remote_info:
                price = 'CNY25/箱'
            elif '超偏远' in remote_info:
                price = 'CNY30/箱'
        
        # 更新价格和状态
        update_data = {
            'updated_at': datetime.now(),
            'status': 1  # 更新状态为已处理
        }
        
        if price is not None:
            update_data['price'] = price
            
        self.customer_info_collection.update_one(
            {'client_id': client_id,'status':0},
            {'$set': update_data}
        )

    def export_to_excel(self, output_path: str = None) -> str:
        """
        将数据库中的信息导出到Excel
        """
        # 从数据库获取所有记录
        records = list(self.customer_info_collection.find({}))
        
        # 转换为DataFrame
        df = pd.DataFrame(records)
        
        # 重命名列（使用中文列名）
        column_mapping = {
            'client_id': '客户',
            'street': '收件人地址1',
            'secondary': '收件人地址2',
            'city': '收件人城市',
            'state': '收件人省洲',
            'zipcode': '收件人邮编',
            'remote_info': '偏远信息',
            'address_info': '地址信息',
            'price': '价格'
        }
        
        # 选择需要的列并重命名
        df = df[list(column_mapping.keys())].rename(columns=column_mapping)
        
        # 如果没有指定输出路径，则生成一个
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = os.path.join(os.getcwd(), f'地址信息_{timestamp}.xlsx')
        
        # 保存到Excel
        df.to_excel(output_path, index=False)
        logger.info(f"数据已导出到: {output_path}")
        
        return output_path

    def retry_failed_addresses(self):
        """
        重新获取失败的地址信息，并发送邮件给相关用户
        """
        # 查找地址信息为None的记录
        failed_records = list(self.customer_info_collection.find({'status': 0}))
        
        if not failed_records:
            logger.info("没有需要重试的记录")
            return 0, 0
        
        success_count = 0
        fail_count = 0
        
        # 记录处理过的excel文件名
        processed_files = set()
        
        # 按excel_name分组处理记录
        excel_records = {}
        for record in failed_records:
            excel_name = record.get('excel_name')
            if excel_name:
                processed_files.add(excel_name)
                if excel_name not in excel_records:
                    excel_records[excel_name] = []
                excel_records[excel_name].append(record)
        
        # 对每个excel文件分别处理
        for excel_name, records in excel_records.items():
            try:
                # 使用process_smarty_address处理该excel文件的所有记录
                self.process_smarty_address(excel_name)
                
                # 统计处理结果
                processed_records = list(self.customer_info_collection.find({
                    'excel_name': excel_name,
                    'status': 1  # 已成功处理的记录
                }))
                success_count += len(processed_records)
                fail_count += len(records) - len(processed_records)
                
            except Exception as e:
                logger.error(f"处理{excel_name}的地址信息时发生错误: {str(e)}")
                fail_count += len(records)
                continue
        
        # 如果有成功处理的记录，则为每个excel文件发送更新后的结果
        if success_count > 0:
            for excel_name in processed_files:
                # 获取相同excel文件名的所有记录
                records = list(self.customer_info_collection.find({'excel_name': excel_name}))
                if not records:
                    continue
                    
                # 获取email_sender
                email_sender = records[0].get('email_sender')
                if not email_sender:
                    logger.warning(f"未找到{excel_name}对应的email_sender")
                    continue
                
                # 使用新的export_to_excel_by_name函数导出结果
                output_path = self.export_to_excel_by_name(excel_name)
                if not output_path:
                    logger.warning(f"导出{excel_name}的结果失败")
                    continue
                
                try:
                    # 发送邮件
                    send_email(
                        receiver_email=email_sender,
                        subject="私人仓地址信息重试处理结果",
                        body=f'已完成地址信息重新处理\n成功: {success_count}条\n失败: {fail_count}条',
                        attachments=[output_path]
                    )
                    logger.info(f"已发送重试结果给 {email_sender}")
                except Exception as e:
                    logger.error(f"发送邮件给 {email_sender} 时出错: {str(e)}")
                
        
        return success_count, fail_count

    def process_excel_file(self, excel_path: str):
        """
        处理Excel文件的主函数
        
        Args:
            excel_path: Excel文件路径
        """
        try:
            # 1. 读取原始Excel文件
            logger.info("开始读取Excel文件...")
            df = pd.read_excel(excel_path)
            df['收件人地址1'] = df['收件人地址1'].fillna("")
            df['收件人地址2'] = df['收件人地址2'].fillna("")
            df['收件人城市'] = df['收件人城市'].fillna("")
            df['收件人省洲'] = df['收件人省洲'].fillna("")
            df['收件人邮编'] = df['收件人邮编'].fillna("")

            # 2. 批量获取偏远信息
            logger.info("开始获取偏远信息...")
            remote_info = self.process_remote_info(excel_path)
            
            # 3. 将数据保存到数据库
            logger.info("开始保存数据到数据库...")
            excel_name = os.path.basename(excel_path)
            
            for _, row in df.iterrows():
                client_id = str(row['客户'])
                street = str(row['收件人地址1'])
                secondary = str(row['收件人地址2'])
                city = str(row['收件人城市'])
                state = str(row['收件人省洲'])
                zipcode = str(row['收件人邮编'])
                
                # 获取偏远信息
                remote_type = remote_info.get(client_id, '未知')
                
                # 准备文档数据
                doc = {
                    'client_id': client_id,
                    'street': street,
                    'secondary': secondary,
                    'city': city,
                    'state': state,
                    'zipcode': zipcode,
                    'remote_info': remote_type,
                    'address_info': None,
                    'price': None,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now(),
                    'excel_name': excel_name,
                    'email_sender': self.email_sender if self.email_sender else "yu.luo@hubs-scs.com",
                    'status': 0  # 初始状态为0
                }
                
                # 使用client_id和excel_name作为唯一标识，如果存在则更新，不存在则插入
                self.customer_info_collection.update_one(
                    {
                        'client_id': client_id,
                        'excel_name': excel_name
                    },
                    {'$set': doc},
                    upsert=True
                )
                
        except Exception as e:
            logger.error(f"处理Excel文件时发生错误: {str(e)}")
            raise

    def process_smarty_address(self, excel_name: str):
        """
        处理数据库中status为0的记录，获取Smarty地址信息
        
        Args:
            excel_name: Excel文件名
        """
        try:
            # 获取指定excel文件中status为0的记录
            records = self.customer_info_collection.find({
                'excel_name': excel_name,
                'status': 0
            })
            
            for record in records:
                client_id = record['client_id']
                address_info = self.get_smarty_address(
                    record['street'],
                    record['secondary'],
                    record['city'],
                    record['state'],
                    record['zipcode']
                )
                
                if address_info:
                    # 更新地址信息和状态
                    self.update_address_info(client_id, address_info)
                    self.update_price(client_id)
                    
                    # 更新状态为1
                    self.customer_info_collection.update_one(
                        {
                            'client_id': client_id,
                            'excel_name': excel_name,
                            "status":0
                        },
                        {
                            '$set': {
                                'status': 1,
                                'updated_at': datetime.now()
                            }
                        }
                    )
                    logger.info(f"成功更新客户 {client_id} 的地址信息")
                else:
                    logger.warning(f"未能获取客户 {client_id} 的地址信息")
                
                time.sleep(2)  # 添加延迟避免请求过快
                
        except Exception as e:
            logger.error(f"处理Smarty地址信息时发生错误: {str(e)}")
            raise

    def export_to_excel_by_name(self, excel_name: str, output_path: str = None) -> str:
        """
        将指定excel_name的数据导出到Excel，并将新信息添加到原始Excel文件中
        
        Args:
            excel_name: Excel文件名
            output_path: 输出文件路径
            
        Returns:
            str: 输出文件路径
        """
        # 检查是否所有记录都已处理完成
        unprocessed = self.customer_info_collection.count_documents({
            'excel_name': excel_name,
            'status': 0
        })
        
        if unprocessed > 0:
            logger.warning(f"还有 {unprocessed} 条记录未处理完成")
            return None
            
        # 查找原始Excel文件
        original_excel_path = os.path.join(os.getcwd(), 'file', 'address_info', excel_name)
        if not os.path.exists(original_excel_path):
            logger.warning(f"未找到原始Excel文件: {original_excel_path}")
            return None
            
        try:
            # 读取原始Excel文件
            original_df = pd.read_excel(original_excel_path)
            
            # 从数据库获取处理后的记录
            records = list(self.customer_info_collection.find({'excel_name': excel_name}))
            
            # 创建新的列
            original_df['偏远信息'] = ''
            original_df['地址信息'] = ''
            original_df['价格'] = ''
            
            # 创建客户ID到记录的映射
            records_map = {str(record['client_id']): record for record in records}
            
            # 更新每一行的信息
            for idx, row in original_df.iterrows():
                client_id = str(row['客户'])
                if client_id in records_map:
                    record = records_map[client_id]
                    original_df.at[idx, '偏远信息'] = record.get('remote_info', '')
                    original_df.at[idx, '地址信息'] = record.get('address_info', '')
                    original_df.at[idx, '价格'] = record.get('price', '')
            
            # 如果没有指定输出路径，则生成一个
            if output_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = os.path.join(os.getcwd(), f'地址信息_{timestamp}.xlsx')
            
            # 保存到Excel
            original_df.to_excel(output_path, index=False)
            logger.info(f"数据已导出到: {output_path}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"导出Excel时发生错误: {str(e)}")
            return None

def main():
    processor = AddressProcessor()
    
    # 1. 先检查是否有新邮件
    email_data = read_email_by_subject(subject_input="私人仓地址信息获取",seen=True,email_num=3)
    
    # 2. 如果有新邮件，处理新邮件
    if email_data:
        # 确保保存路径存在
        save_dir = os.path.join(os.getcwd(), 'file', 'address_info')
        os.makedirs(save_dir, exist_ok=True)
        
        for email in email_data:
            try:
                attachments = email.get('attachments')
                if not attachments:
                    logger.warning("邮件中没有附件")
                    continue
                    
                email_sender = email.get('from')
                processor.email_sender = email_sender
                
                # 生成唯一的文件名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                unique_id = str(uuid.uuid4())[:8]
                excel_name = f'{email_sender}-{timestamp}-{unique_id}.xlsx'
                save_path = os.path.join(save_dir, excel_name)
                
                # 保存附件到本地
                with open(save_path, 'wb') as f:
                    f.write(attachments[0]['content'])
                logger.info(f"附件已保存到 {save_path}")
                
                # 1. 处理Excel文件并保存到数据库
                processor.process_excel_file(save_path)
                logger.info("Excel数据已保存到数据库")
                
                # 2. 处理Smarty地址信息
                processor.process_smarty_address(excel_name)
                logger.info("Smarty地址信息处理完成")
                
                # 3. 导出处理后的数据到Excel
                output_path = processor.export_to_excel_by_name(excel_name)
                if output_path:
                    # 4. 发送处理后的文件
                    send_email(
                        receiver_email=email_sender,
                        subject="私人仓地址信息获取结果",
                        body='已成功处理地址信息',
                        attachments=[output_path]
                    )
                    logger.info(f"已发送处理结果给 {email_sender}")
                    
                 
                else:
                    logger.warning(f"还有未处理完的记录，暂不发送邮件给 {email_sender}")
                
            except Exception as e:
                logger.exception(f"处理来自 {email_sender} 的邮件时出错: {e}")
                send_email(
                    receiver_email=email_sender,
                    subject="私人仓地址信息获取结果",
                    body='处理地址信息时发生错误'
                )
    
    # 3. 无论是否有新邮件，都检查并处理未完成的数据
    time.sleep(60)
    logger.info("检查未完成的数据...")
    success_count, fail_count = processor.retry_failed_addresses()
    if success_count > 0 or fail_count > 0:
        logger.info(f"处理未完成数据结果：成功 {success_count} 条，失败 {fail_count} 条")
    else:
        logger.info("没有未完成的数据需要处理")

def test():
    processor = AddressProcessor()
    # excel_path = r"C:\Users\a1337\Desktop\5.14私人仓地址(1).xlsx"
    # processor.process_excel_file(excel_path)
    processor.retry_failed_addresses()
if __name__ == "__main__":
    # test()
    import schedule
    
    schedule.every(60).seconds.do(main)

    while True:
        schedule.run_pending()
        time.sleep(10)  # 等待1秒以避免CPU占用过高
   
    

