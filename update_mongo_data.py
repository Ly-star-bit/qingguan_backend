import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from bson import ObjectId

load_dotenv()

# MongoDB 连接配置
MONGO_CONFIG = {
    'host': os.getenv("MONGO_HOST"),
    'port': int(os.getenv("MONGO_PORT")),
    'username': os.getenv("MONGO_USER"),
    'password': os.getenv("MONGO_PASS"),
    'database': os.getenv("MONGO_DB")
}
uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
mongo_client = MongoClient(uri)
mongo_db = mongo_client[MONGO_CONFIG['database']]

# # 读取Excel文件并更新products集合中的装箱和单价数据
products_collection = mongo_db['products']
# 将所有包含 'country' 字段的文档中的 'country' 重命名为 'destination'
# 构建批量操作
bulk_operations = []

# 1. 处理 country 为 "China" 或 "Vietnam" 的文档
for doc in products_collection.find({"country": {"$in": ["China", "Vietnam"]}}):
    bulk_operations.append(
        UpdateOne(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "startland": doc["country"],      # 保留原值作为 startland
                    "destination": "America"
                },
                "$unset": {"country": ""}             # 删除原 country 字段
            }
        )
    )

# 2. 处理 country 为 "Canada" 的文档
for doc in products_collection.find({"country": "Canada"}):
    bulk_operations.append(
        UpdateOne(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "startland": "China",
                    "destination": "Canada"
                },
                "$unset": {"country": ""}
            }
        )
    )

# 执行批量更新
if bulk_operations:
    result = products_collection.bulk_write(bulk_operations)
    print(f"成功更新 {result.modified_count} 个文档")
else:
    print("没有符合条件的文档需要更新")
# 读取Excel文件
df = pd.read_excel(r'C:\Users\a1337\Desktop\301-更新-IT-海运.xlsx')  # 请替换为实际的Excel文件名
df.fillna(0,inplace=True)
# 遍历Excel数据并更新MongoDB文档
update_count = 0
for _, row in df.iterrows():
    try:
        # 通过ID查找文档
        product_id = row['id']  # 假设Excel中的ID列名为'id'
        
        # 首先获取当前文档
        doc = products_collection.find_one({"_id": ObjectId(product_id)})
        
        update_fields = {
                    "豁免截止日期说明": row["豁免截止日期说明"].strftime("%Y-%m-%d")
        }
        


        result = products_collection.update_one(
            {"_id": ObjectId(product_id)},
            {"$set": update_fields}
        )
        
        if result.modified_count > 0:
            update_count += 1
    except Exception as e:
        print(f"更新文档 {product_id} 时发生错误: {e}")

print(f"成功更新 {update_count} 个文档的装箱和单价数据")

def add_additional_tax_data():
    """
    向products集合中添加或更新加征数据。
    如果文档中不存在"加征_0409"字段，则添加"加征_0409": "0.1"；
    如果已存在，则更新为"0.1"
    """
    try:
        products_collection = mongo_db['products_sea']
        
        # 查询所有中国的文档
        query = {"country": "China"}
        documents_to_update = products_collection.find(query)
        
        update_count = 0
        for document in documents_to_update:
            # 如果加征字段不存在，创建一个空字典
            if "加征" not in document:

                result = products_collection.update_one(
                    {"_id": document["_id"]},
                    {"$set": {"加征": {"加征_0405": "0.1","加征_0409": "1.15"}}}
                )
            else:
                # 如果加征字段存在,更新加征_0409
                result = products_collection.update_one(
                    {"_id": document["_id"]},
                    {"$set": {"加征.加征_0405": "0.1","加征.加征_0409": "1.15"}}
                )
            
            if result.modified_count > 0:
                update_count += 1
        
        print(f"成功更新{update_count}个文档的加征_0409字段")
        
    except Exception as e:
        print(f"添加/更新加征数据时发生错误: {e}")


def update_estimated_tax_rate():
    """
    读取custom_clear_history_summary_collection，更新字段estimated_tax_rate_cny_per_kg。
    公式为：estimated_tax_amount/gross_weight_kg*7.3，结果保留两位小数。
    """
    try:
        custom_clear_history_summary_collection = mongo_db['custom_clear_history_summary']

        # 查询所有文档
        all_documents = custom_clear_history_summary_collection.find()

        for document in all_documents:
            estimated_tax_amount = document.get('estimated_tax_amount', 0)
            gross_weight_kg = document.get('gross_weight_kg', 1)  # 避免除以0
            
            # 计算 estimated_tax_rate_cny_per_kg
            if gross_weight_kg != 0:
                estimated_tax_rate_cny_per_kg = round(estimated_tax_amount / gross_weight_kg * 7.3, 2)
            else:
                continue
            # 更新文档
            result = custom_clear_history_summary_collection.update_one(
                {'_id': document['_id']},
                {'$set': {'estimated_tax_rate_cny_per_kg': estimated_tax_rate_cny_per_kg}}
            )

            if result.modified_count > 0:
                print(f"成功更新文档ID: {document['_id']}")
            else:
                print(f"文档ID: {document['_id']} 更新失败或未找到")

    except Exception as e:
        print(f"发生错误: {e}")
# MongoDB collection

def update_products_from_excel(excel_file):
    """
    从Excel表格读取数据，根据id更新MongoDB中products collection的中文品名、单价和件/箱。
    
    Args:
        excel_file (str): Excel文件路径。
    """
    try:
        df = pd.read_excel(excel_file, sheet_name="处理过")
        products_collection = mongo_db['products']

        # 确保Excel表格包含必要的列
        required_columns = ['id', '中文品名', '单价更新', '件/箱更新']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Excel文件缺少必要的列。需要列: {required_columns}")
        
        for index, row in df.iterrows():
            product_id = row['id']
            chinese_name = row['中文品名']
            price = row['单价更新']
            package_quantity = row['件/箱更新']
            
            # 构建更新文档
            update_data = {
                '$set': {
                    '中文品名': chinese_name,
                    '单价': price,
                    '件箱': package_quantity
                }
            }
            
            # 使用id更新MongoDB文档
            result = products_collection.update_one({'_id': ObjectId(product_id)}, update_data)
            
            if result.modified_count > 0:
                print(f"成功更新产品ID: {product_id}")
            else:
                print(f"未找到产品ID: {product_id} 或更新失败")
    
    except FileNotFoundError:
        print(f"文件未找到: {excel_file}")
    except ValueError as e:
        print(f"数据错误: {e}")
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == '__main__':
    # excel_file_path = r'C:\Users\a1337\Desktop\美国HS-导出-250211(1).xlsx'  # 替换为你的Excel文件路径
    # update_products_from_excel(excel_file_path)
    # update_estimated_tax_rate()
    # add_additional_tax_data()
    pass
