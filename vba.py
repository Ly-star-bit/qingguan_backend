from datetime import date, datetime, timedelta
import os
import random
import sys
import traceback
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import openpyxl
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import func
from sqlmodel import SQLModel, Field, create_engine, Session, select
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import OperationalError
from tenacity import retry, stop_after_attempt, wait_fixed
from sqlalchemy.pool import QueuePool
from loguru import logger
from dotenv import load_dotenv
load_dotenv()
# 配置日志文件保存
logger.add("app.log",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           level="INFO",
           rotation="1 MB",
           retention="10 days",
           compression="zip")

# 配置 loguru 输出到控制台
# 定义你的数据库连接配置
DATABASE_CONFIG = {
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASS"),
    'host': os.getenv("MYSQL_HOST"),
    'database': os.getenv("MYSQL_DB"),
    "port": int(os.getenv("MYSQL_PORT"))
}

# 创建数据库连接字符串
DATABASE_URL = f"mysql+mysqlconnector://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"
engine = create_engine(
    DATABASE_URL,
    pool_size=10,           # 连接池大小
    max_overflow=20,        # 连接池溢出大小
    pool_timeout=30,        # 连接池超时时间
    pool_recycle=1800,      # 连接池回收时间
    poolclass=QueuePool,     # 使用QueuePool连接池
    pool_pre_ping=True,  # 新增

)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def get_session():
    try:
        return Session(engine)
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        raise

# 定义product表的数据模型，添加主键字段
class Product(SQLModel, table=True):
    __tablename__ = 'product'
    id: Optional[int] = Field(default=None, primary_key=True)
    名称: Optional[str] = None
    DESCRIPTION: Optional[str] = None
    HS_CODE: Optional[str] = None
    PCS_CTN: Optional[float] = None
    UNIT_PRICE: Optional[float] = None
    TEXTURE: Optional[str] = None
    Usage: Optional[str] = None
    Unnamed_7: Optional[str] = None
    Unnamed_8: Optional[str] = None
    Unnamed_9: Optional[str] = None
    Duty_percent: Optional[float] = None
    加征关税比例: Optional[str] = None
    Unnamed_12: Optional[str] = None
    Unnamed_13: Optional[date] = None  # Ensure this is a date type
    商品类型: Optional[str] = None
class Product2(SQLModel, table=True):
    __tablename__ = 'product2'
    id: Optional[int] = Field(default=None, primary_key=True)
    中文品名: Optional[str] = Field(default=None, max_length=255)
    英文品名: Optional[str] = Field(default=None, max_length=255)
    HS_CODE: Optional[str] = Field(default=None, max_length=255)
    PCS_CTN: Optional[str] = Field(default=None, max_length=50)
    UNIT_PRICE: Optional[str] = Field(default=None, max_length=50)
    TEXTURE: Optional[str] = Field(default=None, max_length=255)
    Usage: Optional[str] = Field(default=None, max_length=255)
    属性: Optional[str] = Field(default=None, max_length=255)
    豁免代码: Optional[str] = Field(default=None, max_length=255)
    FDA: Optional[str] = Field(default=None, max_length=255)
    一箱税金: Optional[str] = Field(default=None, max_length=255)
    Duty: Optional[str] = Field(default=None, max_length=50)
    加征: Optional[str] = Field(default=None, max_length=50)
    豁免截止: Optional[str] = Field(default=None, max_length=255)
    豁免过期后: Optional[str] = Field(default=None, max_length=255)
    更新时间: Optional[str] = Field(default=None, max_length=255)
    FZ: Optional[str] = Field(default=None, max_length=255)
    FZ1: Optional[str] = Field(default=None, max_length=255)
# 定义shippersandreceivers表的数据模型，添加主键字段
class ShippersAndReceivers(SQLModel, table=True):
    __tablename__ = 'shippersandreceivers'
    id: Optional[int] = Field(default=None, primary_key=True)
    ShipperName: Optional[str] = None
    ShipperAddress: Optional[str] = None
    ReceiverName: Optional[str] = None
    ReceiverAddress: Optional[str] = None
    Attribute: Optional[str] = None
    ChineseName: Optional[str] = None
    EnglishName: Optional[str] = None
    Address: Optional[str] = None

    __table_args__ = {'comment': 'Shippers and Receivers table'}

# 定义FastAPI应用
app = FastAPI()
# 设置CORS
origins = [
    "http://localhost",
    "http://192.168.20.143:8088",
    "http://localhost:3000"
    # 你可以在这里添加其他允许的源
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 定义请求体数据模型
class ProductData(BaseModel):
    product_name: str
    box_num: int

class ShippingRequest(BaseModel):
    shipper_name: str
    receiver_name: str
    master_bill_no: str
    gross_weight: int
    volume: int
    product_list: List[ProductData]

# 定义处理数据的函数
def process_shipping_data(shipper_name: str, receiver_name: str, master_bill_no: str, gross_weight: int, volume: int, product_list: List[ProductData]):
    # 创建引擎
    with get_session() as session:
        num_products = len(product_list)
        if num_products == 0:
            raise ValueError("Product list cannot be empty")



        avg_gross_weight = gross_weight / num_products
        avg_volume = volume / num_products

        results = []
        # 查询发货人地址
        shipper_query = select(ShippersAndReceivers).where(ShippersAndReceivers.ShipperName == shipper_name)
        shipper_record = session.exec(shipper_query).first()

        # 查询收件人地址
        receiver_query = select(ShippersAndReceivers).where(ShippersAndReceivers.ReceiverName == receiver_name)
        receiver_record = session.exec(receiver_query).first()

        if not shipper_record:
            raise ValueError(f"Shipper '{shipper_name}' not found in database")
        if not receiver_record:
            raise ValueError(f"Receiver '{receiver_name}' not found in database")

        shipper_address = shipper_record.ShipperAddress
        receiver_address = receiver_record.ReceiverAddress


        for product in product_list:
            product_name = product.product_name
            box_num = product.box_num

            product_query = select(Product).where(Product.名称 == product_name)
            product_record = session.exec(product_query).first()

            if not product_record:
                raise ValueError(f"Product '{product_name}' not found in database")
            address = None
                    #获取商品类型
            product_type = session.exec(select(Product.商品类型).where(Product.HS_CODE == product_record.HS_CODE)).first()
    
            address_name_list = session.exec(
                        select(ShippersAndReceivers.Address, ShippersAndReceivers.EnglishName)
                        .where(
                            (ShippersAndReceivers.Address != None) & 
                            (ShippersAndReceivers.Attribute == product_type)
                        )
                    ).all() 
      
               
            random_address_name = random.choice(address_name_list)
            address = random_address_name[0]
            address_name = random_address_name[1]
                # address_name,address_list = session.exec(select(ShippersAndReceivers.Address).where(ShippersAndReceivers.Address != None)).all()
            product_data = {
                'MasterBillNo': master_bill_no,
                "shipper_name": shipper_name,
                "shipper_address":shipper_address,
                "receiver_address": receiver_address,
                "receiver_name": receiver_name,
                'ProductName': product_name,
                'carton': box_num,
                'quanity': box_num * product_record.PCS_CTN,
                "danwei": "PCS" if product_record.HS_CODE else '',
                "unit_price": product_record.UNIT_PRICE,
                'total_price': product_record.UNIT_PRICE * box_num * product_record.PCS_CTN,
                'HS_CODE': product_record.HS_CODE,
                'DESCRIPTION': product_record.DESCRIPTION,
                'GrossWeight': avg_gross_weight,
                'net_weight': avg_gross_weight * 0.8,
                'Volume': avg_volume,
                "usage": product_record.Usage,
                "texture": product_record.TEXTURE,
                "address_name": address_name or "",
                "address": address or ""
            }

            results.append(product_data)

        return results

def generate_excel_from_template(data):
    template_path = r"清关发票箱单模板.xlsx"
    # 读取模板文件
    wb = openpyxl.load_workbook(template_path)
    civ_sheet = wb["CIV"]
    pl_sheet = wb["PL"]
    start_row = 13
    def set_cell_value(sheet, row, column, value):
        cell = sheet.cell(row=row, column=column)
        # 检查是否为合并单元格，如果是，只在左上角单元格写入值
        if cell.coordinate in sheet.merged_cells:
            for merged_range in sheet.merged_cells.ranges:
                if cell.coordinate in merged_range:
                    top_left_cell = merged_range.start_cell
                    sheet[top_left_cell.coordinate].value = value
                    break
        else:
            cell.value = value
    #填充civ一次内容
    set_cell_value(civ_sheet,1,1,data[0]["shipper_name"])
    set_cell_value(civ_sheet,2,1,data[0]["shipper_address"])
    set_cell_value(civ_sheet,5,1,f"{data[0]['receiver_name']}\n{data[0]['receiver_address']}")

    today_minus_5 = datetime.now() - timedelta(days=5)
    formatted_date = today_minus_5.strftime("%Y%m%d")
    random_number = random.randint(1000, 9999)
    result_1 = f"{formatted_date}{random_number}"
    set_cell_value(civ_sheet,5,8,result_1)
    set_cell_value(civ_sheet,6,8,result_1)
    set_cell_value(civ_sheet,7,8,datetime.now().strftime("%Y/%m/%d"))

        
    
    set_cell_value(pl_sheet,1,1,data[0]["shipper_name"])
    set_cell_value(pl_sheet,2,1,data[0]["shipper_address"])
    set_cell_value(pl_sheet,5,1,f"{data[0]['receiver_name']}\n{data[0]['receiver_address']}")

    for index, item in enumerate(data):
        row = start_row + index
        
        # 填充CIV
        set_cell_value(civ_sheet, row, 1, item["HS_CODE"])
        set_cell_value(civ_sheet, row, 2, item["DESCRIPTION"])
        set_cell_value(civ_sheet, row, 3, item["quanity"])
        set_cell_value(civ_sheet, row, 4, item["danwei"])
        set_cell_value(civ_sheet, row, 5, item["unit_price"])
        set_cell_value(civ_sheet, row, 6, item["total_price"])
        set_cell_value(civ_sheet, row, 7, item["texture"])
        set_cell_value(civ_sheet, row, 8, item["address_name"])

        set_cell_value(civ_sheet, row, 9, item["address"])
        
        # 填充PL
        set_cell_value(pl_sheet, row, 2, item["DESCRIPTION"])
        set_cell_value(pl_sheet, row, 3, item["quanity"])
        set_cell_value(pl_sheet, row, 4, item["danwei"])
        set_cell_value(pl_sheet, row, 5, item["carton"])
        set_cell_value(pl_sheet, row, 7, item["net_weight"])
        set_cell_value(pl_sheet, row, 8, item["GrossWeight"])
        set_cell_value(pl_sheet, row, 9, item["Volume"])

    # 保存新的Excel文件
    output_path = f"file/{data[0]['MasterBillNo']}.xlsx"
    wb.save(output_path)
    logger.info(f"文件已成功生成: {output_path}")
    return output_path
# 定义FastAPI接口
@app.post("/process-shipping-data")
async def process_shipping_data_endpoint(request: ShippingRequest):
    try:
        results = process_shipping_data(
            shipper_name=request.shipper_name,
            receiver_name=request.receiver_name,
            master_bill_no=request.master_bill_no,
            gross_weight=request.gross_weight,
            volume=request.volume,
            product_list=request.product_list
        )
        excel_path = generate_excel_from_template(results)
        return FileResponse(path=excel_path, filename=f"{request.master_bill_no}.xlsx")
    except ValueError as e:
        logger.error(f"Value Error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Value Error: {str(e)}")
    except Exception as e:
        logger.error(f"Internal Server Error: {str(e)}---{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get("/products/", response_model=dict)
def read_products(skip: int = 0, limit: int = 10, 名称: Optional[str] = None,get_all:bool=False):
    with get_session() as session:
        query = select(Product)
        if 名称:
            query = query.where(Product.名称.contains(名称))
        total = session.exec(select(func.count()).select_from(query.subquery())).one()
        if get_all:
            products = session.exec(query).all()
        else:
            products = session.exec(query.offset(skip).limit(limit)).all()
        return {"items": products, "total": total}

@app.post("/products/", response_model=Product)
def create_product(product: Product):
    with get_session() as session:
        session.add(product)
        session.commit()
        session.refresh(product)
        return product

@app.put("/products/{product_id}", response_model=Product)
def update_product(product_id: int, product: Product):
    with get_session() as session:
        db_product = session.get(Product, product_id)
        if not db_product:
            raise HTTPException(status_code=404, detail="Product not found")
        for key, value in product.model_dump(exclude_unset=True).items():
            setattr(db_product, key, value)
        session.add(db_product)
        session.commit()
        session.refresh(db_product)
        return db_product

@app.delete("/products/{product_id}", response_model=Product)
def delete_product(product_id: int):
    with get_session() as session:
        db_product = session.get(Product, product_id)
        if not db_product:
            raise HTTPException(status_code=404, detail="Product not found")
        session.delete(db_product)
        session.commit()
        return db_product

@app.get("/shippersandreceivers/", response_model=dict)
def read_shippers_and_receivers(skip: int = 0, limit: int = 10, ShipperName: Optional[str] = None):
    with get_session() as session:
        query = select(ShippersAndReceivers)
        if ShipperName:
            query = query.where(ShippersAndReceivers.ShipperName.contains(ShipperName))
        total = session.exec(select(func.count()).select_from(query.subquery())).one()
        shippers_and_receivers = session.exec(query.offset(skip).limit(limit)).all()
        return {"items": shippers_and_receivers, "total": total}

@app.post("/shippersandreceivers/", response_model=ShippersAndReceivers)
def create_shipper_or_receiver(shipper_or_receiver: ShippersAndReceivers):
    with get_session() as session:
        session.add(shipper_or_receiver)
        session.commit()
        session.refresh(shipper_or_receiver)
        return shipper_or_receiver

@app.put("/shippersandreceivers/{id}", response_model=ShippersAndReceivers)
def update_shipper_or_receiver(id: int, shipper_or_receiver: ShippersAndReceivers):
    with get_session() as session:
        db_shipper_or_receiver = session.get(ShippersAndReceivers, id)
        if not db_shipper_or_receiver:
            raise HTTPException(status_code=404, detail="Shipper or Receiver not found")
        for key, value in shipper_or_receiver.model_dump(exclude_unset=True).items():
            setattr(db_shipper_or_receiver, key, value)
        session.add(db_shipper_or_receiver)
        session.commit()
        session.refresh(db_shipper_or_receiver)
        return db_shipper_or_receiver

@app.delete("/shippersandreceivers/{id}", response_model=ShippersAndReceivers)
def delete_shipper_or_receiver(id: int):
    with get_session() as session:
        db_shipper_or_receiver = session.get(ShippersAndReceivers, id)
        if not db_shipper_or_receiver:
            raise HTTPException(status_code=404, detail="Shipper or Receiver not found")
        session.delete(db_shipper_or_receiver)
        session.commit()
        return db_shipper_or_receiver
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app=app,host="0.0.0.0",port=9008)

