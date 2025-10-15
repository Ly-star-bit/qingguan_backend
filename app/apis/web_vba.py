from io import BytesIO
import json
import os
from pathlib import Path
import random
import traceback
from datetime import datetime, timedelta
from typing import List, Optional
from uuid import UUID
import uuid

import bcrypt
import casbin
import httpx
import requests
from casbin_sqlalchemy_adapter import Adapter
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from loguru import logger
from sqlalchemy import QueuePool, func
from sqlalchemy.exc import InvalidRequestError
from sqlmodel import Session, create_engine, or_, select
from starlette.middleware.base import BaseHTTPMiddleware
from tenacity import retry, stop_after_attempt, wait_fixed
from openpyxl import Workbook as Openpyxl_Workbook

from app.db import engine, get_session, pool_engine
from app.dadan.models import (
    ConsigneeData,
    CustomClearHistoryDetailLog,
    CustomClearHistorySummaryLog,
    Dalei,
    FactoryData,
    HaiYunZiShui,
    IpWhiteList,
    Port,
    Product3,
    ShipmentLog,
    ShippersAndReceivers,
    User,
)
from app.schemas import (
    DaleiCreate,
    FileInfo,
    Group,
    Policy,
    ProductData,
    ShippingRequest,
    SummaryResponse,
    UpdatePolicy,
    UserCreate,
    UserLogin,
    UserUpdate,
    update_cumstom_clear_history_summary_remarks,
)
from app.utils import (
    create_access_token,
    create_email_handler,
    create_refresh_token,
    generate_excel_from_template_test,
    shenzhen_customes_pdf_gennerate,
    compare_ups_zip_files,
    compare_fedex_zip_files
)
from morelink_api import MoreLinkClient
from rpa_tools import find_playwright_node_path
from rpa_tools.email_tools import send_email
from .permission_item import permission_item_router

adapter = Adapter(engine)
enforcer = casbin.Enforcer('model.conf', adapter)
# logger.level("ALERT", no=35, color="<red>")

# logger.add(create_email_handler("yu.luo@hubs-scs.com"), level="ALERT")

class IPWhitelistMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        print(client_ip)
        # 从数据库中获取白名单
        try:
            session = get_session()
            with session:
                statement = select(IpWhiteList.ip)
                result = session.exec(statement)
                ip_whitelist = [row for row in result] + ['127.0.0.1','localhost']
        except Exception as e:
            logger.error(f"Error fetching IP whitelist from database: {e}")
            ip_whitelist = []

        if client_ip not in ip_whitelist:
            return JSONResponse(
                status_code=403,
                content={"detail": f"IP {client_ip} not allowed"}
            )
        
        response = await call_next(request)
        return response
# @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
# def get_session():
#     try:
#         return Session(pool_engine)
#     except Exception as e:
#         logger.error(f"Database connection error: {e}")
#         raise

# 定义处理数据的函数
def process_shipping_data(
    shipper_name: str, 
    receiver_name: str, 
    master_bill_no: str, 
    gross_weight: int, 
    volume: int, 
    product_list: List[ProductData],
    totalyugutax:float,
    predict_tax_price:float
    ):
    # 创建引擎
    with get_session() as session:
        num_products = sum([i.box_num for i in product_list])
        if num_products == 0:
            raise ValueError("Product list cannot be empty")



        avg_gross_weight = gross_weight / num_products
        avg_volume = volume / num_products

        results = []
        accumulated_gross_weight = 0
        accumulated_volume = 0
        if product_list[0].single_price or product_list[0].packing:

            execute_type = "Sea"


        else:

            execute_type = "Air"

        # 查询发货人地址
        shipper_query = select(ConsigneeData).where(ConsigneeData.发货人 == shipper_name)
        shipper_record = session.exec(shipper_query).first()

        # 查询收件人地址
        receiver_query = select(ConsigneeData).where(ConsigneeData.发货人== receiver_name)
        receiver_record = session.exec(receiver_query).first()

        if not shipper_record:
            raise ValueError(f"Shipper '{shipper_name}' not found in database")
        if not receiver_record:
            raise ValueError(f"Receiver '{receiver_name}' not found in database")

        shipper_address = shipper_record.发货人详细地址
        receiver_address = receiver_record.发货人详细地址
        detail_data_log_list = []
        def safe_round(value, default=0.0):
            try:
                return round(float(value), 4)
            except (ValueError, TypeError):
                return default

        for idx, product in enumerate(product_list):
            product_name = product.product_name
            box_num = product.box_num

            
            product_query = select(Product3).where(Product3.中文品名 == product_name)
            product_record = session.exec(product_query).first()
            duty = safe_round(product_record.Duty)
            additional_duty = safe_round(product_record.加征)
            detail_data_log = CustomClearHistoryDetailLog(
                hs_code=product_record.HS_CODE,
                chinese_name=product_record.中文品名,
                transport_mode=execute_type,
                master_bill_number=master_bill_no,
                total_tax_rate=duty + additional_duty,
                exemption_code=product_record.豁免代码,
                category=product_record.类别,
            )
            detail_data_log_list.append(detail_data_log)
            if not product_record:
                raise ValueError(f"Product '{product_name}' not found in database")
            if product.single_price:
                single_price = product.single_price
            else:
                single_price = product_record.单价

            if product.packing:
                packing = product.packing
            else:
                packing = product_record.件箱
            product_type = session.exec(select(Product3.属性绑定工厂).where(Product3.HS_CODE == product_record.HS_CODE)).first()

            address_name_list = session.exec(
                select(FactoryData.地址, FactoryData.英文)
                .where(
                    (FactoryData.地址 != None) & 
                    (FactoryData.属性 == product_type)
                )
            ).all()

            if not address_name_list:
                logger.warning(f"{product_type}工厂地址数据库中没有对应的属性")
                return {"product_attribute": f"{product_type}工厂在地址数据库中不存在"},None

            random_address_name = random.choice(address_name_list)
            address = random_address_name[0]
            address_name = random_address_name[1]

            if idx == len(product_list) - 1:  # 处理最后一个产品
                gross_weight_for_this_product = round(gross_weight - accumulated_gross_weight, 2)
                volume_for_this_product = round(volume - accumulated_volume, 2)
            else:

                if product_record.single_weight:
                    gross_weight_for_this_product = product_record.single_weight* box_num
                    gross_weight -=gross_weight_for_this_product
                    num_products -= box_num
                    avg_gross_weight = gross_weight/num_products
                else:
                    gross_weight_for_this_product = round(avg_gross_weight * box_num, 2)
                    accumulated_gross_weight += gross_weight_for_this_product
                volume_for_this_product = round(avg_volume * box_num, 2)
                
                accumulated_volume += volume_for_this_product

            net_weight_for_this_product = round(gross_weight_for_this_product * 0.8, 2)

            product_data = {
                'MasterBillNo': master_bill_no,
                "shipper_name": shipper_name,
                "shipper_address": shipper_address,
                "receiver_address": receiver_address,
                "receiver_name": receiver_name,
                'ProductName': product_name,
                'carton': box_num,
                # 'quanity': box_num * float(product_record.件箱),
                'quanity': box_num * float(packing),
                "danwei": "PCS" if product_record.HS_CODE else '',
                # "unit_price": product_record.单价,
                "unit_price": single_price,
                # 'total_price': float(product_record.单价) * box_num * float(product_record.件箱),
                'total_price': float(single_price) * box_num * float(packing),

                'HS_CODE': product_record.HS_CODE,
                'DESCRIPTION': product_record.英文品名,
                'GrossWeight': gross_weight_for_this_product,
                'net_weight': net_weight_for_this_product,
                'Volume': volume_for_this_product,
                "usage": product_record.用途,
                "texture": product_record.材质,
                "address_name": address_name or "",
                "address": address or "",
                "note": product_record.豁免代码,
                "note_explaination": product_record.豁免代码含义,
                'execute_type':execute_type,
                "huomian_file_name":product_record.huomian_file_name

            }

            results.append(product_data)
        
        summary_log_data = CustomClearHistorySummaryLog(
            filename=master_bill_no,
            generation_time=datetime.now(),
            port="",
            packing_type="",
            shipper = shipper_name,
            consignee= receiver_name,
            estimated_tax_amount=totalyugutax,
            gross_weight_kg=gross_weight,
            volume_cbm= volume,
            total_boxes=sum([i.box_num for i in product_list]),
            estimated_tax_rate_cny_per_kg=predict_tax_price,
            details=detail_data_log_list

        )
        return results,summary_log_data

web_vba_router = APIRouter()
web_vba_router.include_router(permission_item_router)

# 定义FastAPI接口
@web_vba_router.post("/process-shipping-data")
async def process_shipping_data_endpoint(request: ShippingRequest, session: Session = Depends(get_session)):
    try:
        results,summary_log = process_shipping_data(
            shipper_name=request.shipper_name,
            receiver_name=request.receiver_name,
            master_bill_no=request.master_bill_no,
            gross_weight=request.gross_weight,
            volume=request.volume,
            product_list=request.product_list,
            totalyugutax = request.totalyugutax,
            predict_tax_price = request.predict_tax_price
        )
        if isinstance(results,dict) and results.get("product_attribute"):
            return JSONResponse({"status":"False","content":"产品属性在地址数据库中不存在"})
        if isinstance(results,dict) and results.get("type")=='net_weight大于gross_weight':
            return JSONResponse({"status":"False","content":results.get("msg")})
        
        # excel_path = generate_excel_from_template(results)
        excel_path = generate_excel_from_template_test(results,request.totalyugutax)
        summary_log.packing_type = request.packing_type
        summary_log.port = request.port
        summary_log.filename = Path(excel_path).name
        await create_summary(summary_log,session)
        if results[0]['execute_type'] == "Sea":
            try:
       
                data = {
                    "shipper_name":request.shipper_name +"\n"+ results[0]['shipper_address'],
                    "receiver_name": request.receiver_name +"\n"+ results[0]['receiver_address'],
                    "master_bill_no": request.master_bill_no,
                    "gross_weight":request.gross_weight,
                    "volume":request.volume,
                    "total_boxes":summary_log.total_boxes,
                    "all_english_name":",".join([i['DESCRIPTION'] for i in results]),
                    "other_data":{"totalyugutax":request.totalyugutax}
                }
                await create_shipment_log(ShipmentLog(**data),session)

            except Exception as e:
                logger.error(f"错误为:{e}")
        return FileResponse(path=excel_path, filename=f"{request.master_bill_no} CI&PL.{excel_path.split('.')[-1]}")
    except ValueError as e:
        logger.error(f"Value Error: {traceback.format_exc()}")
        return JSONResponse({"status":"False","content":f"Value Error: {str(e)}"})
    except Exception as e:
        logger.error(f"Internal Server Error: {str(e)}---{traceback.format_exc()}")
        return JSONResponse({"status":"False","content":f"Internal Server Error: {str(e)}"})

@web_vba_router.post("/dalei/", response_model=Dalei)
def create_dalei(dalei: Dalei, session: Session = Depends(get_session)):
    dalei.id = None
    session.add(dalei)
    session.commit()
    session.refresh(dalei)
    return dalei

@web_vba_router.get("/dalei/")
def read_dalei(skip: int = 0, limit: int = 10, 名称: Optional[str] = None, get_all: bool = False, session: Session = Depends(get_session)):
    query = select(Dalei)
    if 名称:
        query = query.where(Dalei.中文大类.contains(名称))
    total = session.exec(select(func.count()).select_from(query.subquery())).one()
    if get_all:
        dalei_list = session.exec(query).all()
    else:
        dalei_list = session.exec(query.offset(skip).limit(limit)).all()
    return {"items": dalei_list, "total": total}

@web_vba_router.get("/dalei/{id}", response_model=Dalei)
def read_dalei_by_id(id: int, session: Session = Depends(get_session)):
    dalei = session.get(Dalei, id)
    if not dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")
    return dalei

@web_vba_router.put("/dalei/{id}", response_model=Dalei)
def update_dalei(id: int, dalei: Dalei, session: Session = Depends(get_session)):
    existing_dalei = session.get(Dalei, id)
    if not existing_dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")
    for key, value in dalei.dict(exclude_unset=True).items():
        setattr(existing_dalei, key, value)
    session.add(existing_dalei)
    session.commit()
    session.refresh(existing_dalei)
    return existing_dalei

@web_vba_router.delete("/dalei/{id}", response_model=Dalei)
def delete_dalei(id: int, session: Session = Depends(get_session)):
    dalei = session.get(Dalei, id)
    if not dalei:
        raise HTTPException(status_code=404, detail="Dalei not found")
    session.delete(dalei)
    session.commit()
    return dalei
@web_vba_router.get("/products/", response_model=dict)
def read_products(skip: int = 0, limit: int = 10, 名称: Optional[str] = None,get_all:bool=False,country:str="China"):
    with get_session() as session:
        query = select(Product3,)
        if 名称:
            query = query.where(Product3.中文品名.contains(名称))
        query = query.where(Product3.country==country)
        total = session.exec(select(func.count()).select_from(query.subquery())).one()

        if get_all:
            products = session.exec(query).all()
        else:
            products = session.exec(query.offset(skip).limit(limit)).all()
        return {"items": products, "total": total}
@web_vba_router.get("/products/upload_huomian_file")
def upload_huomian_file(file: UploadFile = File(...)):
    save_directory = Path("./file/huomian_file/")
    save_directory.mkdir(parents=True, exist_ok=True)
    file_name = f"{uuid.uuid4()}-{file.filename}"
    # 生成文件保存路径
    file_path = save_directory / file_name

    # 保存文件
    with file.file as file_content:
        with open(file_path, "wb") as buffer:
            buffer.write(file_content.read())
    return {"file_name":file_name}
@web_vba_router.post("/products/", response_model=Product3)
def create_product(product: Optional[str] = Form(None), file: Optional[UploadFile] = File(None)):
    product_data = json.loads(product)
    product: Product3 = Product3(**product_data)
    if not product.更新时间:
        product.更新时间 = datetime.utcnow()
        
    try:
        print(product)
        with get_session() as session:
            session.add(product)
            session.commit()
            try:
                session.refresh(product)
            except InvalidRequestError as e:
                raise HTTPException(status_code=500, detail=f"Error refreshing product: {str(e)}")
            if file:
                file_name = upload_huomian_file(file)['file_name']
                product.huomian_file_name = file_name
            session.add(product)
            session.commit()
            session.refresh(product)

            return product
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating product: {str(e)}")
@web_vba_router.get("/products/{pic_name}")
def download_pic(pic_name: str):
    file_path = os.path.join("./file/huomian_file/",pic_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(file_path, media_type='application/octet-stream', filename=pic_name)
@web_vba_router.put("/products/{product_id}", response_model=Product3)
def update_product(product_id: int, product: str = Form(...), file: Optional[UploadFile] = File(None)):
    product_data = json.loads(product)
    product: Product3 = Product3(**product_data)
    print(product)
    with get_session() as session:
        db_product = session.get(Product3, product_id)
        if not db_product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        # 排除主键字段和None值，并确保字段不为空字符串
        update_data = {
            key: value for key, value in product.dict(exclude_unset=True).items()
            if key != '序号' and value is not None and value != ""
        }
        for key, value in update_data.items():
            setattr(db_product, key, value)
        print(f"file:{file}")
        if file:

            file_name = upload_huomian_file(file)['file_name']
            setattr(db_product, 'huomian_file_name', file_name)
        session.add(db_product)
        session.commit()
        session.refresh(db_product)
        return db_product

@web_vba_router.delete("/products/{product_id}", response_model=Product3)
def delete_product(product_id: int):
    with get_session() as session:
        db_product = session.get(Product3, product_id)
        if not db_product:
            raise HTTPException(status_code=404, detail="Product not found")
        session.delete(db_product)
        session.commit()
        return db_product

@web_vba_router.get("/shippersandreceivers/", response_model=dict)
def read_shippers_and_receivers(skip: int = 0, limit: int = 10, ShipperName: Optional[str] = None):
    with get_session() as session:
        query = select(ShippersAndReceivers)
        if ShipperName:
            query = query.where(ShippersAndReceivers.ShipperName.contains(ShipperName))
        total = session.exec(select(func.count()).select_from(query.subquery())).one()
        shippers_and_receivers = session.exec(query.offset(skip).limit(limit)).all()
        return {"items": shippers_and_receivers, "total": total}

@web_vba_router.post("/shippersandreceivers/", response_model=ShippersAndReceivers)
def create_shipper_or_receiver(shipper_or_receiver: ShippersAndReceivers):
    with get_session() as session:
        session.add(shipper_or_receiver)
        session.commit()
        session.refresh(shipper_or_receiver)
        return shipper_or_receiver

@web_vba_router.put("/shippersandreceivers/{id}", response_model=ShippersAndReceivers)
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

@web_vba_router.delete("/shippersandreceivers/{id}", response_model=ShippersAndReceivers)
def delete_shipper_or_receiver(id: int):
    with get_session() as session:
        db_shipper_or_receiver = session.get(ShippersAndReceivers, id)
        if not db_shipper_or_receiver:
            raise HTTPException(status_code=404, detail="Shipper or Receiver not found")
        session.delete(db_shipper_or_receiver)
        session.commit()
        return db_shipper_or_receiver

@web_vba_router.post("/ports/", response_model=Port)
def create_port(port: Port, session: Session = Depends(get_session)):
    session.add(port)
    session.commit()
    session.refresh(port)
    return port

@web_vba_router.get("/ports/", response_model=List[Port])
def read_ports(session: Session = Depends(get_session), skip: int = 0, limit: Optional[int] = None):
    query = select(Port).offset(skip)
    if limit:
        query = query.limit(limit)
    ports = session.exec(query).all()
    return ports

@web_vba_router.get("/ports/{port_id}", response_model=Port)
def read_port(port_id: int, session: Session = Depends(get_session)):
    port = session.get(Port, port_id)
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    return port

@web_vba_router.put("/ports/{port_id}", response_model=Port)
def update_port(port_id: int, updated_port: Port, session: Session = Depends(get_session)):
    port = session.get(Port, port_id)
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    port.port_name = updated_port.port_name
    port.sender_name = updated_port.sender_name
    port.receiver_name = updated_port.receiver_name
    session.commit()
    session.refresh(port)
    return port

@web_vba_router.delete("/ports/{port_id}", response_model=Port)
def delete_port(port_id: int, session: Session = Depends(get_session)):
    port = session.get(Port, port_id)
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    session.delete(port)
    session.commit()
    return port
# 工厂数据CRUD操作
@web_vba_router.post("/factory/", response_model=FactoryData)
def create_factory(factory: FactoryData):
    with get_session() as session:
        session.add(factory)
        session.commit()
        session.refresh(factory)
        return factory

@web_vba_router.get("/factory/")
def read_factories(skip: int = 0, limit: Optional[int] = None):
    with get_session() as session:
        total = session.exec(select(func.count(FactoryData.id))).one()
        query = select(FactoryData).offset(skip)
        if limit is not None:
            query = query.limit(limit)
        factories = session.exec(query).all()
        return {"items": factories, "total": total}

@web_vba_router.get("/factory/{factory_id}", response_model=FactoryData)
def read_factory(factory_id: int):
    with get_session() as session:
        factory = session.get(FactoryData, factory_id)
        if not factory:
            raise HTTPException(status_code=404, detail="Factory not found")
        return factory

@web_vba_router.put("/factory/{factory_id}", response_model=FactoryData)
def update_factory(factory_id: int, factory: FactoryData):
    with get_session() as session:
        db_factory = session.get(FactoryData, factory_id)
        if not db_factory:
            raise HTTPException(status_code=404, detail="Factory not found")
        db_factory.属性 = factory.属性
        db_factory.中文名字 = factory.中文名字
        db_factory.英文 = factory.英文
        db_factory.地址 = factory.地址
        session.commit()
        session.refresh(db_factory)
        return db_factory

@web_vba_router.delete("/factory/{factory_id}", response_model=FactoryData)
def delete_factory(factory_id: int):
    with get_session() as session:
        factory = session.get(FactoryData, factory_id)
        if not factory:
            raise HTTPException(status_code=404, detail="Factory not found")
        session.delete(factory)
        session.commit()
        return factory

# 收发货人CRUD操作
@web_vba_router.post("/consignee/", response_model=ConsigneeData)
def create_consignee(consignee: ConsigneeData):
    with get_session() as session:
        session.add(consignee)
        session.commit()
        session.refresh(consignee)
        return consignee

@web_vba_router.get("/consignee/")
def read_consignees(skip: int = 0, limit: Optional[int] = None):
    with get_session() as session:
        total = session.exec(select(func.count(ConsigneeData.id))).one()
        query = select(ConsigneeData).offset(skip)
        if limit is not None:
            query = query.limit(limit)
        consignees = session.exec(query).all()
        return {"items": consignees, "total": total}

@web_vba_router.get("/consignee/{consignee_id}", response_model=ConsigneeData)
def read_consignee(consignee_id: int):
    with get_session() as session:
        consignee = session.get(ConsigneeData, consignee_id)
        if not consignee:
            raise HTTPException(status_code=404, detail="Consignee not found")
        return consignee

@web_vba_router.put("/consignee/{consignee_id}", response_model=ConsigneeData)
def update_consignee(consignee_id: int, consignee: ConsigneeData):
    with get_session() as session:
        db_consignee = session.get(ConsigneeData, consignee_id)
        if not db_consignee:
            raise HTTPException(status_code=404, detail="Consignee not found")
        db_consignee.中文 = consignee.中文
        db_consignee.发货人 = consignee.发货人
        db_consignee.发货人详细地址 = consignee.发货人详细地址
        db_consignee.类型 = consignee.类型
        db_consignee.备注 = consignee.备注
        db_consignee.hide = consignee.hide

        session.commit()
        session.refresh(db_consignee)
        return db_consignee

@web_vba_router.delete("/consignee/{consignee_id}", response_model=ConsigneeData)
def delete_consignee(consignee_id: int):
    with get_session() as session:
        consignee = session.get(ConsigneeData, consignee_id)
        if not consignee:
            raise HTTPException(status_code=404, detail="Consignee not found")
        session.delete(consignee)
        session.commit()
        return consignee
    

@web_vba_router.get("/api/exchange-rate")
def get_exchange_rate():
    url = "https://finance.pae.baidu.com/selfselect/sug?wd=%E7%BE%8E%E5%85%83%E4%BA%BA%E6%B0%91%E5%B8%81&skip_login=1&finClientType=pc"
    # 12月上汇率
    rate = "7.2767"
    try:
        response = httpx.get(url)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()
        # 提取汇率
        cn_us_rate = [
            i['price']
            for i in data['Result']['stock']
            if i['code'] == "USDCNY" or i['code'] == "CNYUSD"
        ]
        
        if not cn_us_rate:
            logger.warning("Exchange rate not found, using default rate.")
            return {"USDCNY": rate}
        
        return {"USDCNY": cn_us_rate[0]}
    
    except requests.RequestException as e:
        logger.error(f"Service unavailable: {e}")
        return {"USDCNY": rate}
    except (KeyError, ValueError) as e:
        logger.error(f"Error parsing exchange rate data: {e}")
        return {"USDCNY": rate}
    except Exception as e:
        return {"USDCNY": rate}
@web_vba_router.post("/login")
async def login_for_access_token(user: UserLogin, session: Session = Depends(get_session)):
    statement = select(User).where(User.username == user.username)
    result = session.exec(statement)
    user_db = result.first()
    if not user_db or not bcrypt.checkpw(user.password.encode('utf-8'), user_db.password.encode('utf-8')):
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    
    # Assuming the permissions are part of the user_db model
    permissions = enforcer.get_filtered_policy(0, user_db.username)
    
    access_token_expires = timedelta(hours=1)
    access_token = create_access_token(
        data={
            "sub": user_db.username,
            "permissions": permissions
        },
        expires_delta=access_token_expires
    )
    
    refresh_token = create_refresh_token(data={"sub": user_db.username})

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }
@web_vba_router.post("/users/", response_model=User)
def create_user(user: UserCreate, session: Session = Depends(get_session)):
    statement = select(User).where(User.username == user.username)
    result = session.exec(statement)
    user_db = result.first()
    if user_db:
        raise HTTPException(status_code=400, detail="Username already exists")

    hashed_password = bcrypt.hashpw(user.password.encode('utf-8'), bcrypt.gensalt())
    new_user = User(username=user.username, password=hashed_password.decode('utf-8'))
    session.add(new_user)
    session.commit()
    session.refresh(new_user)

    # 添加用户权限
    for perm in user.permissions:
        obj, act = perm.split(':')
        enforcer.add_policy(user.username, obj, act,'allow')

    enforcer.load_policy()  # 重新加载策略
    return new_user
@web_vba_router.put("/users/{user_id}/", response_model=User)
def update_user(user_id: int, user_update: UserUpdate, session: Session = Depends(get_session)):
    user_db = session.get(User, user_id)
    if not user_db:
        raise HTTPException(status_code=404, detail="User not found")

    # 更新用户名和密码
    if user_update.username:
        user_db.username = user_update.username
    if user_update.password:
        hashed_password = bcrypt.hashpw(user_update.password.encode('utf-8'), bcrypt.gensalt())
        user_db.password = hashed_password.decode('utf-8')

    # 更新用户权限
    if user_update.permissions is not None:
        current_policies = enforcer.get_filtered_policy(0, user_db.username)
        current_permissions = {f"{policy[1]}:{policy[2]}" for policy in current_policies if policy[3] == 'allow'}
        update_permissions = set(user_update.permissions)
        print(f"当前权限: {current_permissions}")
        print(f"更新权限: {update_permissions}")
        
        # 需要删除的权限（将 eft 设置为 deny）
        for perm in current_permissions - update_permissions:
            print(f"删除权限{perm}")
            obj, act = perm.split(':')
            enforcer.update_policy([user_db.username, obj, act, "allow"], [user_db.username, obj, act, "deny"])

        # 需要添加的权限
        for perm in update_permissions - current_permissions:
            obj, act = perm.split(':')
            enforcer.add_policy(user_db.username, obj, act, "allow")

    session.add(user_db)
    session.commit()
    session.refresh(user_db)

    enforcer.load_policy()  # 重新加载策略
    return user_db
@web_vba_router.get("/users/", response_model=List[User])
def read_users(skip: int = 0, limit: int = 10, session: Session = Depends(get_session)):
    users = session.exec(select(User).offset(skip).limit(limit)).all()
    return users

@web_vba_router.get("/users/{user_id}/", response_model=User)
def read_user(user_id: int, session: Session = Depends(get_session)):
    user = session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user
@web_vba_router.delete("/users/{user_id}/", response_model=User)
def delete_user(user_id: int, session: Session = Depends(get_session)):
    user = session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # 删除用户权限
    enforcer.delete_roles_for_user(user.username)
    enforcer.delete_user(user.username)
    
    session.delete(user)
    session.commit()
    
    return {"message": "User and associated permissions deleted successfully"}
@web_vba_router.post("/add_policy/")
async def add_policy(policy: Policy):
    if enforcer.add_policy(policy.sub, policy.obj, policy.act, policy.eft):
        enforcer.load_policy()  # 重新加载策略
        return {"message": "策略添加成功"}
    else:
        raise HTTPException(status_code=400, detail="策略已存在或无法添加")

@web_vba_router.delete("/remove_policy/")
async def remove_policy(policy: Policy):
    if enforcer.remove_policy(policy.sub, policy.obj, policy.act, policy.eft,"",""):
        enforcer.load_policy()  # 重新加载策略
        return {"message": "策略删除成功"}
    else:
        raise HTTPException(status_code=400, detail="策略不存在或无法删除")

@web_vba_router.put("/update_policy/")
async def update_policy(update_policy: UpdatePolicy):
    old_policy = [update_policy.old_sub, update_policy.old_obj, update_policy.old_act, update_policy.old_eft,"",""]
    new_policy = [update_policy.new_sub, update_policy.new_obj, update_policy.new_act, update_policy.new_eft,"",""]

    # 检查旧策略是否存在
    if not enforcer.has_policy(*old_policy):
        raise HTTPException(status_code=404, detail="旧策略不存在")
    


    # 尝试更新策略
    result = enforcer.update_policy(old_policy, new_policy)

    if result:
        enforcer.load_policy()  # 重新加载策略
        return {"message": "策略更新成功"}
    else:
        raise HTTPException(status_code=400, detail="策略更新失败或未找到旧策略")


@web_vba_router.get("/get_policies/")
async def get_policies():
    return enforcer.get_policy()

@web_vba_router.get("/get_user_policies/")
async def get_user_policies(user: str):
    user_policies = enforcer.get_filtered_policy(0, user)
    if not user_policies:
        raise HTTPException(status_code=404, detail="该用户没有策略")

    return user_policies

@web_vba_router.post("/add_group/")
async def add_group(group: Group):
    if enforcer.add_grouping_policy(group.user, group.group):
        enforcer.load_policy()  # 重新加载策略
        return {"message": "组添加成功"}
    else:
        raise HTTPException(status_code=400, detail="组已存在或无法添加")

@web_vba_router.delete("/remove_group/")
async def remove_group(group: Group):
    if enforcer.remove_grouping_policy(group.user, group.group):
        enforcer.load_policy()  # 重新加载策略
        return {"message": "组删除成功"}
    else:
        raise HTTPException(status_code=400, detail="组不存在或无法删除")

@web_vba_router.get("/get_groups/")
async def get_groups():
    return enforcer.get_grouping_policy()


# 创建IP白名单
@web_vba_router.post("/ip_white_list/", response_model=IpWhiteList)
def create_ip_white_list(ip_white_list: IpWhiteList, session: Session = Depends(get_session)):
    try:
        db_ip_white_list = session.exec(select(IpWhiteList).where(IpWhiteList.ip == ip_white_list.ip)).first()
        if db_ip_white_list:
            raise HTTPException(status_code=400, detail="IP already exists")
        session.add(ip_white_list)
        session.commit()
        session.refresh(ip_white_list)
        return ip_white_list
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 获取所有IP白名单
@web_vba_router.get("/ip_white_list/", response_model=List[IpWhiteList])
def get_all_ip_white_list(session: Session = Depends(get_session)):
    ip_white_lists = session.exec(select(IpWhiteList)).all()
    return ip_white_lists

# 获取单个IP白名单
@web_vba_router.get("/ip_white_list/{ip_white_list_id}", response_model=IpWhiteList)
def get_ip_white_list(ip_white_list_id: int, session: Session = Depends(get_session)):
    ip_white_list = session.get(IpWhiteList, ip_white_list_id)
    if not ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")
    return ip_white_list

# 更新IP白名单
@web_vba_router.put("/ip_white_list/{ip_white_list_id}", response_model=IpWhiteList)
def update_ip_white_list(ip_white_list_id: int, ip_white_list: IpWhiteList, session: Session = Depends(get_session)):
    db_ip_white_list = session.get(IpWhiteList, ip_white_list_id)
    if not db_ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")
    ip_white_list_data = ip_white_list.dict(exclude_unset=True)
    for key, value in ip_white_list_data.items():
        setattr(db_ip_white_list, key, value)
    session.add(db_ip_white_list)
    session.commit()
    session.refresh(db_ip_white_list)
    return db_ip_white_list

# 删除IP白名单
@web_vba_router.delete("/ip_white_list/{ip_white_list_id}", response_model=IpWhiteList)
def delete_ip_white_list(ip_white_list_id: int, session: Session = Depends(get_session)):
    ip_white_list = session.get(IpWhiteList, ip_white_list_id)
    if not ip_white_list:
        raise HTTPException(status_code=404, detail="IP white list not found")
    session.delete(ip_white_list)
    session.commit()
    return ip_white_list


@web_vba_router.get("/files", response_model=List[FileInfo])
async def get_files():
    directory_path = "./pdf"
    if not os.path.exists(directory_path):
        raise HTTPException(status_code=404, detail="Directory not found")
    
    files = []
    for file_name in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file_name)
        if os.path.isfile(file_path):
            file_info = os.stat(file_path)
            files.append(FileInfo(name=file_name, time=datetime.fromtimestamp(file_info.st_mtime)))
    
    files.sort(key=lambda x: x.time, reverse=True)
    
    return files[:500]

@web_vba_router.get("/download/{file_name}")
async def download_file(file_name: str):
    file_path = os.path.join("./pdf", file_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(file_path, media_type='application/octet-stream', filename=file_name)



# 创建 Summary 记录
@web_vba_router.post("/cumstom_clear_history_summary/", response_model=CustomClearHistorySummaryLog)
async def create_summary(summary: CustomClearHistorySummaryLog, session: Session = Depends(get_session)):
    session.add(summary)
    session.commit()
    session.refresh(summary)

    for detail in summary.details:
        detail.summary_log_id = summary.id
        detail.generation_time = summary.generation_time
        session.add(detail)

    session.commit()
    return summary
@web_vba_router.post("/update_cumstom_clear_history_summary_remarks/", response_model=CustomClearHistorySummaryLog)
async def update_summary(request_body: update_cumstom_clear_history_summary_remarks, session: Session = Depends(get_session)):
    summary = session.get(CustomClearHistorySummaryLog, request_body.id)
    summary.remarks = request_body.remarks
    session.add(summary)
    session.commit()
    session.refresh(summary)

    return summary


@web_vba_router.get("/cumstom_clear_history_summary/", response_model=SummaryResponse)
def read_summaries(
    enable_pagination: bool = Query(False, description="Enable pagination"),
    page: int = Query(1, description="Page number", ge=1),
    page_size: int = Query(10, description="Number of items per page", ge=1, le=100),
    file_name: Optional[str] = Query(None, description="File name to filter by"),
    remarks: Optional[str] = Query(None, description="remarks filter by"),
    session: Session = Depends(get_session)
):
    query = (
        select(CustomClearHistorySummaryLog)
        .filter(or_(CustomClearHistorySummaryLog.remarks != "删除", CustomClearHistorySummaryLog.remarks == None))
        .order_by(CustomClearHistorySummaryLog.generation_time.desc())
    )    
    # 如果 file_name 有值，添加 WHERE 约束
    if file_name:
        query = query.filter(CustomClearHistorySummaryLog.filename.like(f"%{file_name}%"))
    if remarks:
        query = query.filter(CustomClearHistorySummaryLog.remarks.like(f"%{remarks}%"))
    if enable_pagination:
        # 计算偏移量
        offset = (page - 1) * page_size
        
        # 查询分页数据
        summaries = session.exec(
            query.offset(offset).limit(page_size)
        ).all()
        
        # 查询总记录数
        total_query = select(func.count()).select_from(query.subquery())
        total_result = session.exec(total_query).first()
        total = total_result
        
        # 计算总页数
        total_pages = (total + page_size - 1) // page_size
        
        return {
            "summaries": summaries,
            "total": total,
            "total_pages": total_pages
        }
    else:
        # 查询全部数据
        summaries = session.exec(query).all()
        
        return {
            "summaries": summaries,
            "total":summaries.count,
            "total_pages": 1
        }



# 查询单个 Summary 记录
@web_vba_router.get("/cumstom_clear_history_summary/{summary_id}", response_model=CustomClearHistorySummaryLog)
def read_summary(summary_id: UUID, session: Session = Depends(get_session)):
    summary = session.get(CustomClearHistorySummaryLog, summary_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Summary not found")
    return summary

# 创建 Detail 记录
@web_vba_router.post("/cumstom_clear_history_detail/", response_model=CustomClearHistoryDetailLog)
def create_detail(detail: CustomClearHistoryDetailLog, session: Session = Depends(get_session)):
    session.add(detail)
    session.commit()
    session.refresh(detail)
    return detail

# 查询所有 Detail 记录
@web_vba_router.get("/cumstom_clear_history_detail/", response_model=List[CustomClearHistoryDetailLog])
def read_details(session: Session = Depends(get_session)):
    details = session.exec(select(CustomClearHistoryDetailLog)).all()
    return details

# 查询单个 Detail 记录
@web_vba_router.get("/cumstom_clear_history_detail/{detail_id}", response_model=CustomClearHistoryDetailLog)
def read_detail(detail_id: int, session: Session = Depends(get_session)):
    detail = session.get(CustomClearHistoryDetailLog, detail_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Detail not found")
    return detail

@web_vba_router.get("/output_cumtoms_clear_log/")
def output_log(
    start_time: datetime = Query(..., description="开始时间"),
    end_time: datetime = Query(..., description="结束时间"),
     session: Session = Depends(get_session)
):
    # 查询汇总日志
    summary_logs = session.exec(
        select(CustomClearHistorySummaryLog).where(
            CustomClearHistorySummaryLog.generation_time >= start_time,
            CustomClearHistorySummaryLog.generation_time <= end_time,
            or_(CustomClearHistorySummaryLog.remarks != '删除', CustomClearHistorySummaryLog.remarks.is_(None))
        )
    ).all()

    # 查询明细日志
    detail_logs = session.exec(
        select(CustomClearHistoryDetailLog).where(
            CustomClearHistoryDetailLog.generation_time >= start_time,
            CustomClearHistoryDetailLog.generation_time <= end_time
        )
    ).all()

    # 创建 Excel 文件
    wb = Openpyxl_Workbook()
    ws_summary = wb.active
    ws_summary.title = "历史汇总"
    ws_detail = wb.create_sheet(title="历史明细")

    # 写入汇总日志数据
    summary_headers = ["ID", "文件名", "生成时间", "港口", "装箱类型", "发货人", "收货人", "预估税金", "Gross Weight (kg)", "Volume (cbm)", "总箱数", "预估税金单价CNY/Kg", "Remarks"]
    ws_summary.append(summary_headers)
    # 汇总的所有id，隐藏了删除的

    all_summary_id = []
    for log in summary_logs:
        all_summary_id.append(str(log.id))
        ws_summary.append([
            str(log.id), log.filename, log.generation_time.isoformat(), log.port, log.packing_type, log.shipper, log.consignee, log.estimated_tax_amount, log.gross_weight_kg, log.volume_cbm, log.total_boxes, log.estimated_tax_rate_cny_per_kg, log.remarks
        ])

    # 写入明细日志数据
    detail_headers = ["ID", "HS Code", "中文品名", "运输方式", "主单号", "生成时间", "总税率", "豁免代码", "类别","FZ", "Summary Log ID"]
    ws_detail.append(detail_headers)
    for log in detail_logs:
        if str(log.summary_log_id) not in all_summary_id:
            continue

        ws_detail.append([
            log.id, log.hs_code, log.chinese_name, log.transport_mode, log.master_bill_number, log.generation_time.isoformat(), log.total_tax_rate, log.exemption_code, log.category, 1,str(log.summary_log_id)
        ])

    # 将 Excel 文件保存到内存中
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    # 返回 Excel 文件
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=custom_clear_history_log.xlsx"})



@web_vba_router.post("/haiyunzishui/", response_model=HaiYunZiShui)
def create_haiyunzishui(haiyunzishui: HaiYunZiShui, session: Session = Depends(get_session)):
    session.add(haiyunzishui)
    session.commit()
    session.refresh(haiyunzishui)
    return haiyunzishui

@web_vba_router.get("/haiyunzishui/", response_model=List[HaiYunZiShui])
def read_haiyunzishuis(session: Session = Depends(get_session), skip: int = 0, limit: Optional[int] = None):
    query = select(HaiYunZiShui).offset(skip)
    if limit:
        query = query.limit(limit)
    haiyunzishui = session.exec(query).all()
    return haiyunzishui

@web_vba_router.get("/haiyunzishui/{haiyunzishui_id}", response_model=HaiYunZiShui)
def read_haiyunzishui(haiyunzishui_id: int, session: Session = Depends(get_session)):
    haiyunzishui = session.get(HaiYunZiShui, haiyunzishui_id)
    if not haiyunzishui:
        raise HTTPException(status_code=404, detail="Port not found")
    return haiyunzishui

@web_vba_router.put("/haiyunzishui/{haiyunzishui_id}", response_model=HaiYunZiShui)
def update_haiyunzishui(haiyunzishui_id: int, updated_port: HaiYunZiShui, session: Session = Depends(get_session)):
    haiyunzishui = session.get(HaiYunZiShui, haiyunzishui_id)
    if not haiyunzishui:
        raise HTTPException(status_code=404, detail="Port not found")
    haiyunzishui.zishui_name = updated_port.zishui_name
    haiyunzishui.sender = updated_port.sender
    haiyunzishui.receiver = updated_port.receiver
    session.commit()
    session.refresh(haiyunzishui)
    return haiyunzishui

@web_vba_router.delete("/haiyunzishui/{haiyunzishui_id}", response_model=HaiYunZiShui)
def delete_haiyunzishui(haiyunzishui_id: int, session: Session = Depends(get_session)):  # noqa: F811
    haiyunzishui = session.get(HaiYunZiShui, haiyunzishui_id)
    if not haiyunzishui:
        raise HTTPException(status_code=404, detail="Port not found")
    session.delete(haiyunzishui)
    session.commit()
    return haiyunzishui


# 增：创建 ShipmentLog
@web_vba_router.post("/shipment_logs/", response_model=ShipmentLog)
async def create_shipment_log(shipment_log: ShipmentLog, session: Session = Depends(get_session)):
    session.add(shipment_log)
    session.commit()
    session.refresh(shipment_log)
    return shipment_log

# 改：更新 ShipmentLog
@web_vba_router.put("/shipment_logs/{shipment_log_id}", response_model=ShipmentLog)
async def update_shipment_log(shipment_log_id: int, shipment_log: ShipmentLog, session: Session = Depends(get_session)):
    db_shipment_log =  session.get(ShipmentLog, shipment_log_id)
    if not db_shipment_log:
        raise HTTPException(status_code=404, detail="ShipmentLog not found")
    shipment_log_data = shipment_log.dict(exclude_unset=True)
    for key, value in shipment_log_data.items():
        setattr(db_shipment_log, key, value)
    session.add(db_shipment_log)
    session.commit()
    session.refresh(db_shipment_log)
    return db_shipment_log

# 查：查询所有 ShipmentLog
@web_vba_router.get("/shipment_logs/", response_model=dict)
async def read_shipment_logs(
    status: Optional[int] = Query(None, description="Filter by status"),
    offset: int = Query(0, description="Offset for pagination"),
    limit: int = Query(10, description="Limit for pagination"),
    session: Session = Depends(get_session)
):
    query = select(ShipmentLog)
    if status is not None:
        query = query.where((ShipmentLog.status == status) | (ShipmentLog.status == -1))
    # query = query.where(ShipmentLog.status == -1)  # 添加 status == -1 的条件

    query = query.offset(offset).limit(limit)
    result = session.execute(query)
    shipment_logs = result.scalars().all()
    
    # 计算总数和总页数
    total_count = session.query(ShipmentLog).count()
    total_pages = (total_count + limit - 1) // limit
    
    return {
        "shipment_logs": shipment_logs,
        "total": total_count,
        "total_pages": total_pages
    }
# 查：根据 ID 查询 ShipmentLog
@web_vba_router.get("/shipment_logs/{master_bill_no}", response_model=ShipmentLog)
async def read_shipment_log(master_bill_no: str, session: Session = Depends(get_session)):
    shipment_log =  session.get(ShipmentLog, master_bill_no)
    if not shipment_log:
        raise HTTPException(status_code=404, detail="ShipmentLog not found")
    return {
    "shipment_logs": shipment_log,
    "total": 1,
    "total_pages": 1
    }

@web_vba_router.get('/get_tidan_pdf_again/{id}')
async def get_tidan_pdf(id: int, session: Session = Depends(get_session)):
    # 获取 ShipmentLog 数据
    request_data = session.get(ShipmentLog, id)
    if not request_data:
        raise HTTPException(status_code=404, detail="ShipmentLog not found")

    node_path = find_playwright_node_path()
    morelink_client = MoreLinkClient(node_path)
    
    # Assuming zongdan_api_httpx can be called without arguments for this context
    data = morelink_client.zongdan_api_httpx()

    filter_data = [
        row for row in data
        if row.get('billno') == request_data.master_bill_no
    ]
    
    if not filter_data:
        message = f"morelink提单号搜索不到：{request_data.master_bill_no}"
        logger.log("ALERT", message)
        raise HTTPException(status_code=404, detail=message)

    pdf_path = shenzhen_customes_pdf_gennerate(request_data.model_dump(), filter_data[0])
    logger.info(f"已生成pdf文件->{pdf_path}")

    # 更新 ShipmentLog 的状态
    request_data.status = 1
    session.add(request_data)
    session.commit()
    session.refresh(request_data)

    return FileResponse(pdf_path, media_type='application/pdf', filename=os.path.basename(pdf_path))


@web_vba_router.post("/zip-compare/ups")
async def zip_compare_ups(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    """
    比较两个UPS邮政编码定义文件。
    - **file1**: 旧的UPS Excel文件。
    - **file2**: 新的UPS Excel文件。
    """
    try:
        result = await compare_ups_zip_files(file1, file2)
        return result
    except Exception as e:
        logger.error(f"Error comparing UPS zip files: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@web_vba_router.post("/zip-compare/fedex")
async def zip_compare_fedex(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    """
    比较两个FedEx邮政编码定义文件。
    - **file1**: 旧的FedEx PDF文件。
    - **file2**: 新的FedEx PDF文件。
    """
    try:
        result = await compare_fedex_zip_files(file1, file2)
        return result
    except Exception as e:
        logger.error(f"Error comparing FedEx zip files: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))