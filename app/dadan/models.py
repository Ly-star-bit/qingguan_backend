from datetime import  datetime, timezone


from typing import  List, Optional
from uuid import UUID, uuid4
from pydantic import field_validator
from sqlalchemy import JSON, Column, DateTime, Float, ForeignKey, Integer, String, Text,JSON
from sqlmodel import Relationship, SQLModel, Field
from sqlalchemy.dialects.postgresql import UUID as SQLAlchemyUUID

class Port(SQLModel, table=True):
    __tablename__ = 'port'
    id: int = Field(default=None, primary_key=True)
    port_name: str = Field(max_length=255, nullable=False)
    sender_name: str = Field(max_length=255, default="")
    receiver_name: str = Field(max_length=255, nullable=False)
    remarks: str = Field(max_length=255, nullable=False)


class Product3(SQLModel, table=True):
    总税率: str = Field(sa_column=Column("总税率", String(255), ), default="")
    中文品名: str = Field(sa_column=Column("中文品名", String(255), ), default="")
    英文品名: str = Field(sa_column=Column("英文品名", String(255), ), default="")
    HS_CODE: str = Field(sa_column=Column("HS_CODE", String(255), ), default="")
    Duty: str = Field(sa_column=Column("Duty(%)", String(255), ), default="")
    加征: str = Field(sa_column=Column("加征%", String(255), ), default="")
    一箱税金: str = Field(sa_column=Column("一箱税金", String(255), ), default="")
    豁免代码: str = Field(sa_column=Column("豁免代码", String(255), ), default="")
    豁免代码含义: str = Field(sa_column=Column("豁免代码含义", Text, ), default="")

    豁免截止日期说明: str = Field(sa_column=Column("豁免截止日期/说明", String(255), ), default="")
    豁免过期后: str = Field(sa_column=Column("豁免过期后", String(255), ), default="")
    认证: str = Field(sa_column=Column("认证？", String(255), ), default="")
    件箱: str = Field(sa_column=Column("件/箱", String(255), ), default="")
    单价: str = Field(sa_column=Column("单价", String(255), ), default="")
    材质: str = Field(sa_column=Column("材质", String(255), ), default="")
    用途: str = Field(sa_column=Column("用途", String(255), ), default="")
    更新时间: datetime = Field(sa_column=Column("更新时间", DateTime), default_factory=lambda: datetime.now(timezone.utc))
    类别: str = Field(sa_column=Column("类别", String(255), ), default="")
    属性绑定工厂: str = Field(sa_column=Column("属性绑定工厂", String(255), ), default="")
    序号: Optional[int] = Field(default=None, primary_key=True)
    备注: str = Field(sa_column=Column("备注", String(255), ), default="")
    单件重量合理范围: str = Field(sa_column=Column("单件重量合理范围", String(255), ), default="")
    客户: str = Field(sa_column=Column("客户", String(255), ), default="")
    报关代码: str = Field(sa_column=Column("报关代码", String(255), ), default="")
    客人资料美金: str = Field(sa_column=Column("客人资料美金", String(255), ), default="")
    single_weight: float = Field(sa_column=Column("single_weight", Float(), ))
    自税: int = Field(sa_column=Column("自税", Integer, ), default=0)
    类型:str = Field(sa_column=Column("类型", String(255), ), default="")
    huomian_file_name: str = Field(sa_column=Column("huomian_file_name", String(255), ), default="")
    country: str = Field(sa_column=Column("country", String(255), ), default="China")
    

    # @field_validator('更新时间', pre=True, always=True)
    # def parse_datetime(cls, value):
    #     if isinstance(value, str):
    #         try:
    #             return datetime.fromisoformat(value.replace('Z', '+00:00'))
    #         except ValueError:
    #             return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    #     return value

    # class Config:
    #     json_encoders = {
    #         datetime: lambda v: v.strftime('%Y-%m-%d %H:%M:%S')
    #     }

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

class FactoryData(SQLModel, table=True):
    __tablename__ = "工厂数据"
    id: Optional[int] = Field(default=None, primary_key=True)
    属性: str = Field(max_length=255, nullable=False)
    中文名字: str = Field(max_length=255, nullable=False)
    英文: str = Field(nullable=False)
    地址: str = Field(nullable=False)
class ConsigneeData(SQLModel, table=True):
    __tablename__ = "收发货人"
    id: Optional[int] = Field(default=None, primary_key=True)
    中文: str = Field(max_length=255, nullable=False)
    发货人: str = Field(max_length=255, nullable=False)
    发货人详细地址: str = Field(nullable=False)
    类型: str  = Field(nullable=False)
    关税类型: str = Field(nullable=False)
    备注: str = Field(nullable=False)
    hide: str = Field(nullable=False)


class HaiYunZiShui(SQLModel, table=True):
    __tablename__ = '海运自税'
    id: int = Field(default=None, primary_key=True, description="自增主键")
    zishui_name: str = Field(default=None, max_length=100, description="自税名称")
    sender: str = Field(default=None, description="发货人")
    receiver: str = Field(default=None, description="收货人")


class Dalei(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    属性: Optional[str] = Field(default=None, max_length=255)

    hs_code: Optional[str] = Field(default=None, max_length=255)
    类别: Optional[str] = Field(default=None, max_length=255)

    英文大类: Optional[str] = Field(default=None, max_length=255)
    中文大类: Optional[str] = Field(default=None, max_length=255)
    客供: Optional[str] = Field(default=None, max_length=255)
    备注: Optional[str] = Field(default=None, max_length=255)



class User(SQLModel, table=True):
    id: Optional[str] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True, nullable=False)
    password: str = Field(nullable=False)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))




class IpWhiteList(SQLModel, table=True):
    __tablename__ = "ip_white_list"
    id: Optional[str] = Field(default=None, primary_key=True)
    ip: str = Field(max_length=255, nullable=False)
    remarks: str = Field(max_length=255, nullable=False)



class CustomClearHistorySummaryLog(SQLModel, table=True):
    __tablename__ = "custom_clear_history_summary_log"
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    filename: str
    generation_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    port: str
    packing_type: str
    shipper: str
    consignee: str
    estimated_tax_amount: float
    gross_weight_kg: float
    volume_cbm: float
    total_boxes: int
    estimated_tax_rate_cny_per_kg: float
    remarks:str

    details: List["CustomClearHistoryDetailLog"] = Relationship(back_populates="summary")

class CustomClearHistoryDetailLog(SQLModel, table=True):
    __tablename__ = "custom_clear_history_detail_log"
    id: Optional[int] = Field(default=None, primary_key=True)
    hs_code: str
    chinese_name: str
    transport_mode: str
    master_bill_number: str
    generation_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_tax_rate: float
    exemption_code: str
    category: str

    summary_log_id: UUID = Field(sa_column=Column(SQLAlchemyUUID(as_uuid=True), ForeignKey("custom_clear_history_summary_log.id")))
    summary: CustomClearHistorySummaryLog = Relationship(back_populates="details")

class ShipmentLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    shipper_name: str
    receiver_name: str
    master_bill_no: str
    gross_weight: float
    volume: float
    total_boxes: int
    all_english_name: str = Field(sa_column=Column(Text))  # 使用 SQLAlchemy 的 Text 类型
    status: int = Field(default=0)  # 0: 未完成, 1: 已完成, -1: 失败
    other_data: str = Field(sa_column=Column(JSON))  # 使用 SQLAlchemy 的 JSON 类型存储 JSON 数据