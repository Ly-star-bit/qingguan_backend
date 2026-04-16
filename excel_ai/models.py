"""
共享数据模型 - 两阶段智能提取方案基础模块
包含 DataMap 和 ProductRecord 等核心数据模型
"""

import sys

# 设置 UTF-8 编码（解决 Windows 控制台编码问题）
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')


class DataMap:
    """数据地图 - 描述 Excel 文件的数据分布和关联关系"""
    def __init__(self):
        self.data_distribution = {}  # 字段分布：{字段名: {sheet, column, extract_method}}
        self.sheet_structures = {}   # 每个Sheet的详细结构：{sheet_name: {header_row, field_cols, data_start_row}}
        self.relationships = []  # Sheet 关联关系
        self.extraction_strategy = "single_sheet"  # 提取策略：single_sheet 或 multi_sheet_join
        self.confidence = 1.0  # 置信度

    def to_dict(self):
        return {
            "data_distribution": self.data_distribution,
            "sheet_structures": self.sheet_structures,
            "relationships": self.relationships,
            "extraction_strategy": self.extraction_strategy,
            "confidence": self.confidence
        }

    @classmethod
    def from_dict(cls, data):
        dm = cls()
        dm.data_distribution = data.get("data_distribution", {})
        dm.sheet_structures = data.get("sheet_structures", {})
        dm.relationships = data.get("relationships", [])
        dm.extraction_strategy = data.get("extraction_strategy", "single_sheet")
        dm.confidence = data.get("confidence", 1.0)
        return dm


class ProductRecord:
    """单条产品记录"""
    def __init__(self, hscode=None, product_name=None, quantity=None, unit_price=None, material=None):
        self.hscode = str(hscode) if hscode else None
        self.product_name = str(product_name) if product_name else None
        self.quantity = str(quantity) if quantity else None
        self.unit_price = str(unit_price) if unit_price else None
        self.material = str(material) if material else None

    def to_dict(self):
        return {
            "HSCODE": self.hscode or "",
            "品名": self.product_name or "",
            "件数": self.quantity or "",
            "单价": self.unit_price or "",
            "材质": self.material or ""
        }

    def __repr__(self):
        return f"ProductRecord(hscode={self.hscode}, product_name={self.product_name}, quantity={self.quantity}, unit_price={self.unit_price}, material={self.material})"
