from fastapi import APIRouter, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os
import pandas as pd
import json
import numpy as np

router = APIRouter()

@router.get("/excel-preview", response_class=HTMLResponse)
async def get_excel_preview_page():
    # 读取HTML模板
    with open("static/sheet/index.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    return html_content

@router.get("/luckysheet-preview", response_class=HTMLResponse)
async def get_luckysheet_preview_page():
    # 读取Luckysheet HTML模板
    with open("static/luckysheet/index.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    return html_content

@router.post("/upload-excel-luckysheet")
async def upload_excel_luckysheet(file: UploadFile = File(...)):
    # 读取Excel文件
    df = pd.read_excel(file.file)
    
    # 获取列名和数据
    columns = df.columns.tolist()
    values = df.values.tolist()
    
    # 处理数据，确保所有的值都是字符串格式
    processed_values = []
    for row in values:
        processed_row = []
        for value in row:
            if pd.isna(value):
                processed_row.append("")
            elif isinstance(value, (float, np.float64)):
                # 如果是整数形式的浮点数，转换为整数
                if value.is_integer():
                    processed_row.append(int(value))
                else:
                    processed_row.append(value)
            else:
                processed_row.append(value)
        processed_values.append(processed_row)

    # 构建celldata（单元格数据）
    celldata = []
    # 添加表头
    for col_index, col_name in enumerate(columns):
        celldata.append({
            "r": 0,
            "c": col_index,
            "v": {
                "v": str(col_name),
                "ct": {"fa": "@", "t": "s"},
                "m": str(col_name)
            }
        })
    
    # 添加数据
    for row_index, row in enumerate(processed_values, start=1):
        for col_index, value in enumerate(row):
            cell_value = {
                "r": row_index,
                "c": col_index,
                "v": {
                    "v": value,
                    "ct": {"fa": "General", "t": "g"},
                    "m": str(value)
                }
            }
            celldata.append(cell_value)

    # 计算表格范围
    row_count = len(processed_values) + 1  # +1 for header
    col_count = len(columns)

    # 构建sheet数据
    sheet_data = {
        "name": "Sheet1",  # sheet页名称
        "color": "",  # sheet颜色
        "status": 1,  # sheet是否激活，1激活，0未激活
        "order": 0,  # sheet的下标
        "index": 0,  # sheet的索引
        "celldata": celldata,  # 单元格数据
        "config": {
            "merge": {},  # 合并单元格
            "rowlen": {},  # 表格行高
            "columnlen": {},  # 表格列宽
            "rowhidden": {},  # 隐藏行
            "colhidden": {},  # 隐藏列
            "borderInfo": []  # 边框信息
        },
        "row": max(10, row_count),  # 行数
        "column": max(8, col_count),  # 列数
        "defaultRowHeight": 25,  # 默认行高
        "defaultColWidth": 100,  # 默认列宽
        "luckysheet_select_save": [{  # 选中区域
            "row": [0, row_count - 1],
            "column": [0, col_count - 1]
        }],
        "scrollLeft": 0,  # 左右滚动条位置
        "scrollTop": 0,  # 上下滚动条位置
        "zoomRatio": 1,  # 缩放比例
        "showGridLines": 1,  # 是否显示网格线
        "defaultFontSize": 11  # 默认字体大小
    }

    return [sheet_data]  # Luckysheet需要一个数组格式的数据

# 挂载静态文件
def mount_static_files(app):
    app.mount("/static", StaticFiles(directory="static"), name="static") 