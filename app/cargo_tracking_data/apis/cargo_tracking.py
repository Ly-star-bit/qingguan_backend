from fastapi import APIRouter, Depends
from typing import Dict, List, Optional

import openpyxl
from app.db_mongo import get_session
from bson import ObjectId
from fastapi.responses import JSONResponse, StreamingResponse
from loguru import logger
import pandas as pd
from io import BytesIO

cargo_tracking_router = APIRouter(prefix="/cargo_tracking", tags=["货物跟踪"])

@cargo_tracking_router.get("/list", summary="获取货物跟踪列表")
async def get_cargo_tracking_list(
    customer_name: Optional[str] = None,
    current_status: Optional[str] = None, 
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
    session = Depends(get_session)
):
    """获取货物跟踪列表
    
    Args:
        customer_name: 客户名称,可选参数
        current_status: 当前状态,可选参数
        start_date: 提货开始时间,可选参数
        end_date: 提货结束时间,可选参数
        page: 当前页码,默认1
        page_size: 每页数量,默认10
        session: 数据库会话
    
    Returns:
        货物跟踪列表数据,包含分页信息
    """
    try:
        db = session
        # 构建查询条件
        query = {}
        if customer_name:
            query["客户名称"] = {"$regex": customer_name, "$options": "i"}  # i表示不区分大小写
        if current_status:
            if current_status == "未签收":
                query["当前状态"] = {"$ne": "已签收"}
            else:
                query["当前状态"] = {"$regex": current_status, "$options": "i"}  # i表示不区分大小写
        if start_date and end_date:
            if customer_name == "FSQP-佛山七派-SZ":
                query["提货时间"] = {
                    "$gte": f"{start_date} 00:00:00",
                    "$lte": f"{end_date} 23:59:59"
                }
            elif customer_name == "HKLMT-香港兰玛特-SZ":
                query["收货时间"] = {
                    "$gte": f"{start_date} 00:00:00", 
                    "$lte": f"{end_date} 23:59:59"
                }
        # 计算总数
        logger.info(f"查询条件: {query}")
        total = db.cargo_tracking_data.count_documents(query)
        
        # 分页处理
        skip = (page - 1) * page_size
        
        # 执行查询    
        cursor = db.cargo_tracking_data.find(query).skip(skip).limit(page_size)
        result = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])  # ObjectId转为字符串
            
            # 根据不同客户进行字段排序
            if customer_name == "FSQP-佛山七派-SZ":
                sorted_doc = {key: doc.get(key, "") for key in [
                    '客户名称', '提货时间', '开船/起飞', '主单号', 'A/S单号', 
                    '收货地', '件数', 'FBA号', '客户内部号', '预计到港时间',
                    '派送方式', '机场提货/港口提柜', '计划派送时间', '实际送达',
                    '卡车追踪码/快递单号', '时效（按15天/22天计算）', 'POD', '上架情况', 
                ]}
                # 添加其他字段
                for key, value in doc.items():
                    if key not in sorted_doc:
                        sorted_doc[key] = value
                result.append(sorted_doc)
            elif customer_name == "HKLMT-香港兰玛特-SZ":
                # 处理收货时间格式
               
                        
                sorted_doc = {key: doc.get(key, "") for key in [
                    '客户名称', '月份', '收货时间', '备货单号', '起运地',
                    '目的港', '提单号', 'A/S单号', '派送方式',
                    '箱数', '快递单号', '子单号', 'FBA号', '收货地',
                    '是否国内查验', '报关放行时间', '上航班时间', '航班抵达时间',
                    '清关放行时间', '当地提取时间', '当前状态', '签收时间', '时效',
                    '是否进口查验', '异常备注', '航班号'
                ]}
                # 添加其他字段
                for key, value in doc.items():
                    if key not in sorted_doc:
                        sorted_doc[key] = value
                result.append(sorted_doc)
            else:
                result.append(doc)
            
        return {
            "code": 200, 
            "data": {
                "list": result,
                "pagination": {
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (total + page_size - 1) // page_size
                }
            },
            "message": "查询成功"
        }
        
    except Exception as e:
        logger.error(f"获取货物跟踪列表失败: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"code": 500, "message": f"获取货物跟踪列表失败: {str(e)}"}
        )

@cargo_tracking_router.get("/export_excel", summary="导出货物跟踪数据为Excel")
async def export_cargo_tracking(
    customer_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    session = Depends(get_session)
):
    """导出货物跟踪数据
    
    Args:
        customer_name: 客户名称,可选参数
        start_date: 开始日期,可选参数
        end_date: 结束日期,可选参数
        session: 数据库会话
    
    Returns:
        Excel文件下载
    """
    try:
        db = session
        query = {}
        if customer_name:
            query["客户名称"] = {"$regex": customer_name, "$options": "i"}
            
        # 添加日期筛选
        if start_date and end_date:
            if customer_name == "FSQP-佛山七派-SZ":
                query["提货时间"] = {
                    "$gte": f"{start_date}",
                    "$lte": f"{end_date}"
                }
            elif customer_name == "HKLMT-香港兰玛特-SZ":
                query["收货时间"] = {
                    "$gte": f"{start_date}", 
                    "$lte": f"{end_date}"
                }
                
        logger.info(f"导出查询条件: {query}")
        cursor = db.cargo_tracking_data.find(query)
        data = list(cursor)
        
        # 生成文件名（包含日期范围）
        filename = "cargo_tracking"
        if start_date and end_date:
            filename = f"cargo_tracking_{start_date}_to_{end_date}"
        if customer_name:
            filename = f"{filename}_{customer_name}"
        filename = f"{filename}.xlsx"
        
        # 如果是FSQP-佛山七派-SZ客户,使用特定列顺序
        if customer_name == "FSQP-佛山七派-SZ":
            columns = [
                '客户名称', '提货时间', '开船/起飞', '主单号', 'A/S单号', 
                '收货地', '件数', 'FBA号', '客户内部号', '预计到港时间',
                '派送方式', '机场提货/港口提柜', '计划派送时间', '实际送达',
                '卡车追踪码/快递单号', '时效（按15天/22天计算）', 'POD', '上架情况'
            ]
            
            
                    
            # 1. 根据'FBA号'展开数据
            expanded_data = []
            for item in data:
                # 确保fba_numbers_str是字符串类型以便进行分割
                fba_numbers_str = item.get('FBA号', '')
                if not isinstance(fba_numbers_str, str):
                    fba_numbers_str = str(fba_numbers_str)
                
                # 按逗号分割并去除空白字符，过滤掉空字符串
                fba_list = [fba.strip() for fba in fba_numbers_str.split(',') if fba.strip()]
                
                if not fba_list:
                    # 如果没有FBA号，则将该行添加一次，FBA号为空
                    new_row = item.copy()
                    new_row['FBA号'] = ''
                    expanded_data.append(new_row)
                else:
                    for fba_num in fba_list:
                        new_row = item.copy()
                        new_row['FBA号'] = fba_num
                        expanded_data.append(new_row)

            # 2. 从展开后的数据创建DataFrame
            df = pd.DataFrame(expanded_data, columns=columns)

            # 3. 在内存中创建Excel文件
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
                
                # 获取工作表以进行格式化
                worksheet = writer.sheets['Sheet1']
                
                # 定义对齐样式
                alignment = openpyxl.styles.Alignment(horizontal='center', vertical='center')
                
                # 查找'FBA号'列的索引（1-based）
                try:
                    fba_col_idx = columns.index('FBA号') + 1
                except ValueError:
                    fba_col_idx = -1  # 如果列名不正确，则不应发生

                # 首先对所有单元格应用居中对齐
                for row in worksheet.iter_rows():
                    for cell in row:
                        cell.alignment = alignment

                # 4. 合并源自同一记录的行的单元格
                current_excel_row = 2  # Excel行是1-based，且我们有表头
                for item in data:
                    fba_numbers_str = item.get('FBA号', '')
                    if not isinstance(fba_numbers_str, str):
                        fba_numbers_str = str(fba_numbers_str)
                    
                    fba_list = [fba.strip() for fba in fba_numbers_str.split(',') if fba.strip()]
                    
                    num_expanded_rows = len(fba_list) if fba_list else 1
                    
                    if num_expanded_rows > 1:
                        start_row = current_excel_row
                        end_row = current_excel_row + num_expanded_rows - 1
                        
                        for col_idx in range(1, len(columns) + 1):
                            # 跳过'FBA号'列
                            if col_idx != fba_col_idx:
                                worksheet.merge_cells(
                                    start_row=start_row,
                                    start_column=col_idx,
                                    end_row=end_row,
                                    end_column=col_idx
                                )
                    
                    current_excel_row += num_expanded_rows
                
        elif customer_name == "HKLMT-香港兰玛特-SZ":
            # 处理收货时间格式
            for doc in data:
                if '收货时间' in doc and doc['收货时间']:
                    try:
                        # 假设收货时间是字符串格式
                        date_parts = doc['收货时间'].split(' ')[0]  # 只保留年月日部分
                        doc['收货时间'] = date_parts
                    except:
                        pass  # 如果转换失败,保持原值
            
            columns = [
                '客户名称', '月份', '收货时间', '备货单号', '起运地',
                '目的港', '提单号', 'A/S单号', '派送方式',
                '箱数', '快递单号', '子单号', 'FBA号', '收货地',
                '是否国内查验', '报关放行时间', '上航班时间', '航班抵达时间',
                '清关放行时间', '当地提取时间', '当前状态', '签收时间', '时效',
                '是否进口查验', '异常备注', '航班号', 
            ]
            df = pd.DataFrame(data)[columns]
            
            # 创建Excel文件
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
                
        else:
            df = pd.DataFrame(data)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            
        output.seek(0)
        
        headers = {
            'Content-Disposition': f'attachment; filename="{filename}"'
        }
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            # headers=headers
        )
        
    except Exception as e:
        logger.error(f"导出货物跟踪数据失败: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"code": 500, "message": f"导出货物跟踪数据失败: {str(e)}"}
        )

@cargo_tracking_router.get("/customers", summary="获取所有客户名称列表")
async def get_all_customers(session = Depends(get_session)):
    """获取所有客户名称列表
    
    Args:
        session: 数据库会话
    
    Returns:
        所有客户名称列表
    """
    try:
        db = session
        # 使用distinct获取所有不同的客户名称
        customers = db.cargo_tracking_data.distinct("客户名称")
        
        return {
            "code": 200,
            "data": customers,
            "message": "获取客户名称列表成功"
        }
        
    except Exception as e:
        logger.error(f"获取客户名称列表失败: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"code": 500, "message": f"获取客户名称列表失败: {str(e)}"}
        )
