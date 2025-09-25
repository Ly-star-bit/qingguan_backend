from fastapi import APIRouter, Depends, HTTPException
import pandas as pd
import numpy as np
from fastapi import UploadFile, File, Query
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from app.db_mongo import get_session
from typing import Optional, List
from datetime import datetime
from bson import ObjectId
import os
from pathlib import Path
from app.utils import MinioClient
import io

price_card_router = APIRouter(tags=["price_card"],prefix="/price_card")

def clean_record(record):
    """清理记录中的特殊值"""
    for key, value in record.items():
        if isinstance(value, float):
            if np.isnan(value) or np.isinf(value):
                record[key] = None
        elif pd.isna(value):
            record[key] = None
    return record

@price_card_router.post("/upload_price_card", summary="上传价格卡Excel文件并解析入库")
async def upload_price_card(file: UploadFile = File(...), session = Depends(get_session)):
    try:
        # 先保存文件到本地和MinIO
        upload_dir = Path("uploads/price_cards")
        upload_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = upload_dir / file.filename
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
            
        # 上传到MinIO
        try:
            minio_client = MinioClient(
                os.getenv("MINIO_ENDPOINT"),
                os.getenv("MINIO_ACCESS_KEY"),
                os.getenv("MINIO_SECRET_KEY"),
                os.getenv("MINIO_BUCKET_NAME"),
                secure=False,
            )
            minio_client.connect()
            minio_client.upload_file(str(file_path), f"price_cards/{file.filename}")
        except Exception as e:
            # 如果MinIO上传失败，删除本地文件并抛出异常
            if file_path.exists():
                file_path.unlink()
            raise HTTPException(status_code=400, detail=f"MinIO上传失败: {str(e)}")

        # 重新打开文件以读取Excel内容
        df_dict = pd.read_excel(str(file_path), sheet_name=["美森拼箱-KG", "以星拼箱-KG", "合德拼箱-kg", "Cosco拼箱-KG"])
        
        result = {}
        db = session
        all_records = []  # 用于存储所有记录

        # 处理美森拼箱-KG sheet
        meisen_df = df_dict["美森拼箱-KG"]
        # 获取列名
        kuaidi_columns = meisen_df.iloc[4].values
        kapai_columns = meisen_df.iloc[15].values

        # 使用列名重命名数据
        kuaidi_df = meisen_df.iloc[5:12].copy()
        kuaidi_df.iloc[:,0] = kuaidi_df.iloc[:,0].fillna(method='ffill')
        kuaidi_df.columns = kuaidi_columns
        # 去掉列名中存在nan的列
        kuaidi_df = kuaidi_df.loc[:, kuaidi_df.columns.notna()]
        records = kuaidi_df[kuaidi_df['亚马逊仓'].notna()].to_dict(orient="records")
        for record in records:
            record = clean_record(record)  # 清理特殊值
            record['时间'] = Path(file.filename).stem
            record['类型'] = '美森快递'
            record['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            all_records.append(record)
        result["美森快递"] = records

        kapai_df = meisen_df.iloc[15:30].copy()
        kapai_df.columns = kapai_columns
        kapai_df.iloc[:,0] = kapai_df.iloc[:,0].fillna(method='ffill')
        # 去掉列名中存在nan的列
        kapai_df = kapai_df.loc[:, kapai_df.columns.notna()]
        records = kapai_df[kapai_df['亚马逊仓'].notna() & (kapai_df['亚马逊仓'] != '亚马逊仓')].to_dict(orient="records")
        for record in records:
            record = clean_record(record)  # 清理特殊值
            record['时间'] = Path(file.filename).stem
            record['类型'] = '美森卡派'
            record['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            all_records.append(record)
        result["美森卡派"] = records

        # 处理以星拼箱-KG sheet
        yixing_df = df_dict["以星拼箱-KG"]
        yixingkuaidi_columns = yixing_df.iloc[4].values
        yixing_kuaidi_df = yixing_df.iloc[5:10].copy()
        yixing_kuaidi_df.columns = yixingkuaidi_columns
        yixing_kuaidi_df.iloc[:,0] = yixing_kuaidi_df.iloc[:,0].fillna(method='ffill')
        yixing_kuaidi_df = yixing_kuaidi_df.loc[:, yixing_kuaidi_df.columns.notna()]
        records = yixing_kuaidi_df[yixing_kuaidi_df['亚马逊仓'].notna()].to_dict(orient="records")
        for record in records:
            record = clean_record(record)  # 清理特殊值
            record['时间'] = Path(file.filename).stem
            record['类型'] = '以星快递'
            record['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            all_records.append(record)
        result["以星快递"] = records

        # 处理合德拼箱-kg sheet
        hede_df = df_dict["合德拼箱-kg"]
        hedekuaidi_columns = hede_df.iloc[4].values
        hede_kuaidi_df = hede_df.iloc[5:10].copy()
        hede_kuaidi_df.columns = hedekuaidi_columns
        hede_kuaidi_df.iloc[:,0] = hede_kuaidi_df.iloc[:,0].fillna(method='ffill')
        hede_kuaidi_df = hede_kuaidi_df.loc[:, hede_kuaidi_df.columns.notna()]
        records = hede_kuaidi_df[hede_kuaidi_df['亚马逊仓'].notna()].to_dict(orient="records")
        for record in records:
            record = clean_record(record)  # 清理特殊值
            record['时间'] = Path(file.filename).stem
            record['类型'] = '合德快递'
            record['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            all_records.append(record)
        result["合德快递"] = records

        hedekapai_columns = hede_df.iloc[11].values
        hede_kapai_df = hede_df.iloc[12:18].copy()
        hede_kapai_df.columns = hedekapai_columns
        hede_kapai_df.iloc[:,0] = hede_kapai_df.iloc[:,0].fillna(method='ffill')
        hede_kapai_df = hede_kapai_df.loc[:, hede_kapai_df.columns.notna()]
        records = hede_kapai_df[hede_kapai_df['亚马逊仓'].notna() & (hede_kapai_df['亚马逊仓'] != '亚马逊仓')].to_dict(orient="records")
        for record in records:
            record = clean_record(record)  # 清理特殊值
            record['时间'] = Path(file.filename).stem
            record['类型'] = '合德卡派'
            record['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            all_records.append(record)
        result["合德卡派"] = records

        # 处理Cosco拼箱-KG sheet
        cosco_df = df_dict["Cosco拼箱-KG"]
        cosco_kuaidi_columns = cosco_df.iloc[4].values
        cosco_kuaidi_df = cosco_df.iloc[5:10].copy()
        cosco_kuaidi_df.columns = cosco_kuaidi_columns
        cosco_kuaidi_df.iloc[:,0] = cosco_kuaidi_df.iloc[:,0].fillna(method='ffill')
        cosco_kuaidi_df = cosco_kuaidi_df.loc[:, cosco_kuaidi_df.columns.notna()]
        records = cosco_kuaidi_df[cosco_kuaidi_df['亚马逊仓'].notna()].to_dict(orient="records")
        for record in records:
            record = clean_record(record)  # 清理特殊值
            record['时间'] = Path(file.filename).stem
            record['类型'] = 'Cosco快递'
            record['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            all_records.append(record)
        result["Cosco快递"] = records

        cosco_kapai_columns = cosco_df.iloc[11].values
        cosco_kapai_df = cosco_df.iloc[12:18].copy()
        cosco_kapai_df.columns = cosco_kapai_columns
        cosco_kapai_df.iloc[:,0] = cosco_kapai_df.iloc[:,0].fillna(method='ffill')
        cosco_kapai_df = cosco_kapai_df.loc[:, cosco_kapai_df.columns.notna()]
        records = cosco_kapai_df[cosco_kapai_df['亚马逊仓'].notna() & (cosco_kapai_df['亚马逊仓'] != '亚马逊仓')].to_dict(orient="records")
        for record in records:
            record = clean_record(record)  # 清理特殊值
            record['时间'] = Path(file.filename).stem
            record['类型'] = 'Cosco卡派'
            record['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            all_records.append(record)
        result["Cosco卡派"] = records

        # 批量插入前检查数据是否已存在
        if all_records:
            try:
                # 删除同一时间的旧数据
                db.price_cards.delete_many({"时间": Path(file.filename).stem})
                # 批量插入新数据
                insert_result = db.price_cards.insert_many(all_records, ordered=False)
                
                # 处理返回结果中的ObjectId
                processed_result = {}
                for key, records in result.items():
                    processed_records = []
                    for record in records:
                        if '_id' in record and isinstance(record['_id'], ObjectId):
                            record['_id'] = str(record['_id'])
                        processed_records.append(record)
                    processed_result[key] = processed_records

                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "success",
                        "message": f"成功插入{len(insert_result.inserted_ids)}条记录",
                        "data": {
                            "result": processed_result,
                            "local_path": str(file_path),
                            "minio_path": f"price_cards/{file.filename}"
                        }
                    }
                )
            except Exception as e:
                # 如果数据插入失败，删除已上传的文件
                if file_path.exists():
                    file_path.unlink()
                try:
                    minio_client.delete_file(f"price_cards/{file.filename}")
                except:
                    pass  # 忽略MinIO删除失败的错误
                raise HTTPException(status_code=400, detail=f"数据插入失败: {str(e)}")

    except Exception as e:
        # 确保在任何错误情况下都清理文件
        if 'file_path' in locals() and file_path.exists():
            file_path.unlink()
        raise HTTPException(status_code=400, detail=f"处理文件时发生错误: {str(e)}")

@price_card_router.get("/query_price_cards", summary="分页查询价格卡数据，支持多条件筛选")
async def query_price_cards(
    page: int = Query(1, description="页码，从1开始"),
    page_size: int = Query(10, description="每页数量"),
    time_start: Optional[str] = Query(None, description="开始时间，格式：YYYY-MM-DD"),
    time_end: Optional[str] = Query(None, description="结束时间，格式：YYYY-MM-DD"),
    types: Optional[str] = Query(None, description="类型列表，例如：美森快递,美森卡派"),
    sort_field: Optional[str] = Query(None, description="排序字段"),
    sort_order: Optional[int] = Query(1, description="排序方式：1 升序，-1 降序"),
    amazon_warehouse: Optional[str] = Query(None, description="亚马逊仓库"),
    session = Depends(get_session)
):
    try:
        # 构建查询条件
        query = {}
        
        # 处理时间范围
        if time_start or time_end:
            query["时间"] = {}
            if time_start:
                query["时间"]["$gte"] = time_start
            if time_end:
                query["时间"]["$lte"] = time_end
                
        # 处理类型筛选
        if types:
            type_list = types.split(",")
            query["类型"] = {"$in": type_list}
            
        # 处理亚马逊仓库筛选
        if amazon_warehouse:
            query["亚马逊仓"] = amazon_warehouse
            
        # 构建排序条件
        sort_condition = []
        if sort_field:
            sort_condition.append((sort_field, sort_order))
        else:
            # 默认按创建时间倒序
            sort_condition.append(("created_at", -1))
        
        # 计算总数
        total = session.price_cards.count_documents(query)
        
        # 获取分页数据
        skip = (page - 1) * page_size
        cursor = session.price_cards.find(
            query,
        )
        
        # 应用排序
        if sort_condition:
            cursor = cursor.sort(sort_condition)
            
        # 应用分页
        cursor = cursor.skip(skip).limit(page_size)
        
        # 获取数据并处理ObjectId
        records = []
        for record in cursor:
            record["_id"] = str(record["_id"])
            records.append(record)
            
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": {
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "records": records
                }
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"查询数据时发生错误: {str(e)}")

@price_card_router.get("/get_types", summary="获取所有可用的价格卡类型列表")
async def get_types(session = Depends(get_session)):
    """获取所有可用的类型列表"""
    try:
        types =  session.price_cards.distinct("类型")
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": types
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"获取类型列表时发生错误: {str(e)}")

@price_card_router.delete("/delete_price_cards/{record_id}", summary="删除指定ID的价格卡记录")
async def delete_price_card(
    record_id: str,
    session = Depends(get_session)
):
    """删除指定的价格卡记录"""
    try:
        result =  session.price_cards.delete_one({"_id": ObjectId(record_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="记录不存在")
            
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "记录已成功删除"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"删除记录时发生错误: {str(e)}")

@price_card_router.get("/get_amazon_warehouses", summary="获取所有亚马逊仓库名称列表")
async def get_amazon_warehouses(session = Depends(get_session)):
    """获取所有亚马逊仓库列表"""
    try:
        warehouses =  session.price_cards.distinct("亚马逊仓")
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": warehouses
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"获取仓库列表时发生错误: {str(e)}")

@price_card_router.get("/get_files_by_time_range", summary="根据时间范围获取所有价格卡文件信息")
async def get_files_by_time_range(
    time_start: Optional[str] = Query(None, description="开始时间，格式：YYYY-MM-DD"),
    time_end: Optional[str] = Query(None, description="结束时间，格式：YYYY-MM-DD"),
    session = Depends(get_session)
):
    """获取指定时间范围内的所有价格卡文件"""
    try:
        # 构建查询条件
        query = {}
        if time_start or time_end:
            query["时间"] = {}
            if time_start:
                query["时间"]["$gte"] = time_start
            if time_end:
                query["时间"]["$lte"] = time_end

        # 从数据库中获取不重复的文件名
        distinct_files = session.price_cards.distinct("时间", query)
        
        # 获取本地和MinIO中的文件信息
        files_info = []
        upload_dir = Path("uploads/price_cards")
        
        # 初始化MinIO客户端
        minio_client = MinioClient(
            os.getenv("MINIO_ENDPOINT"),
            os.getenv("MINIO_ACCESS_KEY"),
            os.getenv("MINIO_SECRET_KEY"),
            os.getenv("MINIO_BUCKET_NAME"),
            secure=False,
        )
        minio_client.connect()

        for filename in distinct_files:
            file_info = {
                "filename": filename,
                "local_exists": False,
                "minio_exists": False,
                "local_path": None,
                "minio_path": None
            }
            
            # 检查本地文件
            local_path = upload_dir / f"{filename}.xlsx"
            if local_path.exists():
                file_info["local_exists"] = True
                file_info["local_path"] = str(local_path)
            
            # 检查MinIO文件
            minio_path = f"price_cards/{filename}.xlsx"
            try:
                if minio_client.check_file_exists(minio_path):
                    file_info["minio_exists"] = True
                    file_info["minio_path"] = minio_path
            except:
                pass
            
            files_info.append(file_info)

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": {
                    "total": len(files_info),
                    "files": files_info
                }
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"获取文件列表时发生错误: {str(e)}")

@price_card_router.get("/download_price_card_xlsx", summary="下载指定的价格卡Excel文件")
async def download_price_card_xlsx(
    local_file_path: str = Query(None, description="本地文件路径"),
    minio_file_path: str = Query(None, description="MinIO文件路径"),
):
    """下载指定时间的价格卡文件"""
    try:
        # 如果提供了本地文件路径
        if local_file_path:
            file_path = Path(local_file_path)
            if not file_path.exists():
                raise HTTPException(status_code=404, detail="本地文件不存在")
            
            return FileResponse(
                path=file_path,
                filename=file_path.name,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
        # 如果提供了MinIO文件路径
        elif minio_file_path:
            try:
                minio_client = MinioClient(
                    os.getenv("MINIO_ENDPOINT"),
                    os.getenv("MINIO_ACCESS_KEY"),
                    os.getenv("MINIO_SECRET_KEY"),
                    os.getenv("MINIO_BUCKET_NAME"),
                    secure=False,
                )
                minio_client.connect()
                
                # 从MinIO获取文件数据
                file_data = minio_client.get_file(minio_file_path)
                
                # 创建一个流式响应
                return StreamingResponse(
                    io.BytesIO(file_data),
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={
                        "Content-Disposition": f"attachment; filename={Path(minio_file_path).name}"
                    }
                )
                
            except Exception as e:
                raise HTTPException(status_code=404, detail=f"从MinIO下载文件失败: {str(e)}")
        
        else:
            raise HTTPException(status_code=400, detail="必须提供本地文件路径或MinIO文件路径")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"下载文件时发生错误: {str(e)}")

@price_card_router.post("/update_invalid_data", summary="批量更新所有价格卡数据的失效时间")
async def update_invalid_data(session = Depends(get_session)):
    """更新价格卡数据的失效时间
    
    对所有数据按时间排序，每条数据的失效时间设置为下一个时间点
    最新的数据的失效时间设置为None
    """
    try:
        # 获取所有不重复的时间并排序
        times = sorted(session.price_cards.distinct("时间"))
        
        if not times:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": "没有找到需要更新的数据"
                }
            )
        
        # 遍历时间列表，更新每个时间点数据的失效时间
        update_count = 0
        for i, current_time in enumerate(times):
            # 获取下一个时间点（如果是最后一个时间点则为None）
            next_time = times[i + 1] if i < len(times) - 1 else None
            
            # 更新当前时间点的所有数据
            result = session.price_cards.update_many(
                {"时间": current_time},
                {"$set": {"失效时间": next_time}}
            )
            update_count += result.modified_count
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"成功更新{update_count}条记录的失效时间",
                "data": {
                    "total_times": len(times),
                    "updated_records": update_count
                }
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"更新失效时间时发生错误: {str(e)}")

@price_card_router.get("/get_west_usa_grouped", summary="获取美西仓库数据并按时间与失效时间分组")
async def get_west_usa_grouped(session = Depends(get_session)):
    """获取美西仓库的数据，并按时间和失效时间分组"""
    try:
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"亚马逊仓": {"$regex": "美西", "$options": "i"}},
                        {"亚马逊仓": "ONT8/LGB8/LAX9"},
                    ]
                }
            },
            {
                # 按时间和失效时间分组
                "$group": {
                    "_id": {
                        "时间": "$时间",
                        "失效时间": "$失效时间"
                    },
                    "数据": {"$push": "$$ROOT"}
                }
            },
            {
                # 按时间排序
                "$sort": {
                    "_id.时间": -1
                }
            }
        ]

        results = list(session.price_cards.aggregate(pipeline))

        # 处理返回数据，转换ObjectId为字符串
        processed_results = []
        for group in results:
            processed_group = {
                "时间": group["_id"]["时间"],
                "失效时间": group["_id"]["失效时间"],
            }
            for item in group["数据"]:
                if "卡派" in item["类型"]:
                    key = f"{item['类型']}_{item['产品']}_{item['亚马逊仓']}".replace(" ","")
                else:
                    key = f"{item['类型']}_{item['产品']}".replace(" ","")
                processed_group[key] = item["+300Kg"]
            
            processed_results.append(processed_group)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": {
                    "total_groups": len(processed_results),
                    "groups": processed_results
                }
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"获取美西仓库分组数据时发生错误: {str(e)}")



  
        
        
    

