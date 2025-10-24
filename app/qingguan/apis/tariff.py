from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, Field
from typing import Dict, Optional, List
from datetime import datetime
from bson import ObjectId
from app.db_mongo import get_session

# 创建/更新关税请求
class CreateTariffRequest(BaseModel):
    start_land: str = Field(..., example="USA", description="出发国家代码")
    destination: str = Field(..., example="CN", description="目的国家代码")
    category: List[str] = Field(..., example=["电子产品", "服装"], description="产品大类列表")
    tariff_type: str = Field(..., example="加征_301", description="关税类型")
    tariff_rate: float = Field(..., ge=0, le=1, example=0.25, description="加征税率")
    description: Optional[str] = Field(None, description="备注说明")


# 关税响应模型
class TariffResponse(BaseModel):
    id: Optional[str] = Field(None, description="关税ID")
    start_land: str = Field(..., description="出发国家")
    destination: str = Field(..., description="目的国家")
    category: List[str] = Field(..., description="产品大类")
    tariff_type: str = Field(..., description="关税类型")
    tariff_rate: float = Field(..., description="加征税率")
    description: Optional[str] = Field(None)
    created_at: str = Field(..., description="创建时间")
    updated_at: str = Field(..., description="更新时间")


# 查询条件
class TariffQueryRequest(BaseModel):
    start_land: Optional[str] = Field(None, example="USA")
    destination: Optional[str] = Field(None, example="CN")
    category: Optional[str] = Field(None, description="查询单个产品大类")
    tariff_type: Optional[str] = Field(None)


# 创建路由
tariff_router = APIRouter(
    prefix="/tariff",
    tags=["加征关税管理"],
)


@tariff_router.post(
    "/create",
    summary="创建关税规则",
    response_model=TariffResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_tariff(request: CreateTariffRequest, session=Depends(get_session)):
    """
    创建新的关税规则
    
    示例：USA 对中国进口的电子产品征收 25% 的 301 关税
    """
    db = session
    
    # 检查是否已存在相同的关税规则
    existing_tariff = db.tariffs.find_one({
        "start_land": request.start_land.upper(),
        "destination": request.destination.upper(),
        "category": {"$all": request.category},  # 检查是否包含所有类别
        "tariff_type": request.tariff_type
    })
    
    if existing_tariff:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="关税规则已存在"
        )
    
    now = datetime.now().isoformat()
    
    tariff_data = {
        "start_land": request.start_land.upper(),
        "destination": request.destination.upper(),
        "category": request.category,
        "tariff_type": request.tariff_type,
        "tariff_rate": request.tariff_rate,
        "description": request.description,
        "created_at": now,
        "updated_at": now
    }
    
    result = db.tariffs.insert_one(tariff_data)
    tariff_data["id"] = str(result.inserted_id)
    
    return tariff_data


@tariff_router.get(
    "/{tariff_id}",
    summary="获取单个关税规则",
    response_model=TariffResponse
)
async def get_tariff(tariff_id: str, session=Depends(get_session)):
    """获取指定ID的关税规则"""
    db = session
    
    try:
        tariff = db.tariffs.find_one({"_id": ObjectId(tariff_id)})
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="无效的关税ID格式"
        )
    
    if not tariff:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"关税规则 {tariff_id} 不存在"
        )
    
    tariff["id"] = str(tariff["_id"])
    return tariff


@tariff_router.put(
    "/{tariff_id}",
    summary="更新关税规则",
    response_model=TariffResponse
)
async def update_tariff(tariff_id: str, request: CreateTariffRequest, session=Depends(get_session)):
    """更新关税规则"""
    db = session
    
    try:
        tariff_oid = ObjectId(tariff_id)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="无效的关税ID格式"
        )
    
    existing_tariff = db.tariffs.find_one({"_id": tariff_oid})
    if not existing_tariff:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"关税规则 {tariff_id} 不存在"
        )
    
    now = datetime.now().isoformat()
    
    update_data = {
        "start_land": request.start_land.upper(),
        "destination": request.destination.upper(),
        "category": request.category,
        "tariff_type": request.tariff_type,
        "tariff_rate": request.tariff_rate,
        "description": request.description,
        "updated_at": now
    }
    
    db.tariffs.update_one(
        {"_id": tariff_oid},
        {"$set": update_data}
    )
    
    updated_tariff = db.tariffs.find_one({"_id": tariff_oid})
    updated_tariff["id"] = str(updated_tariff["_id"])
    
    return updated_tariff


@tariff_router.delete(
    "/{tariff_id}",
    summary="删除关税规则",
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_tariff(tariff_id: str, session=Depends(get_session)):
    """删除关税规则"""
    db = session
    
    try:
        tariff_oid = ObjectId(tariff_id)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="无效的关税ID格式"
        )
    
    result = db.tariffs.delete_one({"_id": tariff_oid})
    
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"关税规则 {tariff_id} 不存在"
        )
    
    return None


@tariff_router.post(
    "/query",
    summary="查询关税规则",
    response_model=List[TariffResponse]
)
async def query_tariff(request: TariffQueryRequest, session=Depends(get_session)):
    """
    查询关税规则（支持精确查询，条件为空时不过滤）
    
    示例：查询 USA 对 CN 的所有关税规则
    """
    db = session
    
    # 构建查询条件
    query = {}
    
    if request.start_land:
        query["start_land"] = request.start_land.upper()
    
    if request.destination:
        query["destination"] = request.destination.upper()
    
    if request.category:
        # 查询category列表中包含该类别的记录
        query["category"] = {"$in": [request.category]}
    
    if request.tariff_type:
        query["tariff_type"] = request.tariff_type
    
    results = list(db.tariffs.find(query).sort("created_at", -1))
    
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="未找到匹配的关税规则"
        )
    
    for tariff in results:
        tariff["id"] = str(tariff["_id"])
    
    return results


@tariff_router.get(
    "/list/all",
    summary="获取所有关税规则",
    response_model=List[TariffResponse]
)
async def list_all_tariff(session=Depends(get_session)):
    """获取所有关税规则"""
    db = session
    
    results = list(db.tariffs.find({}).sort("created_at", -1))
    
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="暂无关税规则数据"
        )
    
    for tariff in results:
        tariff["id"] = str(tariff["_id"])
    
    return results


@tariff_router.post(
    "/batch/create",
    summary="批量创建关税规则",
    status_code=status.HTTP_201_CREATED
)
async def batch_create_tariff(requests: List[CreateTariffRequest], session=Depends(get_session)):
    """批量创建关税规则"""
    db = session
    
    results = []
    errors = []
    
    for i, req in enumerate(requests):
        try:
            # 检查是否已存在
            existing_tariff = db.tariffs.find_one({
                "start_land": req.start_land.upper(),
                "destination": req.destination.upper(),
                "category": {"$all": req.category},
                "tariff_type": req.tariff_type
            })
            
            if existing_tariff:
                errors.append({
                    "index": i,
                    "error": "关税规则已存在",
                    "data": req.dict()
                })
                continue
            
            now = datetime.now().isoformat()
            
            tariff_data = {
                "start_land": req.start_land.upper(),
                "destination": req.destination.upper(),
                "category": req.category,
                "tariff_type": req.tariff_type,
                "tariff_rate": req.tariff_rate,
                "description": req.description,
                "created_at": now,
                "updated_at": now
            }
            
            result = db.tariffs.insert_one(tariff_data)
            tariff_data["id"] = str(result.inserted_id)
            results.append(tariff_data)
        
        except Exception as e:
            errors.append({
                "index": i,
                "error": str(e),
                "data": req.dict()
            })
    
    return {
        "success_count": len(results),
        "failed_count": len(errors),
        "data": results,
        "errors": errors
    }


@tariff_router.get(
    "/by/route/{start_land}/{destination}/{category}",
    summary="获取特定路线的关税",
    response_model=List[TariffResponse]
)
async def get_tariff_by_route(
    start_land: str,
    destination: str,
    category: str,
    session=Depends(get_session)
):
    """
    获取特定路线的所有关税规则
    
    示例：/api/tariff/by/route/USA/CN/电子产品
    """
    db = session
    
    results = list(db.tariffs.find({
        "start_land": start_land.upper(),
        "destination": destination.upper(),
        "category": {"$in": [category]}  # 查询category列表中包含该类别的记录
    }).sort("created_at", -1))
    
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未找到 {start_land} -> {destination} -> {category} 的关税规则"
        )
    
    for tariff in results:
        tariff["id"] = str(tariff["_id"])
    
    return results


@tariff_router.get(
    "/by/country/{start_land}/{destination}",
    summary="获取两个国家间的所有关税",
    response_model=List[TariffResponse]
)
async def get_tariff_by_country(
    start_land: str,
    destination: str,
    session=Depends(get_session)
):
    """
    获取两个国家间的所有关税规则
    
    示例：/api/tariff/by/country/USA/CN
    """
    db = session
    
    results = list(db.tariffs.find({
        "start_land": start_land.upper(),
        "destination": destination.upper()
    }).sort("created_at", -1))
    
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"未找到 {start_land} -> {destination} 的关税规则"
        )
    
    for tariff in results:
        tariff["id"] = str(tariff["_id"])
    
    return results





@tariff_router.get(
    "/tariff-types/list",
    summary="获取所有关税类型",
    response_model=List[str]
)
async def get_all_tariff_types(session=Depends(get_session)):
    """获取系统中所有的关税类型"""
    db = session
    
    tariff_types = db.tariffs.distinct("tariff_type")
    
    if not tariff_types:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="暂无关税类型数据"
        )
    
    return sorted(tariff_types)
