from datetime import datetime
import json
from typing import Optional, List
import uuid
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlmodel import SQLModel, Field, and_, select, Session
from sse_starlette.sse import EventSourceResponse
from app.db import get_session
import pandas as pd
from pathlib import Path
import os
from app.skudetail.apis.morelink_func_test import (
    dahuo_upload_task_generator,
    login_task_generator,
    shouli_task_generator,
)
from app.utils import MinioClient
import zipfile
import shutil
from starlette.responses import FileResponse
from loguru import logger

# 定义路由
skudetail_router = APIRouter(tags=["SKU详情"])


# 定义数据模型
class SkuDetail(SQLModel, table=True):
    __tablename__ = "skudetail"

    Id: Optional[int] = Field(default=None, primary_key=True)
    oldsku: Optional[str] = None
    newsku: Optional[str] = None
    trackingnumber: Optional[str] = None
    boxno: Optional[str] = None
    labelurl: Optional[str] = None
    pcno: Optional[str] = None
    createtime: Optional[datetime] = None
    all_download_count: Optional[int] = 1
    remaining_download_count: Optional[int] = 1  # 新增字段：剩余下载次数


# 查询接口
@skudetail_router.get("/skudetail/", summary="获取SKU详情列表")
def get_skudetails(
    skip: int = 0,
    limit: int = 10,
    trackingnumber: Optional[str] = None,
    newsku: Optional[str] = None,
    boxno: Optional[str] = None,
    pcno: Optional[str] = None,
    type: Optional[str] = "gui",
    session: Session = Depends(get_session),
):
    try:
        # 基础查询
        query = select(SkuDetail)

        if trackingnumber:
            trackingnumber_list = [
                trackingnumber.strip() for trackingnumber in trackingnumber.split(",")
            ]
            query = query.where(SkuDetail.trackingnumber.in_(trackingnumber_list))

        if newsku:
            newsku_list = [sku.strip() for sku in newsku.split(",")]
            query = query.where(SkuDetail.oldsku.in_(newsku_list))

        if boxno:
            query = query.where(SkuDetail.boxno == boxno)

        if pcno:
            query = query.where(SkuDetail.pcno == pcno)

        # 使用group by进行去重
        if type == "gui":
            query = query.group_by(SkuDetail.trackingnumber, SkuDetail.pcno)
        elif type == "web":
            # web类型不需要分组,返回全部数据
            pass

        # 获取总数
        total = len(session.exec(query).all())

        # 分页
        query = query.offset(skip).limit(limit)
        results = session.exec(query).all()

        return {"total": total, "items": [result.dict() for result in results]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 创建接口
@skudetail_router.post("/skudetail/", summary="创建SKU详情")
def create_skudetail(skudetail: SkuDetail, session: Session = Depends(get_session)):
    try:
        skudetail.createtime = datetime.now()
        session.add(skudetail)
        session.commit()
        session.refresh(skudetail)

        return {"message": "创建成功", "id": skudetail.Id}

    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# 更新接口
@skudetail_router.put("/skudetail/{id}", summary="更新SKU详情")
def update_skudetail(
    id: int, skudetail: SkuDetail, session: Session = Depends(get_session)
):
    try:
        db_skudetail = session.get(SkuDetail, id)
        if not db_skudetail:
            raise HTTPException(status_code=404, detail="记录不存在")

        skudetail_data = skudetail.dict(exclude_unset=True)
        skudetail_data["createtime"] = datetime.now()

        for key, value in skudetail_data.items():
            setattr(db_skudetail, key, value)

        session.add(db_skudetail)
        session.commit()
        session.refresh(db_skudetail)

        return {"message": "更新成功"}

    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# 删除接口
@skudetail_router.delete("/skudetail/{id}", summary="删除SKU详情")
def delete_skudetail(id: int, session: Session = Depends(get_session)):
    try:
        skudetail = session.get(SkuDetail, id)
        if not skudetail:
            raise HTTPException(status_code=404, detail="记录不存在")

        session.delete(skudetail)
        session.commit()

        return {"message": "删除成功"}

    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@skudetail_router.post("/skudetail/upload_excel", summary="上传SKU详情Excel文件")
async def upload_excel(
    file: UploadFile = Form(...), session: Session = Depends(get_session)
):
    try:
        # 确保文件是Excel
        if not file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(
                status_code=400, detail="只支持Excel文件格式(.xlsx, .xls)"
            )

        # 读取Excel文件
        df = pd.read_excel(await file.read())

        # 验证必要的列是否存在
        required_columns = ["原SKU", "新SKU", "跟踪号", "箱号", "PC编号"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Excel缺少必要的列: {', '.join(missing_columns)}",
            )

        # 计算每个组合的出现次数
        df["group"] = df.apply(
            lambda x: f"{x['原SKU']}_{x['新SKU']}_{x['PC编号']}", axis=1
        )
        download_counts = df["group"].value_counts().to_dict()
        print(download_counts)
        # 处理空值
        df = df.fillna("")  # 将所有NaN值替换为空字符串

        # 使用字典记录已处理的组合
        processed_groups = {}

        # 批量插入或更新数据
        for _, row in df.iterrows():
            group_key = f"{row['原SKU']}_{row['新SKU']}_{row['PC编号']}"

            # 如果该组合已经处理过,则跳过
            if group_key in processed_groups:
                continue

            current_count = download_counts[group_key]
            processed_groups[group_key] = True

            # 检查数据库中是否已存在相同的记录
            existing_record = (
                session.query(SkuDetail)
                .filter(
                    SkuDetail.oldsku == row["原SKU"],
                    SkuDetail.newsku == row["新SKU"],
                    SkuDetail.pcno == row["PC编号"],
                )
                .first()
            )

            if existing_record:
                # 如果存在，累加下载次数
                existing_record.all_download_count += current_count
                existing_record.remaining_download_count += (
                    current_count  # 同时更新剩余下载次数
                )
                existing_record.trackingnumber = (
                    row["跟踪号"] if row["跟踪号"] else existing_record.trackingnumber
                )  # 保留原值如果为空
                existing_record.boxno = (
                    row["箱号"] if row["箱号"] else existing_record.boxno
                )  # 保留原值如果为空
                existing_record.createtime = datetime.now()  # 更新时间
            else:
                # 如果不存在，创建新记录
                skudetail = SkuDetail(
                    oldsku=row["原SKU"],
                    newsku=row["新SKU"],
                    trackingnumber=row["跟踪号"],
                    boxno=row["箱号"],
                    pcno=row["PC编号"],
                    all_download_count=current_count,
                    remaining_download_count=current_count,  # 设置初始剩余下载次数
                    createtime=datetime.now(),
                )
                session.add(skudetail)

        session.commit()
        return {"message": "数据导入成功"}

    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@skudetail_router.post("/skudetail/upload_labels", summary="上传SKU标签ZIP文件")
async def upload_labels(
    file: UploadFile = Form(...), session: Session = Depends(get_session)
):
    try:
        if not file.filename.endswith(".zip"):
            raise HTTPException(status_code=400, detail="只支持ZIP文件格式")

        # 创建临时目录
        temp_dir = Path("temp/sku_labels")
        temp_dir.mkdir(parents=True, exist_ok=True)

        # 保存ZIP文件
        zip_path = temp_dir / file.filename
        with open(zip_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)

        # 创建目标目录
        target_dir = Path("file/sku_detail")
        target_dir.mkdir(parents=True, exist_ok=True)

        # 解压文件
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        # 连接MinIO
        minio_client = MinioClient(
            os.getenv("MINIO_ENDPOINT"),
            os.getenv("MINIO_ACCESS_KEY"),
            os.getenv("MINIO_SECRET_KEY"),
            os.getenv("MINIO_BUCKET_NAME"),
            secure=False,
        )
        minio_client.connect()

        # 处理所有PDF文件
        for pdf_file in temp_dir.glob("**/*.pdf"):
            sku = pdf_file.stem  # 获取文件名（不含扩展名）

            # 复制到本地目标目录
            target_path = target_dir / pdf_file.name
            shutil.copy2(pdf_file, target_path)

            # 上传到MinIO
            minio_path = f"sku_detail/{pdf_file.name}"
            minio_client.upload_file(str(pdf_file), minio_path)

            # 更新数据库
            skudetail = session.exec(
                select(SkuDetail).where(SkuDetail.newsku == sku)
            ).first()
            if skudetail:
                skudetail.labelurl = f"./file/sku_detail/{pdf_file.name}"
                session.add(skudetail)

        session.commit()

        # 清理临时文件
        shutil.rmtree(temp_dir)

        return {"message": "标签文件上传成功"}

    except Exception as e:
        session.rollback()
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        raise HTTPException(status_code=500, detail=str(e))


@skudetail_router.get(
    "/skudetail/download_label_preview/{sku}", summary="下载SKU标签预览"
)
async def download_label_preview(sku: str, session: Session = Depends(get_session)):
    try:
        # 查询数据库获取标签URL
        skudetail = session.exec(
            select(SkuDetail).where(
                and_(
                    SkuDetail.oldsku == sku,
                )
            )
        ).first()
        if not skudetail or not skudetail.labelurl:
            logger.error(f"标签文件不存在: {sku}")
            return JSONResponse(
                content={"message": "标签文件不存在", "status": 404}, status_code=404
            )
        newsku = skudetail.newsku
        local_pdf_path = Path("file/sku_detail")
        # 检查本地文件是否存在
        file_path = local_pdf_path / f"{newsku}.pdf"
        if not file_path.exists():
            # 如果本地不存在,从MinIO下载
            try:
                minio_client = MinioClient(
                    os.getenv("MINIO_ENDPOINT"),
                    os.getenv("MINIO_ACCESS_KEY"),
                    os.getenv("MINIO_SECRET_KEY"),
                    os.getenv("MINIO_BUCKET_NAME"),
                    secure=False,
                )
                minio_client.connect()

                # 确保目标目录存在
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # 从MinIO下载文件
                minio_path = f"sku_detail/{newsku}.pdf"
                minio_client.download_file(minio_path, str(file_path))

                if not file_path.exists():
                    logger.error(f"标签文件不存在: {file_path}")
                    raise HTTPException(status_code=404, detail="标签文件不存在")

            except Exception as e:
                raise HTTPException(status_code=404, detail="无法从MinIO获取标签文件")

        return FileResponse(
            file_path, filename=f"{newsku}.pdf", media_type="application/pdf"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@skudetail_router.get("/skudetail/download_label/{sku}", summary="下载SKU标签")
async def download_label(sku: str, session: Session = Depends(get_session)):
    try:
        # 查询数据库获取标签URL
        skudetail = session.exec(
            select(SkuDetail).where(
                and_(
                    SkuDetail.oldsku == sku,
                    SkuDetail.remaining_download_count > 0,  # 修改为检查剩余下载次数
                )
            )
        ).first()
        logger.info(skudetail)
        if not skudetail or not skudetail.labelurl:
            logger.error(f"标签文件不存在: {sku}")
            return JSONResponse(
                content={"message": "标签文件不存在或者下载次数不足", "status": 404},
                status_code=404,
            )
        newsku = skudetail.newsku
        local_pdf_path = Path("file/sku_detail")
        # 检查本地文件是否存在
        file_path = local_pdf_path / f"{newsku}.pdf"
        if not file_path.exists():
            # 如果本地不存在,从MinIO下载
            try:
                minio_client = MinioClient(
                    os.getenv("MINIO_ENDPOINT"),
                    os.getenv("MINIO_ACCESS_KEY"),
                    os.getenv("MINIO_SECRET_KEY"),
                    os.getenv("MINIO_BUCKET_NAME"),
                    secure=False,
                )
                minio_client.connect()

                # 确保目标目录存在
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # 从MinIO下载文件
                minio_path = f"sku_detail/{newsku}.pdf"
                minio_client.download_file(minio_path, str(file_path))

                if not file_path.exists():
                    logger.error(f"标签文件不存在: {file_path}")
                    raise HTTPException(status_code=404, detail="标签文件不存在")

            except Exception as e:
                raise HTTPException(status_code=404, detail="无法从MinIO获取标签文件")
        skudetail.remaining_download_count -= 1  # 减少剩余下载次数而不是总下载次数
        session.add(skudetail)
        session.commit()
        return FileResponse(
            file_path, filename=f"{newsku}.pdf", media_type="application/pdf"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@skudetail_router.get("/skudetail/download_excel_template", summary="下载SKU导入模板")
async def download_excel_template():
    try:
        # 使用固定的模板文件路径
        template_path = Path("file/template")
        file_path = template_path / "sku_import_template.xlsx"

        # 如果模板文件不存在,则创建
        if not file_path.exists():
            template_path.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame(columns=["原SKU", "新SKU", "跟踪号", "箱号", "PC编号"])
            df.to_excel(file_path, index=False)

        return FileResponse(
            file_path,
            filename="sku_import_template.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"模板文件生成失败: {str(e)}")


@skudetail_router.get("/skudetail/execute")
async def execute_tasks(request: Request):
    """按顺序执行所有任务并以SSE流的形式返回结果"""
    task_id = str(uuid.uuid4())
    # 任务执行器列表（按顺序执行）
    TASK_EXECUTORS = [
        {"id":"login","name": "登录任务", "executor": login_task_generator},
        {"id":"dahuo_upload","name": "大货上传任务", "executor": dahuo_upload_task_generator},
        {"id":"shouli","name": "收货任务", "executor": shouli_task_generator},
    ]

    async def event_generator():
        # 发送每一步的任务名称
        for index, task_info in enumerate(TASK_EXECUTORS):
            step_info = {
                "step": index + 1,
                "task_name": task_info["name"],
                'id':task_info['id']
            }
            yield json.dumps(step_info, ensure_ascii=False)
        
        # 执行具体任务
        for task_info in TASK_EXECUTORS:
            async for event in task_info["executor"](request, task_id):
                yield event

    return EventSourceResponse(event_generator())

