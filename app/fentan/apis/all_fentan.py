from fastapi import APIRouter, FastAPI, Query, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from sse_starlette.sse import EventSourceResponse
import uuid
import json
import asyncio
import os
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
import re
from fentan import (
    close_fentan_main,
    guangzhou_hangjie_aspose_execute,
    shanghai_pingzheng_aspose_execute,
    hangjie_hubs_execute,
    pingzheng_hubs_execute,
    xiamen_zhixuan_fentan_execute
)

TASK_MAPPING = {
    "close分摊": close_fentan_main,
    "广州航捷": guangzhou_hangjie_aspose_execute,
    "上海平政": shanghai_pingzheng_aspose_execute,
    "广州航捷_新系统": hangjie_hubs_execute,
    "上海平政_新系统": pingzheng_hubs_execute,
    "厦门_致宣":xiamen_zhixuan_fentan_execute
}

# ========== 配置 ==========
BASE_UPLOAD_DIR = Path("./file/fentan")
BASE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)  # 自动创建目录

executor = ThreadPoolExecutor(max_workers=4)
fentan_router = APIRouter(tags=["分摊"], prefix="/fentan")


# ========== 接口 ==========
@fentan_router.post("/execute")
async def execute_tasks(task_type: str = Form(...), file: UploadFile = File(...)):
    """
    上传文件，保存到 ./file/fentan/ 目录（带时间戳命名），
    根据 task_type 执行对应任务，通过 SSE 返回执行过程。
    """
    if task_type not in TASK_MAPPING:
        raise HTTPException(status_code=400, detail=f"不支持的任务类型: {task_type}")

    task_func = TASK_MAPPING[task_type]
    task_id = str(uuid.uuid4())

    # 生成带时间戳的文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_filename = "".join(c if c.isalnum() or c in "._-" else "_" for c in file.filename)
    name_stem = Path(safe_filename).stem
    suffix = Path(safe_filename).suffix
    saved_filename = f"origin_{name_stem}_{timestamp}{suffix}"
    saved_file_path = BASE_UPLOAD_DIR  /saved_filename

    # 保存上传文件
    content = await file.read()
    with open(saved_file_path, "wb") as f:
        f.write(content)

    async def event_generator():
        try:
            # 1. 注册步骤（使用默认事件类型，即 event: message）
            step_info = {
                "id": task_id,
                "step": 1,
                "task_name": f"处理分摊-{task_type}"
            }
            yield {"data": json.dumps(step_info, ensure_ascii=False)}

            # 2. 发送状态更新（event: status）
            status_update = {
                "task": task_id,
                "status": "running",  # 必须是 'running' 或 'processing' 才会被前端识别为执行中
                "message": "文件已成功保存，开始处理..."
            }
            yield {"event": "status", "data": json.dumps(status_update, ensure_ascii=False)}

            # 3. 执行任务（在后台线程中）
            loop = asyncio.get_event_loop()
            result_file_path = await loop.run_in_executor(
                executor, task_func, str(saved_file_path)
            )

            # 4. 发送结果（event: result）
            # 返回相对于 ./file/ 的路径
            relative_path = Path(result_file_path).relative_to(Path("./file"))
            # 修复可能的重复路径问题，如 fentan/fentan -> fentan
            file_name = str(relative_path).replace("fentan/fentan", "fentan").replace("fentan\\fentan", "fentan")
            result_data = {
                "task": task_id,
                "result": {
                    "file_name": file_name,
                },
                "message": "任务执行成功"
            }
            yield {"event": "result", "data": json.dumps(result_data, ensure_ascii=False)}

        except Exception as e:
            error_msg = f"任务执行失败: {str(e)}"
            logger.exception(error_msg)
            error_result = {
                "task": task_id,
                "result": {
                    "error": error_msg
                },
                "message": error_msg
            }
            yield {"event": "result", "data": json.dumps(error_result, ensure_ascii=False)}

    return EventSourceResponse(event_generator())


ALLOWED_ROOT_FOLDER = "fentan"

@fentan_router.get("/download")
async def download_file(file_path: str = Query(..., description="文件路径")):
    if not file_path or not file_path.strip():
        raise HTTPException(status_code=400, detail="文件路径不能为空")

    BASE_FILE_DIR = Path("./file").resolve()

    # ================= 核心清洗逻辑 (逐步执行，确保每一步都正确) =================
    
    # 1. 统一分隔符：将所有反斜杠 \ 替换为正斜杠 /
    step1 = file_path.replace("\\", "/")
    
    # 2. 合并连续斜杠：将 // 或 /// 等统一替换为单个 /
    step2 = re.sub(r'/+', '/', step1)
    
    # 3. 去除开头斜杠：确保是相对路径
    step3 = step2.lstrip("/")
    
    # 4. 【关键去重】：如果开头是 "fentan/fentan/"，则去掉第一个 "fentan/"
    # 注意：这里必须严格匹配 "fentan/fentan/"
    if step3.startswith("fentan/fentan/"):
        # 截取掉第一个 "fentan/" (长度为7)
        step3 = step3[7:]
        
    # 此时 step3 应该是: "fentan/origin_bill-....xlsx"
    
    # ================= 构建与校验 =================

    # 5. 构建路径
    # Path 会自动将 / 转换为当前系统的分隔符 (\)
    target_path = BASE_FILE_DIR / step3
    
    # resolve() 会解析绝对路径，消除任何潜在的 . 或 ..
    target_path = target_path.resolve()

    # 6. 安全校验：防止路径穿越
    try:
        target_path.relative_to(BASE_FILE_DIR)
    except ValueError:
        raise HTTPException(status_code=403, detail="非法路径：禁止访问上级目录")

    # 7. 文件存在性检查
    if not target_path.is_file():
        # 为了让你看清到底找了哪，我把路径打印出来
        raise HTTPException(
            status_code=404, 
            detail=f"文件不存在。系统实际查找的绝对路径: {target_path}"
        )

    return FileResponse(
        path=target_path,
        filename=target_path.name,
        media_type='application/octet-stream'
    )