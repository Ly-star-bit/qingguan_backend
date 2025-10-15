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
from fentan import (
    close_fentan_main,
    guangzhou_hangjie_aspose_execute,
    shanghai_pingzheng_aspose_execute,
)

TASK_MAPPING = {
    "close分摊": close_fentan_main,
    "广州航捷": guangzhou_hangjie_aspose_execute,
    "上海平政": shanghai_pingzheng_aspose_execute,
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
            # 注意：前端会检查 result 中是否有 file_name / file_url 等字段
            result_data = {
                "task": task_id,
                "result": {
                    "file_name": Path(result_file_path).name,  # 👈 关键：提供 file_name
                    # 可选：如果你有公开下载URL，也可以加 file_url
                    # "file_url": f"/api/download/{Path(result_file_path).name}"
                },
                "message": "任务执行成功"
            }
            yield {"event": "result", "data": json.dumps(result_data, ensure_ascii=False)}

        except Exception as e:
            error_msg = f"任务执行失败: {str(e)}"
            logger.exception(error_msg)
            # 发送错误结果（仍用 result 事件，但包含错误信息）
            error_result = {
                "task": task_id,
                "result": {
                    "error": error_msg
                },
                "message": error_msg
            }
            yield {"event": "result", "data": json.dumps(error_result, ensure_ascii=False)}
            # 或者发送 error 事件（前端也监听了 error）
            # yield {"event": "error", "data": error_msg}

    return EventSourceResponse(event_generator())


@fentan_router.get("/download")
async def download_file(file_path: str = Query(..., description="文件路径（相对于 ./file/ 目录）")):
    """
    下载 ./file/ 目录下的指定文件。
    
    参数:
        file_path: 文件路径，例如 "fentan/report.xlsx" 或 "/fentan/report.xlsx"
                  （开头的 / 会被自动去除）
    """
    BASE_FILE_DIR = Path("./file").resolve()

    if not file_path:
        raise HTTPException(status_code=400, detail="文件路径不能为空")

    # 🔧 关键修复：去除开头的斜杠，确保是相对路径
    file_path = file_path.lstrip("/")

    # 禁止空路径或包含危险片段（额外防护）
    if ".." in file_path or file_path.startswith("/") or file_path == "":
        raise HTTPException(status_code=403, detail="非法文件路径")

    # 构建目标文件的绝对路径
    target_path = (BASE_FILE_DIR / file_path).resolve()

    # 安全检查：确保目标路径在 BASE_FILE_DIR 之内
    try:
        target_path.relative_to(BASE_FILE_DIR)
    except ValueError:
        raise HTTPException(status_code=403, detail="非法文件路径：路径穿越被阻止")

    if not target_path.is_file():
        raise HTTPException(status_code=404, detail="文件不存在")

    return FileResponse(
        path=target_path,
        filename=target_path.name,
        media_type='application/octet-stream'
    )