from datetime import datetime
import json
import uuid
from fastapi import APIRouter, HTTPException, UploadFile, File
from sse_starlette.sse import EventSourceResponse
from pathlib import Path
from starlette.responses import FileResponse
from loguru import logger
import asyncio
from concurrent.futures import ThreadPoolExecutor
from app.tools_auto.product_name_excel_processor import find_matching_products
from app.tools_auto.utils.dadan.work_num_dadan import work_num_dadan_main
from app.tools_auto.utils.hubs.hubs_client import HubsClient
from app.tools_auto.utils.hubs.lasen_huitian import process_excel_to_json2
import pandas as pd

# 定义路由
tool_auto_router = APIRouter(tags=["工具自动化"], prefix="/tools_auto")

# ========== 配置 ==========
BASE_UPLOAD_DIR = Path("./file/tools_auto")
BASE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)  # 自动创建目录

executor = ThreadPoolExecutor(max_workers=4)


# ========== 接口 ==========
@tool_auto_router.get("/tasks")
async def get_tasks():
    """
    获取所有可用的任务配置

    此接口为前端 TodoComponent 组件提供任务配置数据。
    每个任务配置定义了组件的行为、参数和UI展示。

    Returns:
        List[dict]: 任务配置列表，每个任务包含以下字段：

        - id (str): 任务唯一标识符，用于区分不同的任务
        - title (str): 任务显示标题，在卡片标题栏显示
        - apiEndpoint (str): SSE 任务执行的后端接口路径（如 /fentan/execute）
        - enableFileUpload (bool): 是否启用文件上传功能
            - True: 显示文件上传区域，允许用户上传文件
            - False: 隐藏文件上传功能
        - enableApiParams (bool): 是否允许用户手动添加动态键值对参数
            - True: 显示"添加参数"按钮，用户可以自定义额外参数
            - False: 只使用预定义的参数
        - downloadBaseUrl (str): 文件下载的基础路径，用于构建完整的下载URL
        - columnNames (List[str], optional): 表格输入的列名数组
            - 如果提供，前端会显示一个表格让用户输入数据
            - 每个元素代表一列的标题
        - apiParams (dict): 预设的 API 参数配置
            - key: 参数名称（会作为请求参数发送到后端）
            - value: 可以是简单字符串，或者是 ApiParamConfig 对象

        ApiParamConfig 对象结构：
        - type (str): 输入类型，可选值：
            - "text": 文本输入框
            - "select": 下拉选择框
        - label (str): 显示的标签名称
        - options (List[dict]): 当 type="select" 时，提供下拉选项
            - label: 显示给用户的文本
            - value: 实际提交的值
        - defaultValue (str): 默认值，组件加载时自动填充
        - value (str, optional): 固定值，用户无法修改

    Example:
        返回示例：
        [
            {
                "id": "fentan_task_001",
                "title": "分摊任务处理",
                "apiEndpoint": "/fentan/execute",
                "enableFileUpload": True,
                "enableApiParams": True,
                "downloadBaseUrl": "/fentan/download",
                "columnNames": ["日期", "客户", "金额", "备注"],
                "apiParams": {
                    "task_type": {
                        "type": "select",
                        "label": "任务类型",
                        "options": [
                            {"label": "上海平政", "value": "上海平政"},
                            {"label": "close分摊", "value": "close分摊"}
                        ],
                        "defaultValue": "上海平政"
                    }
                }
            }
        ]
    """
    tasks = [
        {
            "id": "fentan_task_001",  # 任务唯一ID
            "title": "分摊任务处理",  # 卡片标题
            "apiEndpoint": "/fentan/execute",  # 执行任务的API端点
            "enableFileUpload": True,  # 启用文件上传
            "enableApiParams": True,  # 允许用户添加自定义参数
            "downloadBaseUrl": "/fentan/download",  # 下载文件的基础URL
            "columnNames": ["日期", "客户", "金额", "备注"],  # 表格列名（可选）
            "apiParams": {
                # 任务类型参数：下拉选择框
                "task_type": {
                    "type": "select",  # 使用下拉选择框
                    "label": "任务类型",  # 显示标签
                    "options": [  # 下拉选项列表
                        {"label": "上海平政", "value": "上海平政"},
                        {"label": "close分摊", "value": "close分摊"},
                        {"label": "广州航捷", "value": "广州航捷"},
                        {"label": "广州航捷_新系统", "value": "广州航捷_新系统"},
                        {"label": "上海平政_新系统", "value": "上海平政_新系统"},
                    ],
                    "defaultValue": "上海平政",  # 默认选中值
                }
            },
        },
        {
            "id": "product_excel_task_001",
            "title": "产品Excel数据匹配处理",
            "apiEndpoint": "/tools_auto/process_product_excel",
            "enableFileUpload": False,  # 设为 False，因为我们使用 apiParams 来自定义文件位
            "enableApiParams": False,
            "downloadBaseUrl": "/tools_auto/download",
            "apiParams": {
                "file1": {
                    "type": "file",  # 新增的类型
                    "label": "第一个Excel文件（订单数据）",
                    "description": "包含订单编号、SKU、库存SKU英文名称、商品数量",
                },
                "file2": {
                    "type": "file",
                    "label": "第二个Excel文件（用户单号）",
                    "description": "包含用户单号",
                },
            },
        },
        {
            "id": "work_num_dadan_task_001",
            "title": "工作单号打单",
            "apiEndpoint": "/tools_auto/work_num_dadan",
            "enableFileUpload": True,
            "enableApiParams": False,
            "downloadBaseUrl": "/tools_auto/download",
            "apiParams": {},
        },
        {
            "id": "larsen_backfill_task_001",
            "title": "拉森大货运单回填",
            "apiEndpoint": "/tools_auto/larsen/backfill",
            "enableFileUpload": True,
            "enableApiParams": False,
            "downloadBaseUrl": "/tools_auto/download",
            "apiParams": {},
        },
    ]
    return tasks


@tool_auto_router.post("/process_product_excel")
async def process_product_excel(
    file1: UploadFile = File(
        ...,
        description="第一个Excel文件（包含订单编号、SKU、库存SKU英文名称、商品数量）",
    ),
    file2: UploadFile = File(..., description="第二个Excel文件（包含用户单号）"),
):
    """
    处理产品Excel文件，匹配订单数据并提取SKU信息
    通过 SSE 返回执行过程

    Args:
        file1: 第一个Excel文件（包含订单编号、SKU、库存SKU英文名称、商品数量）
        file2: 第二个Excel文件（包含用户单号）

    Returns:
        EventSourceResponse: SSE流式响应
    """
    task_id = str(uuid.uuid4())

    # 生成带时间戳的文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 保存第一个文件
    safe_filename1 = "".join(
        c if c.isalnum() or c in "._-" else "_" for c in file1.filename
    )
    name_stem1 = Path(safe_filename1).stem
    suffix1 = Path(safe_filename1).suffix
    saved_filename1 = f"file1_{name_stem1}_{timestamp}{suffix1}"
    saved_file_path1 = BASE_UPLOAD_DIR / saved_filename1

    # 保存第二个文件
    safe_filename2 = "".join(
        c if c.isalnum() or c in "._-" else "_" for c in file2.filename
    )
    name_stem2 = Path(safe_filename2).stem
    suffix2 = Path(safe_filename2).suffix
    saved_filename2 = f"file2_{name_stem2}_{timestamp}{suffix2}"
    saved_file_path2 = BASE_UPLOAD_DIR / saved_filename2

    # 输出文件路径
    output_filename = f"result_{timestamp}.xlsx"
    output_file_path = BASE_UPLOAD_DIR / output_filename

    # 保存上传文件
    content1 = await file1.read()
    with open(saved_file_path1, "wb") as f:
        f.write(content1)

    content2 = await file2.read()
    with open(saved_file_path2, "wb") as f:
        f.write(content2)

    async def event_generator():
        try:
            # 1. 注册步骤
            step_info = {
                "id": task_id,
                "step": 1,
                "task_name": "处理产品Excel-匹配订单数据",
            }
            yield {"data": json.dumps(step_info, ensure_ascii=False)}

            # 2. 发送状态更新
            status_update = {
                "task": task_id,
                "status": "running",
                "message": "文件已成功保存，开始处理...",
            }
            yield {
                "event": "status",
                "data": json.dumps(status_update, ensure_ascii=False),
            }

            # 3. 执行任务（在后台线程中）
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                executor,
                find_matching_products,
                str(saved_file_path1),
                str(saved_file_path2),
                str(output_file_path),
            )

            # 4. 发送结果
            result_data = {
                "task": task_id,
                "result": {
                    "file_name": output_filename,
                    "file_path": f"tools_auto/{output_filename}",
                },
                "message": "任务执行成功",
            }
            yield {
                "event": "result",
                "data": json.dumps(result_data, ensure_ascii=False),
            }

        except Exception as e:
            error_msg = f"任务执行失败: {str(e)}"
            logger.exception(error_msg)
            error_result = {
                "task": task_id,
                "result": {"error": error_msg},
                "message": error_msg,
            }
            yield {
                "event": "result",
                "data": json.dumps(error_result, ensure_ascii=False),
            }

    return EventSourceResponse(event_generator())


@tool_auto_router.post("/work_num_dadan")
async def work_num_dadan(
    file: UploadFile = File(..., description="工作单号打单Excel文件"),
):
    """
    工作单号打单处理
    通过 SSE 返回执行过程

    Args:
        file: 工作单号打单Excel文件

    Returns:
        EventSourceResponse: SSE流式响应
    """
    task_id = str(uuid.uuid4())

    # 生成带时间戳的文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 保存上传的文件
    safe_filename = "".join(
        c if c.isalnum() or c in "._-" else "_" for c in file.filename
    )
    name_stem = Path(safe_filename).stem
    suffix = Path(safe_filename).suffix
    saved_filename = f"work_num_dadan_{name_stem}_{timestamp}{suffix}"
    saved_file_path = BASE_UPLOAD_DIR / saved_filename

    # 保存上传文件
    content = await file.read()
    with open(saved_file_path, "wb") as f:
        f.write(content)

    async def event_generator():
        # 用于存储进度消息的队列
        progress_queue = asyncio.Queue()
        # 显式捕获正在运行的事件循环
        main_loop = asyncio.get_running_loop()

        def progress_callback(step_name, status, message):
            """进度回调函数，将进度信息放入队列"""
            try:
                # 使用主线程的 loop 安全地提交协程
                asyncio.run_coroutine_threadsafe(
                    progress_queue.put((step_name, status, message)), main_loop
                )
            except Exception as e:
                logger.error(f"进度回调错误: {e}")

        try:
            # 1. 注册步骤
            step_info = {
                "id": task_id,
                "step": 1,
                "task_name": "工作单号打单处理",
            }
            yield {"data": json.dumps(step_info, ensure_ascii=False)}

            # 2. 发送初始状态
            status_update = {
                "task": task_id,
                "status": "running",
                "message": "文件已成功保存，开始处理...",
            }
            yield {
                "event": "status",
                "data": json.dumps(status_update, ensure_ascii=False),
            }

            # 3. 启动后台任务执行
            loop = asyncio.get_event_loop()
            task_future = loop.run_in_executor(
                executor,
                work_num_dadan_main,
                content,  # 传入二进制内容
                True,  # is_binary=True
                progress_callback,  # 传入进度回调
            )

            # 4. 持续从队列中获取进度更新并推送
            while not task_future.done():
                try:
                    # 等待进度更新，设置超时避免阻塞
                    step_name, status, message = await asyncio.wait_for(
                        progress_queue.get(), timeout=0.5
                    )

                    # 发送进度更新
                    status_update = {
                        "task": task_id,
                        "status": status,
                        "step_name": step_name,
                        "message": message,
                    }
                    yield {
                        "event": "status",
                        "data": json.dumps(status_update, ensure_ascii=False),
                    }
                except asyncio.TimeoutError:
                    # 超时继续等待
                    continue

            # 5. 处理剩余的进度消息
            while not progress_queue.empty():
                try:
                    step_name, status, message = await asyncio.wait_for(
                        progress_queue.get(), timeout=0.1
                    )
                    status_update = {
                        "task": task_id,
                        "status": status,
                        "step_name": step_name,
                        "message": message,
                    }
                    yield {
                        "event": "status",
                        "data": json.dumps(status_update, ensure_ascii=False),
                    }
                except asyncio.TimeoutError:
                    break

            # 6. 获取任务结果
            (
                exists_CourierNumber_worknum,
                root_path,
                new_file_path,
                huitian_data,
            ) = await task_future

            # 7. 发送结果
            result_data = {
                "task": task_id,
                "result": {},
                "message": "任务执行成功",
            }

            # 处理返回的文件路径
            if new_file_path:
                # new_file_path 格式: file/ups_excel_file/template_整柜{timestamp}.xlsx
                new_file_path_obj = Path(new_file_path)
                result_data["result"]["excel_file_name"] = new_file_path_obj.name
                result_data["result"]["excel_file_path"] = str(new_file_path)

            if root_path:
                # root_path 是 PDF 文件的根目录
                root_path_obj = Path(root_path)
                result_data["result"]["pdf_folder"] = str(root_path_obj)
                result_data["result"]["pdf_folder_name"] = root_path_obj.name

            if exists_CourierNumber_worknum:
                result_data["result"]["exists_courier_numbers"] = (
                    exists_CourierNumber_worknum
                )

            yield {
                "event": "result",
                "data": json.dumps(result_data, ensure_ascii=False),
            }

            # 8. 在后台保存回填数据，等待定时任务处理
            if huitian_data:
                from app.tools_auto.utils.dadan.work_num_dadan import save_backfilling_data

                await loop.run_in_executor(
                    executor,
                    save_backfilling_data,
                    huitian_data,
                )

        except Exception as e:
            error_msg = f"任务执行失败: {str(e)}"
            logger.exception(error_msg)
            error_result = {
                "task": task_id,
                "result": {"error": error_msg},
                "message": error_msg,
            }
            yield {
                "event": "result",
                "data": json.dumps(error_result, ensure_ascii=False),
            }

    return EventSourceResponse(event_generator())


@tool_auto_router.get("/download")
async def download_file(file_path: str):
    """
    下载 ./file/ 目录下的指定文件

    Args:
        file_path: 文件路径，例如 "tools_auto/result.xlsx"
    """
    BASE_FILE_DIR = Path("./file/tools_auto").resolve()

    if not file_path:
        raise HTTPException(status_code=400, detail="文件路径不能为空")

    # 去除开头的斜杠，确保是相对路径
    file_path = file_path.lstrip("/")

    # 禁止空路径或包含危险片段
    if ".." in file_path or file_path.startswith("/") or file_path == "":
        raise HTTPException(status_code=403, detail="非法文件路径")

    # 构建目标文件的绝对路径
    target_path = (BASE_FILE_DIR / file_path).resolve()
    print(target_path)
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
        media_type="application/octet-stream",
    )


@tool_auto_router.post("/larsen/backfill")
async def larsen_backfill(
    file: UploadFile = File(..., description="拉森回填Excel文件"),
):
    """
    拉森系统大货运单回填处理
    通过 SSE 返回执行过程，逐条回填数据

    Excel文件格式要求（参考 lasen_huitian.py）：
    - 仓库、发货批次、箱数、总实重、总箱数、进仓号 等列

    Args:
        file: 拉森回填Excel文件

    Returns:
        EventSourceResponse: SSE流式响应，每条数据回填后立即返回进度
    """
    task_id = str(uuid.uuid4())

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    safe_filename = "".join(
        c if c.isalnum() or c in "._-" else "_" for c in file.filename
    )
    name_stem = Path(safe_filename).stem
    suffix = Path(safe_filename).suffix
    saved_filename = f"larsen_backfill_{name_stem}_{timestamp}{suffix}"
    saved_file_path = BASE_UPLOAD_DIR / saved_filename

    content = await file.read()
    # 将文件指针重置到开头（如果需要再次读取）
    await file.seek(0)
    with open(saved_file_path, "wb") as f:
        f.write(content)

    async def event_generator():
        progress_queue = asyncio.Queue()
        main_loop = asyncio.get_running_loop()

        def progress_callback(index, status, message, have_no=None, data=None):
            try:
                asyncio.run_coroutine_threadsafe(
                    progress_queue.put((index, status, message, have_no, data)), main_loop
                )
            except Exception as e:
                logger.error(f"进度回调错误: {e}")

        try:
            step_info = {
                "id": task_id,
                "step": 1,
                "task_name": "拉森大货运单回填处理",
            }
            yield {"data": json.dumps(step_info, ensure_ascii=False)}

            status_update = {
                "task": task_id,
                "status": "running",
                "message": "文件已成功保存，正在读取数据...",
            }
            yield {
                "event": "status",
                "data": json.dumps(status_update, ensure_ascii=False),
            }

            try:
                client = HubsClient()
                if not client.httpx_client:
                    raise Exception("无法获取Hubs客户端，请检查登录配置")
            except Exception as e:
                error_msg = f"初始化Hubs客户端失败: {str(e)}"
                logger.error(error_msg)
                error_result = {
                    "task": task_id,
                    "result": {"error": error_msg},
                    "message": error_msg,
                }
                yield {
                    "event": "result",
                    "data": json.dumps(error_result, ensure_ascii=False),
                }
                return

            try:
                all_fbx_address = client.get_base_fbx_addr()
                data_list = process_excel_to_json2(content, is_binary=True, all_fbx_address=all_fbx_address)
                total_rows = len(data_list)
                if total_rows == 0:
                    raise ValueError("Excel处理后无有效数据")
            except Exception as e:
                error_msg = f"处理Excel文件失败: {str(e)}"
                logger.error(error_msg)
                error_result = {
                    "task": task_id,
                    "result": {"error": error_msg},
                    "message": error_msg,
                }
                yield {
                    "event": "result",
                    "data": json.dumps(error_result, ensure_ascii=False),
                }
                return

            status_update = {
                "task": task_id,
                "status": "running",
                "message": f"已读取 {total_rows} 条数据，开始逐条回填...",
                "total": total_rows,
            }
            yield {
                "event": "status",
                "data": json.dumps(status_update, ensure_ascii=False),
            }

            results = []
            success_count = 0
            fail_count = 0

            for index, data in enumerate(data_list):
                try:
                    have_no = data.get("order", {}).get("have_no")
                    if not have_no:
                        progress_callback(
                            index + 1,
                            "skipped",
                            "have_no为空，跳过",
                            have_no=None,
                        )
                        fail_count += 1
                        continue

                    progress_callback(
                        index + 1,
                        "processing",
                        f"正在回填第 {index + 1}/{total_rows} 条，have_no: {have_no}",
                        have_no=have_no,
                    )

                    result = client.update_bigwaybill_order([have_no], data)
                    logger.info(f'result:{result}')
                    logger.info(f'data:{data}')
                    if result and result.get("success"):
                        success_count += 1
                        progress_callback(
                            index + 1,
                            "success",
                            f"回填成功: have_no={have_no}",
                            have_no=have_no,
                            data=data,
                        )
                        results.append(
                            {
                                "index": index + 1,
                                "have_no": have_no,
                                "status": "success",
                                "message": "回填成功",
                            }
                        )
                    else:
                        fail_count += 1
                        error_msg = result.get("message", "未知错误") if result else "返回结果为空"
                        progress_callback(
                            index + 1,
                            "failed",
                            f"回填失败: have_no={have_no}，原因: {error_msg}",
                            have_no=have_no,
                        )
                        results.append(
                            {
                                "index": index + 1,
                                "have_no": have_no,
                                "status": "failed",
                                "message": error_msg,
                            }
                        )

                except Exception as e:
                    fail_count += 1
                    error_msg = f"处理第 {index + 1} 行时出错: {str(e)}"
                    logger.error(error_msg)
                    progress_callback(
                        index + 1,
                        "failed",
                        error_msg,
                        have_no=str(have_no),
                    )
                    results.append(
                        {
                            "index": index + 1,
                            "have_no": str(have_no),
                            "status": "failed",
                            "message": str(e),
                        }
                    )

            status_update = {
                "task": task_id,
                "status": "completed",
                "message": f"处理完成，成功 {success_count} 条，失败 {fail_count} 条",
                "total": total_rows,
                "success_count": success_count,
                "fail_count": fail_count,
            }
            yield {
                "event": "status",
                "data": json.dumps(status_update, ensure_ascii=False),
            }

            result_data = {
                "task": task_id,
                "result": {
                    "file_name": saved_filename,
                    "file_path": f"tools_auto/{saved_filename}",
                    "total": total_rows,
                    "success_count": success_count,
                    "fail_count": fail_count,
                    "details": results,
                },
                "message": f"任务执行成功，成功 {success_count} 条，失败 {fail_count} 条",
            }
            yield {
                "event": "result",
                "data": json.dumps(result_data, ensure_ascii=False),
            }

        except Exception as e:
            error_msg = f"任务执行失败: {str(e)}"
            logger.exception(error_msg)
            error_result = {
                "task": task_id,
                "result": {"error": error_msg},
                "message": error_msg,
            }
            yield {
                "event": "result",
                "data": json.dumps(error_result, ensure_ascii=False),
            }

    return EventSourceResponse(event_generator())
