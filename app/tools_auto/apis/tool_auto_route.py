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
