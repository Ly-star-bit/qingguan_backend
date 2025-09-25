import asyncio
import json
from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from datetime import datetime
from rpa_tools import find_playwright_node_path
import os
from rpa_tools.morelink_utils import (
    MoreLinkClient,
    execute_tools,
    maitou_export_api,
    dahuo_upload,
)
from pathlib import Path
from loguru import logger
from feapder.db.mysqldb import MysqlDB
import traceback

mysql_client = MysqlDB(
    ip=os.getenv("MYSQL_HOST"),
    port=int(os.getenv("MYSQL_PORT")),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASS"),
    db=os.getenv("MYSQL_DB"),
)


def exec_generated_code(code, path):
    # 使用 exec 执行提取的代码
    try:
        exec(code, globals())  # 使用 globals() 确保生成的函数在全局命名空间中可用
        # 调用生成的函数

        error_msg = None
        generated_file_path = None
        generated_function = globals().get("process_excel_to_json")
        if generated_function:
            generated_file_path = generated_function(path)
            logger.info(f"生成的Json文件路径: {generated_file_path}")
        else:
            logger.error("未找到生成的函数 process_excel_to_json")
            error_msg = "未找到生成的函数 process_excel_to_json"
    except KeyError as e:
        error_msg = f"没有列名: {str(e)}"
        logger.error(f"没有列名: {str(e)}")

    except Exception as e:
        error_msg = f"执行代码时出错: {traceback.format_exc()}"
        logger.error(f"执行代码时出错: {traceback.format_exc()}")
    finally:
        return generated_file_path, error_msg


async def send_task_steps():
    """发送任务步骤信息"""
    task_todos = [
        {"id": "file_process_verify", "name": "文件处理和验证"},
        {"id": "login", "name": "登录任务"},
        {"id": "morelink_batch_create", "name": "morelink批量建单"},
        {"id": "other_tasks", "name": "其它任务(唛头下载,授理)"},
    ]
    
    for index, task_info in enumerate(task_todos):
        step_info = {
            "step": index + 1,
            "task_name": task_info["name"],
            "id": task_info["id"],
        }
        yield {
            "event": "step",
            "data": json.dumps(step_info, ensure_ascii=False)
        }


async def process_file_verification(file_bytes, client_name):
    """文件处理和验证任务"""
    yield {
        "event": "status",
        "data": json.dumps(
            {"task": "file_process_verify", "message": "开始文件处理和验证", "status": "running"}
        ),
    }

    # 保存file_bytes
    origin_file_path = f"./file/dahuo_upload/origin/{client_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
    # 确保目录存在
    Path(origin_file_path).parent.mkdir(parents=True, exist_ok=True)
    # 保存文件
    with open(origin_file_path, "wb") as f:
        f.write(file_bytes)

    yield {
        "event": "status",
        "data": json.dumps(
            {"task": "file_process_verify", "message": "文件保存成功，开始处理", "status": "processing"}
        ),
    }

    client = mysql_client.find(
        f"select success_code,tool from client where client_name='{client_name}'",
        to_json=True,
    )
    str_python_code = client["success_code"]
    tools = client["tool"].split(",")
    
    dataframe, error_msg = exec_generated_code(str_python_code, origin_file_path)
    if error_msg:
        yield {
            "event": "result",
            "data": json.dumps(
                {
                    "task": "file_process_verify",
                    "message": "文件处理检验失败",
                    "result": {"error_msg": error_msg}
                }
            ),
        }
        return None, None, None, error_msg
    
    # 从处理后的dataframe获取订单数据和建单条数
    orders_data = []
    if dataframe is not None:
        try:
            # 将DataFrame转换为字典列表格式的订单数据
            orders_data = dataframe.to_dict('records')
            order_count = len(orders_data)
        except Exception as e:
            logger.error(f"DataFrame转换失败: {str(e)}")
            order_count = 0
    else:
        order_count = 0
    
    yield {
        "event": "result",
        "data": json.dumps(
            {
                "task": "file_process_verify",
                "message": "文件处理检验成功",
                "result": {"建单条数": order_count, "文件路径": origin_file_path}
            }
        ),
    }
    
    return origin_file_path, tools, orders_data, None


async def login_task(client_name):
    """登录任务"""
    yield {
        "event": "status",
        "data": json.dumps(
            {"task": "login", "message": "开始执行登录任务", "status": "running"}
        ),
    }

    yield {
        "event": "status",
        "data": json.dumps(
            {"task": "login", "message": "正在初始化客户端", "status": "processing"}
        ),
    }

    node_path = find_playwright_node_path()
    morelink_client = MoreLinkClient(node_path)

    yield {
        "event": "result",
        "data": json.dumps(
            {
                "task": "login",
                "message": "登录成功",
                "result": {"username": "test_user", "token": "abc123xyz", "client_name": client_name}
            }
        ),
    }
    
    return morelink_client


async def batch_create_orders(orders_data, morelink_client):
    """批量建单任务"""
    order_count = len(orders_data) if orders_data else 0
    
    yield {
        "event": "status",
        "data": json.dumps(
            {
                "task": "morelink_batch_create",
                "message": "开始批量建单",
                "status": "running",
            }
        ),
    }

    yield {
        "event": "status",
        "data": json.dumps(
            {
                "task": "morelink_batch_create",
                "message": f"正在处理 {order_count} 条订单数据",
                "status": "processing",
            }
        ),
    }

    success_upload_data, fail_upload_data, error_msg = dahuo_upload(
        orders_data, morelink_client
    )
    
    if error_msg:
        yield {
            "event": "result",
            "data": json.dumps(
                {
                    "task": "morelink_batch_create",
                    "message": "批量建单失败",
                    "result": {"error_msg": error_msg, "失败条数": len(fail_upload_data) if fail_upload_data else 0}
                }
            ),
        }
        return None, None, error_msg
        
    yield {
        "event": "result",
        "data": json.dumps(
            {
                "task": "morelink_batch_create",
                "message": "批量建单完成",
                "result": {
                    "成功条数": len(success_upload_data) if success_upload_data else 0,
                    "失败条数": len(fail_upload_data) if fail_upload_data else 0
                }
            }
        ),
    }
    
    return success_upload_data, fail_upload_data, None


async def execute_other_tasks(success_upload_data, tools, origin_file_path, client_name):
    """执行其它任务（唛头下载,受理）"""
    if success_upload_data:
        yield {
            "event": "status",
            "data": json.dumps(
                {"task": "other_tasks", "message": "开始执行其它任务", "status": "running"}
            ),
        }

        yield {
            "event": "status",
            "data": json.dumps(
                {"task": "other_tasks", "message": "正在执行唛头下载和受理", "status": "processing"}
            ),
        }

        a_number_output_file, maitou_path = execute_tools(
            tools,
            success_upload_data,
            origin_file_path=origin_file_path,
            client_name=client_name,
        )

        yield {
            "event": "result",
            "data": json.dumps(
                {
                    "task": "other_tasks",
                    "message": "其它任务完成",
                    "result": {
                        "唛头文件路径": maitou_path,
                        "A号文件路径": a_number_output_file,
                        "使用工具": tools
                    }
                }
            ),
        }
    else:
        yield {
            "event": "result",
            "data": json.dumps(
                {
                    "task": "other_tasks",
                    "message": "跳过其它任务，无成功上传数据",
                    "result": {"跳过原因": "没有成功上传的数据"}
                }
            ),
        }


async def morelink_jiandan_execute(file_bytes, client_name):
    """主执行函数 - 拆分后的版本"""
    # 发送任务步骤信息
    async for step_data in send_task_steps():
        yield step_data

    # 文件处理和验证
    origin_file_path = None
    tools = None
    orders_data = None
    error_occurred = False
    
    async for result in process_file_verification(file_bytes, client_name):
        yield result
        if result["event"] == "result":
            result_data = json.loads(result["data"])
            if "error_msg" in result_data["result"]:
                error_occurred = True
                return
            else:
                # 从结果中提取文件路径
                origin_file_path = result_data["result"]["文件路径"]
    
    if error_occurred:
        return
    
    # 获取客户端配置信息和订单数据
    client = mysql_client.find(
        f"select success_code,tool from client where client_name='{client_name}'",
        to_json=True,
    )
    tools = client["tool"].split(",")
    
    # 重新执行代码获取订单数据
    str_python_code = client["success_code"]
    dataframe, error_msg = exec_generated_code(str_python_code, origin_file_path)
    if error_msg:
        yield {
            "event": "result",
            "data": json.dumps(
                {
                    "task": "file_process_verify",
                    "message": "获取订单数据失败",
                    "result": {"error_msg": error_msg}
                }
            ),
        }
        return
    
    # 从 DataFrame 获取订单数据
    orders_data = []
    if dataframe is not None:
        try:
            orders_data = dataframe.to_dict('records')
        except Exception as e:
            logger.error(f"DataFrame转换失败: {str(e)}")
            orders_data = []

    # 登录任务
    morelink_client = None
    async for result in login_task(client_name):
        yield result
        if result["event"] == "result":
            # 在这里获取登录后的客户端实例
            node_path = find_playwright_node_path()
            morelink_client = MoreLinkClient(node_path)

    # 批量建单任务
    success_upload_data = None
    async for result in batch_create_orders(orders_data, morelink_client):
        yield result
        if result["event"] == "result":
            result_data = json.loads(result["data"])
            if "error_msg" in result_data["result"]:
                return
            else:
                # 重新调用dahuo_upload获取实际结果
                success_upload_data, fail_upload_data, error_msg = dahuo_upload(
                    orders_data, morelink_client
                )

    # 其它任务
    async for result in execute_other_tasks(success_upload_data, tools, origin_file_path, client_name):
        yield result


