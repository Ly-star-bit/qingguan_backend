import io
import os
from dotenv import load_dotenv
from fastapi import APIRouter, File, HTTPException, UploadFile, Form
from fastapi.responses import StreamingResponse,JSONResponse
import pandas as pd
from app.hubs_new_morelink.upload import dahuo_upload, exec_generated_code
from pathlib import Path
import uuid
from app.hubs_new_morelink.hubs_client import HubsClient
from app.hubs_new_morelink.schemas import DahuoUploadResponse,DahuoUploadSuccessItem
from feapder.db.mysqldb import MysqlDB
from loguru import logger
import traceback
from fastapi import status
from app.hubs_new_morelink.smarty import validate_address as smarty_validate_address
from app.hubs_new_morelink.remotecheck import all_remoteaddresscheck_process
from pydantic import BaseModel, Field
from typing import Optional
hubs_router = APIRouter(tags=["hubs_client"],prefix='/hubs_client')
load_dotenv()



DATABASE_CONFIG = {
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASS"),
    'host': os.getenv("MYSQL_HOST"),
    'database':  os.getenv("MYSQL_DB"),
    "port": int(os.getenv("MYSQL_PORT"))
}
mysql_client = MysqlDB(
    ip=DATABASE_CONFIG['host'],
    port=DATABASE_CONFIG['port'],
    db=DATABASE_CONFIG['database'],
    user_name=DATABASE_CONFIG['user'],
    user_pass=DATABASE_CONFIG['password']
)
@hubs_router.get("/client_names", summary="获取所有客户名称")
async def get_all_client_names():
    """获取所有客户名称"""
    try:
        result = mysql_client.find(
            "SELECT client_name FROM client ORDER BY create_time DESC",
            to_json=True
        )
        client_names = [row["client_name"] for row in result] if result else []
        return JSONResponse({"success": True, "data": client_names, "total": len(client_names),'code':200})
    except Exception as e:
        return JSONResponse({"success": False, "message": str(e),"code":500})

@hubs_router.post("/execute_dahuo_upload", summary="执行大货上传建单", response_model=DahuoUploadResponse)
async def execute_dahuo_upload(
    client_name: str = Form(...),
    upload_file: UploadFile = File(...)
):
    """执行大货上传建单
    
    Args:
        client_name: 客户名称
        upload_file: 上传的Excel文件
    """
    file_path = None
    try:
        # 1. 从数据库获取 success_code
        client_data = mysql_client.find(
            f"SELECT success_code FROM client WHERE client_name='{client_name}'",
            to_json=True,
            limit=1
        )
        # logger.info(client_data)
        if not client_data:

            return JSONResponse({
                "code":404,
                "message":f"客户 {client_name} 不存在"
            })
          
        success_code = client_data["success_code"]
        
        # 2. 保存上传的文件到 ./file/hubs_client 目录
        save_directory = Path("./file/hubs_client/")
        save_directory.mkdir(parents=True, exist_ok=True)
        
        file_name = f"{uuid.uuid4()}-{upload_file.filename}"
        file_path = save_directory / file_name
        
        with open(file_path, "wb") as buffer:
            buffer.write(await upload_file.read())  # 注意：FastAPI 中需 await
        
        # 3. 调用处理逻辑
        dataframe, error_msg = exec_generated_code(success_code, file_path)
        if dataframe is None:
             return JSONResponse({
                "code":400,
                "message":f"数据处理失败: {error_msg}"
            })
        
        data = dataframe.to_dict(orient="records")
        # hubs_client = HubsClient()
        success_data, fail_data,upload_error_msg = dahuo_upload(data)  # 修正拼写：sucess_data → success_data

        # 4. 将 success_data 转为 Excel 并返回
               # ✅ 关键：将 dict 列表转换为 SuccessItem 列表（Pydantic 会自动验证）
        items = []
        for item in success_data:
            # 确保字段存在，缺失字段用空字符串填充（根据你的原始逻辑）
            items.append(
                DahuoUploadSuccessItem(
                    shipmendID=item.get("shipmendID", ""),
                    operNo=item.get("A单号", ""),
                    boxNum=item.get("箱数", ""),
                    Volume=item.get("体积", ""),
                    Weight=item.get("实重", ""),
                    fbaWarehouse=item.get("fba仓库", ""),
                    zipCode=item.get("邮编", ""),
                    Sono=item.get("sono", "")
                )
            )

        return DahuoUploadResponse(
            code=200,
            message="上传并建单成功",
            data=items
        )
        # if not success_data:
        #     # 如果没有成功数据，返回空 Excel 或提示
        #     df = pd.DataFrame(columns=[
        #         "shipmendID", "A单号", "箱数", "体积", "实重", "fba仓库", "邮编", "sono"
        #     ])
        # else:
            
        #     df = pd.DataFrame(success_data)

        # # 写入内存中的 Excel
        # output = io.BytesIO()
        # with pd.ExcelWriter(output, engine='openpyxl') as writer:
        #     df.to_excel(writer, index=False, sheet_name="成功数据")
        # output.seek(0)

        # # 返回 Excel 文件流
        # return StreamingResponse(
        #     output,
        #     media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        #     headers={
        #         "Content-Disposition": f'attachment; filename="success_upload_{client_name}.xlsx"'
        #     }
        # )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@hubs_router.post("/login", summary="无滑块登录，返回token")
async def login(username,password):
    try:
        hubs_client = HubsClient(username=username,password=password)
        return hubs_client.httpx_client
    except Exception as e:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


class CheckRdiRemoteRequest(BaseModel):
    street: str = Field(..., example="123 Main St")
    city: str = Field(..., example="Austin")
    state: str = Field(..., min_length=2, max_length=2, example="TX")
    zipcode: str = Field(..., min_length=5, max_length=10, example="78701")
#
@hubs_router.post(
    "/check_rdi_remote",
    summary="检测住宅地址（RDI）和是否偏远地区",
    response_description="返回地址验证结果与偏远状态"
)
async def check_rdi_remote(rdi_remote_data: CheckRdiRemoteRequest):
    try:
        # Step 1: 调用 Smarty 验证地址并获取 RDI 信息
        rdi_response = smarty_validate_address(
            street=rdi_remote_data.street,
            city=rdi_remote_data.city,
            state=rdi_remote_data.state.upper().strip(),
            zipcode=rdi_remote_data.zipcode.strip()
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"地址验证服务异常: {str(e)}"
        )



    # Step 3: 检查是否偏远地区（基于 ZIP）
    try:
        remote_result = all_remoteaddresscheck_process(rdi_remote_data.zipcode.strip())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"偏远地址检查失败: {str(e)}"
        )

    # Step 4: 构造清晰的响应
    return {
        "rdi":rdi_response,
        "remote_result": remote_result,  # 假设 remote_result 为 True/False 或非空即偏远
        "zipcode": rdi_remote_data.zipcode.strip()
    }
