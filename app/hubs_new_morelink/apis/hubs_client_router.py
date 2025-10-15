import io
import os
from dotenv import load_dotenv
from fastapi import APIRouter, File, HTTPException, UploadFile, Form
from fastapi.responses import StreamingResponse
import pandas as pd
from app.hubs_new_morelink.upload import dahuo_upload, exec_generated_code
from pathlib import Path
import uuid
from app.hubs_new_morelink.hubs_client import HubsClient
from app.hubs_new_morelink.schemas import DahuoUploadResponse,DahuoUploadSuccessItem
from feapder.db.mysqldb import MysqlDB
from loguru import logger
import traceback

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
        return {"success": True, "data": client_names, "total": len(client_names)}
    except Exception as e:
        return {"success": False, "message": str(e)}

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
        logger.info(client_data)
        if not client_data:
            raise HTTPException(status_code=404, detail=f"客户 {client_name} 不存在")
        
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
            raise HTTPException(status_code=400, detail=f"数据处理失败: {error_msg}")
        
        data = dataframe.to_dict(orient="records")
        # hubs_client = HubsClient()
        success_data, fail_data = dahuo_upload(data)  # 修正拼写：sucess_data → success_data

        # 4. 将 success_data 转为 Excel 并返回
               # ✅ 关键：将 dict 列表转换为 SuccessItem 列表（Pydantic 会自动验证）
        items = []
        for item in success_data:
            # 确保字段存在，缺失字段用空字符串填充（根据你的原始逻辑）
            items.append(
                DahuoUploadSuccessItem(
                    shipmendID=item.get("shipmendID", ""),
                    A单号=item.get("A单号", ""),
                    箱数=item.get("箱数", ""),
                    体积=item.get("体积", ""),
                    实重=item.get("实重", ""),
                    fba仓库=item.get("fba仓库", ""),
                    邮编=item.get("邮编", ""),
                    sono=item.get("sono", "")
                )
            )

        return DahuoUploadResponse(
            code=200,
            msg="上传并建单成功",
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



