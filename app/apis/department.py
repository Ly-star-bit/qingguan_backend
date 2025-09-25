from fastapi import APIRouter, HTTPException
from typing import Optional, List
from pydantic import BaseModel
from bson import ObjectId
from app.db_mongo import get_db
from loguru import logger
# 配置日志

# 创建路由
department_router = APIRouter(tags=['部门'])

# 定义部门模型
class DepartmentBase(BaseModel):
    name: str
    description: Optional[str] = None

class DepartmentCreate(DepartmentBase):
    pass

class DepartmentUpdate(DepartmentBase):
    pass

class Department(DepartmentBase):
    id: str

    
# 创建部门
@department_router.post("/departments", response_model=Department, summary="创建部门")
async def create_department(department: DepartmentCreate):
    try:
        with get_db() as db:
            department_dict = department.dict()
            result = db.departments.insert_one(department_dict)
            department_dict["id"] = str(result.inserted_id)
            return department_dict
    except Exception as e:
        logger.error(f"创建部门失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"创建部门失败: {str(e)}")

# 获取所有部门
@department_router.get("/departments", response_model=List[Department], summary="获取所有部门")
async def get_departments():
    try:
        with get_db() as db:
            departments = []
            for dept in db.departments.find():
                dept["id"] = str(dept.pop("_id"))
                departments.append(dept)
            return departments
    except Exception as e:
        logger.error(f"获取部门列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取部门列表失败: {str(e)}")

# 获取单个部门
@department_router.get("/departments/{department_id}", response_model=Department, summary="获取单个部门")
async def get_department(department_id: str):
    try:
        with get_db() as db:
            department = db.departments.find_one({"_id": ObjectId(department_id)})
            if department:
                department["id"] = str(department.pop("_id"))
                return department
            raise HTTPException(status_code=404, detail="部门不存在")
    except Exception as e:
        logger.error(f"获取部门详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取部门详情失败: {str(e)}")

# 更新部门
@department_router.put("/departments/{department_id}", response_model=Department, summary="更新部门")
async def update_department(department_id: str, department: DepartmentUpdate):
    try:
        with get_db() as db:
            update_result = db.departments.update_one(
                {"_id": ObjectId(department_id)},
                {"$set": department.dict()}
            )
            if update_result.modified_count:
                updated_department = db.departments.find_one({"_id": ObjectId(department_id)})
                updated_department["id"] = str(updated_department.pop("_id"))
                return updated_department
            raise HTTPException(status_code=404, detail="部门不存在")
    except Exception as e:
        logger.error(f"更新部门失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新部门失败: {str(e)}")

# 删除部门
@department_router.delete("/departments/{department_id}", summary="删除部门")
async def delete_department(department_id: str):
    try:
        with get_db() as db:
            delete_result = db.departments.delete_one({"_id": ObjectId(department_id)})
            if delete_result.deleted_count:
                return {"message": "部门删除成功"}
            raise HTTPException(status_code=404, detail="部门不存在")
    except Exception as e:
        logger.error(f"删除部门失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"删除部门失败: {str(e)}")
