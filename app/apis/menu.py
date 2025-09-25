from fastapi import APIRouter, Body, Depends, HTTPException, Request
from typing import Dict, List, Optional
from app.db_mongo import get_session, enforcer
from bson import ObjectId
from datetime import datetime
from pydantic import BaseModel

from app.schemas import UpdateUserMenuPermissions

# 定义菜单模型
class MenuItem(BaseModel):
    id: Optional[str] = None
    name: str
    parent_id: Optional[str] = None
    children: Optional[List['MenuItem']] = None
    path: Optional[str] = None
    api_endpoint_ids: Optional[List[str]] = None
menu_router = APIRouter(tags=["menu"])

@menu_router.get("/menu", response_model=List[MenuItem], summary="获取菜单树")
async def get_menu_tree(session = Depends(get_session)):
    """获取菜单树"""
    db = session
    
    def get_children(parent_id):
        """递归获取子菜单"""
        children = list(db.menu.find({"parent_id": parent_id}))
        children_items = []
        for child in children:
            child_id = str(child["_id"])
            child_dict = {
                "id": child_id,
                "name": child["name"], 
                "parent_id": child["parent_id"],
                "path": child.get("path",""),
                "api_endpoint_ids": child.get("api_endpoint_ids", []),
                "children": get_children(child_id)
            }
            children_items.append(MenuItem(**child_dict))
        return children_items
    
    # 获取所有一级菜单
    root_menus = list(db.menu.find({"parent_id": None}))
    
    menu_tree = []
    for root in root_menus:
        root_id = str(root["_id"])
        menu_item = MenuItem(
            id=root_id,
            name=root["name"],
            parent_id=root.get("parent_id"),
            path=root.get("path",""),
            api_endpoint_ids=root.get("api_endpoint_ids", []),
            children=get_children(root_id)
        )
        menu_tree.append(menu_item)
        
    return menu_tree

@menu_router.post("/menu", summary="创建菜单项")
async def create_menu_item(menu_item: MenuItem, session = Depends(get_session)):
    """创建菜单项"""
    db = session
    menu_dict = menu_item.dict(exclude_unset=True)
    if "id" in menu_dict:
        del menu_dict["id"]
    if "children" in menu_dict:
        del menu_dict["children"]
        
    result = db.menu.insert_one(menu_dict)
    return {"id": str(result.inserted_id)}

@menu_router.put("/menu/{menu_id}", summary="更新菜单项")
async def update_menu_item(menu_id: str, menu_item: MenuItem, session = Depends(get_session)):
    """更新菜单项"""
    db = session
    menu_dict = menu_item.dict(exclude_unset=True)
    print(menu_dict)
    if "id" in menu_dict:
        del menu_dict["id"]
    if "children" in menu_dict:
        del menu_dict["children"]
        
    result = db.menu.update_one(
        {"_id": ObjectId(menu_id)},
        {"$set": menu_dict}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Menu item not found")
    return {"message": "Menu item updated"}

@menu_router.delete("/menu/{menu_id}", summary="删除菜单项")
async def delete_menu_item(menu_id: str, session = Depends(get_session)):
    """删除菜单项"""
    db = session
    # 检查是否有子菜单
    if db.menu.find_one({"parent_id": menu_id}):
        raise HTTPException(status_code=400, detail="Cannot delete menu item with children")
        
    result = db.menu.delete_one({"_id": ObjectId(menu_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Menu item not found")
    return {"message": "Menu item deleted"}

@menu_router.post("/menu/generate_test_data", summary="生成测试菜单数据")
async def generate_test_menu(session = Depends(get_session)):
    """生成测试菜单数据"""
    db = session
    
    # 清空现有菜单数据
    db.menu.delete_many({})
    
    # 创建一级菜单
    root_menus = [
        {"name": "系统管理"},
        {"name": "业务管理"},
        {"name": "报表管理"}
    ]
    
    for root_menu in root_menus:
        result = db.menu.insert_one(root_menu)
        root_id = str(result.inserted_id)
        
        # 为每个一级菜单创建子菜单
        if root_menu["name"] == "系统管理":
            children = [
                {"name": "用户管理", "parent_id": root_id},
                {"name": "角色管理", "parent_id": root_id},
                {"name": "权限管理", "parent_id": root_id}
            ]
        elif root_menu["name"] == "业务管理":
            children = [
                {"name": "订单管理", "parent_id": root_id},
                {"name": "客户管理", "parent_id": root_id},
                {"name": "产品管理", "parent_id": root_id}
            ]
        else:
            children = [
                {"name": "销售报表", "parent_id": root_id},
                {"name": "财务报表", "parent_id": root_id},
                {"name": "库存报表", "parent_id": root_id}
            ]
            
        db.menu.insert_many(children)
        
    return {"message": "测试菜单数据已生成"}


@menu_router.get("/menu/user/get_user_menu_permissions", summary="获取用户菜单权限")
async def get_user_menu_permissions(user_id: str, session = Depends(get_session)):
    """获取用户菜单权限"""
    db = session
    user = db.users.find_one({"_id": ObjectId(user_id)})
    if user.get("username") == "admin":
        return ["*"]
    return user.get("menu_ids", [])








@menu_router.put("/menu/user/update_user_menu_permissions", summary="更新用户菜单权限")
async def update_user_menu_permissions(update_user_menu_permissions: UpdateUserMenuPermissions, session = Depends(get_session)):
    """更新用户菜单权限"""
    db = session
    user_id = update_user_menu_permissions.user_id
    menu_ids = update_user_menu_permissions.menu_ids
    db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"menu_ids": menu_ids}}
    )
    return {"message": "菜单权限更新成功"}
