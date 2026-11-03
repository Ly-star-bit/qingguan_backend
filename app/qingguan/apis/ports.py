
from ast import Set
import json
from typing import List, Optional
from bson import ObjectId
from pymongo import MongoClient

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    logger
)
from typing import Any, Dict
from fastapi.encoders import jsonable_encoder



from app.db_mongo import _as_list, get_session,enforcer, norm_key
from app.permission_core import ADMIN_ROLE
from casbin import util

def get_forbidden_fields_optimized(e, user: str, obj: str, act: str, fields: List[str]) -> List[str]:
    if not fields:
        return []
    
    obj = obj if obj.startswith("/") else f"/{obj}"
    
    try:
        roles = set(e.get_implicit_roles_for_user(user))
    except Exception:
        roles = set()
    
    if user == "admin" or ADMIN_ROLE in roles:
        return []
    
    try:
        perms = e.get_implicit_permissions_for_user(user)
    except Exception as ex:
        logger.warning(f"get_implicit_permissions_for_user error: {ex}")
        perms = []
    
    allowed: Set[str] = set()
    denied: Set[str] = set()
    wildcard_allow = False
    
    for p in perms:
        if len(p) < 5:
            continue
        try:
            sub, p_obj, p_act, attrs_json, eft, *_ = p
            
            if p_act != act:
                continue
            if not util.key_match4(obj, p_obj):
                continue
            
            # 解析 attrs
            attrs = json.loads(attrs_json)[0] if attrs_json else {}
            editable = attrs.get("editable_fields")
            
            # ⭐ 关键修复：如果 editable 是字符串，再解析一次
            if isinstance(editable, str):
                try:
                    editable = json.loads(editable)
                except json.JSONDecodeError:
                    editable = None
            
            if eft == "allow":
                if editable == "*":
                    wildcard_allow = True
                elif isinstance(editable, (list, tuple)):
                    allowed.update(norm_key(f) for f in editable)
                elif isinstance(editable, dict) and "in" in editable:
                    allowed.update(norm_key(f) for f in editable["in"])
            
            elif eft == "deny":
                if isinstance(editable, (list, tuple)):
                    denied.update(norm_key(f) for f in editable)
                elif isinstance(editable, dict) and "in" in editable:
                    denied.update(norm_key(f) for f in editable["in"])
        
        except Exception as ex:
            logger.warning(f"Error processing implicit perm {p}: {ex}")
            continue
    
    requested = {norm_key(f) for f in fields}
    
    if wildcard_allow:
        return list(requested & denied)
    
    not_allowed = requested - allowed
    explicitly_denied = requested & denied
    forbidden = not_allowed | explicitly_denied
    
    return list(forbidden)


ports_router = APIRouter(tags=['港口'],prefix="/ports")


@ports_router.post("/", summary="创建港口")
def create_port(port: Dict[str, Any], session: MongoClient = Depends(get_session)):
    db = session  # 假设这里已经是 Database 对象
    # 先把请求体里的潜在 ObjectId 等可序列化处理好，并去掉 id/_id
    print(port)
    doc = jsonable_encoder(
        port,
        custom_encoder={ObjectId: str}
    )
    print(doc)
    doc.pop("id", None)
    doc.pop("_id", None)

    result = db.ports.insert_one(doc)

    # 返回值同样走一次 encoder，确保没有原始 ObjectId
    response = {**doc, "id": str(result.inserted_id)}
    return jsonable_encoder(response, custom_encoder={ObjectId: str})


@ports_router.get("/", summary="获取港口列表")
def read_ports(
    context_request: Request,
    session: MongoClient = Depends(get_session),
    skip: int = 0,
    country: Optional[str] = "",
    limit: Optional[int] = None,
):
    db = session
    query = {"country": country} if country else {}

    cursor = db.ports.find(query).skip(skip)
    if limit:
        cursor = cursor.limit(limit)
    raw_ports = list(cursor)

    user = context_request.state.user["sub"]
    act = "PUT"  # 判断"可编辑字段"用更新动作
    obj = "qingguan/ports/{port_id}"  # 通配符路径，获取通用权限

    # 🆕 一次性获取可编辑字段（不需要 for 循环）
    # 假设所有字段都要检查
    all_possible_fields =list(raw_ports[0].keys())
    all_possible_fields.remove("_id")
    
    forbidden_fields = get_forbidden_fields_optimized(
        enforcer, user, obj, act, all_possible_fields
    )
    print('forbidden_fields',forbidden_fields)
    # 可编辑 = 所有可能字段 - 禁止字段
    editable_fields = [f for f in all_possible_fields if f not in forbidden_fields]

    # 现在直接遍历港口，无需再检查权限
    ports = []
    for port in raw_ports:
        # 先整理 id
        port_id = str(port["_id"])
        port["id"] = port_id
        port.pop("_id", None)

        # 直接添加可编辑字段列表
        port["editable_fields"] = editable_fields
        ports.append(port)

    return ports





@ports_router.get("/{port_id}", summary="获取港口详情")
def read_port(port_id: str, session: MongoClient = Depends(get_session)):
    db = session
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    port["id"] = str(port["_id"])
    port.pop("_id", None)


    return port


@ports_router.put("/{port_id}", summary="更新港口")
def update_port(
    context_request: Request,
    port_id: str,
    updated_port: dict,
    session: MongoClient = Depends(get_session),
):
    user = context_request.state.user["sub"]
    obj = context_request.url.path
    act = context_request.method

    # 🆕 一次性获取禁止字段（替代逐字段检查）
    editable_fields = list(updated_port.keys())
    
    # 排除系统字段
    check_fields = [f for f in editable_fields if f not in ("id",)]
    
    # 一次性获取禁止字段
    forbidden_fields = get_forbidden_fields_optimized(
        enforcer, user, obj, act, check_fields
    )

    # 如果有字段不允许修改
    if forbidden_fields:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "FIELD_FORBIDDEN",
                "message": "以下字段没有修改权限",
                "fields": forbidden_fields,
            },
        )

    # === 通过后执行更新 ===
    db = session
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")

    update_data = dict(updated_port)
    update_data.pop("id", None)
    db.ports.update_one({"_id": ObjectId(port_id)}, {"$set": update_data})

    updated = db.ports.find_one({"_id": ObjectId(port_id)})
    updated["id"] = str(updated["_id"])
    updated.pop("_id", None)
    return updated




@ports_router.delete("/{port_id}", summary="删除港口")
def delete_port(port_id: str, session: MongoClient = Depends(get_session)):
    db = session
    port = db.ports.find_one({"_id": ObjectId(port_id)})
    if not port:
        raise HTTPException(status_code=404, detail="Port not found")
    db.ports.delete_one({"_id": ObjectId(port_id)})
    port["id"] = str(port["_id"])
    port.pop("_id", None)
    return port

