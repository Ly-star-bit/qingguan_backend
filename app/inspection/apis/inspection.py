"""HUBS 查验工作台：由独立部署的 HUBS-Inspection 服务移植而来。

登录沿用本系统的 JWT，接口访问仍由 Casbin 控制；模块内的业务角色沿用 HUBS：
系统管理员即 owner，其他用户需要在「人员与权限」中绑定 monitor / analyst / viewer，
未绑定的用户即使有接口权限也读不到数据。数据只写入 inspection_* 集合，不影响其他模块。
"""
import hashlib
import os
import re
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from loguru import logger
from pymongo.errors import DuplicateKeyError

from app.auth_utils import is_admin_user
from app.db_mongo import db as _db, get_session

inspection_router = APIRouter(prefix="/inspection", tags=["查验工作台"])

SHIPMENTS = "inspection_shipments"
ROUNDS = "inspection_rounds"
CARGO = "inspection_cargo"
FILES = "inspection_files"
AUDIT = "inspection_audit"
MEMBERS = "inspection_members"

# 附件按「附件ID 的 SHA-256」存放，与 HUBS 原 data/attachments 目录的命名一致，迁移时可直接复制
FILE_DIR = Path(os.getenv("INSPECTION_FILE_DIR", "./file/inspection"))
MAX_FILE_SIZE = 15 * 1024 * 1024

ROLES = ("monitor", "analyst", "viewer")
MONITOR_FIELDS = (
    "port", "noticeDate", "stationDate", "stationPlanDate", "stationPlanPeriod", "stationPeriod",
    "firstResult", "status", "releaseDate", "resultDate", "applicationDate", "approved",
    "returnFlight", "returnMawb", "etd", "eta", "departed",
)
ANALYST_FIELDS = (
    "description", "sensitive", "exportSummary", "customerMix", "updatedFiles", "fileNotes",
    "ior", "structure", "declarationIssue", "declarationNotes",
)
CORE_FIELDS = (
    "port", "noticeDate", "stationDate", "stationPlanDate", "stationPlanPeriod", "stationPeriod",
    "firstResult", "status", "releaseDate", "resultDate",
)
MONITOR_FILES = ("查验截图", "第一轮结果截屏", "7512文件")
VIEWER_FILES = ("查验截图", "第一轮结果截屏")
ANALYST_FILES = ("主单", "分单", "仓单", "分单及仓单", "Segregation", "出口资料汇总", "整理完的出口资料", "更新后的发票及补充文件")
ROUND_FILE_RE = re.compile(r"^轮次:[^:]+:(提交|反馈)$")
STATUSES = ("在查", "已放行", "安排退运")
FIRST_RESULTS = ("待反馈", "放行", "要求更新文件", "RTME 扣留", "其他扣留")
PERIODS = ("", "上午", "下午")

try:
    _db[SHIPMENTS].create_index("mawb", unique=True)
    for _col in (ROUNDS, CARGO, FILES, AUDIT):
        _db[_col].create_index("shipment_id")
except Exception:
    logger.exception("查验工作台索引创建失败")


def _now() -> str:
    # 与 HUBS 保持同样的 ISO 格式（毫秒 + Z），前端按字符串排序
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _valid_date(value: Any) -> bool:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def blob_path(file_id: str) -> Path:
    return FILE_DIR / hashlib.sha256(file_id.encode()).hexdigest()


def _identity(request: Request, db) -> Dict[str, str]:
    username = (getattr(request.state, "user", None) or {}).get("sub")
    if not username:
        raise HTTPException(status_code=401, detail="请先登录")
    if is_admin_user(username):
        return {"username": username, "name": username, "role": "owner"}
    member = db[MEMBERS].find_one({"_id": username})
    if not member:
        raise HTTPException(status_code=403, detail="账号尚未开通查验工作台权限，请联系管理员")
    return {"username": username, "name": member.get("name") or username, "role": member["role"]}


def _allow(user: Dict[str, str], roles) -> None:
    if user["role"] != "owner" and user["role"] not in roles:
        raise HTTPException(status_code=403, detail="当前角色没有此操作权限")


def _ship(db, shipment_id: str, user: Dict[str, str]) -> Dict[str, Any]:
    s = db[SHIPMENTS].find_one({"_id": shipment_id})
    if not s:
        raise HTTPException(status_code=404, detail="未找到货物")
    if user["role"] == "viewer" and s["data"].get("status") != "在查":
        raise HTTPException(status_code=403, detail="该货物不在查询范围内")
    return s


def _can_read_file(role: str, category: str) -> bool:
    if role == "viewer":
        return category in VIEWER_FILES
    if role == "monitor":
        return category in MONITOR_FILES
    return True


def _log(db, shipment_id: str, user: Dict[str, str], action: str) -> None:
    db[AUDIT].insert_one({
        "_id": str(uuid.uuid4()),
        "shipment_id": shipment_id,
        "actor": user["username"],
        "action": action,
        "created": _now(),
    })


def _save_shipment(db, s: Dict[str, Any], revision: Any, data: Dict[str, Any]) -> None:
    """按版本号乐观锁保存，别人先保存过时返回 409"""
    try:
        revision = int(revision)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="缺少记录版本，请刷新后再保存")
    r = db[SHIPMENTS].update_one(
        {"_id": s["_id"], "revision": revision},
        {"$set": {"data": data, "updated": _now()}, "$inc": {"revision": 1}},
    )
    if not r.matched_count:
        raise HTTPException(status_code=409, detail="记录已被其他人更新，请刷新后再保存")


@inspection_router.get("/workspace", summary="查验工作台数据（按业务角色过滤）")
def get_workspace(request: Request, db=Depends(get_session)):
    user = _identity(request, db)
    role = user["role"]
    shipments = [
        s for s in db[SHIPMENTS].find().sort("updated", -1)
        if role != "viewer" or s["data"].get("status") == "在查"
    ]
    ids = [s["_id"] for s in shipments]

    def related(col: str):
        return list(db[col].find({"shipment_id": {"$in": ids}}))

    def shipment_out(s):
        d = s["data"]
        if role == "viewer":
            d = {k: d[k] for k in CORE_FIELDS if k in d}
        elif role == "monitor":
            d = {k: d[k] for k in MONITOR_FIELDS if k in d}
        return {"id": s["_id"], "mawb": s["mawb"], "revision": s["revision"], "updated": s["updated"], **d}

    full = role in ("owner", "analyst")
    rounds = [
        {"id": r.pop("_id"), "shipmentId": r.pop("shipment_id"), **r} for r in related(ROUNDS)
    ] if full else []
    cargo = [{"id": c.pop("_id"), **c} for c in related(CARGO)] if full else []
    files = [{"id": f.pop("_id"), **f} for f in related(FILES) if _can_read_file(role, f["category"])]
    audit = [{"id": a.pop("_id"), **a} for a in related(AUDIT)] if role != "viewer" else []
    members, users = [], []
    if role == "owner":
        members = [{"username": m["_id"], "name": m.get("name", ""), "role": m["role"]} for m in db[MEMBERS].find()]
        users = sorted(u["username"] for u in db.users.find({}, {"username": 1}) if u.get("username"))
    return {
        "user": user,
        "shipments": [shipment_out(s) for s in shipments],
        "rounds": rounds,
        "cargo": cargo,
        "files": files,
        "audit": audit,
        "members": members,
        "users": users,
    }


@inspection_router.get("/file", summary="下载查验附件")
def download_file(file_id: str, request: Request, db=Depends(get_session)):
    user = _identity(request, db)
    f = db[FILES].find_one({"_id": file_id})
    if not f:
        raise HTTPException(status_code=404, detail="附件不存在")
    _ship(db, f["shipment_id"], user)
    if not _can_read_file(user["role"], f["category"]):
        raise HTTPException(status_code=403, detail="无权访问该附件")
    path = blob_path(file_id)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="附件文件不存在")
    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=f["name"],
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )


@inspection_router.post("/upload", summary="上传查验附件")
def upload_file(
    request: Request,
    shipment_id: str = Form(...),
    category: str = Form(...),
    file: UploadFile = File(...),
    db=Depends(get_session),
):
    user = _identity(request, db)
    _allow(user, ("monitor", "analyst"))
    _ship(db, shipment_id, user)
    is_monitor_file = category in MONITOR_FILES
    if not (is_monitor_file or category in ANALYST_FILES or ROUND_FILE_RE.match(category)):
        raise HTTPException(status_code=400, detail="无效的附件类别")
    if (user["role"] == "monitor" and not is_monitor_file) or (user["role"] == "analyst" and is_monitor_file):
        raise HTTPException(status_code=403, detail="附件类别无权上传")
    content = file.file.read(MAX_FILE_SIZE + 1)
    if not content or len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="请选择不超过15MB的文件")

    file_id = str(uuid.uuid4())
    FILE_DIR.mkdir(parents=True, exist_ok=True)
    target = blob_path(file_id)
    temp = target.with_name(f"{target.name}.{uuid.uuid4().hex}.tmp")
    temp.write_bytes(content)
    os.replace(temp, target)
    db[FILES].insert_one({
        "_id": file_id,
        "shipment_id": shipment_id,
        "category": category,
        "name": file.filename or "附件",
        "type": file.content_type or "",
        "size": len(content),
    })
    _log(db, shipment_id, user, "上传附件：" + category)
    return {"ok": True}


@inspection_router.post("/workspace", summary="查验工作台操作（登记/跟进/处理/授权）")
def post_workspace(request: Request, p: Dict[str, Any] = Body(...), db=Depends(get_session)):
    user = _identity(request, db)
    action = p.get("action")

    if action == "member":
        _allow(user, ())
        username = str(p.get("username") or "").strip()
        name = str(p.get("name") or "").strip()
        if p.get("role") not in ROLES or not username or not name:
            raise HTTPException(status_code=400, detail="请选择账号并填写姓名和角色")
        if not db.users.find_one({"username": username}):
            raise HTTPException(status_code=400, detail="系统中不存在该账号")
        db[MEMBERS].update_one({"_id": username}, {"$set": {"name": name, "role": p["role"]}}, upsert=True)
        return {"ok": True}

    if action == "revoke":
        _allow(user, ())
        db[MEMBERS].delete_one({"_id": str(p.get("username") or "")})
        return {"ok": True}

    if action == "create":
        _allow(user, ("monitor",))
        mawb = str(p.get("mawb") or "").strip()
        port = str(p.get("port") or "").strip()
        if not re.fullmatch(r"\d{3}-\d{8}", mawb):
            raise HTTPException(status_code=400, detail="主单号格式应为 160-12345678")
        if not port or not _valid_date(p.get("noticeDate")):
            raise HTTPException(status_code=400, detail="口岸和通知日期必填")
        shipment_id = str(uuid.uuid4())
        try:
            db[SHIPMENTS].insert_one({
                "_id": shipment_id,
                "mawb": mawb,
                "revision": 1,
                "updated": _now(),
                "data": {"port": port.upper(), "noticeDate": p["noticeDate"], "status": "在查", "firstResult": "待反馈"},
            })
        except DuplicateKeyError:
            raise HTTPException(status_code=409, detail="这票主单已登记，请搜索后跟进")
        _log(db, shipment_id, user, "登记查验")
        return {"id": shipment_id}

    s = _ship(db, str(p.get("shipmentId") or ""), user)
    data = s["data"]

    if action == "station":
        _allow(user, ("monitor",))
        mode = p.get("mode")
        if mode not in ("plan", "actual", "confirm"):
            raise HTTPException(status_code=400, detail="无效送查操作")
        if mode == "plan" and data.get("stationDate"):
            raise HTTPException(status_code=400, detail="已确认实际送达，请修改实际时间")
        if mode == "confirm" and (data.get("stationDate") or not data.get("stationPlanDate")):
            raise HTTPException(status_code=400, detail="没有可确认的送查计划")
        d = data.get("stationPlanDate") if mode == "confirm" else p.get("date")
        period = (data.get("stationPlanPeriod") or "") if mode == "confirm" else (p.get("period") or "")
        if not _valid_date(d):
            raise HTTPException(status_code=400, detail="请选择有效的送查日期")
        if period not in PERIODS:
            raise HTTPException(status_code=400, detail="无效时段")
        if mode == "plan":
            new_data = {**data, "stationPlanDate": d, "stationPlanPeriod": period}
        else:
            new_data = {**data, "stationDate": d, "stationPeriod": period}
        _save_shipment(db, s, p.get("revision"), new_data)
        _log(db, s["_id"], user, ("计划送查：" if mode == "plan" else "确认实际送查：") + d + (" " + period if period else ""))
        return {"ok": True}

    if action == "update":
        _allow(user, ("monitor", "analyst"))
        fields = p.get("fields")
        if not isinstance(fields, dict) or not fields:
            raise HTTPException(status_code=400, detail="没有需要保存的内容")
        if user["role"] == "owner":
            allowed = (*MONITOR_FIELDS, *ANALYST_FIELDS, "pickupDate")
        else:
            allowed = MONITOR_FIELDS if user["role"] == "monitor" else ANALYST_FIELDS
        bad = next((k for k in fields if k not in allowed), None)
        if bad:
            raise HTTPException(status_code=403, detail="无权修改字段：" + bad)
        for k, v in fields.items():
            if not isinstance(v, (str, bool)):
                raise HTTPException(status_code=400, detail="无效字段内容")
            if isinstance(v, str) and len(v) > 20000:
                raise HTTPException(status_code=400, detail="内容过长")
            if k.endswith("Date") and v and not _valid_date(v):
                raise HTTPException(status_code=400, detail="日期格式无效")
        new_data = {**data, **fields}
        if new_data.get("status") not in STATUSES:
            raise HTTPException(status_code=400, detail="无效状态")
        if new_data.get("firstResult") not in FIRST_RESULTS:
            raise HTTPException(status_code=400, detail="无效查验结果")
        if fields.get("firstResult") == "放行":
            new_data["status"] = "已放行"
        if new_data["status"] == "已放行" and not new_data.get("releaseDate"):
            new_data["releaseDate"] = date.today().isoformat()
        _save_shipment(db, s, p.get("revision"), new_data)
        _log(db, s["_id"], user, "更新：" + "、".join(fields))
        return {"ok": True}

    if action == "round":
        _allow(user, ("analyst",))
        body = p.get("body") or {}
        measure = str(body.get("measure") or "").strip()
        if not _valid_date(body.get("date")) or not measure:
            raise HTTPException(status_code=400, detail="处理日期和措施必填")
        if data.get("status") != "在查":
            raise HTTPException(status_code=400, detail="该票已离开在查池，不能新增轮次")
        db[ROUNDS].insert_one({
            "_id": str(uuid.uuid4()),
            "shipment_id": s["_id"],
            "created": _now(),
            "date": body["date"],
            "measure": measure,
            "notes": str(body.get("notes") or ""),
            "feedbackDate": "",
            "feedback": "",
        })
        _log(db, s["_id"], user, "新增处理轮次")
        return {"ok": True}

    if action == "feedback":
        _allow(user, ("analyst",))
        feedback_date = p.get("feedbackDate") or ""
        if feedback_date and not _valid_date(feedback_date):
            raise HTTPException(status_code=400, detail="日期格式无效")
        r = db[ROUNDS].update_one(
            {"_id": str(p.get("roundId") or ""), "shipment_id": s["_id"]},
            {"$set": {"feedbackDate": feedback_date, "feedback": str(p.get("feedback") or "")}},
        )
        if not r.matched_count:
            raise HTTPException(status_code=404, detail="未找到处理轮次")
        _log(db, s["_id"], user, "更新CBP反馈")
        return {"ok": True}

    if action == "cargo":
        _allow(user, ("analyst",))
        rows = p.get("rows")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 1000:
            raise HTTPException(status_code=400, detail="一次需提供1至1000行明细")
        docs = []
        for row in rows:
            row = row if isinstance(row, dict) else {}
            customer = str(row.get("customer") or "").strip()
            cartons = row.get("cartons")
            is_integer = (isinstance(cartons, int) and not isinstance(cartons, bool)) or (
                isinstance(cartons, float) and cartons.is_integer()
            )
            if not customer or not is_integer or cartons < 1:
                raise HTTPException(status_code=400, detail="客户必填，箱数须为正整数")
            docs.append({
                "_id": str(uuid.uuid4()),
                "shipment_id": s["_id"],
                "customer": customer,
                "cartons": int(cartons),
                "hawb": str(row.get("hawb") or "").strip(),
            })
        db[CARGO].insert_many(docs)
        _log(db, s["_id"], user, f"新增客户明细 {len(docs)} 行")
        return {"ok": True}

    raise HTTPException(status_code=400, detail="未知操作")
