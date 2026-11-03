"""HUBS 查验工作台：由独立部署的 HUBS-Inspection 服务移植而来。

登录沿用本系统的 JWT，接口访问由 Casbin 控制。业务角色（登记 / 处理 / 只读）也来自系统角色：
在「角色管理」里的 inspection_monitor / inspection_analyst / inspection_viewer 三个角色
（由 app/inspection/setup_roles.py 创建），给账号分配角色后，菜单、接口权限和业务角色一次生效。
数据只写入 inspection_* 集合，不影响其他模块。
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
from app.db_mongo import db as _db, enforcer, get_session

inspection_router = APIRouter(prefix="/inspection", tags=["查验工作台"])

SHIPMENTS = "inspection_shipments"
ROUNDS = "inspection_rounds"
CARGO = "inspection_cargo"
FILES = "inspection_files"
AUDIT = "inspection_audit"
# 旧版在模块内按账号绑定业务角色的集合，已改用系统角色，只有 setup_roles.py 迁移时读取
MEMBERS = "inspection_members"

# 附件按「附件ID 的 SHA-256」存放，与 HUBS 原 data/attachments 目录的命名一致，迁移时可直接复制
FILE_DIR = Path(os.getenv("INSPECTION_FILE_DIR", "./file/inspection"))
MAX_FILE_SIZE = 15 * 1024 * 1024

# 系统角色 → 业务角色；admin 以及同时拥有登记、处理两个角色的账号视为 owner（全部权限）
SYSTEM_ROLES = {
    "inspection_monitor": "monitor",
    "inspection_analyst": "analyst",
    "inspection_viewer": "viewer",
}
# 以下字段、附件类别和校验规则与 HUBS V2（2026-11-03）一致
MONITOR_FIELDS = (
    "port", "noticeDate", "stationDate", "stationPlanDate", "stationPlanPeriod", "stationPeriod",
    "pickupConfirmed", "releasedPickupDate", "openedCartons", "warehouseNotes",
    "applicationDate", "approved", "returnFlight", "returnMawb", "etd", "eta", "departed",
)
ANALYST_FIELDS = (
    "description", "sensitive", "exportSummary", "customerMix", "updatedFiles", "fileNotes",
    "ior", "structure", "declarationIssue", "declarationNotes", "declarationStatus",
    "inspectionType", "inspectionTypeOther", "preprocessComplete",
    "firstResult", "resultDate", "resultNotes", "officer",
)
# 只有 owner 能直接改状态，用来纠正误放行、误退运（HUBS V2 没有这个入口）
OWNER_FIELDS = ("status", "releaseDate", "pickupDate")
CORE_FIELDS = (
    "port", "noticeDate", "stationDate", "stationPlanDate", "stationPlanPeriod", "stationPeriod",
    "firstResult", "status", "releaseDate", "resultDate",
    "publicProgress", "publicRound", "publicPlan", "publicFeedback", "approved", "hasFirstFeedback",
)
BOOL_FIELDS = ("pickupConfirmed", "approved", "departed", "sensitive", "updatedFiles", "preprocessComplete")
MONITOR_FILES = ("查验截图", "7512文件", "回仓照片")
# 登记角色还能查看查验结果附件
MONITOR_READ_FILES = (*MONITOR_FILES, "第一轮结果截屏", "结果文件")
VIEWER_FILES = ("查验截图", "第一轮结果截屏")
# 处理角色的附件类别。后一行是旧类别：页面上并入新类别显示（分单、仓单 → 分单及仓单，出口资料汇总 → 发票，
# 第一轮结果截屏 → 结果文件，其它 → 更新后的发票及补充文件），不再上传新文件，已有文件照常查看和删除
ANALYST_FILES = (
    "主单", "分单及仓单", "发票", "Segregation", "相关预报文件", "更新后的发票及补充文件", "结果文件",
    "分单", "仓单", "出口资料汇总", "整理完的出口资料", "第一轮结果截屏", "其它",
)
# 预处理勾选「调整文件」后，必须先上传的更新文件
UPDATE_FILES = ("更新后的发票及补充文件", "其它")
RESULT_FILES = ("结果文件", "第一轮结果截屏")
ROUND_FILE_RE = re.compile(r"^轮次:([^:]+):(提交|反馈)$")
STATUSES = ("在查", "申请退运", "安排退运", "已放行")
FEEDBACK_RESULTS = ("", "放行", "提出新要求")
FIRST_RESULTS = ("待反馈", "放行", "要求更新货值", "要求更新文件", "要求补充文件", "RTME 扣留", "其他扣留", "其他")
DECLARATION_STATUSES = ("已申报", "未申报", "未知")
INSPECTION_TYPES = ("舱单查验", "申报查验", "放行后查验", "不明", "其他")
OPENED_CARTONS = ("1–5箱", "6–10箱", "11–20箱", "21箱及以上")
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


def _identity(request: Request) -> Dict[str, str]:
    username = (getattr(request.state, "user", None) or {}).get("sub")
    if not username:
        raise HTTPException(status_code=401, detail="请先登录")
    if is_admin_user(username):
        role = "owner"
    else:
        # 含角色继承；被禁用的角色不会出现在这里
        roles = {SYSTEM_ROLES[r] for r in enforcer.get_implicit_roles_for_user(username) if r in SYSTEM_ROLES}
        if {"monitor", "analyst"} <= roles:
            role = "owner"
        else:
            role = next((r for r in ("monitor", "analyst", "viewer") if r in roles), None)
        if not role:
            raise HTTPException(
                status_code=403,
                detail="账号尚未分配查验角色，请联系管理员在「角色管理」中分配 inspection_monitor、inspection_analyst 或 inspection_viewer",
            )
    return {"username": username, "name": username, "role": role}


def _allow(user: Dict[str, str], roles) -> None:
    if user["role"] != "owner" and user["role"] not in roles:
        raise HTTPException(status_code=403, detail="当前角色没有此操作权限")


def _check_file_category(user: Dict[str, str], category: str, verb: str) -> None:
    """上传、删除附件的权限相同：登记角色管登记类附件，处理角色管其余附件，owner 都可以"""
    is_monitor_file = category in MONITOR_FILES
    if not (is_monitor_file or category in ANALYST_FILES or ROUND_FILE_RE.match(category)):
        raise HTTPException(status_code=400, detail="无效的附件类别")
    if (user["role"] == "monitor" and not is_monitor_file) or (user["role"] == "analyst" and is_monitor_file):
        raise HTTPException(status_code=403, detail=f"附件类别无权{verb}")


def _file_label(category: str, name: str) -> str:
    """操作记录里的附件描述；轮次附件的类别带轮次 ID，换成可读的名称"""
    m = ROUND_FILE_RE.match(category)
    return f"{'轮次' + m.group(2) + '附件' if m else category} · {name}"


def _feedback_result(rnd: Dict[str, Any]) -> str:
    """反馈结果；「提出新要求」以前叫「继续」「新要求」，旧记录按新名称处理"""
    value = rnd.get("feedbackResult") or ""
    return "提出新要求" if value in ("继续", "新要求") else value


def _rounds_of(db, shipment_id: str) -> list:
    """按创建顺序排列的处理轮次，最后一个是最新一轮"""
    return list(db[ROUNDS].find({"shipment_id": shipment_id}).sort([("created", 1), ("_id", 1)]))


def _check_round_file(db, s: Dict[str, Any], category: str) -> None:
    """轮次附件只能加到、删自在查货物的最新一轮，历史轮次锁定"""
    m = ROUND_FILE_RE.match(category)
    if not m:
        return
    rounds = _rounds_of(db, s["_id"])
    if not any(r["_id"] == m.group(1) for r in rounds):
        raise HTTPException(status_code=400, detail="处理轮次不存在")
    if s["data"].get("status") != "在查" or rounds[-1]["_id"] != m.group(1):
        raise HTTPException(status_code=400, detail="历史轮次已锁定")


def _ship(db, shipment_id: str, user: Dict[str, str]) -> Dict[str, Any]:
    s = db[SHIPMENTS].find_one({"_id": shipment_id})
    if not s:
        raise HTTPException(status_code=404, detail="未找到货物")
    return s


def _can_read_file(role: str, category: str) -> bool:
    if role == "viewer":
        return category in VIEWER_FILES
    if role == "monitor":
        return category in MONITOR_READ_FILES
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
    user = _identity(request)
    role = user["role"]
    shipments = list(db[SHIPMENTS].find().sort("updated", -1))
    ids = [s["_id"] for s in shipments]

    def related(col: str):
        return list(db[col].find({"shipment_id": {"$in": ids}}))

    all_rounds = sorted(
        (
            {"id": r.pop("_id"), "shipmentId": r.pop("shipment_id"), **r,
             "feedbackResult": _feedback_result(r), "revision": r.get("revision") or 1}
            for r in related(ROUNDS)
        ),
        key=lambda r: (r.get("created") or "", r["id"]),
    )
    rounds_by: Dict[str, list] = {}
    for r in all_rounds:
        rounds_by.setdefault(r["shipmentId"], []).append(r)
    all_files = [f for f in related(FILES) if not f.get("deleted")]
    with_result_files = {f["shipment_id"] for f in all_files if f["category"] in RESULT_FILES}

    def shipment_out(s):
        d = dict(s["data"])
        # 旧版字段名 cbpOfficer
        if "officer" not in d and d.get("cbpOfficer"):
            d["officer"] = d["cbpOfficer"]
        rs = rounds_by.get(s["_id"], [])
        last = rs[-1] if rs else None
        # 登记、只读角色拿不到轮次明细，进度由最新一轮算出（HUBS V2 写在货物记录里，这里实时计算，避免两边不一致）
        d.update({
            "publicRound": len(rs),
            "publicPlan": last["measure"] if last else "",
            "publicFeedback": "：".join(x for x in (last["feedbackResult"], last.get("feedback") or "") if x) if last else "",
            "publicProgress": "沟通处理中" if rs else "",
            "hasFirstFeedback": bool(
                (d.get("firstResult") and d["firstResult"] != "待反馈") or d.get("resultDate") or d.get("resultNotes")
                or rs or s["_id"] in with_result_files
            ),
        })
        if role == "viewer":
            d = {k: d[k] for k in CORE_FIELDS if k in d}
        elif role == "monitor":
            d = {k: d[k] for k in (*MONITOR_FIELDS, *CORE_FIELDS) if k in d}
        return {"id": s["_id"], "mawb": s["mawb"], "revision": s["revision"], "updated": s["updated"], **d}

    full = role in ("owner", "analyst")
    rounds = all_rounds if full else []
    cargo = [{"id": c.pop("_id"), **c} for c in related(CARGO)] if full else []
    files = [{"id": f.pop("_id"), **f} for f in all_files if _can_read_file(role, f["category"])]
    audit = [{"id": a.pop("_id"), **a} for a in related(AUDIT)] if role != "viewer" else []
    return {
        "user": user,
        "shipments": [shipment_out(s) for s in shipments],
        "rounds": rounds,
        "cargo": cargo,
        "files": files,
        "audit": audit,
    }


@inspection_router.get("/file", summary="下载查验附件")
def download_file(file_id: str, request: Request, db=Depends(get_session)):
    user = _identity(request)
    f = db[FILES].find_one({"_id": file_id, "deleted": {"$ne": True}})
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
    user = _identity(request)
    _allow(user, ("monitor", "analyst"))
    s = _ship(db, shipment_id, user)
    _check_file_category(user, category, "上传")
    _check_round_file(db, s, category)
    content = file.file.read(MAX_FILE_SIZE + 1)
    if not content or len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="请选择不超过15MB的文件")

    name = file.filename or "附件"
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
        "name": name,
        "type": file.content_type or "",
        "size": len(content),
    })
    _log(db, shipment_id, user, "上传附件：" + _file_label(category, name))
    return {"ok": True}


@inspection_router.post("/workspace", summary="查验工作台操作（登记/跟进/处理）")
def post_workspace(request: Request, p: Dict[str, Any] = Body(...), db=Depends(get_session)):
    user = _identity(request)
    action = p.get("action")

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

    if action not in ("station", "update", "returnApply", "round", "feedback", "cargo", "deleteFile"):
        raise HTTPException(status_code=400, detail="未知操作")
    s = _ship(db, str(p.get("shipmentId") or ""), user)
    data = s["data"]

    if action == "deleteFile":
        _allow(user, ("monitor", "analyst"))
        f = db[FILES].find_one({"_id": str(p.get("fileId") or ""), "shipment_id": s["_id"], "deleted": {"$ne": True}})
        if not f:
            raise HTTPException(status_code=404, detail="附件不存在或已删除")
        _check_file_category(user, f["category"], "删除")
        _check_round_file(db, s, f["category"])
        # 只做标记：页面不再显示，文件留在服务器上，误删时可以恢复
        r = db[FILES].update_one(
            {"_id": f["_id"], "deleted": {"$ne": True}},
            {"$set": {"deleted": True, "deleted_by": user["username"], "deleted_at": _now()}},
        )
        if not r.modified_count:
            raise HTTPException(status_code=404, detail="附件不存在或已删除")
        _log(db, s["_id"], user, "删除附件：" + _file_label(f["category"], f["name"]))
        return {"ok": True}

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
            allowed = (*MONITOR_FIELDS, *ANALYST_FIELDS, *OWNER_FIELDS)
        else:
            allowed = MONITOR_FIELDS if user["role"] == "monitor" else ANALYST_FIELDS
        bad = next((k for k in fields if k not in allowed), None)
        if bad:
            raise HTTPException(status_code=403, detail="无权修改字段：" + bad)
        for k, v in fields.items():
            if k in BOOL_FIELDS and not isinstance(v, bool):
                raise HTTPException(status_code=400, detail="勾选字段内容无效")
            if not isinstance(v, (str, bool)):
                raise HTTPException(status_code=400, detail="无效字段内容")
            if isinstance(v, str) and len(v) > 20000:
                raise HTTPException(status_code=400, detail="内容过长")
            if k.endswith("Date") and v and not _valid_date(v):
                raise HTTPException(status_code=400, detail="日期格式无效")
        new_data = {**data, **fields}
        status = data.get("status")
        notes = []

        if "firstResult" in fields and fields["firstResult"] != data.get("firstResult"):
            if status != "在查":
                raise HTTPException(status_code=400, detail="已归档或退运的货物不能修改查验结果")
            if fields["firstResult"] not in FIRST_RESULTS:
                raise HTTPException(status_code=400, detail="无效查验结果")
        if fields.get("firstResult") and fields["firstResult"] != "待反馈" and not new_data.get("resultDate"):
            raise HTTPException(status_code=400, detail="请填写查验结果日期")
        if ("firstResult" in fields or "resultNotes" in fields) and new_data.get("firstResult") == "其他" \
                and not (new_data.get("resultNotes") or "").strip():
            raise HTTPException(status_code=400, detail="请填写其他查验结果")
        if new_data.get("declarationStatus") and new_data["declarationStatus"] not in DECLARATION_STATUSES:
            raise HTTPException(status_code=400, detail="无效申报状态")
        if new_data.get("inspectionType") and new_data["inspectionType"] not in INSPECTION_TYPES:
            raise HTTPException(status_code=400, detail="无效查验类型")
        if "inspectionType" in fields and new_data.get("inspectionType") == "其他" \
                and not (new_data.get("inspectionTypeOther") or "").strip():
            raise HTTPException(status_code=400, detail="请说明其他查验类型")
        if new_data.get("openedCartons") and new_data["openedCartons"] not in OPENED_CARTONS:
            raise HTTPException(status_code=400, detail="无效拆查箱数")
        if ("pickupConfirmed" in fields or "releasedPickupDate" in fields) and new_data.get("pickupConfirmed") \
                and (new_data.get("status") != "已放行" or not new_data.get("releasedPickupDate")):
            raise HTTPException(status_code=400, detail="放行后填写提货时间才能确认")

        # 完成预处理：调整过文件的，要写明调整内容并上传更新文件；与 V2 一样，每次勾选保存都刷新完成时间
        preprocessed = bool(fields.get("preprocessComplete"))
        if fields.get("preprocessComplete") and new_data.get("updatedFiles"):
            if not (new_data.get("fileNotes") or "").strip():
                raise HTTPException(status_code=400, detail="请说明文件调整内容")
            if not db[FILES].find_one({"shipment_id": s["_id"], "category": {"$in": UPDATE_FILES}, "deleted": {"$ne": True}}):
                raise HTTPException(status_code=400, detail="请先上传更新文件再完成预处理")
        if preprocessed:
            new_data["preprocessedAt"] = _now()

        if fields.get("firstResult") == "放行" and data.get("firstResult") != "放行":
            new_data.update(status="已放行", releaseDate=new_data.get("resultDate"))
            notes.append("转入放行归档")
        # 只在「已批复」变化时切换状态：旧数据里有安排退运但没记批复的，保存航班等信息不能把它改回申请退运
        if "approved" in fields and "status" not in fields and fields["approved"] != bool(data.get("approved")):
            if status not in ("申请退运", "安排退运"):
                raise HTTPException(status_code=400, detail="请先申请退运")
            new_data["status"] = "安排退运" if fields["approved"] else "申请退运"
        if "status" in fields and fields["status"] != status:
            if fields["status"] not in STATUSES:
                raise HTTPException(status_code=400, detail="无效状态")
            if fields["status"] == "已放行" and not new_data.get("releaseDate"):
                new_data["releaseDate"] = date.today().isoformat()
            if fields["status"] == "申请退运" and not new_data.get("applicationDate"):
                new_data["applicationDate"] = date.today().isoformat()
            # 「已批复」跟着状态走，退运安排页的勾选才和状态一致
            new_data["approved"] = fields["status"] == "安排退运"
            # 离开已放行时清空放行日期，除非这次同时改了放行日期
            if status == "已放行" and fields.get("releaseDate", data.get("releaseDate")) == data.get("releaseDate"):
                new_data["releaseDate"] = ""
            notes.append(f"状态改为{fields['status']}")

        _save_shipment(db, s, p.get("revision"), new_data)
        if preprocessed:
            text = f"完成预处理：{new_data.get('declarationStatus') or '未知'} / {'已调整文件' if new_data.get('updatedFiles') else '未调整文件'}"
        else:
            text = "更新：" + "、".join(fields)
        _log(db, s["_id"], user, text + ("，" + "，".join(notes) if notes else ""))
        return {"ok": True}

    if action == "returnApply":
        _allow(user, ("analyst",))
        application_date = p.get("date") or date.today().isoformat()
        if not _valid_date(application_date):
            raise HTTPException(status_code=400, detail="申请日期无效")
        if data.get("status") != "在查":
            raise HTTPException(status_code=400, detail="只有在查货物可申请退运")
        # 管理员把退运改回在查后再次申请，上一次的批复不再有效
        _save_shipment(db, s, p.get("revision"), {**data, "status": "申请退运", "applicationDate": application_date, "approved": False})
        _log(db, s["_id"], user, "申请退运")
        return {"ok": True}

    if action == "round":
        _allow(user, ("analyst",))
        body = p.get("body") or {}
        measure = str(body.get("measure") or "").strip()[:20000]
        if not measure:
            raise HTTPException(status_code=400, detail="请填写处理方案")
        if data.get("status") != "在查":
            raise HTTPException(status_code=400, detail="该票已离开在查池，不能新增轮次")
        rounds = _rounds_of(db, s["_id"])
        if rounds and not _feedback_result(rounds[-1]) and not (rounds[-1].get("feedback") or "").strip():
            raise HTTPException(status_code=400, detail="请先补齐上一轮CBP反馈")
        # 只升版本号：两个人同时新增时只有一个成功
        _save_shipment(db, s, p.get("revision"), data)
        round_id = str(uuid.uuid4())
        db[ROUNDS].insert_one({
            "_id": round_id,
            "shipment_id": s["_id"],
            "created": _now(),
            "date": date.today().isoformat(),
            "measure": measure,
            "notes": str(body.get("notes") or ""),
            "feedbackDate": "",
            "feedback": "",
            "feedbackResult": "",
            "revision": 1,
        })
        _log(db, s["_id"], user, f"第{len(rounds) + 1}轮处理：{measure}")
        return {"roundId": round_id}

    if action == "feedback":
        _allow(user, ("analyst",))
        if data.get("status") != "在查":
            raise HTTPException(status_code=400, detail="已归档或申请退运的货物不能继续处理")
        rounds = _rounds_of(db, s["_id"])
        rnd = next((r for r in rounds if r["_id"] == str(p.get("roundId") or "")), None)
        if not rnd:
            raise HTTPException(status_code=404, detail="未找到处理轮次")
        if rnd is not rounds[-1]:
            raise HTTPException(status_code=400, detail="历史轮次已锁定，请在最新轮次记录")
        old_revision = rnd.get("revision") or 1
        if p.get("roundRevision") != old_revision or p.get("revision") != s["revision"]:
            raise HTTPException(status_code=409, detail="记录已更新，请刷新")
        result = p.get("feedbackResult") or ""
        feedback_date = p.get("feedbackDate") or ""
        feedback = str(p.get("feedback") or "")
        if result not in FEEDBACK_RESULTS:
            raise HTTPException(status_code=400, detail="无效反馈结果")
        if (result or feedback_date) and not _valid_date(feedback_date):
            raise HTTPException(status_code=400, detail="请填写反馈日期")
        if result == "提出新要求" and not feedback.strip():
            raise HTTPException(status_code=400, detail="请填写CBP新要求")
        # 先按页面上的版本号保存货物（放行时同时改状态），再写轮次：与申请退运、新增一轮等操作互斥，
        # 不会出现轮次写了放行、货物却已被别人改成申请退运的情况（HUBS V2 在同一个事务里完成）
        next_data = {**data, "status": "已放行", "releaseDate": feedback_date} if result == "放行" else data
        _save_shipment(db, s, p.get("revision"), next_data)
        changes = {
            "measure": str(p.get("measure") or "").strip()[:20000] or rnd.get("measure", ""),
            "feedbackDate": feedback_date,
            "feedback": feedback,
            "feedbackResult": result,
            "revision": old_revision + 1,
        }
        # 旧轮次没有版本号，按第 1 版处理
        same_revision = old_revision if rnd.get("revision") else {"$exists": False}
        r = db[ROUNDS].update_one({"_id": rnd["_id"], "revision": same_revision}, {"$set": changes})
        if not r.matched_count:
            raise HTTPException(status_code=409, detail="记录已更新，请刷新")
        note = "，转入放行归档" if result == "放行" else ""
        _log(db, s["_id"], user, f"第{len(rounds)}轮：{result or '更新处理方案'}{note}")
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
        # 只升版本号：两个人同时替换时只有一个成功，不会留下两套明细
        _save_shipment(db, s, p.get("revision"), data)
        # 替换时先写入新明细再删旧的，写入失败不会丢原有明细
        old_ids = [c["_id"] for c in db[CARGO].find({"shipment_id": s["_id"]}, {"_id": 1})] if p.get("replace") else []
        db[CARGO].insert_many(docs)
        if old_ids:
            db[CARGO].delete_many({"_id": {"$in": old_ids}})
            _log(db, s["_id"], user, f"替换客户明细：删除原有 {len(old_ids)} 行，新增 {len(docs)} 行")
        else:
            _log(db, s["_id"], user, f"新增客户明细 {len(docs)} 行")
        return {"ok": True}
