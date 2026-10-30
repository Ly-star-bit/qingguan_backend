"""
统一认证中心的账号事件（离职回收）：订阅 RabbitMQ 上的停用、恢复、结束全部登录事件，落到绑定的本系统账号。

- 停用（企业微信离职 / 禁用、管理员停用、账号到期）：禁用绑定的本系统账号，已签发的令牌全部作废，
  与在用户管理里禁用账号的效果相同（在线的页面下一个请求就会回到登录页）
- 恢复：只恢复当初因统一认证停用的账号；本系统管理员禁用的账号保持禁用
- 结束全部登录（本人改密码、退出所有设备，管理员强制下线）：账号仍可用，已签发的令牌作废，重新登录不受影响
没有绑定统一认证的账号不受影响，需在用户管理里手动禁用。

事件至少投递一次、不保证到达顺序：按 EventId 去重；停用 / 恢复按 OccurredAt 只认较新的一条。
未配置 OIDC_EVENTS_HOST 时不订阅。消息格式见统一认证中心的 docs/events.md。
"""
import json
import os
import re
import threading
from datetime import datetime, timezone
from typing import Optional

import pika
from dotenv import load_dotenv
from loguru import logger

from app.apis.oidc import BINDINGS, BUILTIN_ADMIN, OIDC_ENABLED
from app.db_mongo import db
from app.session_guard import forget

load_dotenv()

EVENTS_HOST = os.getenv("OIDC_EVENTS_HOST", "").strip()
EVENTS_PORT = int(os.getenv("OIDC_EVENTS_PORT") or 5672)
EVENTS_USER = os.getenv("OIDC_EVENTS_USER", "").strip()
EVENTS_PASSWORD = os.getenv("OIDC_EVENTS_PASSWORD", "")
EVENTS_VHOST = os.getenv("OIDC_EVENTS_VHOST", "").strip() or "/"
# 本系统自己的持久队列：不要和别的系统共用，否则同一条事件只会被其中一个系统收到；停机期间的事件不丢
EVENTS_QUEUE = os.getenv("OIDC_EVENTS_QUEUE", "").strip() or "qingguan.identity-events"

# 统一认证中心（CAP）的默认交换机：topic 类型、持久
EXCHANGE = "cap.default.router"
USER_DISABLED = "hubs.identity.user.disabled"
USER_ENABLED = "hubs.identity.user.enabled"
USER_SESSIONS_REVOKED = "hubs.identity.user.sessions_revoked"
TOPICS = (USER_DISABLED, USER_ENABLED, USER_SESSIONS_REVOKED)

# 已处理的事件（按 EventId 去重），保留 7 天
PROCESSED = "oidc_events_processed"
try:
    db[PROCESSED].create_index("processed_at", expireAfterSeconds=7 * 24 * 3600)
except Exception:
    logger.exception("统一认证事件索引创建失败")


# ---- 事件落到本系统账号 ----

def _bound_users(subject: str):
    """该统一认证用户绑定的本系统账号：(绑定, 账号)"""
    for binding in db[BINDINGS].find({"subject": subject}):
        user = db.users.find_one({"_id": binding["user_id"]})
        if user:
            yield binding, user


def _is_newer(binding: dict, occurred_at: datetime) -> bool:
    """停用 / 恢复可能乱序到达：比这个绑定上次处理的事件旧的，忽略"""
    last = binding.get("identity_event_at")
    return last is None or last.replace(tzinfo=timezone.utc) <= occurred_at


def user_disabled(subject: str, occurred_at: datetime, reason: Optional[str]) -> None:
    for binding, user in _bound_users(subject):
        username = user["username"]
        if not _is_newer(binding, occurred_at):
            logger.info(f"统一认证停用 {subject} 的事件早于已处理的事件，忽略（账号 {username}）")
            continue
        # 与用户管理一致：admin 账号不能禁用
        if username == BUILTIN_ADMIN:
            logger.warning(f"统一认证停用了 {subject}（{reason}），但绑定的是 admin 账号，不禁用")
            continue

        # 只有原来启用的账号才记为「因统一认证停用」，本系统管理员已禁用的账号恢复时不动
        changed = db.users.update_one(
            {"_id": user["_id"], "status": {"$ne": 0}},
            {"$set": {"status": 0}, "$inc": {"token_version": 1}},
        ).modified_count == 1
        forget(username)
        db[BINDINGS].update_one(
            {"_id": binding["_id"]},
            {"$set": {
                "identity_event_at": occurred_at,
                "disabled_by_identity": changed or bool(binding.get("disabled_by_identity")),
            }},
        )
        if changed:
            logger.warning(f"统一认证停用了 {subject}（{reason}）：已禁用本系统账号 {username}，已签发的令牌作废")


def user_enabled(subject: str, occurred_at: datetime) -> None:
    for binding, user in _bound_users(subject):
        username = user["username"]
        if not _is_newer(binding, occurred_at):
            logger.info(f"统一认证恢复 {subject} 的事件早于已处理的事件，忽略（账号 {username}）")
            continue
        restore = bool(binding.get("disabled_by_identity"))
        if restore:
            db.users.update_one({"_id": user["_id"]}, {"$set": {"status": 1}})
            forget(username)
        db[BINDINGS].update_one(
            {"_id": binding["_id"]},
            {"$set": {"identity_event_at": occurred_at, "disabled_by_identity": False}},
        )
        if restore:
            logger.warning(f"统一认证恢复了 {subject}：已恢复本系统账号 {username}")


def user_sessions_revoked(subject: str, reason: Optional[str]) -> None:
    for _, user in _bound_users(subject):
        # 与改密码相同：令牌版本加一，此前签发的令牌（含刷新令牌）全部作废，重新登录不受影响
        db.users.update_one({"_id": user["_id"]}, {"$inc": {"token_version": 1}})
        forget(user["username"])
        logger.warning(f"统一认证结束了 {subject} 的全部登录（{reason}）：本系统账号 {user['username']} 已签发的令牌作废")


def _parse_time(value) -> datetime:
    """事件时间（带时区）。.NET 可能给 7 位小数，Python 只认 6 位"""
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", value).replace("Z", "+00:00"))
            if parsed.tzinfo:
                return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def handle(topic: str, body: bytes, headers: Optional[dict] = None) -> bool:
    """处理一条事件；返回 False 表示稍后重试"""
    try:
        event = json.loads(body)
        uid = str(event.get("Uid") or "")
    except (ValueError, AttributeError):
        # 格式不对的消息重试也没用：记录后丢弃
        logger.error(f"无法解析统一认证事件 {topic}，已丢弃：{body[:200]!r}")
        return True
    if not uid:
        logger.error(f"统一认证事件 {topic} 缺少 Uid，已丢弃")
        return True

    event_id = event.get("EventId") or (headers or {}).get("cap-msg-id") or ""
    event_id = event_id.decode() if isinstance(event_id, bytes) else str(event_id)
    try:
        if event_id and db[PROCESSED].find_one({"_id": event_id}):
            return True
        if topic == USER_DISABLED:
            user_disabled(uid, _parse_time(event.get("OccurredAt")), event.get("Reason"))
        elif topic == USER_ENABLED:
            user_enabled(uid, _parse_time(event.get("OccurredAt")))
        elif topic == USER_SESSIONS_REVOKED:
            user_sessions_revoked(uid, event.get("Reason"))
        if event_id:
            db[PROCESSED].update_one(
                {"_id": event_id},
                {"$setOnInsert": {"topic": topic, "uid": uid, "processed_at": datetime.now(timezone.utc)}},
                upsert=True,
            )
        return True
    except Exception:
        logger.exception(f"处理统一认证事件 {topic}（{uid}）失败，稍后重试")
        return False


# ---- 订阅 ----

class _Consumer(threading.Thread):
    """后台线程订阅事件；连接断开时 30 秒后整体重连（RabbitMQ 不可用不影响本系统启动）"""

    def __init__(self):
        super().__init__(name="identity-events", daemon=True)
        self._stopping = threading.Event()
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel = None

    def run(self):
        while not self._stopping.is_set():
            try:
                self._consume()
            except Exception:
                if self._stopping.is_set():
                    return
                logger.exception("订阅统一认证事件失败，30 秒后重连")
            self._stopping.wait(30)

    def _consume(self):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=EVENTS_HOST,
            port=EVENTS_PORT,
            virtual_host=EVENTS_VHOST,
            credentials=pika.PlainCredentials(EVENTS_USER, EVENTS_PASSWORD),
            client_properties={"connection_name": EVENTS_QUEUE},
        ))
        try:
            channel = self._channel = self._connection.channel()
            # 参数必须与 CAP 声明的一致（topic、持久、不自动删除），否则 RabbitMQ 会关闭通道
            channel.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
            channel.queue_declare(EVENTS_QUEUE, durable=True)
            for topic in TOPICS:
                channel.queue_bind(EVENTS_QUEUE, EXCHANGE, routing_key=topic)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(EVENTS_QUEUE, self._on_message)
            logger.info(f"已订阅统一认证事件：{EVENTS_HOST}:{EVENTS_PORT} 队列 {EVENTS_QUEUE}")
            channel.start_consuming()
        finally:
            if self._connection.is_open:
                self._connection.close()

    def _on_message(self, channel, method, properties, body):
        if handle(method.routing_key, body, properties.headers):
            channel.basic_ack(method.delivery_tag)
        else:
            # 处理失败（如数据库暂时不可用）：稍等再放回队列，避免立刻重投造成空转
            self._connection.sleep(5)
            channel.basic_nack(method.delivery_tag, requeue=True)

    def stop(self):
        self._stopping.set()
        connection, channel = self._connection, self._channel
        if connection and channel and connection.is_open:
            try:
                connection.add_callback_threadsafe(channel.stop_consuming)
            except Exception:
                pass


_consumer: Optional[_Consumer] = None


def start_identity_events() -> None:
    """应用启动时调用"""
    global _consumer
    if not EVENTS_HOST:
        if OIDC_ENABLED:
            logger.warning("统一认证登录已启用，但没有配置 OIDC_EVENTS_HOST：员工在统一认证中心离职停用后，本系统账号不会自动禁用")
        return
    _consumer = _Consumer()
    _consumer.start()


def stop_identity_events() -> None:
    """应用停止时调用"""
    if _consumer:
        _consumer.stop()
