import fcntl
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from telegram import BotCommand, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

UTC8 = timezone(timedelta(hours=8))


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise RuntimeError(f"Missing required env: {name}")
    return str(value) if value is not None else ""


BOT_TOKEN = env("BOT_TOKEN", required=True)
ALLOWED_TG_IDS = {int(x.strip()) for x in env("ALLOWED_TG_IDS", "7611295576").split(",") if x.strip()}
ALERT_TG_IDS = {int(x.strip()) for x in env("ALERT_TG_IDS", ",".join(str(x) for x in ALLOWED_TG_IDS)).split(",") if x.strip()}
ALLOWED_CHAT_IDS = {int(x.strip()) for x in env("ALLOWED_CHAT_IDS", "").split(",") if x.strip()}
# 单向转发规则，格式: "source>target,source2>target2"
FORWARD_ONEWAY_PAIRS: set[tuple[int, int]] = set()
for _rule in env("FORWARD_ONEWAY_PAIRS", "").split(","):
    _rule = _rule.strip()
    if not _rule or ">" not in _rule:
        continue
    _src, _dst = _rule.split(">", 1)
    try:
        FORWARD_ONEWAY_PAIRS.add((int(_src.strip()), int(_dst.strip())))
    except ValueError:
        pass

DB_HOST = env("DB_HOST", "18.139.56.230")
DB_PORT = int(env("DB_PORT", "35010"))
DB_NAME = env("DB_NAME", "mytops_db")
DB_USER = env("DB_USER", required=True)
DB_PASSWORD = env("DB_PASSWORD", required=True)
DB_SSLMODE = env("DB_SSLMODE", "prefer")
DB_CONNECT_TIMEOUT = int(env("DB_CONNECT_TIMEOUT", "8"))
# 访问白名单使用 Telegram user id；数据库统计默认不按用户字段过滤
ORDER_USER_COLUMN = env("ORDER_USER_COLUMN", "")

LOG_FILE = env("LOG_FILE", "bot.log")
LOG_MAX_BYTES = int(env("LOG_MAX_BYTES", str(10 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(env("LOG_BACKUP_COUNT", "7"))
FORWARD_MAP_FILE = env("FORWARD_MAP_FILE", "forward_map.json")
INSTANCE_LOCK_FILE = env("INSTANCE_LOCK_FILE", "hybot.instance.lock")

STATUS_LABELS = {2: "完成", 1: "已付款", 0: "进行中", -1: "取消"}
ORDER_TYPE_LABELS = {0: "兑入", 1: "兑出"}
DEFAULT_HIDDEN_ORDER_FIELDS = {
    "active_time",
    "appeal_status",
    "cancel_time",
    "fund_status",
    "remind_status",
    "remind_time",
    "user_path",
    "user_id",
    "source",
    "mch_secret",
    "real_pay_amount",
    "ad_id",
    "mch_id",
    "buy_amount",
    "id",
}
HIDDEN_ORDER_FIELDS = {
    x.strip() for x in env("HIDDEN_ORDER_FIELDS", ",".join(sorted(DEFAULT_HIDDEN_ORDER_FIELDS))).split(",") if x.strip()
}


logger = logging.getLogger("hybot")
_INSTANCE_LOCK_FH = None


def acquire_instance_lock() -> None:
    global _INSTANCE_LOCK_FH
    _INSTANCE_LOCK_FH = open(INSTANCE_LOCK_FILE, "w", encoding="utf-8")
    try:
        fcntl.flock(_INSTANCE_LOCK_FH.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _INSTANCE_LOCK_FH.write(str(os.getpid()))
        _INSTANCE_LOCK_FH.flush()
    except BlockingIOError as e:
        raise RuntimeError("HyBot 已在运行（实例锁已占用）") from e


def _map_key(chat_id: int, message_id: int) -> str:
    return f"{chat_id}:{message_id}"


def _load_forward_map() -> dict:
    try:
        if not os.path.exists(FORWARD_MAP_FILE):
            return {}
        with open(FORWARD_MAP_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.exception("Failed to load forward map")
        return {}


def _save_forward_map(data: dict):
    try:
        with open(FORWARD_MAP_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        logger.exception("Failed to save forward map")


def _remember_forward_pair(src_chat_id: int, src_msg_id: int, dst_chat_id: int, dst_msg_id: int):
    data = _load_forward_map()
    src_key = _map_key(src_chat_id, src_msg_id)
    dst_key = _map_key(dst_chat_id, dst_msg_id)

    src_links = data.get(src_key, {})
    if not isinstance(src_links, dict):
        src_links = {}
    src_links[str(dst_chat_id)] = int(dst_msg_id)
    data[src_key] = src_links

    dst_links = data.get(dst_key, {})
    if not isinstance(dst_links, dict):
        dst_links = {}
    dst_links[str(src_chat_id)] = int(src_msg_id)
    data[dst_key] = dst_links

    _save_forward_map(data)


def _find_mirrored_message_id(chat_id: int, message_id: int, target_chat_id: int) -> int | None:
    data = _load_forward_map()
    links = data.get(_map_key(chat_id, message_id), {})
    if not isinstance(links, dict):
        return None
    v = links.get(str(target_chat_id))
    return int(v) if v is not None else None


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)

    fh = RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8")
    fh.setFormatter(formatter)

    root.handlers.clear()
    root.addHandler(sh)
    root.addHandler(fh)

    # 防止 token 出现在 httpx INFO 日志
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def alert_admins(app: Application, text: str):
    for tg_id in ALERT_TG_IDS:
        try:
            await app.bot.send_message(chat_id=tg_id, text=f"⚠️ HyBot告警\n{text}")
        except Exception:
            logger.exception("Failed to send alert to %s", tg_id)


def is_allowed(user_id: int | None) -> bool:
    return user_id is not None and user_id in ALLOWED_TG_IDS


def is_chat_allowed(chat_id: int | None) -> bool:
    if not ALLOWED_CHAT_IDS:
        return True
    return chat_id is not None and chat_id in ALLOWED_CHAT_IDS


def is_authorized(update: Update) -> bool:
    chat_id = update.effective_chat.id if update.effective_chat else None
    # 改为仅按群/会话鉴权
    return is_chat_allowed(chat_id)


def get_default_forward_target(source_chat_id: int) -> int | None:
    """当 source 只有一个可转发目标时，返回该目标（用于免填 chat_id）。"""
    if FORWARD_ONEWAY_PAIRS:
        targets = sorted({dst for (src, dst) in FORWARD_ONEWAY_PAIRS if src == source_chat_id})
        return targets[0] if len(targets) == 1 else None

    if ALLOWED_CHAT_IDS and len(ALLOWED_CHAT_IDS) == 2 and source_chat_id in ALLOWED_CHAT_IDS:
        others = [x for x in ALLOWED_CHAT_IDS if x != source_chat_id]
        return others[0] if len(others) == 1 else None

    return None


def is_forward_pair_allowed(source_chat_id: int, target_chat_id: int) -> bool:
    if source_chat_id == target_chat_id:
        return False

    # 若配置了单向规则，则仅允许命中规则的方向
    if FORWARD_ONEWAY_PAIRS:
        return (source_chat_id, target_chat_id) in FORWARD_ONEWAY_PAIRS

    # 启用 ALLOWED_CHAT_IDS 且正好配置2个群时，只允许这两个群互转（1对1）
    if len(ALLOWED_CHAT_IDS) == 2:
        pair = set(ALLOWED_CHAT_IDS)
        return {source_chat_id, target_chat_id} == pair

    # 未配置或非2个时，至少要求目标也在 ALLOWED_CHAT_IDS（若配置）
    if ALLOWED_CHAT_IDS:
        return source_chat_id in ALLOWED_CHAT_IDS and target_chat_id in ALLOWED_CHAT_IDS
    return True


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
        connect_timeout=DB_CONNECT_TIMEOUT,
    )


def utc_bounds_from_utc8_dates(start_date: str, end_date: str) -> tuple[datetime, datetime]:
    start_local = datetime.combine(date.fromisoformat(start_date), datetime.min.time(), tzinfo=UTC8)
    end_local_exclusive = datetime.combine(date.fromisoformat(end_date) + timedelta(days=1), datetime.min.time(), tzinfo=UTC8)
    return start_local.astimezone(timezone.utc), end_local_exclusive.astimezone(timezone.utc)


def query_summary(user_id: int, start_date: str, end_date: str, mch_id: int | None = None):
    start_utc, end_utc = utc_bounds_from_utc8_dates(start_date, end_date)

    base_sql = """
        SELECT
          COUNT(*) AS total_orders,
          SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS completed_orders,
          SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS paid_orders,
          SUM(CASE WHEN order_type = 0 THEN 1 ELSE 0 END) AS in_orders,
          COALESCE(SUM(CASE WHEN order_type = 0 AND status IN (1, 2) THEN real_order_amount ELSE 0 END), 0) AS in_amount,
          SUM(CASE WHEN order_type = 1 THEN 1 ELSE 0 END) AS out_orders,
          COALESCE(SUM(CASE WHEN order_type = 1 AND status IN (1, 2) THEN real_order_amount ELSE 0 END), 0) AS out_amount,
          SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS processing_orders,
          SUM(CASE WHEN status = -1 THEN 1 ELSE 0 END) AS canceled_orders
        FROM orders
        WHERE create_time >= %s AND create_time < %s
    """

    params: list = [start_utc, end_utc]
    query = sql.SQL(base_sql)

    if ORDER_USER_COLUMN.strip():
        query += sql.SQL(" AND {} = %s").format(sql.Identifier(ORDER_USER_COLUMN))
        params.append(user_id)

    if mch_id is not None:
        query += sql.SQL(" AND mch_id = %s")
        params.append(mch_id)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            row = cur.fetchone() or {}
            return {
                "total_orders": row.get("total_orders", 0),
                "completed_orders": row.get("completed_orders", 0),
                "paid_orders": row.get("paid_orders", 0),
                "in_orders": row.get("in_orders", 0),
                "in_amount": row.get("in_amount", 0),
                "out_orders": row.get("out_orders", 0),
                "out_amount": row.get("out_amount", 0),
                "processing_orders": row.get("processing_orders", 0),
                "canceled_orders": row.get("canceled_orders", 0),
            }


def resolve_mch_id(mch_input: str) -> tuple[int | None, str | None, str | None]:
    s = (mch_input or "").strip()
    if not s:
        return None, None, "mch 不能为空"

    mch_filter_sql = "COALESCE(is_del, false) = false AND NOT (COALESCE(buy_fee, 0) = 0 AND COALESCE(sell_fee, 0) = 0)"

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 纯数字按 id 处理
            if s.isdigit():
                mch_id = int(s)
                cur.execute(
                    f"SELECT id, name FROM public.mchs WHERE id = %s AND {mch_filter_sql} LIMIT 1",
                    (mch_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None, None, f"未找到可用商户 mch_id={mch_id}（可能已删除或费率为0）"
                return int(row["id"]), str(row["name"]), None

            # 非数字按 name 查询（先精确，再模糊）
            cur.execute(
                f"SELECT id, name FROM public.mchs WHERE name = %s AND {mch_filter_sql} ORDER BY id LIMIT 5",
                (s,),
            )
            rows = cur.fetchall() or []
            if len(rows) == 1:
                return int(rows[0]["id"]), str(rows[0]["name"]), None
            if len(rows) > 1:
                return None, None, "mch_name 命中多个精确结果，请改用 mch_id"

            cur.execute(
                f"SELECT id, name FROM public.mchs WHERE name ILIKE %s AND {mch_filter_sql} ORDER BY id LIMIT 6",
                (f"%{s}%",),
            )
            rows = cur.fetchall() or []
            if len(rows) == 1:
                return int(rows[0]["id"]), str(rows[0]["name"]), None
            if len(rows) == 0:
                return None, None, f"未找到可用商户 mch_name={s}（已过滤 is_del=true 或买卖费率均为0）"

            preview = "；".join([f"{r['id']}:{r['name']}" for r in rows[:5]])
            return None, None, f"mch_name 命中多个结果，请改用 mch_id。候选: {preview}"


def query_mch_list(keyword: str | None = None, limit: int = 50) -> list[dict]:
    limit = max(1, min(limit, 200))
    mch_filter_sql = "COALESCE(is_del, false) = false AND NOT (COALESCE(buy_fee, 0) = 0 AND COALESCE(sell_fee, 0) = 0)"
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if keyword and keyword.strip():
                cur.execute(
                    f"SELECT id, name FROM public.mchs WHERE {mch_filter_sql} AND name ILIKE %s ORDER BY id LIMIT %s",
                    (f"%{keyword.strip()}%", limit),
                )
            else:
                cur.execute(f"SELECT id, name FROM public.mchs WHERE {mch_filter_sql} ORDER BY id LIMIT %s", (limit,))
            return [dict(r) for r in (cur.fetchall() or [])]


def query_order_by_no(user_id: int, order_no: str) -> dict | None:
    base_sql = """
        SELECT
          o.*,
          m.name AS mch_name,
          u.uname AS uname,
          ot.path AS thumb_path
        FROM orders o
        LEFT JOIN public.mchs m ON m.id = o.mch_id
        LEFT JOIN public.users u ON u.id = o.user_id
        LEFT JOIN LATERAL (
            SELECT path
            FROM public.order_thumbs
            WHERE order_id = o.id
            ORDER BY id DESC
            LIMIT 1
        ) ot ON TRUE
        WHERE o.order_no = %s
    """
    params: list = [order_no]
    query = sql.SQL(base_sql)

    if ORDER_USER_COLUMN.strip():
        query += sql.SQL(" AND o.{} = %s").format(sql.Identifier(ORDER_USER_COLUMN))
        params.append(user_id)

    query += sql.SQL(" ORDER BY o.id DESC LIMIT 1")

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return dict(row) if row else None


def query_order_by_token(user_id: int, token: str) -> dict | None:
    t = (token or "").strip()
    if not t:
        return None

    candidates = {t}
    if t.startswith("#") and len(t) > 1:
        candidates.add(t[1:])
    else:
        candidates.add(f"#{t}")

    base_sql = """
        SELECT
          o.*,
          m.name AS mch_name,
          u.uname AS uname,
          ot.path AS thumb_path
        FROM orders o
        LEFT JOIN public.mchs m ON m.id = o.mch_id
        LEFT JOIN public.users u ON u.id = o.user_id
        LEFT JOIN LATERAL (
            SELECT path
            FROM public.order_thumbs
            WHERE order_id = o.id
            ORDER BY id DESC
            LIMIT 1
        ) ot ON TRUE
        WHERE (
            o.order_no = ANY(%s) OR
            COALESCE(o.mch_order_no, '') = ANY(%s)
        )
    """
    vals = list(candidates)
    params: list = [vals, vals]
    query = sql.SQL(base_sql)

    if ORDER_USER_COLUMN.strip():
        query += sql.SQL(" AND o.{} = %s").format(sql.Identifier(ORDER_USER_COLUMN))
        params.append(user_id)

    query += sql.SQL(" ORDER BY o.id DESC LIMIT 1")

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return dict(row) if row else None


def fmt_order(order: dict) -> str:
    def _fmt(v):
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.strftime("%Y-%m-%d %H:%M:%S")
            return v.astimezone(UTC8).strftime("%Y-%m-%d %H:%M:%S %z")
        return "" if v is None else str(v)

    status = order.get("status")
    order_type = order.get("order_type")
    status_label = STATUS_LABELS.get(status, "未知")
    order_type_label = ORDER_TYPE_LABELS.get(order_type, "未知")

    lines = [
        "订单详情：",
        f"order_no: {order.get('order_no', '')}",
        f"status: {status} ({status_label})",
        f"order_type: {order_type} ({order_type_label})",
    ]
    if order.get("thumb_path"):
        lines.append(f"path: {_fmt(order.get('thumb_path'))}")

    # 优先展示常用字段
    priority_keys = [
        "id",
        "mch_id",
        "mch_name",
        "mch_order_no",
        "order_amount",
        "real_order_amount",
        "mch_order_amount",
        "order_fee",
        "buy_amount",
        "order_price",
        "real_pay_amount",
        "ad_id",
        "true_name",
        "uname",
        "create_time",
        "pay_time",
        "finish_time",
        "update_time",
    ]

    used = {"order_no", "status", "order_type", "thumb_path"}
    for k in priority_keys:
        if k in HIDDEN_ORDER_FIELDS:
            continue
        if k in order and k not in used:
            lines.append(f"{k}: {_fmt(order.get(k))}")
            used.add(k)

    # 其余字段补齐（排除隐藏字段）
    for k in sorted(order.keys()):
        if k in used or k in HIDDEN_ORDER_FIELDS:
            continue
        lines.append(f"{k}: {_fmt(order.get(k))}")

    text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n...(字段较多，已截断)"
    return text


def query_orders_by_keyword(user_id: int, keyword: str, limit: int = 10) -> list[dict]:
    like = f"%{keyword.strip()}%"
    query = sql.SQL(
        """
        SELECT o.order_no, o.mch_order_no, o.mch_id, m.name AS mch_name,
               o.status, o.order_type, o.real_order_amount, o.create_time,
               o.true_name, o.source
        FROM orders o
        LEFT JOIN public.mchs m ON m.id = o.mch_id
        WHERE (
            o.order_no ILIKE %s OR
            COALESCE(o.mch_order_no, '') ILIKE %s OR
            COALESCE(CAST(o.true_name AS text), '') ILIKE %s OR
            COALESCE(CAST(o.source AS text), '') ILIKE %s OR
            COALESCE(CAST(o.ad_id AS text), '') ILIKE %s OR
            COALESCE(m.name, '') ILIKE %s
        )
        AND COALESCE(m.is_del, false) = false
        AND NOT (COALESCE(m.buy_fee, 0) = 0 AND COALESCE(m.sell_fee, 0) = 0)
        """
    )
    params: list = [like, like, like, like, like, like]
    if ORDER_USER_COLUMN.strip():
        query += sql.SQL(" AND o.{} = %s").format(sql.Identifier(ORDER_USER_COLUMN))
        params.append(user_id)
    query += sql.SQL(" ORDER BY o.id DESC LIMIT %s")
    params.append(max(1, min(limit, 20)))

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(r) for r in (cur.fetchall() or [])]


def fmt_order_quick_list(rows: list[dict]) -> str:
    lines = ["订单快捷列表："]
    for r in rows:
        lines.append(
            f"{r.get('order_no')} | mch={r.get('mch_id')}:{r.get('mch_name') or ''} | "
            f"status={r.get('status')}({STATUS_LABELS.get(r.get('status'), '未知')}) | "
            f"type={r.get('order_type')}({ORDER_TYPE_LABELS.get(r.get('order_type'), '未知')}) | "
            f"amt={r.get('real_order_amount')}"
        )
    text = "\n".join(lines)
    return text[:3800]


def query_recent_in_deals_by_true_name(user_id: int, name: str, limit: int = 100) -> list[dict]:
    keyword = (name or "").strip()
    if not keyword:
        return []

    like = f"%{keyword}%"
    since_utc = datetime.now(timezone.utc) - timedelta(days=30)
    base_sql = """
        SELECT
            o.order_no,
            o.mch_order_no,
            o.true_name,
            o.user_id,
            u.uname,
            o.mch_id,
            m.name AS mch_name,
            o.real_order_amount,
            o.create_time,
            o.finish_time,
            o.status,
            o.order_type
        FROM orders o
        LEFT JOIN public.users u ON u.id = o.user_id
        LEFT JOIN public.mchs m ON m.id = o.mch_id
        WHERE o.order_type = 0
          AND o.status = 2
          AND o.create_time >= %s
          AND COALESCE(CAST(o.true_name AS text), '') ILIKE %s
    """

    params: list = [since_utc, like]
    query = sql.SQL(base_sql)
    if ORDER_USER_COLUMN.strip():
        query += sql.SQL(" AND o.{} = %s").format(sql.Identifier(ORDER_USER_COLUMN))
        params.append(user_id)

    query += sql.SQL(" ORDER BY o.create_time DESC LIMIT %s")
    params.append(max(1, min(limit, 200)))

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(r) for r in (cur.fetchall() or [])]


def fmt_recent_in_deals(rows: list[dict], name: str) -> str:
    def _fmt_dt(v):
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.strftime("%Y-%m-%d %H:%M:%S")
            return v.astimezone(UTC8).strftime("%Y-%m-%d %H:%M:%S")
        return ""

    lines = [f"近30天兑入成交清单（姓名包含：{name}）", f"共 {len(rows)} 笔"]
    for i, r in enumerate(rows, 1):
        lines.append(
            f"{i}. {r.get('order_no')} | uname={r.get('uname') or '-'} | "
            f"true_name={r.get('true_name') or '-'} | amt={r.get('real_order_amount')} | "
            f"mch={r.get('mch_id')}:{r.get('mch_name') or ''} | "
            f"create={_fmt_dt(r.get('create_time'))}"
        )

    text = "\n".join(lines)
    return text[:3800]


def fmt_summary(title: str, data: dict) -> str:
    return (
        f"{title}\n"
        f"总订单: {data['total_orders']}\n"
        f"完成(status=2): {data['completed_orders']}\n"
        f"已付款(status=1): {data['paid_orders']}\n"
        f"兑入(order_type=0): {data['in_orders']} 笔 / {data['in_amount']}\n"
        f"兑出(order_type=1): {data['out_orders']} 笔 / {data['out_amount']}\n"
        f"进行中(status=0): {data['processing_orders']}\n"
        f"取消(status=-1): {data['canceled_orders']}"
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return
    await update.message.reply_text(
        "HyBot 已启动。\n"
        "可用命令：\n"
        "/today [mch_id或mch_name] - 今日（UTC+8）汇总\n"
        "/yesterday [mch_id或mch_name] - 昨日（UTC+8）汇总\n"
        "/range YYYY-MM-DD YYYY-MM-DD [mch_id或mch_name] - 日期区间汇总（UTC+8）\n"
        "/find 关键词 - 订单关键词识别（order_no/mch_order_no/姓名/source/ad_id）\n"
        "/indeals 姓名 - 近30天兑入成交订单筛选（附uname）\n"
        "/order 订单号 - 查询单笔订单详情\n"
        "/debugchat - 群组/会话访问调试信息\n"
        "/status - 状态说明"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return
    lines = ["状态映射："] + [f"status={k}: {v}" for k, v in STATUS_LABELS.items()]
    lines += ["", "订单类型映射："] + [f"order_type={k}: {v}" for k, v in ORDER_TYPE_LABELS.items()]
    await update.message.reply_text("\n".join(lines))


async def dbcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
        await update.message.reply_text("✅ DB连接正常")
    except Exception as e:
        err = f"DB连接失败: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await update.message.reply_text("❌ 数据库连接失败，已触发告警")
        await alert_admins(context.application, err)


async def send_summary(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    start_date: str,
    end_date: str,
    title: str,
    mch_input: str | None = None,
):
    user_id = update.effective_user.id if update.effective_user else None
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return
    try:
        mch_id = None
        mch_name = None
        if mch_input:
            mch_id, mch_name, resolve_err = resolve_mch_id(mch_input)
            if resolve_err:
                await update.message.reply_text(f"❌ {resolve_err}")
                return

        data = query_summary(user_id, start_date, end_date, mch_id=mch_id)
        subtitle = f"\n商户: {mch_name} (mch_id={mch_id})" if mch_id is not None else ""
        await update.message.reply_text(fmt_summary(title + subtitle, data))
    except Exception as e:
        err = f"查询失败 user={user_id} {start_date}~{end_date} mch={mch_input}: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await update.message.reply_text("❌ 查询失败，已触发告警")
        await alert_admins(context.application, err)


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = datetime.now(UTC8).date()
    mch_input = context.args[0] if context.args else None
    await send_summary(update, context, str(d), str(d), f"今日汇总（UTC+8 {d}）", mch_input=mch_input)


async def yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = datetime.now(UTC8).date() - timedelta(days=1)
    mch_input = context.args[0] if context.args else None
    await send_summary(update, context, str(d), str(d), f"昨日汇总（UTC+8 {d}）", mch_input=mch_input)


async def range_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) not in (2, 3):
        await update.message.reply_text("用法: /range YYYY-MM-DD YYYY-MM-DD [mch_id或mch_name]")
        return
    start_date, end_date = context.args[0], context.args[1]
    mch_input = context.args[2] if len(context.args) == 3 else None
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("日期格式错误，请使用 YYYY-MM-DD")
        return
    await send_summary(update, context, start_date, end_date, f"区间汇总（UTC+8 {start_date} ~ {end_date}）", mch_input=mch_input)


async def mchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return

    keyword = context.args[0] if context.args else None
    try:
        rows = query_mch_list(keyword=keyword, limit=80)
        if not rows:
            await update.message.reply_text("未找到商户。")
            return

        lines = ["商户列表（id:name）："]
        lines.extend([f"{r['id']}: {r['name']}" for r in rows])
        text = "\n".join(lines)
        if len(text) > 3500:
            text = text[:3500] + "\n...(已截断)"
        await update.message.reply_text(text)
    except Exception as e:
        err = f"商户列表查询失败: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await update.message.reply_text("❌ 商户列表查询失败，已触发告警")
        await alert_admins(context.application, err)


async def order_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return

    if len(context.args) != 1:
        await update.message.reply_text("用法: /order 订单号")
        return

    order_no = (context.args[0] or "").strip()
    if not order_no:
        await update.message.reply_text("订单号不能为空")
        return

    try:
        order = query_order_by_no(user_id, order_no)
        if not order:
            await update.message.reply_text(f"未找到订单: {order_no}")
            return
        await update.message.reply_text(fmt_order(order))
    except Exception as e:
        err = f"订单查询失败 user={user_id} order_no={order_no}: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await update.message.reply_text("❌ 订单查询失败，已触发告警")
        await alert_admins(context.application, err)


async def find_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return
    if not context.args:
        await update.message.reply_text("用法: /find 关键词")
        return
    keyword = " ".join(context.args).strip()
    try:
        rows = query_orders_by_keyword(user_id, keyword, limit=10)
        if not rows:
            await update.message.reply_text("未匹配到订单。")
            return
        await update.message.reply_text(fmt_order_quick_list(rows))
    except Exception as e:
        err = f"关键词订单查询失败 user={user_id} kw={keyword}: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await update.message.reply_text("❌ 关键词查询失败，已触发告警")
        await alert_admins(context.application, err)


async def in_deals_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return

    if not context.args:
        await update.message.reply_text("用法: /indeals 姓名")
        return

    name = " ".join(context.args).strip()
    if not name:
        await update.message.reply_text("姓名不能为空")
        return

    try:
        rows = query_recent_in_deals_by_true_name(user_id, name, limit=100)
        if not rows:
            await update.message.reply_text(f"近30天未找到与“{name}”匹配的兑入成交订单。")
            return
        await update.message.reply_text(fmt_recent_in_deals(rows, name))
    except Exception as e:
        err = f"兑入成交筛选失败 user={user_id} name={name}: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await update.message.reply_text("❌ 筛选失败，已触发告警")
        await alert_admins(context.application, err)


async def quick_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return
    limit = 10
    if context.args and context.args[0].isdigit():
        limit = max(1, min(int(context.args[0]), 20))
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                q = sql.SQL(
                    "SELECT o.order_no, o.mch_order_no, o.mch_id, m.name AS mch_name, o.status, o.order_type, o.real_order_amount, o.create_time, o.true_name, o.source FROM orders o LEFT JOIN public.mchs m ON m.id=o.mch_id"
                )
                params: list = []
                where_clauses: list[sql.SQL] = [
                    sql.SQL("COALESCE(m.is_del, false) = false"),
                    sql.SQL("NOT (COALESCE(m.buy_fee, 0) = 0 AND COALESCE(m.sell_fee, 0) = 0)"),
                ]
                if ORDER_USER_COLUMN.strip():
                    where_clauses.append(sql.SQL("o.{} = %s").format(sql.Identifier(ORDER_USER_COLUMN)))
                    params.append(user_id)
                if where_clauses:
                    q += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses)
                q += sql.SQL(" ORDER BY o.id DESC LIMIT %s")
                params.append(limit)
                cur.execute(q, params)
                rows = [dict(r) for r in (cur.fetchall() or [])]
        if not rows:
            await update.message.reply_text("暂无订单。")
            return
        await update.message.reply_text(fmt_order_quick_list(rows))
    except Exception as e:
        err = f"快捷列表查询失败 user={user_id}: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await update.message.reply_text("❌ 快捷列表查询失败，已触发告警")
        await alert_admins(context.application, err)


def extract_inline_order_tokens(text: str) -> list[str]:
    raw = (text or "").strip()
    if not raw:
        return []

    # 1) 兼容原有格式: order + xxx
    m = re.search(r"(?i)\border\s*\+\s*(.+)$", raw)
    if m:
        payload = (m.group(1) or "").strip()
        return [t.strip() for t in re.split(r"[\/\s,，|]+", payload) if t.strip()]

    # 2) 新增关键词触发: order/订单号/订单/訂單號/訂單/查询/查/find/search/mch_no
    # 示例: "查询 12345" / "订单号 #A001" / "find mch_no 9988"
    kw_pattern = r"(?i)(?:\border\b|订单号|订单|訂單號|訂單|查询|查|\bfind\b|\bsearch\b|\bmch_no\b)"
    # 提取关键词后的内容
    m2 = re.search(rf"{kw_pattern}[\s:：#-]*(.+)$", raw)
    if m2:
        payload = (m2.group(1) or "").strip()
        tokens = [t.strip() for t in re.split(r"[\/\s,，|]+", payload) if t.strip()]
        if tokens:
            return tokens

    # 3) 单独 #xxx 也触发
    hash_tokens = re.findall(r"(?<!\w)#([A-Za-z0-9_-]{3,})", raw)
    if hash_tokens:
        return [f"#{t}" for t in hash_tokens]

    return []


async def auto_relay_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """在已建立映射的转发会话中，支持“直接回复即自动回传”（无需再输入 /fwd）。"""
    if not is_authorized(update):
        return

    msg = update.effective_message
    if not msg or not getattr(msg, "reply_to_message", None):
        return


    # 忽略机器人消息，避免回环
    if update.effective_user and bool(getattr(update.effective_user, "is_bot", False)):
        return

    source_chat_id = update.effective_chat.id if update.effective_chat else None
    if source_chat_id is None:
        return

    parent_msg = msg.reply_to_message

    # 严格模式：仅当“回复机器人消息”时才允许自动回传
    is_reply_to_bot = bool(getattr(getattr(parent_msg, "from_user", None), "is_bot", False))
    if not is_reply_to_bot:
        return

    mapped = _load_forward_map().get(_map_key(source_chat_id, parent_msg.message_id), {})

    # 1) 优先走已存在的镜像映射（可精确挂到目标父消息）
    target_chat_id = None
    target_reply_to_message_id = None
    if isinstance(mapped, dict) and mapped:
        for k, v in mapped.items():
            try:
                cid = int(k)
                mid = int(v)
            except (TypeError, ValueError):
                continue
            if is_forward_pair_allowed(source_chat_id, cid):
                target_chat_id = cid
                target_reply_to_message_id = mid
                break

    # 2) 若无映射，仅当“回复机器人消息”时按默认目标自动回传
    #    （关闭“回复任意消息自动回传”）
    if target_chat_id is None:
        is_reply_to_bot = bool(getattr(getattr(parent_msg, "from_user", None), "is_bot", False))
        if not is_reply_to_bot:
            return

        default_target = get_default_forward_target(source_chat_id)
        if default_target is None or not is_forward_pair_allowed(source_chat_id, default_target):
            return
        target_chat_id = default_target

        try:
            parent_sent = await context.bot.copy_message(
                chat_id=target_chat_id,
                from_chat_id=source_chat_id,
                message_id=parent_msg.message_id,
            )
            parent_sent_message_id = int(parent_sent.message_id) if hasattr(parent_sent, "message_id") else int(parent_sent)
            _remember_forward_pair(source_chat_id, parent_msg.message_id, target_chat_id, parent_sent_message_id)
            target_reply_to_message_id = parent_sent_message_id
        except Exception as e:
            # 父消息同步失败时，降级为“仅回传当前回复消息”（不挂回复链），避免整条回传被阻断
            logger.warning(
                "auto relay parent sync failed, fallback to no-reply-thread source_chat=%s parent_msg=%s target_chat=%s err=%s: %s",
                source_chat_id,
                getattr(parent_msg, "message_id", None),
                target_chat_id,
                e.__class__.__name__,
                e,
            )
            target_reply_to_message_id = None

    try:
        copy_kwargs = dict(
            chat_id=target_chat_id,
            from_chat_id=source_chat_id,
            message_id=msg.message_id,
        )
        if target_reply_to_message_id is not None:
            copy_kwargs["reply_to_message_id"] = target_reply_to_message_id
        sent = await context.bot.copy_message(**copy_kwargs)
        sent_message_id = int(sent.message_id) if hasattr(sent, "message_id") else int(sent)
        _remember_forward_pair(source_chat_id, msg.message_id, target_chat_id, sent_message_id)
        logger.info(
            "auto relay source_chat=%s source_msg=%s target_chat=%s target_reply=%s sent_msg=%s",
            source_chat_id,
            msg.message_id,
            target_chat_id,
            target_reply_to_message_id,
            sent_message_id,
        )
    except Exception as e:
        err = f"自动回复回传失败 source={source_chat_id} target={target_chat_id}: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await alert_admins(context.application, err)


async def order_plus_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return

    text = (update.message.text or "").strip() if update.message else ""
    tokens = extract_inline_order_tokens(text)
    if not tokens:
        return

    try:
        seen_order_no: set[str] = set()
        found_texts: list[str] = []
        for token in tokens:
            order = query_order_by_token(user_id, token)
            if not order:
                continue
            order_no = str(order.get("order_no") or "").strip()
            if order_no and order_no in seen_order_no:
                continue
            if order_no:
                seen_order_no.add(order_no)
            found_texts.append(fmt_order(order))

        # 静默处理：仅返回命中的订单，未命中不提示
        if not found_texts:
            return

        for msg in found_texts:
            await update.message.reply_text(msg[:3800])
    except Exception as e:
        err = f"内联订单查询失败 user={user_id}: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await update.message.reply_text("❌ 订单查询失败，已触发告警")
        await alert_admins(context.application, err)


async def fwd_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not is_authorized(update):
        if msg:
            await msg.reply_text("未授权。")
        return
    source_chat_id = update.effective_chat.id if update.effective_chat else None
    if source_chat_id is None:
        return

    target_chat_id: int | None = None
    if len(context.args) >= 1:
        try:
            target_chat_id = int(context.args[0])
        except ValueError:
            await msg.reply_text("chat_id 格式错误")
            return
    else:
        target_chat_id = get_default_forward_target(source_chat_id)
        if target_chat_id is None:
            await msg.reply_text("用法: /hyfwd 目标chat_id（当前群有多个或无默认目标）")
            return

    if not msg or not msg.reply_to_message:
        if msg:
            await msg.reply_text("请先回复一条消息，再执行 /fwd")
        return

    logger.info("fwd request source_chat=%s target_chat=%s by_user=%s", source_chat_id, target_chat_id, update.effective_user.id if update.effective_user else None)
    if not is_forward_pair_allowed(source_chat_id, target_chat_id):
        await msg.reply_text(
            "❌ 当前转发策略不允许该方向/目标（请检查 ALLOWED_CHAT_IDS 或 FORWARD_ONEWAY_PAIRS 配置）。"
        )
        return

    replied = msg.reply_to_message
    # 避免二次转发：如果回复的是一条“转发而来”的消息，则不再转发
    if (
        getattr(replied, "forward_origin", None) is not None
        or getattr(replied, "forward_date", None) is not None
        or getattr(replied, "forward_from", None) is not None
        or getattr(replied, "forward_from_chat", None) is not None
        or bool(getattr(replied, "is_automatic_forward", False))
    ):
        await msg.reply_text("⚠️ 这条是转发消息，已阻止二次转发。请直接回复原消息再执行 /fwd。")
        return

    # 如果当前消息是“回复某条消息”，且该父消息在目标群有镜像，则在目标群里挂到对应父消息下
    target_reply_to_message_id = None
    parent = replied.reply_to_message
    if parent:
        target_reply_to_message_id = _find_mirrored_message_id(source_chat_id, parent.message_id, target_chat_id)

    try:
        if target_reply_to_message_id:
            # 需要挂到目标消息下时，使用 copy_message（forward_message 不支持 reply_to_message_id）
            sent = await context.bot.copy_message(
                chat_id=target_chat_id,
                from_chat_id=source_chat_id,
                message_id=replied.message_id,
                reply_to_message_id=target_reply_to_message_id,
            )
            sent_message_id = int(sent.message_id) if hasattr(sent, "message_id") else int(sent)
        else:
            sent = await context.bot.forward_message(
                chat_id=target_chat_id,
                from_chat_id=source_chat_id,
                message_id=replied.message_id,
            )
            sent_message_id = int(sent.message_id)

        _remember_forward_pair(source_chat_id, replied.message_id, target_chat_id, sent_message_id)
        await msg.reply_text(f"✅ 已转发到 chat_id={target_chat_id}")
    except Exception as e:
        err = f"消息转发失败 target={target_chat_id}: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await msg.reply_text("❌ 转发失败，已触发告警")
        await alert_admins(context.application, err)


async def debugchat_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    chat = update.effective_chat
    chat_id = chat.id if chat else None
    chat_type = chat.type if chat else None
    lines = [
        "调试信息：",
        f"user_id={user_id}",
        f"chat_id={chat_id}",
        f"chat_type={chat_type}",
        f"user_allowed={is_allowed(user_id)}",
        f"chat_allowed={is_chat_allowed(chat_id)}",
        f"authorized={is_authorized(update)}",
        f"ALLOWED_CHAT_IDS={sorted(ALLOWED_CHAT_IDS) if ALLOWED_CHAT_IDS else '未配置(默认所有chat)'}",
        f"forward_mode={'oneway_rules' if FORWARD_ONEWAY_PAIRS else ('pair_only' if len(ALLOWED_CHAT_IDS)==2 else 'allowlist')}",
        f"FORWARD_ONEWAY_PAIRS={sorted(FORWARD_ONEWAY_PAIRS) if FORWARD_ONEWAY_PAIRS else '未配置'}",
    ]
    await update.message.reply_text("\n".join(lines))


async def range_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 兜底支持 /range 或 /range@bot_username 文本触发
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    cmd = text.split()[0].lower()
    if not (cmd == "/range" or cmd.startswith("/range@")):
        return

    parts = text.split()
    args = parts[1:]
    if len(args) not in (2, 3):
        await update.message.reply_text("用法: /range YYYY-MM-DD YYYY-MM-DD [mch_id或mch_name]")
        return

    start_date, end_date = args[0], args[1]
    mch_input = args[2] if len(args) == 3 else None
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("日期格式错误，请使用 YYYY-MM-DD")
        return

    await send_summary(update, context, start_date, end_date, f"区间汇总（UTC+8 {start_date} ~ {end_date}）", mch_input=mch_input)


async def debugchat_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 兜底支持 /debugchat@bot_username
    text = (update.message.text or "").strip() if update.message else ""
    if text.lower().startswith("/debugchat"):
        await debugchat_cmd(update, context)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error", exc_info=context.error)


async def post_init(app: Application):
    logger.info("HyBot started")
    try:
        await app.bot.set_my_commands(
            [
                BotCommand("start", "查看可用命令"),
                BotCommand("today", "今日汇总（可带商户）"),
                BotCommand("yesterday", "昨日汇总（可带商户）"),
                BotCommand("range", "区间汇总 YYYY-MM-DD YYYY-MM-DD [mch]"),
                BotCommand("mchlist", "商户列表 [关键词]"),
                BotCommand("find", "关键词查订单"),
                BotCommand("indeals", "近30天兑入成交筛选 [姓名]"),
                BotCommand("quick", "快捷订单列表 [数量]"),
                BotCommand("order", "按订单号查详情"),
                BotCommand("fwd", "转发回复消息到 chat_id"),
                BotCommand("debugchat", "会话/群组访问调试"),
                BotCommand("status", "状态与类型映射"),
                BotCommand("dbcheck", "数据库连通性检查"),
            ]
        )
        logger.info("Telegram command menu updated")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
        logger.info("DB init check passed")
    except Exception as e:
        err = f"启动自检失败: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await alert_admins(app, err)


def main():
    setup_logging()
    acquire_instance_lock()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("dbcheck", dbcheck))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("yesterday", yesterday))
    app.add_handler(CommandHandler("range", range_cmd))
    app.add_handler(MessageHandler(filters.Regex(r"^/range(@\w+)?(\s.*)?$"), range_alias))
    app.add_handler(CommandHandler("mchlist", mchlist))
    app.add_handler(CommandHandler("find", find_cmd))
    app.add_handler(CommandHandler("indeals", in_deals_cmd))
    app.add_handler(CommandHandler("quick", quick_cmd))
    app.add_handler(CommandHandler("order", order_cmd))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), order_plus_inline), group=0)
    # 自动回传：支持回复中的文本与媒体消息（图片/文件/视频等）
    app.add_handler(MessageHandler(filters.REPLY & (~filters.COMMAND), auto_relay_reply), group=1)
    app.add_handler(CommandHandler("fwd", fwd_cmd))
    app.add_handler(CommandHandler("hyfwd", fwd_cmd))
    app.add_handler(CommandHandler("debugchat", debugchat_cmd))
    app.add_handler(MessageHandler(filters.Regex(r"^/debugchat(@\w+)?(\s.*)?$"), debugchat_alias))
    app.add_error_handler(on_error)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
