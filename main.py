import logging
import os
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

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

STATUS_LABELS = {2: "完成", 1: "已付款", 0: "进行中", -1: "取消"}
ORDER_TYPE_LABELS = {0: "兑入", 1: "兑出"}


logger = logging.getLogger("hybot")


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
    user_id = update.effective_user.id if update.effective_user else None
    chat_id = update.effective_chat.id if update.effective_chat else None
    return is_allowed(user_id) and is_chat_allowed(chat_id)


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

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 纯数字按 id 处理
            if s.isdigit():
                mch_id = int(s)
                cur.execute("SELECT id, name FROM public.mchs WHERE id = %s LIMIT 1", (mch_id,))
                row = cur.fetchone()
                if not row:
                    return None, None, f"未找到 mch_id={mch_id}"
                return int(row["id"]), str(row["name"]), None

            # 非数字按 name 查询（先精确，再模糊）
            cur.execute("SELECT id, name FROM public.mchs WHERE name = %s ORDER BY id LIMIT 5", (s,))
            rows = cur.fetchall() or []
            if len(rows) == 1:
                return int(rows[0]["id"]), str(rows[0]["name"]), None
            if len(rows) > 1:
                return None, None, "mch_name 命中多个精确结果，请改用 mch_id"

            cur.execute(
                "SELECT id, name FROM public.mchs WHERE name ILIKE %s ORDER BY id LIMIT 6",
                (f"%{s}%",),
            )
            rows = cur.fetchall() or []
            if len(rows) == 1:
                return int(rows[0]["id"]), str(rows[0]["name"]), None
            if len(rows) == 0:
                return None, None, f"未找到 mch_name={s}"

            preview = "；".join([f"{r['id']}:{r['name']}" for r in rows[:5]])
            return None, None, f"mch_name 命中多个结果，请改用 mch_id。候选: {preview}"


def query_mch_list(keyword: str | None = None, limit: int = 50) -> list[dict]:
    limit = max(1, min(limit, 200))
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if keyword and keyword.strip():
                cur.execute(
                    "SELECT id, name FROM public.mchs WHERE name ILIKE %s ORDER BY id LIMIT %s",
                    (f"%{keyword.strip()}%", limit),
                )
            else:
                cur.execute("SELECT id, name FROM public.mchs ORDER BY id LIMIT %s", (limit,))
            return [dict(r) for r in (cur.fetchall() or [])]


def query_order_by_no(user_id: int, order_no: str) -> dict | None:
    base_sql = """
        SELECT
          o.*,
          m.name AS mch_name
        FROM orders o
        LEFT JOIN public.mchs m ON m.id = o.mch_id
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

    # 优先展示常用字段
    priority_keys = [
        "id",
        "mch_id",
        "mch_name",
        "merchant_order_no",
        "order_amount",
        "real_order_amount",
        "fee",
        "rate",
        "pay_type",
        "coin_type",
        "wallet_address",
        "txid",
        "remark",
        "create_time",
        "pay_time",
        "finish_time",
        "update_time",
    ]

    used = {"order_no", "status", "order_type"}
    for k in priority_keys:
        if k in order and k not in used:
            lines.append(f"{k}: {_fmt(order.get(k))}")
            used.add(k)

    # 其余字段全部补齐，确保“所有信息”
    for k in sorted(order.keys()):
        if k in used:
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
        SELECT o.order_no, o.merchant_order_no, o.mch_id, m.name AS mch_name,
               o.status, o.order_type, o.real_order_amount, o.create_time,
               o.txid, o.remark
        FROM orders o
        LEFT JOIN public.mchs m ON m.id = o.mch_id
        WHERE (
            o.order_no ILIKE %s OR
            COALESCE(o.merchant_order_no, '') ILIKE %s OR
            COALESCE(o.remark, '') ILIKE %s OR
            COALESCE(o.txid, '') ILIKE %s
        )
        """
    )
    params: list = [like, like, like, like]
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
        "/mchlist [关键词] - 查询商户列表（public.mchs）\n"
        "/find 关键词 - 订单关键词识别（order_no/商户单号/备注/txid）\n"
        "/quick [数量] - 快捷订单列表（默认10）\n"
        "/order 订单号 - 查询单笔订单全部信息\n"
        "/fwd 目标chat_id - 转发你回复的消息到目标chat\n"
        "/debugchat - 群组/会话访问调试信息\n"
        "/status - 状态说明\n"
        "/dbcheck - 数据库连通性检查"
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
                    "SELECT o.order_no, o.merchant_order_no, o.mch_id, m.name AS mch_name, o.status, o.order_type, o.real_order_amount, o.create_time, o.txid, o.remark FROM orders o LEFT JOIN public.mchs m ON m.id=o.mch_id"
                )
                params: list = []
                if ORDER_USER_COLUMN.strip():
                    q += sql.SQL(" WHERE o.{} = %s").format(sql.Identifier(ORDER_USER_COLUMN))
                    params.append(user_id)
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


async def fwd_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("未授权。")
        return
    if len(context.args) != 1:
        await update.message.reply_text("用法: /fwd 目标chat_id（需回复一条消息使用）")
        return
    if not update.message or not update.message.reply_to_message:
        await update.message.reply_text("请先回复一条消息，再执行 /fwd")
        return
    try:
        target_chat_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("chat_id 格式错误")
        return

    try:
        await context.bot.forward_message(
            chat_id=target_chat_id,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.reply_to_message.message_id,
        )
        await update.message.reply_text(f"✅ 已转发到 chat_id={target_chat_id}")
    except Exception as e:
        err = f"消息转发失败 target={target_chat_id}: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await update.message.reply_text("❌ 转发失败，已触发告警")
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
    ]
    await update.message.reply_text("\n".join(lines))


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error", exc_info=context.error)


async def post_init(app: Application):
    logger.info("HyBot started")
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
        logger.info("DB init check passed")
    except Exception as e:
        err = f"启动自检DB失败: {e.__class__.__name__}: {e}"
        logger.exception(err)
        await alert_admins(app, err)


def main():
    setup_logging()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("dbcheck", dbcheck))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("yesterday", yesterday))
    app.add_handler(CommandHandler("range", range_cmd))
    app.add_handler(CommandHandler("mchlist", mchlist))
    app.add_handler(CommandHandler("find", find_cmd))
    app.add_handler(CommandHandler("quick", quick_cmd))
    app.add_handler(CommandHandler("order", order_cmd))
    app.add_handler(CommandHandler("fwd", fwd_cmd))
    app.add_handler(CommandHandler("debugchat", debugchat_cmd))
    app.add_error_handler(on_error)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
