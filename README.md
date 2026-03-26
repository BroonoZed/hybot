# HyBot

Telegram bot for order summary queries.

## Implemented

- Whitelist access (`ALLOWED_TG_IDS`) — currently includes `7611295576`
- Status mapping:
  - `2` = 兑入
  - `1` = 兑出
  - `0` = 进行中
  - `-1` = 取消
- UTC+8 summary queries from PostgreSQL (`orders` table)
- Commands:
  - `/start`
  - `/status`
  - `/today [mch_id|mch_name]`
  - `/yesterday [mch_id|mch_name]`
  - `/range YYYY-MM-DD YYYY-MM-DD [mch_id|mch_name]`
  - `/mchlist [keyword]`
  - `/order <order_no>`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env
```

## Run

```bash
set -a; source .env; set +a
python3 main.py
```

## Notes

- Query uses `orders.create_time` as date basis in UTC+8.
- If you want summaries based on `pay_time` for completed orders, adjust SQL in `query_summary`.
