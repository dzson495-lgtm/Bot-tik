# -*- coding: utf-8 -*-
# ============================================================
#  QUẢN LÝ & BÁO CÁO XU TIKTOK TỰ ĐỘNG
#  Telegram Bot + Web Dashboard (Flask) + Google Sheets + Upstash Redis
#  Chạy: python main.py   (cấu hình qua biến môi trường)
# ============================================================

import os
import re
import json
import time
import html
import logging
import threading
import traceback
import unicodedata
from datetime import datetime, timedelta, timezone

import requests
import redis
import telebot
from telebot import types
from flask import Flask, request, jsonify, Response

# ------------------------------------------------------------
# 0. LOGGING
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("TIKXU")
logging.getLogger("telebot").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

# ------------------------------------------------------------
# 1. BIẾN MÔI TRƯỜNG (đọc an toàn: cắt khoảng trắng + ngoặc kép)
# ------------------------------------------------------------
def env(key, default=""):
    raw = os.environ.get(key)
    if raw is None:
        raw = default
    v = str(raw).strip()
    while len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1]
    return v.strip()

TELEGRAM_TOKEN = env("TELEGRAM_TOKEN")
GOOGLE_SHEET_WEBHOOK = env("GOOGLE_SHEET_WEBHOOK")
REDIS_URL = env("REDIS_URL")

try:
    PORT = int(env("PORT", "5000") or 5000)
except ValueError:
    PORT = 5000

try:
    TZ_OFFSET = float(env("TZ_OFFSET", "7") or 7)   # Mặc định GMT+7 (Việt Nam)
except ValueError:
    TZ_OFFSET = 7.0

try:
    SHEET_POLL = int(env("SHEET_POLL", "60") or 60)  # Chu kỳ kéo ngược Sheet -> Redis (giây), 0 = tắt
except ValueError:
    SHEET_POLL = 60

TZ = timezone(timedelta(hours=TZ_OFFSET))

# ------------------------------------------------------------
# 2. HẰNG SỐ NGHIỆP VỤ
# ------------------------------------------------------------
DEFAULT_RATE = {"dnt": 135, "tha": 170}

DEFAULT_MEMBERS = [
    {"name": "DƯƠNG T", "alias": "dt"},
    {"name": "DƯƠNG",   "alias": "d"},
    {"name": "LONG",    "alias": "l"},
    {"name": "PHƯƠNG",  "alias": "p"},
    {"name": "KHÁNH",   "alias": "k"},
]

THA_WORDS = {"tha", "t", "th", "x"}
DNT_WORDS = {"dnt", "dn", "d", "nt", "n"}

K_MEMBERS = "tt:members"
K_CHAT = "tt:chat"
K_WEBHOOK = "tt:webhook"
K_MONTHS = "tt:months"

# ------------------------------------------------------------
# 3. TIỆN ÍCH CHUNG
# ------------------------------------------------------------
def vnnow():
    return datetime.now(TZ)

def strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFD", str(s)) if unicodedata.category(c) != "Mn")

def norm_token(s):
    return strip_accents(str(s)).lower().strip().strip(".,;:!?()[]\"'")

def esc(s):
    return html.escape(str(s), quote=False)

def fmt_n(n):
    try:
        return "{:,}".format(int(round(float(n)))).replace(",", ".")
    except Exception:
        return str(n)

def fmt_money(n):
    return fmt_n(n) + "đ"

def mask_url(u):
    return re.sub(r"(://[^:/@]+:)[^@]+(@)", r"\1***\2", u or "")

def parse_month_str(s):
    try:
        parts = str(s).split("-")
        y, m = int(parts[0]), int(parts[1])
        if 1 <= m <= 12 and 2000 <= y <= 2100:
            return (y, m)
    except Exception:
        pass
    return None

# ------------------------------------------------------------
# 4. TẦNG LƯU TRỮ — REDIS CLOUD + FALLBACK JSON CỤC BỘ
# ------------------------------------------------------------
LOCAL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_backup.json")
_store_lock = threading.RLock()
local_store = {}
redis_client = None
redis_alive = False
redis_last_err = ""

def _load_local():
    global local_store
    try:
        with open(LOCAL_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict):
            local_store = d
            log.info("Đã nạp backup cục bộ: %d key.", len(local_store))
    except Exception:
        local_store = {}

def _save_local():
    try:
        tmp = LOCAL_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(local_store, f, ensure_ascii=False)
        os.replace(tmp, LOCAL_FILE)
    except Exception as e:
        log.warning("Lưu backup cục bộ lỗi: %s", e)

def ensure_redis():
    global redis_client, redis_last_err
    if redis_client is not None or not REDIS_URL:
        return
    common = dict(decode_responses=True, socket_connect_timeout=8,
                  socket_timeout=8, health_check_interval=30, retry_on_timeout=True)
    try:
        redis_client = redis.Redis.from_url(REDIS_URL, ssl_cert_reqs=None, **common)
        return
    except Exception:
        pass
    try:
        redis_client = redis.Redis.from_url(REDIS_URL, ssl_cert_reqs="none", **common)
    except Exception as e:
        redis_client = None
        redis_last_err = str(e)[:200]

def _push_local_to_redis():
    """Đẩy toàn bộ dữ liệu cục bộ lên Redis (phục hồi sau khi mất kết nối / /khoiphuc)."""
    global redis_alive, redis_last_err
    if redis_client is None:
        return 0
    pushed = 0
    with _store_lock:
        items = list(local_store.items())
    for k, v in items:
        try:
            redis_client.set(k, json.dumps(v, ensure_ascii=False))
            pushed += 1
        except Exception as e:
            redis_alive = False
            redis_last_err = str(e)[:200]
            return pushed
    redis_alive = True
    redis_last_err = ""
    return pushed

def db_set(key, value):
    global redis_alive, redis_last_err
    payload = json.dumps(value, ensure_ascii=False)
    with _store_lock:
        local_store[key] = value
        _save_local()
    if redis_client is not None:
        try:
            redis_client.set(key, payload)
            was = redis_alive
            redis_alive = True
            redis_last_err = ""
            if not was:
                threading.Thread(target=_push_local_to_redis, daemon=True).start()
            return True
        except Exception as e:
            redis_alive = False
            redis_last_err = str(e)[:200]
    else:
        ensure_redis()
    return False

def db_get(key, default=None):
    global redis_alive, redis_last_err
    if redis_client is not None:
        try:
            v = redis_client.get(key)
            redis_alive = True
            redis_last_err = ""
            if v is not None:
                try:
                    parsed = json.loads(v)
                except Exception:
                    parsed = v
                if parsed is not None:
                    return parsed
        except Exception as e:
            redis_alive = False
            redis_last_err = str(e)[:200]
    with _store_lock:
        if key in local_store:
            return local_store[key]
    return default

def db_del(key):
    with _store_lock:
        local_store.pop(key, None)
        _save_local()
    if redis_client is not None:
        try:
            redis_client.delete(key)
        except Exception:
            pass

def bootstrap_storage():
    _load_local()
    ensure_redis()
    if redis_client is not None:
        try:
            redis_client.ping()
            redis_alive = True
            if redis_client.get(K_MEMBERS) is None and K_MEMBERS in local_store:
                n = _push_local_to_redis()
                log.info("Redis trống — tự phục hồi %d key từ backup cục bộ.", n)
        except Exception as e:
            redis_alive = False
            redis_last_err = str(e)[:200]
            log.warning("Redis Cloud chưa kết nối được: %s", e)

# ------------------------------------------------------------
# 5. NHÂN SỰ (5 SLOT + ALIAS)
# ------------------------------------------------------------
def get_members():
    m = db_get(K_MEMBERS)
    if not isinstance(m, list) or not m:
        m = [dict(x) for x in DEFAULT_MEMBERS]
    out = []
    for i in range(5):
        if i < len(m) and isinstance(m[i], dict):
            nm = str(m[i].get("name") or ("[TRỐNG_%d]" % (i + 1)))
            al = str(m[i].get("alias") or "")
            out.append({"name": nm, "alias": al})
        else:
            out.append({"name": "[TRỐNG_%d]" % (i + 1), "alias": ""})
    return out

def save_members(members, push_sheet=True):
    db_set(K_MEMBERS, members)
    if push_sheet:
        gas_post({"action": "set_members", "members": [x["name"] for x in members]})

def match_member(token, members):
    t = norm_token(token)
    if not t:
        return None
    for i, mm in enumerate(members):
        if mm.get("alias") and norm_token(mm["alias"]) == t:
            return i
    for i, mm in enumerate(members):
        if norm_token(mm["name"]) == t:
            return i
    return None

def match_member_bigram(a, b, members):
    t = (norm_token(a) + " " + norm_token(b)).strip()
    if not t.strip():
        return None
    for i, mm in enumerate(members):
        if norm_token(mm["name"]) == t:
            return i
    return None

def resolve_slot(tokens, members):
    """Trả về (slot_index, số_token_đã_dùng) hoặc (None, 0). Hỗ trợ số slot (1-5) và bigram tên."""
    if not tokens:
        return None, 0
    t0 = norm_token(tokens[0])
    if t0.isdigit() and 1 <= int(t0) <= len(members):
        return int(t0) - 1, 1
    if len(tokens) >= 2:
        bi = match_member_bigram(tokens[0], tokens[1], members)
        if bi is not None:
            return bi, 2
    mi = match_member(tokens[0], members)
    if mi is not None:
        return mi, 1
    return None, 0

def _default_alias(name):
    nm = norm_token(name)
    for d in DEFAULT_MEMBERS:
        if norm_token(d["name"]) == nm:
            return d["alias"]
    return ""

# ------------------------------------------------------------
# 6. DỮ LIỆU THÁNG + TỶ GIÁ ĐA MỐC
# ------------------------------------------------------------
def month_key(y, m):
    return "tt:data:%04d-%02d" % (y, m)

def rates_key(y, m):
    return "tt:rates:%04d-%02d" % (y, m)

def dirty_key(y, m):
    return "tt:dirty:%04d-%02d" % (y, m)

def mark_dirty(y, m):
    db_set(dirty_key(y, m), 1)

def clear_dirty(y, m):
    db_del(dirty_key(y, m))

def register_month(y, m):
    k = "%04d-%02d" % (y, m)
    lst = db_get(K_MONTHS, [])
    if not isinstance(lst, list):
        lst = []
    if k not in lst:
        lst.append(k)
        db_set(K_MONTHS, lst)

def get_month_data(y, m):
    d = db_get(month_key(y, m), {})
    return d if isinstance(d, dict) else {}

def set_record(y, m, day, slot, field, value):
    data = get_month_data(y, m)
    dkey, skey = str(day), str(slot)
    day_map = data.get(dkey) if isinstance(data.get(dkey), dict) else {}
    rec = day_map.get(skey) if isinstance(day_map.get(skey), dict) else {"dnt": 0, "tha": 0}
    rec[field] = int(value)
    day_map[skey] = rec
    data[dkey] = day_map
    db_set(month_key(y, m), data)
    register_month(y, m)

def get_tiers(y, m):
    t = db_get(rates_key(y, m), [])
    if not isinstance(t, list):
        t = []
    out = []
    for x in t:
        if isinstance(x, dict) and x.get("from") is not None:
            try:
                out.append({"from": int(x.get("from", 1)),
                            "dnt": int(x.get("dnt", 0) or 0),
                            "tha": int(x.get("tha", 0) or 0)})
            except Exception:
                continue
    out.sort(key=lambda z: z["from"])
    return out

def get_rate_for_day(y, m, day):
    cur = {"dnt": int(DEFAULT_RATE["dnt"]), "tha": int(DEFAULT_RATE["tha"])}
    for t in get_tiers(y, m):
        try:
            fr = int(t.get("from", 1))
        except Exception:
            continue
        if fr <= day:
            if int(t.get("dnt", 0) or 0) > 0:
                cur["dnt"] = int(t["dnt"])
            if int(t.get("tha", 0) or 0) > 0:
                cur["tha"] = int(t["tha"])
    return cur

def rates_map_month(y, m):
    out = {}
    for d in range(1, 32):
        r = get_rate_for_day(y, m, d)
        out[str(d)] = {"dnt": r["dnt"], "tha": r["tha"]}
    return out

def set_rate_tier(y, m, from_day, ftype, value):
    tiers = get_tiers(y, m)
    target = None
    for t in tiers:
        if t["from"] == from_day:
            target = t
            break
    if target is None:
        target = {"from": from_day, "dnt": 0, "tha": 0}
        tiers.append(target)
    target[ftype] = int(value)
    tiers.sort(key=lambda z: z["from"])
    cur = {"dnt": int(DEFAULT_RATE["dnt"]), "tha": int(DEFAULT_RATE["tha"])}
    for t in tiers:
        for k2 in ("dnt", "tha"):
            v = int(t.get(k2, 0) or 0)
            if v > 0:
                cur[k2] = v
            else:
                t[k2] = cur[k2]
    db_set(rates_key(y, m), tiers)
    return tiers

def compute_month(y, m):
    members = get_members()
    data = get_month_data(y, m)
    stats = [{"dnt": 0, "tha": 0, "money": 0} for _ in members]
    per_day_money = {}
    for dkey, slots in data.items():
        try:
            day = int(dkey)
        except Exception:
            continue
        if not (1 <= day <= 31) or not isinstance(slots, dict):
            continue
        rate = get_rate_for_day(y, m, day)
        day_total = 0
        for skey, rec in slots.items():
            try:
                si = int(skey)
            except Exception:
                continue
            if si < 0 or si >= len(stats) or not isinstance(rec, dict):
                continue
            dv = int(rec.get("dnt", 0) or 0)
            tv = int(rec.get("tha", 0) or 0)
            stats[si]["dnt"] += dv
            stats[si]["tha"] += tv
            money = dv * rate["dnt"] + tv * rate["tha"]
            stats[si]["money"] += money
            day_total += money
        per_day_money[str(day)] = per_day_money.get(str(day), 0) + day_total
    kpi = {"money": sum(s["money"] for s in stats),
           "dnt": sum(s["dnt"] for s in stats),
           "tha": sum(s["tha"] for s in stats)}
    return stats, per_day_money, kpi

def all_time_total():
    tot = {"money": 0, "dnt": 0, "tha": 0}
    months = db_get(K_MONTHS, [])
    if not isinstance(months, list):
        months = []
    for k in months:
        ym = parse_month_str(k)
        if not ym:
            continue
        _, _, kpi = compute_month(ym[0], ym[1])
        tot["money"] += kpi["money"]
        tot["dnt"] += kpi["dnt"]
        tot["tha"] += kpi["tha"]
    return tot

# ------------------------------------------------------------
# 7. TẦNG GOOGLE SHEETS (Web App JSON POST/GET)
# ------------------------------------------------------------
SESSION = requests.Session()

def get_webhook():
    v = db_get(K_WEBHOOK)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return GOOGLE_SHEET_WEBHOOK or ""

def gas_post(payload, timeout=20):
    url = get_webhook()
    if not url:
        return {"ok": False, "error": "chua cau hinh GOOGLE_SHEET_WEBHOOK (/sheet <url>)"}
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "text/plain;charset=utf-8"}
    try:
        r = SESSION.post(url, data=body, headers=headers, timeout=timeout, allow_redirects=False)
        if r.status_code in (301, 302, 303, 307, 308):
            loc = r.headers.get("Location")
            if loc:
                r = SESSION.post(loc, data=body, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return {"ok": False, "error": "HTTP %s" % r.status_code}
        try:
            j = r.json()
            return j if isinstance(j, dict) else {"ok": False, "error": "response khong phai JSON"}
        except Exception:
            return {"ok": False, "error": "response khong phai JSON"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:180]}

def gas_get(timeout=25):
    url = get_webhook()
    if not url:
        return None
    try:
        sep = "&" if "?" in url else "?"
        r = SESSION.get(url + sep + "action=get", timeout=timeout)
        return r.json()
    except Exception:
        return None

def push_month_to_sheet(y, m):
    members = get_members()
    payload = {
        "action": "push_all",
        "month": "%04d-%02d" % (y, m),
        "members": [x["name"] for x in members],
        "days": get_month_data(y, m),
        "rates": rates_map_month(y, m),
    }
    res = gas_post(payload)
    if isinstance(res, dict) and res.get("ok"):
        clear_dirty(y, m)
    else:
        mark_dirty(y, m)
    return res

def sheet_update_cell(y, m, day, slot, field, value):
    res = gas_post({"action": "update_cell", "month": "%04d-%02d" % (y, m),
                    "day": int(day), "slot": int(slot), "field": field, "value": int(value)})
    if isinstance(res, dict) and res.get("ok"):
        return res
    if isinstance(res, dict) and res.get("error") == "month_mismatch":
        push_month_to_sheet(y, m)
    else:
        mark_dirty(y, m)
    return res

def pull_from_sheet():
    res = gas_get()
    if not isinstance(res, dict) or not res.get("ok"):
        return None
    days = res.get("days") if isinstance(res.get("days"), dict) else {}
    rates = res.get("rates") if isinstance(res.get("rates"), dict) else {}
    members_in = res.get("members") if isinstance(res.get("members"), list) else []
    month_raw = str(res.get("month") or "")
    if not month_raw and not days and not rates and not members_in:
        return {"empty": True}
    now = vnnow()
    y, m = parse_month_str(month_raw) or (now.year, now.month)
    if members_in:
        members = []
        for i in range(5):
            nm = str(members_in[i]).strip() if i < len(members_in) else ""
            if nm.startswith("SLOT_"):
                nm = ""
            members.append({"name": nm if nm else ("[TRỐNG_%d]" % (i + 1)),
                            "alias": _default_alias(nm)})
        db_set(K_MEMBERS, members)
    if days:
        db_set(month_key(y, m), days)
        register_month(y, m)
    tiers = []
    prev = None
    for d in range(1, 32):
        r = rates.get(str(d))
        if not isinstance(r, dict):
            continue
        try:
            dv = int(r.get("dnt", 0) or 0)
            tv = int(r.get("tha", 0) or 0)
        except Exception:
            continue
        if dv <= 0 and tv <= 0:
            continue
        if prev is None or dv != prev[0] or tv != prev[1]:
            tiers.append({"from": d, "dnt": dv, "tha": tv})
            prev = (dv, tv)
    if tiers:
        db_set(rates_key(y, m), tiers)
    return {"month": "%04d-%02d" % (y, m), "days": len(days), "tiers": len(tiers)}

def sheet_reconcile_loop():
    """Kéo ngược định kỳ: chỉnh sửa trực tiếp trên Google Sheets sẽ chảy về Redis."""
    if SHEET_POLL <= 0:
        return
    while True:
        time.sleep(SHEET_POLL)
        try:
            res = gas_get()
            if not isinstance(res, dict) or not res.get("ok"):
                continue
            ym = parse_month_str(res.get("month") or "")
            if not ym:
                continue
            y, m = ym
            if db_get(dirty_key(y, m)) == 1:
                continue  # có ghi chưa đẩy xong — chờ /dongbo
            days = res.get("days") if isinstance(res.get("days"), dict) else {}
            data = get_month_data(y, m)
            changed = 0
            for d in range(1, 32):
                sd = days.get(str(d)) or {}
                for s in range(5):
                    src = sd.get(str(s)) or {}
                    dv = int(src.get("dnt", 0) or 0)
                    tv = int(src.get("tha", 0) or 0)
                    cur = (data.get(str(d)) or {}).get(str(s)) or {}
                    if dv != int(cur.get("dnt", 0) or 0) or tv != int(cur.get("tha", 0) or 0):
                        set_record(y, m, d, s, "dnt", dv)
                        set_record(y, m, d, s, "tha", tv)
                        changed += 1
            if changed:
                log.info("2-Way Sync: nhận %d ô thay đổi từ Google Sheets (tháng %04d-%02d).", changed, y, m)
        except Exception:
            log.error("Reconcile lỗi:\n%s", traceback.format_exc())

# ------------------------------------------------------------
# 8. TELEGRAM BOT
# ------------------------------------------------------------
bot = None
BOT_READY = False
if TELEGRAM_TOKEN and ":" in TELEGRAM_TOKEN:
    try:
        bot = telebot.TeleBot(TELEGRAM_TOKEN, parse_mode="HTML")
        BOT_READY = True
    except Exception as e:
        log.error("Không khởi tạo được Telegram Bot: %s", e)
else:
    log.warning("TELEGRAM_TOKEN thiếu hoặc sai định dạng (phải chứa ':') — Bot sẽ không chạy, Web vẫn hoạt động.")

HELP_TEXT = (
    "🤖 <b>QUẢN LÝ XU TIKTOK — HƯỚNG DẪN</b>\n\n"
    "📥 <b>Nhập liệu</b> (thứ tự từ tự do):\n"
    "<code>k 500 tha</code> → Khánh 500 xu Thả (hôm nay)\n"
    "<code>l tha 300 5</code> → Long 300 xu Thả ngày 5\n"
    "<code>k 100 d</code> → Khánh 100 xu DNT\n"
    "<code>dt 2000 dnt 12</code> → DƯƠNG T, ngày 12\n\n"
    "📊 <b>Báo cáo</b>\n"
    "/tong — tổng tháng này + cả năm\n"
    "/thang 7 — tổng theo tháng chỉ định\n"
    "/lichsu [tên] [ngày] — lịch sử nhập\n\n"
    "⚙️ <b>Quản trị</b>\n"
    "/tygia — xem mốc tỷ giá\n"
    "/tygia dnt 140 — DNT 140đ từ ngày 1\n"
    "/tygia tha 190 tu 8 — Thả 190đ từ ngày 8\n"
    "/doiten &lt;tên_cũ&gt; &lt;tên_mới&gt; [alias]\n"
    "/xoaten &lt;tên&gt; — đưa slot về [TRỐNG]\n"
    "/dongbo — đẩy toàn bộ lên Google Sheets\n"
    "/khoiphuc — phục hồi từ backup cục bộ\n"
    "/khoiphuc sheet — phục hồi từ Google Sheets\n"
    "/redis — kiểm tra Redis Cloud\n"
    "/sheet &lt;url&gt; — đổi webhook Sheets\n\n"
    "🖥 <i>Dashboard web: mở trang chủ của app.</i>"
)

def save_chat(chat_id):
    try:
        db_set(K_CHAT, int(chat_id))
    except Exception:
        pass

def safe_handler(fn):
    def wrapper(msg, *a, **kw):
        try:
            save_chat(msg.chat.id)
            fn(msg, *a, **kw)
        except Exception as e:
            log.error("Handler lỗi: %s\n%s", e, traceback.format_exc())
            try:
                bot.reply_to(msg, "⚠️ Đã xảy ra lỗi nội bộ, thử lại sau.")
            except Exception:
                pass
    return wrapper

def parse_record(text, members):
    """Nhận dạng thứ tự từ tự do: <tên> <số_xu> <loại> [ngày]"""
    tokens = [t for t in re.split(r"\s+", (text or "").strip()) if t]
    slot = None
    coins = None
    day = None
    dtype = None
    unknown = []
    i, n = 0, len(tokens)
    while i < n:
        raw = tokens[i]
        t = norm_token(raw)
        tnum = t.replace(",", "").replace(".", "")
        if tnum.isdigit():
            num = int(tnum)
            if coins is None:
                coins = num
            elif day is None and 1 <= num <= 31:
                day = num
            else:
                unknown.append(raw)
            i += 1
            continue
        if slot is None and i + 1 < n:
            bi = match_member_bigram(raw, tokens[i + 1], members)
            if bi is not None:
                slot = bi
                i += 2
                continue
        if slot is None:
            mi = match_member(raw, members)
            if mi is not None:
                slot = mi
                i += 1
                continue
        if dtype is None:
            if t in THA_WORDS:
                dtype = "tha"
                i += 1
                continue
            if t in DNT_WORDS:
                dtype = "dnt"
                i += 1
                continue
        unknown.append(raw)
        i += 1
    return {"slot": slot, "coins": coins, "day": day, "type": dtype, "tokens": tokens}

def handle_record(msg):
    text = (msg.text or "").strip()
    if not text or text.startswith("/"):
        return
    members = get_members()
    p = parse_record(text, members)
    mentioned = p["slot"] is not None or any(match_member(t, members) is not None for t in p["tokens"])
    if not mentioned:
        return  # tin nhắn thường trong nhóm → bỏ qua
    if p["slot"] is None or p["coins"] is None or p["type"] is None:
        bot.reply_to(msg, "⚠️ Cú pháp: <code>&lt;tên&gt; &lt;số_xu&gt; &lt;loại&gt; [ngày]</code>\n"
                          "Ví dụ: <code>k 500 tha</code> • <code>l tha 300 5</code> • <code>k 100 d</code>")
        return
    now = vnnow()
    y, m = now.year, now.month
    day = p["day"] if p["day"] else now.day
    slot, field = p["slot"], p["type"]
    value = int(p["coins"])
    if value < 0:
        bot.reply_to(msg, "⚠️ Số xu không được âm.")
        return
    set_record(y, m, day, slot, field, value)
    res = sheet_update_cell(y, m, day, slot, field, value)
    synced = isinstance(res, dict) and res.get("ok")
    name = members[slot]["name"]
    label = "DNT" if field == "dnt" else "THẢ"
    note = "☁️ đã đồng bộ Sheet" if synced else "⚠️ Sheet chưa đồng bộ (chạy /dongbo để đẩy lại)"
    if value == 0:
        reply = "🗑 <b>%s</b> — đã xóa dữ liệu %s ngày %02d/%02d/%d\n%s" % (esc(name), label, day, m, y, note)
    else:
        rate = get_rate_for_day(y, m, day)
        unit = rate["dnt"] if field == "dnt" else rate["tha"]
        money = value * unit
        reply = ("✅ <b>%s</b> +%s xu <b>%s</b>\n📅 Ngày %02d/%02d/%d • Giá %sđ/xu\n💰 Thành tiền: <b>%s</b>\n%s"
                 % (esc(name), fmt_n(value), label, day, m, y, fmt_n(unit), fmt_money(money), note))
    if name.startswith("[TRỐNG"):
        reply += "\nℹ️ Slot này đang trống — dùng /doiten để đặt tên."
    bot.reply_to(msg, reply)

def month_report_text(y, m, include_year=False):
    members = get_members()
    stats, per_day, kpi = compute_month(y, m)
    lines = ["📊 <b>TỔNG KẾT THÁNG %02d/%d</b>" % (m, y), ""]
    order = sorted(range(len(members)), key=lambda i: -stats[i]["money"])
    shown = 0
    for pos, i in enumerate(order, 1):
        s = stats[i]
        if s["money"] == 0 and s["dnt"] == 0 and s["tha"] == 0:
            continue
        lines.append("%d. <b>%s</b> — %s" % (pos, esc(members[i]["name"]), fmt_money(s["money"])))
        lines.append("    <code>DNT %s xu • Thả %s xu</code>" % (fmt_n(s["dnt"]), fmt_n(s["tha"])))
        shown += 1
    if shown == 0:
        lines.append("(Tháng này chưa có dữ liệu)")
    lines.append("")
    lines.append("🪙 Xu DNT: <b>%s</b> • Xu Thả: <b>%s</b>" % (fmt_n(kpi["dnt"]), fmt_n(kpi["tha"])))
    lines.append("💰 <b>TỔNG THỰC NHẬN: %s</b>" % fmt_money(kpi["money"]))
    if include_year:
        at = all_time_total()
        n_months = len(db_get(K_MONTHS, []) or [])
        lines.append("")
        lines.append("🗂 <b>Cả năm (%d tháng): %s</b>" % (n_months, fmt_money(at["money"])))
    return "\n".join(lines)

def rates_text(y, m):
    tiers = get_tiers(y, m)
    lines = ["📅 <b>TỶ GIÁ THÁNG %02d/%d</b>" % (m, y)]
    if not tiers:
        lines.append("• Toàn tháng: DNT %sđ/xu • Thả %sđ/xu (mặc định)"
                     % (fmt_n(DEFAULT_RATE["dnt"]), fmt_n(DEFAULT_RATE["tha"])))
    else:
        for t in tiers:
            lines.append("• Từ ngày %02d: DNT %sđ/xu • Thả %sđ/xu" % (t["from"], fmt_n(t["dnt"]), fmt_n(t["tha"])))
    return "\n".join(lines)

def register_handlers(b):
    @b.message_handler(commands=["start", "help"])
    @safe_handler
    def _start(msg):
        b.reply_to(msg, HELP_TEXT)

    @b.message_handler(commands=["tong"])
    @safe_handler
    def _tong(msg):
        now = vnnow()
        b.reply_to(msg, month_report_text(now.year, now.month, include_year=True))

    @b.message_handler(commands=["thang"])
    @safe_handler
    def _thang(msg):
        now = vnnow()
        args = (msg.text or "").split()[1:]
        m = now.month
        if args and args[0].strip().isdigit() and 1 <= int(args[0]) <= 12:
            m = int(args[0])
        b.reply_to(msg, month_report_text(now.year, m))

    @b.message_handler(commands=["lichsu"])
    @safe_handler
    def _lichsu(msg):
        now = vnnow()
        y, m = now.year, now.month
        members = get_members()
        args = (msg.text or "").split()[1:]
        who, day = None, None
        for a in args:
            t = norm_token(a).replace(",", "").replace(".", "")
            if t.isdigit() and 1 <= int(t) <= 31 and day is None:
                day = int(t)
                continue
            if who is None:
                mi = match_member(a, members)
                if mi is not None:
                    who = mi
        if who is None and day is None:
            day = now.day
        if who is not None and day is not None:
            title = "LỊCH SỬ %s — NGÀY %02d/%02d/%d" % (esc(members[who]["name"]), day, m, y)
        elif who is not None:
            title = "LỊCH SỬ %s — THÁNG %02d/%d" % (esc(members[who]["name"]), m, y)
        elif day is not None:
            title = "LỊCH SỬ NGÀY %02d/%02d/%d" % (day, m, y)
        else:
            title = "LỊCH SỬ THÁNG %02d/%d" % (m, y)
        data = get_month_data(y, m)
        lines = ["🕘 <b>%s</b>" % title, ""]
        count = 0
        for dstr in sorted(data.keys(), key=lambda x: int(x) if str(x).isdigit() else 0):
            d = int(dstr) if str(dstr).isdigit() else 0
            if d < 1 or (day is not None and d != day):
                continue
            slots = data[dstr]
            for skey in sorted(slots.keys(), key=lambda x: int(x) if str(x).isdigit() else 0):
                si = int(skey) if str(skey).isdigit() else -1
                if si < 0 or si >= len(members) or (who is not None and si != who):
                    continue
                rec = slots[skey]
                dv = int(rec.get("dnt", 0) or 0)
                tv = int(rec.get("tha", 0) or 0)
                if dv == 0 and tv == 0:
                    continue
                rate = get_rate_for_day(y, m, d)
                money = dv * rate["dnt"] + tv * rate["tha"]
                lines.append("• Ngày %02d — <b>%s</b>: %s DNT + %s Thả = %s"
                             % (d, esc(members[si]["name"]), fmt_n(dv), fmt_n(tv), fmt_money(money)))
                count += 1
                if count >= 60:
                    lines.append("… (thu hẹp phạm vi để xem thêm)")
                    break
            if count >= 60:
                break
        if count == 0:
            lines.append("(Không tìm thấy bản ghi nào)")
        elif who is not None and day is None:
            s = compute_month(y, m)[0][who]
            lines.append("")
            lines.append("➤ Tổng %s tháng %02d: <b>%s</b>" % (esc(members[who]["name"]), m, fmt_money(s["money"])))
        b.reply_to(msg, "\n".join(lines))

    @b.message_handler(commands=["tygia"])
    @safe_handler
    def _tygia(msg):
        now = vnnow()
        y, m = now.year, now.month
        args = (msg.text or "").split()[1:]
        if not args:
            b.reply_to(msg, rates_text(y, m) + "\n\nĐặt mốc mới: <code>/tygia tha 190 tu 8</code>")
            return
        t0 = norm_token(args[0])
        ftype = None
        if t0 in DNT_WORDS:
            ftype = "dnt"
        if t0 in THA_WORDS:
            ftype = "tha"
        if ftype is None or len(args) < 2:
            b.reply_to(msg, "⚠️ Cú pháp: <code>/tygia &lt;dnt|tha&gt; &lt;giá&gt; [tu &lt;ngày&gt;]</code>")
            return
        tnum = norm_token(args[1]).replace(",", "").replace(".", "")
        if not tnum.isdigit():
            b.reply_to(msg, "⚠️ Giá tiền phải là số. VD: <code>/tygia tha 190 tu 8</code>")
            return
        value = int(tnum)
        from_day = 1
        if len(args) >= 4 and norm_token(args[2]) in ("tu", "from") and norm_token(args[3]).isdigit():
            from_day = int(norm_token(args[3]))
        elif len(args) >= 3 and norm_token(args[2]).isdigit():
            from_day = int(norm_token(args[2]))
        from_day = max(1, min(31, from_day))
        tiers = set_rate_tier(y, m, from_day, ftype, value)
        register_month(y, m)
        push_month_to_sheet(y, m)
        lines = ["✅ Đã đặt <b>%s</b> = <b>%sđ/xu</b> (áp dụng từ ngày %02d)"
                 % ("DNT" if ftype == "dnt" else "THẢ", fmt_n(value), from_day), "",
                 "📅 Tỷ giá tháng %02d/%d:" % (m, y)]
        for t in tiers:
            lines.append("• Từ ngày %02d: DNT %sđ/xu • Thả %sđ/xu" % (t["from"], fmt_n(t["dnt"]), fmt_n(t["tha"])))
        b.reply_to(msg, "\n".join(lines))

    @b.message_handler(commands=["doiten"])
    @safe_handler
    def _doiten(msg):
        members = get_members()
        parts = [p.strip().strip('"').strip("'") for p in (msg.text or "").split()[1:] if p.strip()]
        if not parts:
            b.reply_to(msg, "⚠️ Cú pháp: <code>/doiten &lt;tên_cũ|alias|số_slot&gt; &lt;tên_mới&gt; [alias]</code>\n"
                            "VD: <code>/doiten k Quang Dũng qd</code> hoặc <code>/doiten 2 Minh Anh ma</code>")
            return
        idx, used = resolve_slot(parts, members)
        if idx is None:
            b.reply_to(msg, "⚠️ Không tìm thấy nhân sự <code>%s</code>." % esc(parts[0]))
            return
        rest = parts[used:]
        if not rest:
            b.reply_to(msg, "⚠️ Thiếu tên mới. VD: <code>/doiten k Quang Dũng qd</code>")
            return
        if len(rest) == 1:
            new_name, new_alias = rest[0], members[idx]["alias"]
        elif len(rest) == 2:
            new_name, new_alias = rest[0], rest[1]
        else:
            new_name, new_alias = " ".join(rest[:-1]), rest[-1]
        new_alias = strip_accents(new_alias).lower().replace(" ", "") if new_alias else ""
        old_name = members[idx]["name"]
        members[idx]["name"] = new_name.strip()[:40]
        members[idx]["alias"] = new_alias[:20]
        save_members(members)
        slot_list = "\n".join("%d. <b>%s</b> (alias: <code>%s</code>)" % (i + 1, esc(x["name"]), esc(x["alias"] or "-"))
                              for i, x in enumerate(members))
        b.reply_to(msg, "✅ Đã đổi slot %d: <b>%s</b> → <b>%s</b>\n\n👥 <b>Slot hiện tại:</b>\n%s"
                   % (idx + 1, esc(old_name), esc(members[idx]["name"]), esc(members[idx]["alias"] or "-"), slot_list))

    @b.message_handler(commands=["xoaten"])
    @safe_handler
    def _xoaten(msg):
        members = get_members()
        parts = [p.strip().strip('"').strip("'") for p in (msg.text or "").split()[1:] if p.strip()]
        if not parts:
            b.reply_to(msg, "⚠️ Cú pháp: <code>/xoaten &lt;tên|alias|số_slot&gt;</code>")
            return
        idx, _used = resolve_slot(parts, members)
        if idx is None:
            b.reply_to(msg, "⚠️ Không tìm thấy nhân sự <code>%s</code>." % esc(parts[0]))
            return
        old_name = members[idx]["name"]
        members[idx]["name"] = "[TRỐNG_%d]" % (idx + 1)
        members[idx]["alias"] = ""
        save_members(members)
        b.reply_to(msg, "🗑 Đã xóa nhân sự <b>%s</b> → slot %d giờ là <b>[TRỐNG_%d]</b>.\n"
                        "<i>Dữ liệu cũ trong bảng vẫn được giữ đến khi ghi đè.</i>"
                   % (esc(old_name), idx + 1, idx + 1))

    @b.message_handler(commands=["dongbo"])
    @safe_handler
    def _dongbo(msg):
        now = vnnow()
        months = db_get(K_MONTHS, [])
        if not isinstance(months, list):
            months = []
        cur = "%04d-%02d" % (now.year, now.month)
        if cur not in months:
            months.append(cur)
        months = sorted(months)
        bot.reply_to(msg, "🔄 Đang đồng bộ %d tháng lên Google Sheets…" % len(months))
        results = []
        for k in months:
            ym = parse_month_str(k)
            if not ym:
                continue
            res = push_month_to_sheet(ym[0], ym[1])
            okk = isinstance(res, dict) and res.get("ok")
            results.append("• %s: %s" % (k, "✅" if okk else "❌ " + esc(str(res.get("error", "?")))))
            time.sleep(0.3)
        b.reply_to(msg, "☁️ <b>KẾT QUẢ ĐỒNG BỘ</b>\n" + "\n".join(results))

    @b.message_handler(commands=["khoiphuc"])
    @safe_handler
    def _khoiphuc(msg):
        global redis_alive
        args = (msg.text or "").split()[1:]
        if args and norm_token(args[0]) in ("sheet", "sheets", "google"):
            res = pull_from_sheet()
            if res is None:
                b.reply_to(msg, "❌ Không đọc được dữ liệu từ Google Sheets. Kiểm tra /sheet và quyền Web App (Anyone).")
            elif res.get("empty"):
                b.reply_to(msg, "⚠️ Sheet đang trống — không ghi đè dữ liệu hiện có.")
            else:
                b.reply_to(msg, "✅ Đã phục hồi từ Google Sheets — tháng <b>%s</b> (%d ngày có dữ liệu, %d mốc tỷ giá)."
                           % (res["month"], res["days"], res["tiers"]))
            return
        ensure_redis()
        if redis_client is None:
            b.reply_to(msg, "❌ Redis chưa cấu hình (thiếu REDIS_URL) — dữ liệu đang dùng backup cục bộ.")
            return
        n = _push_local_to_redis()
        redis_alive = True
        b.reply_to(msg, "✅ Đã phục hồi <b>%d</b> bản ghi từ backup cục bộ lên Redis Cloud." % n)

    @b.message_handler(commands=["redis"])
    @safe_handler
    def _redis(msg):
        global redis_alive, redis_last_err
        ensure_redis()
        if redis_client is None:
            b.reply_to(msg, "🔴 <b>Redis: CHƯA CẤU HÌNH</b> (thiếu REDIS_URL)\n📁 Backup cục bộ: %d key" % len(local_store))
            return
        t0 = time.time()
        try:
            redis_client.ping()
            dt = int((time.time() - t0) * 1000)
            redis_alive = True
            redis_last_err = ""
            b.reply_to(msg, "🟢 <b>Redis Cloud: HOẠT ĐỘNG</b>\n• Ping: <b>%d ms</b>\n• Endpoint: <code>%s</code>\n• Fallback cục bộ: %d key"
                       % (dt, esc(mask_url(REDIS_URL)), len(local_store)))
        except Exception as e:
            redis_alive = False
            redis_last_err = str(e)[:200]
            b.reply_to(msg, "🔴 <b>Redis Cloud: MẤT KẾT NỐI</b>\n• Lỗi: <code>%s</code>\n• Đang dùng fallback JSON cục bộ: %d key"
                       % (esc(redis_last_err), len(local_store)))

    @b.message_handler(commands=["sheet"])
    @safe_handler
    def _sheet(msg):
        args = (msg.text or "").split()[1:]
        if args:
            url = args[0].strip().strip('"').strip("'")
            if not url.startswith("http"):
                b.reply_to(msg, "⚠️ URL không hợp lệ (phải bắt đầu bằng http và có đuôi /exec).")
                return
            db_set(K_WEBHOOK, url)
            res = gas_post({"action": "ping"})
            if isinstance(res, dict) and res.get("ok"):
                b.reply_to(msg, "✅ Đã lưu webhook mới và kết nối thành công!\n<code>%s</code>" % esc(url))
            else:
                b.reply_to(msg, "⚠️ Đã lưu webhook nhưng test lỗi: <code>%s</code>\nKiểm tra lại deployment (quyền Anyone)." % esc(str(res.get("error"))))
        else:
            url = get_webhook()
            if url:
                res = gas_post({"action": "ping"})
                okk = isinstance(res, dict) and res.get("ok")
                b.reply_to(msg, "🔗 Webhook hiện tại:\n<code>%s</code>\nTrạng thái: %s"
                           % (esc(url), "🟢 OK" if okk else "🔴 " + esc(str(res.get("error") if isinstance(res, dict) else "?"))))
            else:
                b.reply_to(msg, "⚠️ Chưa cấu hình webhook. Dùng: <code>/sheet &lt;url_exec&gt;</code>")

    @b.message_handler(func=lambda m: True, content_types=["text"])
    @safe_handler
    def _catch(msg):
        handle_record(msg)

def setup_bot():
    if bot is None:
        return
    register_handlers(bot)
    try:
        bot.set_my_commands([
            types.BotCommand("tong", "Tổng kết tháng này"),
            types.BotCommand("thang", "Tổng kết theo tháng"),
            types.BotCommand("lichsu", "Lịch sử nhập liệu"),
            types.BotCommand("tygia", "Xem/đặt tỷ giá xu"),
            types.BotCommand("dongbo", "Đồng bộ lên Google Sheets"),
            types.BotCommand("khoiphuc", "Phục hồi dữ liệu backup"),
            types.BotCommand("redis", "Trạng thái Redis Cloud"),
            types.BotCommand("sheet", "Xem/đổi webhook Sheets"),
        ])
    except Exception:
        pass

def bot_loop():
    while True:
        try:
            if bot is None:
                time.sleep(30)
                continue
            bot.infinity_polling(timeout=25, long_polling_timeout=25, skip_pending=True)
        except Exception as e:
            log.error("Polling lỗi: %s — thử lại sau 5s", e)
            time.sleep(5)

# ------------------------------------------------------------
# 9. CHỐT SỔ TỰ ĐỘNG 23:59 MỖI TỐI
# ------------------------------------------------------------
def build_daily_report(y, m, d):
    members = get_members()
    data = get_month_data(y, m)
    rate = get_rate_for_day(y, m, d)
    today_slots = data.get(str(d)) or {}
    lines = ["🌙 <b>CHỐT SỔ — NGÀY %02d/%02d/%d</b>" % (d, m, y), ""]
    any_rec = False
    total_today = 0
    for si in range(len(members)):
        rec = today_slots.get(str(si)) or {}
        dv = int(rec.get("dnt", 0) or 0)
        tv = int(rec.get("tha", 0) or 0)
        if dv == 0 and tv == 0:
            continue
        any_rec = True
        money = dv * rate["dnt"] + tv * rate["tha"]
        total_today += money
        lines.append("• <b>%s</b>: %s DNT + %s Thả = <b>%s</b>"
                     % (esc(members[si]["name"]), fmt_n(dv), fmt_n(tv), fmt_money(money)))
    if not any_rec:
        lines.append("(Hôm nay chưa có giao dịch nào)")
    lines.append("")
    lines.append("💰 Tổng trong ngày: <b>%s</b>" % fmt_money(total_today))
    stats, per_day, kpi = compute_month(y, m)
    lines.append("📈 Cộng dồn tháng %02d: <b>%s</b>" % (m, fmt_money(kpi["money"])))
    lines.append("🪙 DNT %s xu • Thả %s xu" % (fmt_n(kpi["dnt"]), fmt_n(kpi["tha"])))
    return "\n".join(lines)

def scheduler_loop():
    while True:
        try:
            now = vnnow()
            if now.hour == 23 and now.minute >= 59:
                dkey = "tt:report:%04d-%02d-%02d" % (now.year, now.month, now.day)
                if db_get(dkey) != 1:
                    db_set(dkey, 1)
                    chat = db_get(K_CHAT)
                    if chat and bot is not None:
                        try:
                            bot.send_message(int(chat), build_daily_report(now.year, now.month, now.day),
                                             parse_mode="HTML", disable_web_page_preview=True)
                            log.info("Đã gửi báo cáo chốt sổ tối %s.", now.strftime("%d/%m"))
                        except Exception as e:
                            log.error("Gửi báo cáo tối lỗi: %s", e)
        except Exception:
            log.error("Scheduler lỗi:\n%s", traceback.format_exc())
        time.sleep(20)

# ------------------------------------------------------------
# 10. WEB DASHBOARD (FINTECH DARK THEME)
# ------------------------------------------------------------
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>TIKXU • Quản lý Xu TikTok</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🪙</text></svg>"/>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700;800&family=JetBrains+Mono:wght@400;600;700;800&display=swap" rel="stylesheet"/>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#090d16;--card:rgba(255,255,255,.045);--card2:rgba(255,255,255,.07);
--line:rgba(255,255,255,.09);--txt:#e9edf5;--dim:rgba(233,237,245,.4);
--gold:#f5c04a;--gold2:#ffe29a;--green:#3ddc97;--red:#ff6b6b;
--mono:'JetBrains Mono',ui-monospace,monospace;--sans:'Plus Jakarta Sans',system-ui,sans-serif}
body{background:var(--bg);color:var(--txt);font-family:var(--sans);min-height:100vh;overflow-x:hidden}
.glow{position:fixed;inset:0;z-index:0;pointer-events:none;background:
radial-gradient(700px 340px at 12% -6%,rgba(245,192,74,.10),transparent 60%),
radial-gradient(760px 380px at 88% -4%,rgba(110,168,254,.09),transparent 60%),
radial-gradient(640px 420px at 50% 112%,rgba(61,220,151,.07),transparent 62%)}
.glass{background:var(--card);border:1px solid var(--line);border-radius:16px;
backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);box-shadow:0 10px 34px rgba(0,0,0,.35)}
header{position:relative;z-index:5;display:flex;align-items:center;gap:14px;justify-content:space-between;
margin:16px auto 0;max-width:1180px;padding:12px 20px;flex-wrap:wrap}
.brand{font-weight:800;font-size:16px;letter-spacing:.06em;display:flex;align-items:center;gap:11px;line-height:1.25}
.brand .logo{width:32px;height:32px;border-radius:9px;display:flex;align-items:center;justify-content:center;
background:linear-gradient(135deg,#f5c04a,#ff8a5c);font-size:16px;box-shadow:0 4px 14px rgba(245,192,74,.35)}
.brand small{color:var(--dim);font-weight:700;letter-spacing:.24em;font-size:9px}
.status{display:flex;align-items:center;gap:16px;font-size:11px;font-weight:700;letter-spacing:.08em;color:var(--dim)}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;vertical-align:1px}
.dot.on{background:var(--green);box-shadow:0 0 8px var(--green)}
.dot.off{background:var(--red);box-shadow:0 0 8px var(--red)}
main{position:relative;z-index:1;max-width:1180px;margin:0 auto;padding:0 16px 60px}
.kpis{display:grid;grid-template-columns:1.5fr 1fr 1fr;gap:14px;margin:18px 0}
.kpi{padding:18px 22px}
.kpi label{font-size:10.5px;letter-spacing:.14em;color:var(--dim);text-transform:uppercase;font-weight:800}
.kpi .v{font-family:var(--mono);font-size:30px;font-weight:800;margin-top:8px}
.kpi.big .v{font-size:40px;background:linear-gradient(90deg,#f5c04a,#ffe29a);
-webkit-background-clip:text;background-clip:text;color:transparent}
.kpi .hint{margin-top:8px;font-size:11px;color:var(--dim);font-family:var(--mono)}
.c-dnt{color:var(--gold)}.c-tha{color:var(--green)}
.toolbar{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:12px 16px;flex-wrap:wrap}
.months{display:flex;gap:6px;flex-wrap:wrap}
.tools{display:flex;gap:8px}
.chip{background:rgba(255,255,255,.05);border:1px solid var(--line);color:var(--dim);border-radius:10px;
padding:7px 12px;font-weight:700;font-size:12px;cursor:pointer;transition:.15s;font-family:var(--sans)}
.chip:hover{color:var(--txt);border-color:rgba(255,255,255,.28)}
.chip.on{background:rgba(245,192,74,.14);border-color:rgba(245,192,74,.5);color:var(--gold)}
.btn{background:var(--card2);border:1px solid var(--line);color:var(--txt);font-family:var(--sans);
font-weight:700;font-size:12.5px;border-radius:11px;padding:9px 14px;cursor:pointer;transition:.15s}
.btn:hover{border-color:rgba(255,255,255,.28);transform:translateY(-1px)}
.btn.primary{background:linear-gradient(135deg,rgba(245,192,74,.22),rgba(245,192,74,.10));
border-color:rgba(245,192,74,.45);color:var(--gold)}
.tablewrap{margin-top:16px;overflow:auto;max-height:64vh}
table{width:100%;border-collapse:separate;border-spacing:0;font-family:var(--mono);font-size:12.5px;min-width:760px}
thead th{position:sticky;top:0;z-index:3;background:rgba(11,15,26,.94);backdrop-filter:blur(10px);
font-family:var(--sans);font-size:10.5px;letter-spacing:.1em;color:var(--dim);text-transform:uppercase;
padding:10px 8px;border-bottom:1px solid var(--line);text-align:center;font-weight:800}
thead th.sub{color:rgba(233,237,245,.5);font-size:9px;padding:6px}
th.sub.dh{color:rgba(245,192,74,.8)}th.sub.th{color:rgba(61,220,151,.8)}
tbody td{padding:7px 8px;text-align:center;border-bottom:1px solid rgba(255,255,255,.045);white-space:nowrap}
td.cday{font-weight:700;color:var(--dim);text-align:left;padding-left:16px;width:92px}
tbody tr:hover{background:rgba(255,255,255,.03)}
tr.today{background:rgba(245,192,74,.07)}
tr.today td.cday{color:var(--gold)}
.badge-nay{background:linear-gradient(90deg,var(--gold),var(--gold2));color:#151001;font-family:var(--sans);
font-size:8.5px;font-weight:800;letter-spacing:.08em;padding:2px 7px;border-radius:99px;margin-left:8px;vertical-align:1px}
td.val{cursor:pointer;min-width:58px;transition:.12s}
td.val:hover{background:rgba(245,192,74,.10);box-shadow:inset 0 0 0 1px rgba(245,192,74,.35)}
td.val.dnt{color:var(--gold);font-weight:700;text-shadow:0 0 14px rgba(245,192,74,.35)}
td.val.tha{color:var(--green);font-weight:700;text-shadow:0 0 14px rgba(61,220,151,.3)}
td.val.zero{color:rgba(233,237,245,.14);font-weight:400}
td.money{font-weight:700}
td.money.zero{color:rgba(233,237,245,.14)}
.cellin{width:100%;min-width:58px;background:#0c1220;border:1px solid var(--gold);color:#fff;
font-family:var(--mono);font-size:12.5px;text-align:center;border-radius:7px;padding:3px 4px;outline:none}
.modal{position:fixed;inset:0;z-index:60;display:none;align-items:center;justify-content:center;
background:rgba(4,6,12,.72);backdrop-filter:blur(6px);padding:16px}
.modal.open{display:flex}
.box{width:min(620px,96vw);max-height:86vh;overflow:auto;padding:22px}
.boxhead{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}
.boxhead h3{font-size:14px;letter-spacing:.08em}
.x{background:none;border:none;color:var(--dim);font-size:16px;cursor:pointer;padding:4px 8px}
.x:hover{color:var(--txt)}
.tier{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;
border:1px solid var(--line);border-radius:11px;margin-bottom:8px;background:rgba(255,255,255,.03);font-size:12.5px}
.tier .from{color:var(--dim);font-weight:700;font-family:var(--mono)}
.formrow{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:14px}
.formrow select,.formrow input{background:#0c1220;border:1px solid var(--line);color:var(--txt);border-radius:10px;
padding:9px 10px;font-family:var(--mono);font-size:13px;outline:none}
.formrow select:focus,.formrow input:focus{border-color:rgba(245,192,74,.5)}
.formrow input{width:130px}
.fromwrap{color:var(--dim);font-size:12px;display:flex;align-items:center;gap:6px}
.fromwrap input{width:64px}
.mem-row{display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap}
.mem-row .slot{width:26px;height:26px;border-radius:8px;background:rgba(255,255,255,.06);display:flex;
align-items:center;justify-content:center;font-family:var(--mono);font-size:12px;color:var(--dim);font-weight:700;flex-shrink:0}
.mem-row input{flex:1;min-width:120px;background:#0c1220;border:1px solid var(--line);color:var(--txt);
border-radius:10px;padding:8px 10px;font-size:13px;outline:none;font-family:var(--sans)}
.mem-row input:focus{border-color:rgba(245,192,74,.5)}
.mini{padding:8px 12px;border-radius:9px;font-size:11.5px;font-weight:800;cursor:pointer;
border:1px solid var(--line);background:var(--card2);color:var(--txt);font-family:var(--sans)}
.mini.save{color:var(--gold);border-color:rgba(245,192,74,.4)}
.mini.del{color:var(--red);border-color:rgba(255,107,107,.35)}
.note{margin-top:12px;font-size:11.5px;color:var(--dim);line-height:1.6}
#toasts{position:fixed;right:18px;bottom:18px;z-index:99;display:flex;flex-direction:column;gap:8px}
.toast{background:rgba(16,22,36,.95);border:1px solid rgba(61,220,151,.4);color:var(--txt);border-radius:12px;
padding:11px 16px;font-size:12.5px;font-weight:600;box-shadow:0 8px 30px rgba(0,0,0,.5);animation:tin .25s ease}
.toast.err{border-color:rgba(255,107,107,.5)}
@keyframes tin{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
footer{margin-top:18px;text-align:center;color:rgba(233,237,245,.28);font-size:11px;line-height:2}
footer code{font-family:var(--mono);color:rgba(233,237,245,.5);background:rgba(255,255,255,.05);padding:2px 7px;border-radius:6px}
@media(max-width:760px){.kpis{grid-template-columns:1fr 1fr}.kpi.big{grid-column:1/-1}.kpi .v{font-size:25px}.kpi.big .v{font-size:32px}}
</style>
</head>
<body>
<div class="glow"></div>
<header class="glass">
  <div class="brand"><div class="logo">🪙</div><div>TIKXU<br/><small>XU TIKTOK TERMINAL</small></div></div>
  <div class="status">
    <span><span class="dot off" id="dot-redis"></span>REDIS</span>
    <span><span class="dot off" id="dot-sheet"></span>SHEET</span>
    <button class="btn primary" id="btn-sync">⟳ Đồng bộ</button>
  </div>
</header>
<main>
  <section class="kpis">
    <div class="kpi glass big">
      <label>Tổng tiền thực nhận (tháng <span id="kpi-mlabel">–</span>)</label>
      <div class="v" id="kpi-money">—</div>
      <div class="hint" id="rate-chip">—</div>
    </div>
    <div class="kpi glass"><label>Tổng xu DNT</label><div class="v c-dnt" id="kpi-dnt">—</div></div>
    <div class="kpi glass"><label>Tổng xu Thả</label><div class="v c-tha" id="kpi-tha">—</div></div>
  </section>
  <section class="toolbar glass">
    <div class="months" id="months"></div>
    <div class="tools">
      <button class="btn" id="btn-rates">⌛ Tỷ giá</button>
      <button class="btn" id="btn-members">👥 Nhân sự</button>
    </div>
  </section>
  <section class="tablewrap glass"><table id="tbl"></table></section>
  <footer>
    Cú pháp Telegram: <code>k 500 tha</code> • <code>l tha 300 5</code> • <code>k 100 d</code><br/>
    Click vào ô để sửa — tự động lưu &amp; đồng bộ Google Sheets + Redis theo thời gian thực.
  </footer>
</main>

<div class="modal" id="m-rates">
  <div class="box glass">
    <div class="boxhead"><h3>⌛ CÀI ĐẶT MỐC TỶ GIÁ</h3><button class="x" data-close="m-rates">✕</button></div>
    <div id="tier-list"></div>
    <div class="formrow">
      <select id="rt-type"><option value="dnt">Xu DNT</option><option value="tha">Xu Thả</option></select>
      <input id="rt-value" type="number" min="0" placeholder="Giá (đ/xu)"/>
      <div class="fromwrap">áp dụng từ ngày <input id="rt-from" type="number" min="1" max="31" value="1"/></div>
      <button class="btn primary" id="rt-save">Lưu mốc</button>
    </div>
    <div class="note">Mốc tỷ giá áp dụng cho các ngày từ mốc trở đi. VD: Thả 190₫ từ ngày 8 → ngày 1–7 giữ giá cũ.</div>
  </div>
</div>

<div class="modal" id="m-members">
  <div class="box glass">
    <div class="boxhead"><h3>👥 QUẢN LÝ NHÂN SỰ</h3><button class="x" data-close="m-members">✕</button></div>
    <div id="mem-list"></div>
    <div class="note">Đổi tên &amp; alias rồi bấm <b>Lưu</b>. Bấm <b>Xóa</b> để đưa slot về [TRỐNG]. Dữ liệu cũ trong bảng vẫn được giữ đến khi ghi đè.</div>
  </div>
</div>

<div id="toasts"></div>
<script>
const $=s=>document.querySelector(s);
const state={month:null,data:null};
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
function fmt(n){return (Number(n)||0).toLocaleString('vi-VN')}
function money(n){return fmt(Math.round(Number(n)||0))+'₫'}
async function api(path,body){
  const opt=body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{};
  const r=await fetch(path,opt);return r.json();
}
function toast(msg,cls){
  const t=document.createElement('div');t.className='toast'+(cls?' '+cls:'');t.textContent=msg;
  $('#toasts').appendChild(t);setTimeout(()=>t.remove(),3400);
}
function dot(id,on){const el=$(id);el.className='dot '+(on?'on':'off')}
async function load(){
  try{
    const q=state.month?('?month='+state.month):'';
    const d=await api('/api/data'+q);
    if(!d.ok){toast(d.error||'Tải dữ liệu lỗi','err');return}
    if(!state.month)state.month=d.month;
    state.data=d;
    dot('#dot-redis',d.redis);dot('#dot-sheet',d.sheet);
    render();
  }catch(e){toast('Không kết nối được máy chủ','err')}
}
function render(){
  const d=state.data;
  $('#kpi-mlabel').textContent=d.month;
  $('#kpi-money').textContent=money(d.kpi.money);
  $('#kpi-dnt').textContent=fmt(d.kpi.dnt);
  $('#kpi-tha').textContent=fmt(d.kpi.tha);
  const rr=(d.rates&&d.rates[String(d.today)])||{};
  $('#rate-chip').textContent='Giá hôm nay: DNT '+fmt(rr.dnt!=null?rr.dnt:d.default_rates.dnt)+'₫/xu • Thả '+fmt(rr.tha!=null?rr.tha:d.default_rates.tha)+'₫/xu';
  renderMonths();renderTable();
}
function renderMonths(){
  const el=$('#months');el.innerHTML='';
  for(let m=1;m<=12;m++){
    const b=document.createElement('button');
    b.className='chip'+(m===state.month?' on':'');
    b.textContent='Tháng '+m;
    b.onclick=()=>{state.month=m;load()};
    el.appendChild(b);
  }
}
function renderTable(){
  const d=state.data;
  let h='<thead><tr><th rowspan="2" style="text-align:left;padding-left:16px">Ngày</th>';
  d.members.forEach(mm=>{h+='<th colspan="2">'+esc(mm.name)+'</th>'});
  h+='<th>Tiền / Ngày</th></tr><tr>';
  d.members.forEach(()=>{h+='<th class="sub dh">DNT</th><th class="sub th">THẢ</th>'});
  h+='</tr></thead><tbody>';
  for(let day=1;day<=31;day++){
    const isToday=d.is_current&&day===d.today;
    h+='<tr class="'+(isToday?'today':'')+'"><td class="cday">'+day+(isToday?'<span class="badge-nay">NAY</span>':'')+'</td>';
    d.members.forEach((mm,si)=>{
      const rec=(d.days[String(day)]||{})[String(si)]||{};
      const dv=Number(rec.dnt)||0,tv=Number(rec.tha)||0;
      h+='<td class="val '+(dv?'dnt':'zero')+'" data-day="'+day+'" data-slot="'+si+'" data-field="dnt">'+fmt(dv)+'</td>';
      h+='<td class="val '+(tv?'tha':'zero')+'" data-day="'+day+'" data-slot="'+si+'" data-field="tha">'+fmt(tv)+'</td>';
    });
    const dm=Number(d.per_day_money[String(day)])||0;
    h+='<td class="money '+(dm?'':'zero')+'">'+(dm?money(dm):'—')+'</td></tr>';
  }
  h+='</tbody>';
  $('#tbl').innerHTML=h;
}
 $('#tbl').addEventListener('click',e=>{
  const td=e.target.closest('td.val');
  if(td)editCell(td);
});
let editing=false;
function editCell(td){
  if(editing)return;
  editing=true;
  const old=Number(td.textContent.replace(/[^\d-]/g,''))||0;
  const inp=document.createElement('input');
  inp.type='number';inp.className='cellin';inp.value=old||'';
  td.textContent='';td.appendChild(inp);inp.focus();inp.select();
  let done=false;
  const finish=async(save)=>{
    if(done)return;done=true;
    const v=Math.max(0,Math.floor(Number(inp.value)||0));
    if(save&&v!==old){
      const day=Number(td.dataset.day),slot=Number(td.dataset.slot),field=td.dataset.field;
      td.textContent=fmt(v);
      td.classList.remove('zero','dnt','tha');
      td.classList.add(v?(field==='dnt'?'dnt':'tha'):'zero');
      try{
        const res=await api('/api/update_cell',{month:state.month,day:day,slot:slot,field:field,value:v});
        if(res.ok){toast('Đã lưu • đồng bộ Google Sheets ✓')}else{toast(res.error||'Lưu thất bại','err')}
        load();
      }catch(err){toast('Lỗi kết nối khi lưu','err');load();}
    }else{
      td.textContent=fmt(old);
      td.classList.remove('zero','dnt','tha');
      if(old)td.classList.add(td.dataset.field==='dnt'?'dnt':'tha');else td.classList.add('zero');
    }
    editing=false;
  };
  inp.addEventListener('keydown',e=>{
    if(e.key==='Enter'){e.preventDefault();finish(true);}
    else if(e.key==='Escape'){finish(false);}
  });
  inp.addEventListener('blur',()=>finish(true));
}
function openModal(id){document.getElementById(id).classList.add('open')}
function closeModal(id){document.getElementById(id).classList.remove('open')}
document.querySelectorAll('.modal').forEach(m=>{
  m.addEventListener('click',e=>{if(e.target===m)m.classList.remove('open')});
});
document.querySelectorAll('.x').forEach(b=>b.addEventListener('click',()=>closeModal(b.dataset.close)));
function openRates(){
  const d=state.data;
  const tiers=(d.tiers&&d.tiers.length)?d.tiers:[{from:1,dnt:d.default_rates.dnt,tha:d.default_rates.tha}];
  let h='';
  tiers.forEach(t=>{
    h+='<div class="tier"><span class="from">Từ ngày '+t.from+'</span><span>'
      +'<b style="color:var(--gold)">DNT '+fmt(t.dnt)+'₫</b> &nbsp;•&nbsp; <b style="color:var(--green)">Thả '+fmt(t.tha)+'₫</b></span></div>';
  });
  $('#tier-list').innerHTML=h;
  $('#rt-type').value='tha';$('#rt-value').value='';$('#rt-from').value=1;
  openModal('m-rates');
}
 $('#rt-save').onclick=async()=>{
  const value=Number($('#rt-value').value)||0;
  if(value<=0){toast('Nhập giá tiền hợp lệ','err');return}
  const from=Math.min(31,Math.max(1,Number($('#rt-from').value)||1));
  try{
    const res=await api('/api/rates',{month:state.month,type:$('#rt-type').value,value:value,from:from});
    if(res.ok){toast('Đã lưu mốc tỷ giá ✓');closeModal('m-rates');load();}
    else toast(res.error||'Lỗi lưu tỷ giá','err');
  }catch(e){toast('Lỗi kết nối','err')}
};
function openMembers(){
  const box=$('#mem-list');box.innerHTML='';
  state.data.members.forEach((mm,i)=>{
    const row=document.createElement('div');row.className='mem-row';
    row.innerHTML='<span class="slot">'+(i+1)+'</span>'
      +'<input id="mn-'+i+'" value="'+esc(mm.name)+'" placeholder="Tên hiển thị"/>'
      +'<input id="ma-'+i+'" value="'+esc(mm.alias||'')+'" placeholder="Alias (k, dt…)" style="max-width:140px"/>'
      +'<button class="mini save" data-i="'+i+'">Lưu</button>'
      +'<button class="mini del" data-i="'+i+'">Xóa</button>';
    box.appendChild(row);
  });
  box.querySelectorAll('.save').forEach(b=>b.addEventListener('click',async()=>{
    const i=Number(b.dataset.i);
    try{
      const res=await api('/api/members',{action:'rename',slot:i,name:document.getElementById('mn-'+i).value,alias:document.getElementById('ma-'+i).value});
      if(res.ok){toast('Đã cập nhật nhân sự ✓');state.data.members=res.members;openMembers();load();}
      else toast(res.error||'Lỗi','err');
    }catch(e){toast('Lỗi kết nối','err')}
  }));
  box.querySelectorAll('.del').forEach(b=>b.addEventListener('click',async()=>{
    const i=Number(b.dataset.i);
    if(!confirm('Xóa nhân sự slot '+(i+1)+'?'))return;
    try{
      const res=await api('/api/members',{action:'delete',slot:i});
      if(res.ok){toast('Đã xóa nhân sự ✓');state.data.members=res.members;openMembers();load();}
      else toast(res.error||'Lỗi','err');
    }catch(e){toast('Lỗi kết nối','err')}
  }));
  openModal('m-members');
}
 $('#btn-rates').onclick=()=>openRates();
 $('#btn-members').onclick=()=>openMembers();
 $('#btn-sync').onclick=async()=>{
  toast('Đang đẩy dữ liệu lên Google Sheets…');
  try{
    const res=await api('/api/sync',{month:state.month});
    if(res.ok)toast('Đồng bộ hoàn tất ✓');else toast(res.error||'Đồng bộ lỗi','err');
  }catch(e){toast('Lỗi kết nối','err')}
};
load();
setInterval(load,60000);
</script>
</body>
</html>"""

app = Flask(__name__)
try:
    app.json.ensure_ascii = False
except Exception:
    app.config["JSON_AS_ASCII"] = False

@app.errorhandler(Exception)
def on_error(e):
    traceback.print_exc()
    try:
        return jsonify({"ok": False, "error": str(e)}), 500
    except Exception:
        return "internal error", 500

@app.route("/health")
def health():
    return "OK", 200

@app.route("/")
def dashboard():
    return Response(HTML_PAGE, mimetype="text/html")

@app.route("/api/data")
def api_data():
    try:
        now = vnnow()
        y = now.year
        try:
            m = int(request.args.get("month", now.month))
        except Exception:
            m = now.month
        m = max(1, min(12, m))
        members = get_members()
        stats, per_day, kpi = compute_month(y, m)
        return jsonify({
            "ok": True, "year": y, "month": m, "today": now.day,
            "is_current": (m == now.month),
            "members": members,
            "days": get_month_data(y, m),
            "rates": rates_map_month(y, m),
            "tiers": get_tiers(y, m),
            "per_day_money": per_day,
            "stats": stats,
            "kpi": kpi,
            "redis": bool(redis_client is not None and redis_alive),
            "sheet": bool(get_webhook()),
            "default_rates": DEFAULT_RATE,
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/update_cell", methods=["POST"])
def api_update_cell():
    try:
        b = request.get_json(force=True, silent=True) or {}
        now = vnnow()
        y = now.year
        try:
            m = int(b.get("month", now.month))
        except Exception:
            m = now.month
        m = max(1, min(12, m))
        try:
            day = int(b.get("day", 0))
            slot = int(b.get("slot", -1))
            value = int(float(b.get("value", 0) or 0))
        except Exception:
            return jsonify({"ok": False, "error": "dữ liệu không hợp lệ"}), 400
        field = str(b.get("field", ""))
        if field not in ("dnt", "tha"):
            return jsonify({"ok": False, "error": "loại ô không hợp lệ"}), 400
        if not (1 <= day <= 31):
            return jsonify({"ok": False, "error": "ngày không hợp lệ"}), 400
        if not (0 <= slot < 5):
            return jsonify({"ok": False, "error": "slot không hợp lệ"}), 400
        if value < 0:
            return jsonify({"ok": False, "error": "giá trị không được âm"}), 400
        set_record(y, m, day, slot, field, value)
        sheet_update_cell(y, m, day, slot, field, value)
        stats, per_day, kpi = compute_month(y, m)
        return jsonify({"ok": True, "kpi": kpi, "per_day_money": per_day})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/rates", methods=["POST"])
def api_rates():
    try:
        b = request.get_json(force=True, silent=True) or {}
        now = vnnow()
        y = now.year
        try:
            m = int(b.get("month", now.month))
        except Exception:
            m = now.month
        m = max(1, min(12, m))
        ftype = str(b.get("type", ""))
        if ftype not in ("dnt", "tha"):
            return jsonify({"ok": False, "error": "loại xu không hợp lệ"}), 400
        try:
            value = int(float(b.get("value", 0)))
        except Exception:
            value = 0
        try:
            from_day = int(b.get("from", 1))
        except Exception:
            from_day = 1
        from_day = max(1, min(31, from_day))
        if value <= 0:
            return jsonify({"ok": False, "error": "giá tiền phải lớn hơn 0"}), 400
        tiers = set_rate_tier(y, m, from_day, ftype, value)
        register_month(y, m)
        push_month_to_sheet(y, m)
        return jsonify({"ok": True, "tiers": tiers})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/members", methods=["POST"])
def api_members():
    try:
        b = request.get_json(force=True, silent=True) or {}
        action = str(b.get("action", ""))
        members = get_members()
        try:
            slot = int(b.get("slot", -1))
        except Exception:
            slot = -1
        if slot < 0 or slot >= 5:
            return jsonify({"ok": False, "error": "slot không hợp lệ"}), 400
        if action == "rename":
            name = str(b.get("name", "")).strip()
            alias = strip_accents(str(b.get("alias", ""))).lower().replace(" ", "").strip()
            if not name:
                return jsonify({"ok": False, "error": "Tên không được để trống"}), 400
            members[slot]["name"] = name[:40]
            members[slot]["alias"] = alias[:20]
        elif action == "delete":
            members[slot]["name"] = "[TRỐNG_%d]" % (slot + 1)
            members[slot]["alias"] = ""
        else:
            return jsonify({"ok": False, "error": "action không hợp lệ"}), 400
        save_members(members)
        return jsonify({"ok": True, "members": members})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/sync", methods=["POST"])
def api_sync():
    try:
        now = vnnow()
        y, m = now.year, now.month
        b = request.get_json(force=True, silent=True) or {}
        if b.get("month"):
            try:
                mm = int(b.get("month"))
                if 1 <= mm <= 12:
                    m = mm
            except Exception:
                pass
        register_month(y, m)
        res = push_month_to_sheet(y, m)
        okk = isinstance(res, dict) and res.get("ok")
        return jsonify({"ok": bool(okk), "result": res if isinstance(res, dict) else {}})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/status")
def api_status():
    return jsonify({
        "ok": True,
        "redis": bool(redis_client is not None and redis_alive),
        "redis_err": redis_last_err,
        "local_keys": len(local_store),
        "sheet": bool(get_webhook()),
        "bot": BOT_READY,
        "months": db_get(K_MONTHS, []) or [],
    })

# ------------------------------------------------------------
# 11. KHỞI ĐỘNG
# ------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("Khởi động HỆ THỐNG QUẢN LÝ XU TIKTOK (GMT+%g)", TZ_OFFSET)
    log.info("=" * 60)
    bootstrap_storage()
    setup_bot()
    threading.Thread(target=bot_loop, name="telegram-bot", daemon=True).start()
    threading.Thread(target=scheduler_loop, name="scheduler-2359", daemon=True).start()
    if SHEET_POLL > 0:
        threading.Thread(target=sheet_reconcile_loop, name="sheet-2way", daemon=True).start()
    log.info("Web Dashboard: http://0.0.0.0:%s — Health-check: /health", PORT)
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False, use_reloader=False)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.critical("Tiến trình chính gặp lỗi nghiêm trọng:\n%s", traceback.format_exc())
        raise