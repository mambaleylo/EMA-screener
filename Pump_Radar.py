#!/usr/bin/env python3
"""
Pump Radar v0.1.0 (fork of EMA Invert Experiment v0.1.10, itself a fork of
EMA Bounce Dossier v3.6.14 / SMC Optimizer v3.52.96)
- Отдельный, самостоятельный проект-песочница. Взят как копия рабочего
  EMA_Invert_Experiment.py v0.1.10 (весь EMA-инверт движок ниже перенесён
  1:1, без изменений в логике входа/SL/TP/time-stop/диагностики) + добавлен
  независимый Live Pump Detector (см. секцию "Live Pump Detector" ниже —
  детектит резкий рост цены за короткое окно и шлёт в Telegram картинку в
  стиле стороннего скринера: чёрная линия цены с точками, красно-зелёная
  гистограмма объёма, сплошная красная база, синяя штрихованная сетка).
  Полная история изменений (SMC Optimizer v1.0 → v3.52.96 → EMA Bounce
  Dossier v1.0 → v3.6.14 → EMA Invert Experiment v0.1.0 → v0.1.10) — в
  докстринге EMA_Invert_Experiment.py, здесь сознательно не дублируется
  построчно, чтобы не раздувать файл историей другого проекта; сама логика
  унаследована без потерь, только докстринг сокращён.
  Независимая идентичность на случай, если этот файл и оригинальный
  EMA_Invert_Experiment.py (PORT 8766) когда-нибудь понадобится гонять
  ОДНОВРЕМЕННО на одном устройстве: свой PORT (8767) и свои файлы
  состояния/логов/диагностики (префикс "pumpradar_" вместо "ema_invert_").
  Telegram/ntfy и Gate.io ключи по-прежнему в общих ~/.smc_alert_cfg.json /
  ~/.smc_gate_cfg.json — это осознанно (одни и те же реквизиты для всех
  ботов на устройстве, как и было в родительском проекте), не отдельные.
  Дальше этот файл — открытая песочница: логику EMA-инверта можно смело
  резать/переделывать/выключать, ничего в оригинальном EMA_Invert_
  Experiment.py от этого не пострадает.
"""
import os, sys, json, time, math, random, threading, base64, hashlib, subprocess, io, gc
import multiprocessing
import http.server, urllib.request, urllib.parse
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait as _fw, as_completed as _as_completed
import sys as _sys
# python3.14t (free-threaded, no GIL) не поддерживает ProcessPoolExecutor
if hasattr(_sys, '_is_gil_enabled') and not _sys._is_gil_enabled():
    _PoolExecutor = ThreadPoolExecutor
    _POOL_TYPE = "thread"
else:
    _PoolExecutor = ProcessPoolExecutor
    _POOL_TYPE = "process"

try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests -q")
    import requests

APP_VERSION  = "0.1.0"

# ── Проверка консистентности версии (защита от забытого обновления) ──────────
def _check_version():
    import re as _re
    _src = open(__file__).read()
    _m1  = _re.search(r'APP_VERSION\s+=\s+"([^"]+)"', _src)
    _m2  = _re.search(r'Pump Radar v([\d.]+)', _src)
    if not _m1 or not _m2:
        raise RuntimeError("VERSION CHECK: не найдены строки версии в файле!")
    if _m1.group(1) != _m2.group(1):
        raise RuntimeError(
            f"VERSION MISMATCH: APP_VERSION={_m1.group(1)} "
            f"но docstring={_m2.group(1)} — обновите оба!"
        )
_check_version()
# ─────────────────────────────────────────────────────────────────────────────

GATE_API     = "https://api.gateio.ws/api/v4"
# автоопределение по числу ядер устройства (одно ядро оставляем ОС/интерфейсу)
_cpu_count   = multiprocessing.cpu_count() or 2
_env_workers = os.environ.get("SMC_NUM_WORKERS")
NUM_WORKERS  = max(1, int(_env_workers)) if _env_workers else max(1, _cpu_count - 1)
_ema_dossier_lock = threading.Lock()
_ema_dossier_running = {"v": False}

PORT         = 8767   # отдельный порт — можно держать запущенным одновременно
                       # с EMA_Invert_Experiment.py (8766) и оригинальным
                       # EMA-screener (8765) без коллизий
TG_TOKEN     = os.environ.get("TG_TOKEN", "")
TG_CHAT      = os.environ.get("TG_CHAT", "")
NTFY_URL     = os.environ.get("NTFY_URL", "")
WATCHDOG_ENABLED      = True   # увед. в TG/ntfy, если пропал интернет
WATCHDOG_TIMEOUT_MIN  = 60     # порог простоя (мин) до первого алерта
HC_URL       = os.environ.get("HC_URL", "")  # heartbeat healthchecks.io
ALERT_CFG_PATH   = os.path.expanduser("~/.smc_alert_cfg.json")
GATE_CFG_PATH    = os.path.expanduser("~/.smc_gate_cfg.json")
GATE_KEY         = os.environ.get("GATE_KEY", "")
GATE_SECRET      = os.environ.get("GATE_SECRET", "")

_C_GRN = "\033[92m"; _C_YEL = "\033[93m"; _C_RED = "\033[91m"
_C_GREY = "\033[90m"; _C_RST = "\033[0m"

TF_SECONDS = {
    "1m":60,"5m":300,"15m":900,"30m":1800,
    "1h":3600,"4h":14400,"1d":86400
}

# ─── Глобальное состояние ───────────────────────────────────────────────────
opt_lock   = threading.Lock()
_log_lock  = threading.Lock()  # отдельный лок для olog

opt_state = {"logs": [], "logs_dropped": 0}

import hmac, hashlib, urllib.parse as _uparse

def _gate_req(method, path, params=None, body=None, _retries=3, _retry_delay=2.0):
    """Подписанный запрос к Gate.io Futures USDT API.
    При сетевых ошибках (timeout, connection) делает до _retries попыток."""
    if not GATE_KEY or not GATE_SECRET:
        raise RuntimeError("Gate.io ключи не настроены")
    import hashlib as _hl
    query_str = _uparse.urlencode(params) if params else ""
    body_str  = json.dumps(body) if body else ""
    url_path  = f"/api/v4{path}"
    body_hash = _hl.sha512(body_str.encode()).hexdigest()
    ts = str(int(time.time()))
    msg = "\n".join([method, url_path, query_str, body_hash, ts])
    sig = hmac.new(GATE_SECRET.encode(), msg.encode(), _hl.sha512).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "KEY":       GATE_KEY,
        "Timestamp": ts,
        "SIGN":      sig,
    }
    url = f"https://fx-api.gateio.ws{url_path}"
    if query_str: url += "?" + query_str

    last_exc = None
    for attempt in range(1, _retries + 1):
        try:
            r = requests.request(method, url, headers=headers,
                                 data=body_str if body_str else None, timeout=10)
            if r.status_code == 429:
                last_exc = RuntimeError(f"Gate {method} {path} → 429: {r.text[:200]}")
                if attempt < _retries:
                    olog(f"⚠ Gate 429 rate limit (попытка {attempt}/{_retries}) — повтор через {_retry_delay*2}с")
                    time.sleep(_retry_delay * 2)
                    continue
                raise last_exc
            if not r.ok:
                raise RuntimeError(f"Gate {method} {path} → {r.status_code}: {r.text[:200]}")
            return r.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ReadTimeout) as e:
            last_exc = e
            if attempt < _retries:
                olog(f"⚠ Gate сеть (попытка {attempt}/{_retries}): {e} — повтор через {_retry_delay}с")
                time.sleep(_retry_delay)
        except RuntimeError:
            raise
        except Exception as e:
            last_exc = e
            if attempt < _retries:
                olog(f"⚠ Gate ошибка (попытка {attempt}/{_retries}): {e} — повтор через {_retry_delay}с")
                time.sleep(_retry_delay)
    raise last_exc

def _gate_get_position(symbol):
    """Текущая позиция по символу (None если ПОДТВЕРЖДЁННО нет)."""
    try:
        data = _gate_req("GET", f"/futures/usdt/positions/{symbol}")
    except RuntimeError as e:
        if "POSITION_NOT_FOUND" in str(e):
            return None
        raise
    size = float(data.get("size", 0))
    if size == 0: return None
    return {
        "dir":    "long" if size > 0 else "short",
        "size":   abs(size),
        "entry":  float(data.get("entry_price", 0)),
    }

def _gate_get_price(symbol):
    """Текущая цена фьючерса (mark price)."""
    try:
        data = requests.get(f"{GATE_API}/futures/usdt/contracts/{symbol}", timeout=5).json()
        return float(data.get("mark_price") or data.get("last_price", 0))
    except:
        return 0.0

def _gate_get_quanto(symbol):
    """Размер одного контракта в USDT (quanto_multiplier)."""
    return _gate_get_contract_info(symbol)["quanto_multiplier"]

_gate_contract_cache      = {}
_gate_contract_cache_lock = threading.Lock()

def _gate_get_contract_info(symbol, force_refresh=False):
    """Спецификация контракта Gate.io (quanto_multiplier, шаг цены
    order_price_round) с кэшем."""
    contract = symbol.replace("/", "_").upper()
    if not force_refresh:
        with _gate_contract_cache_lock:
            cached = _gate_contract_cache.get(contract)
        if cached:
            return cached
    r = requests.get(f"{GATE_API}/futures/usdt/contracts/{contract}", timeout=5)
    if r.status_code != 200:
        raise RuntimeError(f"Gate.io contracts/{contract} → {r.status_code}: {r.text[:200]}")
    data = r.json()
    if not isinstance(data, dict) or "quanto_multiplier" not in data:
        raise RuntimeError(f"Gate.io contracts/{contract}: неожиданный ответ {data}")
    info = {
        "quanto_multiplier": float(data["quanto_multiplier"]),
        "order_price_round": str(data.get("order_price_round", "0.01")),
    }
    with _gate_contract_cache_lock:
        _gate_contract_cache[contract] = info
    return info

def _gate_get_balance():
    """Свободный баланс USDT (для расчёта размера новой позиции)."""
    try:
        data = _gate_req("GET", "/futures/usdt/accounts")
        cross_bal = data.get("cross_margin_balance")
        avail     = data.get("available")
        try:
            cross_bal = float(cross_bal) if cross_bal not in (None, "") else None
        except (TypeError, ValueError):
            cross_bal = None
        try:
            avail = float(avail) if avail not in (None, "") else 0.0
        except (TypeError, ValueError):
            avail = 0.0
        olog(f"🔍 gate_balance: margin_mode={data.get('margin_mode')} "
             f"available={avail} cross_margin_balance={cross_bal}")
        if cross_bal is not None and cross_bal > 0:
            return cross_bal
        return avail
    except Exception as e:
        olog(f"⚠ gate_get_balance: {e}")
        return 0.0

def _gate_get_equity():
    """Баланс аккаунта С УЧЁТОМ открытой позиции (для отображения в UI/AMOLED)."""
    try:
        data = _gate_req("GET", "/futures/usdt/accounts")
        def _f(key):
            v = data.get(key)
            if v in (None, ""):
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None
        cross_bal = _f("cross_margin_balance")
        total     = _f("total")
        upnl      = _f("unrealised_pnl") or 0.0
        avail     = _f("available")
        olog(f"🔍 gate_equity: margin_mode={data.get('margin_mode')} "
             f"enable_credit={data.get('enable_credit')} total={total} "
             f"available={avail} unrealised_pnl={upnl} cross_margin_balance={cross_bal}")
        if cross_bal is not None and cross_bal > 0:
            return cross_bal
        if total is not None:
            return total + upnl
        return avail
    except Exception as e:
        olog(f"⚠ gate_equity: {e}")
        return None

def _gate_cancel_orders(symbol):
    """Отменить все открытые ордера по символу — лимитные/рыночные и триггерные."""
    contract = symbol.replace("/", "_").upper()
    try:
        _gate_req("DELETE", "/futures/usdt/orders",
                  params={"contract": contract, "status": "open"})
    except Exception as e:
        olog(f"⚠ gate_cancel_orders (orders) {contract}: {e}")
    try:
        _gate_req("DELETE", "/futures/usdt/price_orders",
                  params={"contract": contract, "status": "open"})
    except Exception as e:
        olog(f"⚠ gate_cancel_orders (price_orders) {contract}: {e}")

def _gate_get_pnl_from_account_book(symbol, since_ts, until_ts=None, max_lookback_sec=3600):
    """Fallback, когда _gate_get_last_pnl_from_position_close не смог
    сматчить закрытие. Использует /futures/usdt/account_book — общий леджер
    счёта (изменения баланса: pnl/комиссии/фандинг) как ВТОРОЙ, независимый
    от /position_close источник результата закрытия."""
    contract = symbol.replace("/", "_").upper()
    until_ts = until_ts or (since_ts + max_lookback_sec)
    try:
        entries = _gate_req("GET", "/futures/usdt/account_book", params={
            "contract": contract,
            "from": int(since_ts) - 30,
            "to":   int(until_ts) + 30,
            "type": "pnl",
            "limit": 50,
        })
    except Exception as e:
        olog(f"[gate_pnl_fallback] {symbol}: account_book запрос не удался: {e}")
        return None
    if not isinstance(entries, list) or not entries:
        return None
    matched = [e for e in entries if contract in str(e.get("text", ""))] or entries
    if not matched:
        return None
    total = 0.0
    for e in matched:
        try:
            total += float(e.get("change", 0))
        except (TypeError, ValueError):
            continue
    olog(f"[gate_pnl_fallback] {symbol}: {len(matched)} записей в account_book, "
         f"суммарный pnl={total:.4f} (использован как fallback — /position_close не дал результата)")
    return {"pnl": total, "pnl_pct": None, "close_price": None,
            "pnl_fee": None, "pnl_fund": None, "pnl_price": None,
            "source": "account_book_fallback"}

def _gate_get_last_pnl(symbol, max_age_sec=1800, fallback_since_ts=None):
    """Тонкая обёртка. Основная логика — в
    _gate_get_last_pnl_from_position_close. Если она вернула None и передан
    fallback_since_ts — пробуем _gate_get_pnl_from_account_book как второй
    источник, вместо того чтобы молча терять $-результат сделки."""
    result = _gate_get_last_pnl_from_position_close(symbol, max_age_sec)
    if result is not None:
        return result
    if fallback_since_ts:
        return _gate_get_pnl_from_account_book(symbol, fallback_since_ts)
    return None

def _gate_get_last_pnl_from_position_close(symbol, max_age_sec=1800):
    """Возвращает PnL последней закрытой позиции по символу (USDT).
    Проверяет rec["time"] — если запись закрытия старше max_age_sec от
    момента вызова, считаем её НЕ нашей и возвращаем None вместо того,
    чтобы приписать чужой PnL."""
    try:
        contract = symbol.replace("/", "_").upper()
        data = _gate_req("GET", "/futures/usdt/position_close",
                         params={"contract": contract, "limit": 1})
        if not isinstance(data, list) or not data:
            return None
        rec = data[0]
        rec_time = float(rec.get("time", 0) or 0)
        age = time.time() - rec_time
        if rec_time <= 0 or age > max_age_sec:
            olog(f"⚠ gate_get_last_pnl({symbol}): последняя запись закрытия "
                 f"старше {max_age_sec}с (age={age:.0f}с) — похоже на ЧУЖОЕ "
                 f"закрытие (старая ручная сделка/прошлая сессия), а не "
                 f"наше только что закрытое — игнорирую, PnL не приписываю")
            return None
        pnl       = float(rec.get("pnl",       0))
        pnl_fee   = float(rec.get("pnl_fee",   0) or 0)
        pnl_fund  = float(rec.get("pnl_fund",  0) or 0)
        pnl_price = float(rec.get("pnl_pnl",   0) or 0)
        olog(f"[gate_pnl] {symbol}: pnl={pnl:.4f} (price={pnl_price:.4f} "
             f"fee={pnl_fee:.4f} fund={pnl_fund:.4f} сумма_частей="
             f"{pnl_price+pnl_fee+pnl_fund:.4f})")
        side      = rec.get("side")
        long_px   = float(rec.get("long_price", 0) or 0)
        short_px  = float(rec.get("short_price", 0) or 0)
        if side == "long":
            entry_px, close_px = long_px, short_px
            pnl_pct = (close_px - entry_px) / entry_px * 100.0 if entry_px > 0 else 0.0
        else:
            entry_px, close_px = short_px, long_px
            pnl_pct = (entry_px - close_px) / entry_px * 100.0 if entry_px > 0 else 0.0
        return {"pnl": pnl, "pnl_pct": pnl_pct, "close_price": close_px,
                "pnl_fee": pnl_fee, "pnl_fund": pnl_fund, "pnl_price": pnl_price}
    except Exception as e:
        olog(f"⚠ gate_get_last_pnl: {e}")
        return None


def _check_position_closed_and_alert(sym, our_pos_now):
    """Проверяет, не закрылась ли отслеживаемая позиция на бирже; если
    закрылась — шлёт алерт с реальным PnL."""
    try:
        still_open = _gate_get_position(sym)
    except Exception as e:
        olog(f"⚠ Не удалось проверить статус позиции {sym} ({e}) — "
             f"считаем ещё открытой, проверим на следующем тике")
        return False
    if still_open:
        return False
    dirru = our_pos_now.get("dir", "?").upper()
    entry = our_pos_now.get("entry", 0)
    olog(f"✅ Позиция {dirru} закрыта на бирже (TP/SL или вручную)")
    pnl_info = _gate_get_last_pnl(sym)
    if pnl_info:
        pnl     = pnl_info["pnl"]
        pnl_pct = pnl_info["pnl_pct"]
        close_p = pnl_info["close_price"]
        sign    = "+" if pnl >= 0 else ""
        result_emoji = "✅" if pnl >= 0 else "❌"
        _send_alert(
            f"{result_emoji} <b>{sym}</b> — позиция {dirru} закрыта\n"
            f"Entry {_fmt_px(entry)} → Close {_fmt_px(close_p)}\n"
            f"PnL: <b>{sign}{pnl:.2f} USDT ({sign}{pnl_pct:.2f}%)</b>"
        )
    else:
        _send_alert(
            f"{'🟢' if dirru=='LONG' else '🔴'} <b>{sym}</b> — "
            f"позиция {dirru} закрыта\n"
            f"TP/SL сработал или закрыта вручную. Жду следующего сигнала."
        )
    return True

def _gate_close_position(symbol):
    """Закрыть позицию рыночным ордером."""
    try:
        contract = symbol.replace("/", "_").upper()
        pos = _gate_get_position(contract)
        if not pos: return
        _gate_cancel_orders(contract)
        close_size = -int(pos["size"]) if pos["dir"] == "long" else int(pos["size"])
        _gate_req("POST", "/futures/usdt/orders", body={
            "contract":    contract,
            "size":        close_size,
            "price":       "0",
            "tif":         "ioc",
            "reduce_only": True,
            "text":        "t-smc-close",
        })
        olog(f"📤 Позиция закрыта: {contract} {pos['dir']}")
    except Exception as e:
        olog(f"⚠ gate_close_position {symbol}: {e}")

def _gate_round_price(price, contract):
    """Округляет цену TP/SL до реального шага цены (order_price_round)
    контракта на Gate.io."""
    try:
        info = _gate_get_contract_info(contract)
        tick_str = info.get("order_price_round", "0.01")
        tick = float(tick_str)
        decimals = len(tick_str.split(".")[1]) if "." in tick_str else 0
        rounded = round(round(price / tick) * tick, decimals)
        return f"{rounded:.{decimals}f}"
    except Exception as e:
        olog(f"⚠ _gate_round_price: не удалось получить шаг цены для {contract} "
             f"({e}) — использую грубое округление по диапазону цены")
        if price > 1000:  return f"{price:.1f}"
        if price > 10:    return f"{price:.2f}"
        if price > 1:     return f"{price:.4f}"
        return f"{price:.6f}"

SL_SLIPPAGE_BUFFER_PCT = 0.3

def _gate_place_protective_orders(symbol, direction, close_size, sl_px, tp_px, text_prefix="smc"):
    """Выставляет TP/SL price_orders (reduce_only) для уже открытой позиции."""
    contract = symbol.replace("/", "_").upper()
    is_long = (direction == "long")
    _gate_req("POST", "/futures/usdt/price_orders", body={
        "initial": {
            "contract":    contract,
            "size":        close_size,
            "price":       "0",
            "tif":         "ioc",
            "reduce_only": True,
            "text":        f"t-{text_prefix}-tp",
        },
        "trigger": {
            "strategy_type": 0,
            "price_type":    0,
            "price":         _gate_round_price(tp_px, contract),
            "rule":          1 if is_long else 2,
            "expiration":    86400,
        },
    })
    sl_limit_px = sl_px * (1 - SL_SLIPPAGE_BUFFER_PCT/100) if is_long else sl_px * (1 + SL_SLIPPAGE_BUFFER_PCT/100)
    _gate_req("POST", "/futures/usdt/price_orders", body={
        "initial": {
            "contract":    contract,
            "size":        close_size,
            "price":       _gate_round_price(sl_limit_px, contract),
            "tif":         "gtc",
            "reduce_only": True,
            "text":        f"t-{text_prefix}-sl",
        },
        "trigger": {
            "strategy_type": 0,
            "price_type":    0,
            "price":         _gate_round_price(sl_px, contract),
            "rule":          2 if is_long else 1,
            "expiration":    86400,
        },
    })

def _gate_has_protective_orders(symbol, text_prefix):
    """Смотрит открытые price_orders по контракту и проверяет, есть ли
    среди них НАШИ TP/SL (по тегу t-{prefix}-tp/-sl)."""
    contract = symbol.replace("/", "_").upper()
    try:
        r = _gate_req("GET", "/futures/usdt/price_orders",
                       params={"contract": contract, "status": "open"})
        orders = r if isinstance(r, list) else []
    except Exception as e:
        olog(f"⚠ _gate_has_protective_orders {symbol}: {e}")
        return None
    tags = {(o.get("initial") or {}).get("text", "") for o in orders}
    return (f"t-{text_prefix}-tp" in tags, f"t-{text_prefix}-sl" in tags)

def _ema_rearm_missing_protection():
    """Watchdog — для каждого live-сигнала, у которого позиция на бирже
    РЕАЛЬНО ещё открыта, проверяет, что там же висят наши TP/SL. Если нет —
    позиция голая; перевыставляет TP/SL по сохранённым в истории sl/tp."""
    with _ema_history_lock:
        state = _load_ema_history()
    live_open = [(k, v) for k, v in state["items"].items()
                 if v.get("status") == "open" and v.get("live")]
    for key, item in live_open:
        symbol = item["symbol"]
        try:
            pos = _gate_get_position(symbol.replace("/", "_").upper())
        except Exception:
            continue
        if not pos:
            continue
        check = _gate_has_protective_orders(symbol, "emainv")
        if check is None:
            continue
        has_tp, has_sl = check
        if has_tp and has_sl:
            continue
        olog(f"[ema_rearm] ⚠ {symbol}: позиция открыта, но TP/SL отсутствуют "
             f"(has_tp={has_tp} has_sl={has_sl}) — перевыставляю по item sl/tp")
        close_size = -int(pos["size"]) if pos["dir"] == "long" else int(pos["size"])
        try:
            _gate_place_protective_orders(symbol, pos["dir"], close_size,
                                           item["sl"], item["tp"], text_prefix="emainv")
            olog(f"[ema_rearm] ✓ {symbol}: TP/SL перевыставлены")
            _send_alert(f"🛡 <b>{symbol} EMA INVERT</b> — TP/SL отсутствовали на голой "
                        f"позиции, перевыставил автоматически "
                        f"(SL {_fmt_px(item['sl'])} / TP {_fmt_px(item['tp'])})")
        except Exception as e:
            olog(f"[ema_rearm] 🚨 {symbol}: не смог перевыставить TP/SL: {e}")
            _send_alert(f"🚨 <b>{symbol} EMA INVERT</b> — позиция ГОЛАЯ (без TP/SL), "
                        f"автоперевыставление не удалось: {e}\nВЫСТАВЬ ВРУЧНУЮ!")

EMA_INVERT_TIME_STOP_BARS = {"1m": 6, "5m": 6, "15m": 5, "1h": 3}
EMA_INVERT_TIME_STOP_DEFAULT_BARS = 5
EMA_INVERT_SAFETY_RR = 1.1
EMA_INVERT_MAX_VOL_RATIO = 1.5
EMA_INVERT_REJECT_CONFIRMED_PATTERN = True
_ema_invert_filter_stats = {"vol_ratio": 0, "candle_pattern": 0}

def _ema_invert_time_limit_sec(tf):
    bars = EMA_INVERT_TIME_STOP_BARS.get(tf, EMA_INVERT_TIME_STOP_BARS.get(
        "1h" if tf in ("4h", "1d", "1w") else tf, EMA_INVERT_TIME_STOP_DEFAULT_BARS))
    return bars * TF_SECONDS.get(tf, 3600)

def _ema_invert_timestop_watchdog():
    """Раз в цикл проверяет все реально живые сигналы этого форка — если с
    момента открытия прошло больше time_limit_sec, закрывает позицию по
    рынку и сам помечает историю status='time_stop'."""
    with _ema_history_lock:
        state = _load_ema_history()
    live_open = [(k, v) for k, v in state["items"].items()
                 if v.get("status") == "open" and v.get("live")]
    now = time.time()
    for key, item in live_open:
        symbol = item["symbol"]
        opened_at = item.get("opened_at")
        limit_sec = item.get("time_limit_sec") or _ema_invert_time_limit_sec(item["tf"])
        if opened_at is None or now - opened_at < limit_sec:
            continue
        contract = symbol.replace("/", "_").upper()
        try:
            pos = _gate_get_position(contract)
        except Exception as e:
            olog(f"[ema_invert_timestop] {symbol}: не смог проверить биржу ({e}) — пропуск")
            continue
        if not pos:
            continue
        live_price = _gate_get_price(symbol)
        try:
            _gate_close_position(symbol)
        except Exception as e:
            olog(f"[ema_invert_timestop] 🚨 {symbol}: не удалось закрыть по времени: {e}")
            _ema_event_log_write("timestop_close_failed", symbol=symbol, error=str(e))
            continue
        held_sec = now - opened_at
        olog(f"[ema_invert_timestop] ✓ {symbol}: время вышло ({held_sec:.0f}с "
             f">= {limit_sec:.0f}с лимита) — закрыто по рынку")
        pnl_info = None
        try:
            pnl_info = _gate_get_last_pnl(symbol, fallback_since_ts=opened_at)
        except Exception as e:
            olog(f"[ema_invert_timestop] {symbol}: не смог получить PnL закрытия ({e})")
        _ema_event_log_write("timestop_closed", symbol=symbol, held_sec=round(held_sec, 1),
                              limit_sec=limit_sec, tf=item["tf"],
                              pnl=pnl_info["pnl"] if pnl_info else None)
        _send_alert(f"⏱ <b>{symbol} EMA INVERT</b> — вышло время ({held_sec:.0f}с), "
                    f"закрыто по рынку")
        with _ema_history_lock:
            state2 = _load_ema_history()
            it2 = state2["items"].get(key)
            if it2:
                it2["status"] = "time_stop"
                it2["closed_at"] = int(now)
                it2["close_price"] = (pnl_info["close_price"] if pnl_info else live_price) or it2.get("price")
                it2["closed_externally"] = False
                if pnl_info:
                    it2["live_pnl"]     = pnl_info["pnl"]
                    it2["live_pnl_pct"] = pnl_info["pnl_pct"]
                    it2["live_pnl_fee"]   = pnl_info.get("pnl_fee")
                    it2["live_pnl_fund"]  = pnl_info.get("pnl_fund")
                    it2["live_pnl_price"] = pnl_info.get("pnl_price")
                it2["diag_status"] = "pending"
                _save_ema_history(state2)
                _ema_invert_diag_schedule(it2)

EMA_INVERT_DIAG_FILE = os.path.expanduser("~/pumpradar_invert_diagnostics.jsonl")
EMA_INVERT_DIAG_WAIT_BARS = 10
_ema_invert_diag_lock = threading.Lock()

def _ema_invert_diag_log_write(record):
    try:
        with _ema_invert_diag_lock:
            with open(EMA_INVERT_DIAG_FILE, "a") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        olog(f"[ema_invert_diag] ошибка записи лога: {e}")

def _ema_invert_diag_schedule(item):
    item["diag_status"] = "pending"

def _ema_invert_diagnose_one(item):
    """Разбирает одну ЗАКРЫТУЮ (любым исходом) сделку инвертированной
    логики."""
    symbol, tf, ema_period = item["symbol"], item["tf"], item["ema_period"]
    direction = item["dir"]
    entry, tp, safety_sl = item["price"], item["tp"], item.get("sl")
    exit_reason = item["status"]
    exit_price  = item.get("close_price")
    opened_at, closed_at = item["opened_at"], item["closed_at"]
    fetch_tf = "1d" if tf == "1w" else tf
    span_sec = max(1, int(time.time()) - closed_at)
    days = math.ceil((int(time.time()) - opened_at) / 86400) + EMA_DIAG_LOOKBACK_PAD
    raw = _fetch_candles(symbol, fetch_tf, days)
    candles = _resample_to_weekly(raw) if tf == "1w" else raw
    if not candles:
        return None
    during = [c for c in candles if opened_at <= c["t"] < closed_at]
    after  = [c for c in candles if c["t"] >= closed_at]
    tf_sec = TF_SECONDS.get(fetch_tf, 3600)
    if len(after) < EMA_INVERT_DIAG_WAIT_BARS and \
       (int(time.time()) - closed_at) < EMA_INVERT_DIAG_WAIT_BARS * tf_sec:
        return None
    risk_tp = abs(entry - tp)
    mfe_toward_tp = 0.0
    if during:
        if direction == "long":
            mfe_toward_tp = max([c["high"] for c in during], default=entry) - entry
        else:
            mfe_toward_tp = entry - min([c["low"] for c in during], default=entry)
    mfe_r = round(mfe_toward_tp / risk_tp, 3) if risk_tp else None
    post_would_hit_tp = None
    if exit_reason == "time_stop" and after:
        window = after[:EMA_INVERT_DIAG_WAIT_BARS]
        if direction == "long":
            post_would_hit_tp = max(c["high"] for c in window) >= tp
        else:
            post_would_hit_tp = min(c["low"] for c in window) <= tp
    held_sec = closed_at - opened_at
    bars_held = round(held_sec / tf_sec, 2) if tf_sec else None
    pnl_pct = None
    if exit_price and entry:
        raw_pnl = (exit_price - entry) / entry if direction == "long" else (entry - exit_price) / entry
        pnl_pct = round(raw_pnl * 100.0, 4)
    record = {
        "ts": int(time.time()), "symbol": symbol, "tf": tf,
        "ema_period": ema_period, "dir": direction,
        "entry": entry, "tp": tp, "safety_sl": safety_sl,
        "exit_reason": exit_reason, "exit_price": exit_price,
        "held_sec": round(held_sec, 1), "bars_held": bars_held,
        "time_limit_sec": item.get("time_limit_sec") or _ema_invert_time_limit_sec(tf),
        "mfe_r_to_tp": mfe_r,
        "post_timestop_would_hit_tp": post_would_hit_tp,
        "pnl_pct": pnl_pct,
        "dist_atr_at_entry": item.get("dist_atr"), "ladder_at_entry": item.get("ladder"),
        "rsi_at_entry": item.get("rsi"), "adx_at_entry": item.get("adx"),
        "plus_di_at_entry": item.get("plus_di"), "minus_di_at_entry": item.get("minus_di"),
        "vol_ratio_at_entry": item.get("vol_ratio"),
        "atr_percentile_at_entry": item.get("atr_percentile"),
        "ribbon_spread_atr_at_entry": item.get("ribbon_spread_atr"),
        "candle_pattern_at_entry": item.get("candle_pattern"),
        "session_at_entry": item.get("session"),
        "htf_trend_at_entry": item.get("htf_trend"),
    }
    return record

def _ema_invert_run_diagnostics():
    """Аналог _ema_run_diagnostics для ЛЮБОГО исхода (tp/sl/time_stop)
    закрытых сделок этого форка."""
    with _ema_history_lock:
        state = _load_ema_history()
    pending = [(k, v) for k, v in state["items"].items()
               if v.get("status") in ("tp", "sl", "time_stop")
               and v.get("diag_status") == "pending"]
    if not pending:
        return
    done = {}
    for key, item in pending:
        try:
            record = _ema_invert_diagnose_one(item)
        except Exception as e:
            olog(f"[ema_invert_diag] {item.get('symbol')} ошибка разбора: {e}")
            continue
        if record is None:
            continue
        _ema_invert_diag_log_write(record)
        olog(f"[ema_invert_diag] {record['symbol']} {record['tf']} EMA{record['ema_period']} "
             f"{record['dir']} closed={record['exit_reason']} held={record['bars_held']}bar "
             f"pnl={record['pnl_pct']}%")
        item["diag_status"] = "done"
        done[key] = item
    if done:
        with _ema_history_lock:
            state2 = _load_ema_history()
            for key, item in done.items():
                state2["items"][key] = item
            _save_ema_history(state2)

def _gate_open_position(symbol, direction, entry_px, sl_px, tp_px, risk_pct, **kwargs):
    """
    Полный цикл открытия позиции:
      leverage = round(risk_pct / sl_pct)
      margin   = balance * position_pct%
      size     = (margin * leverage) / (entry_px * qm)
      TP/SL    — price_orders с price="0" (маркет при триггере)
    """
    try:
        sl_pct_val   = abs(entry_px - sl_px) / entry_px * 100.0
        position_pct = kwargs.get("position_pct", risk_pct)
        label       = kwargs.get("label", "SMC AUTO")
        text_prefix = kwargs.get("text_prefix", "smc")

        balance = _gate_get_balance()
        if not balance or balance <= 0:
            raise RuntimeError(f"Нет баланса: {balance}")

        leverage_raw = round(risk_pct / sl_pct_val)
        leverage = max(1, min(MAX_LEVERAGE, leverage_raw))
        if leverage_raw > MAX_LEVERAGE:
            olog(f"⚠ [{symbol}] расчётное плечо {leverage_raw}× превышает "
                 f"MAX_LEVERAGE={MAX_LEVERAGE} (risk_pct={risk_pct}%, "
                 f"sl_pct={sl_pct_val:.4f}%) — ограничено до {leverage}×")

        applied_leverage = leverage
        contract = symbol.replace("/", "_").upper()
        try:
            r = _gate_req("POST", f"/futures/usdt/positions/{contract}/leverage",
                          params={"leverage": str(leverage)})
            applied_leverage = int(r.get("leverage", leverage)) if isinstance(r, dict) else leverage
            olog(f"✓ Плечо: {applied_leverage}×")
        except Exception as e:
            olog(f"⚠ Плечо попытка 1: {e} — повтор через 1с")
            time.sleep(1.0)
            try:
                r = _gate_req("POST", f"/futures/usdt/positions/{contract}/leverage",
                              params={"leverage": str(leverage)})
                applied_leverage = int(r.get("leverage", leverage)) if isinstance(r, dict) else leverage
                olog(f"✓ Плечо (попытка 2): {applied_leverage}×")
            except Exception as e2:
                olog(f"⚠ Плечо не применено: {e2} — используем leverage=1")
                applied_leverage = 1

        qm = _gate_get_quanto(symbol)
        margin   = balance * (position_pct / 100.0)
        size_raw = (margin * applied_leverage) / (entry_px * qm)
        size     = round(size_raw)
        forced_min_size = False
        if size < 1:
            forced_size_max_multiple = kwargs.get("forced_size_max_multiple", 3.0)
            max_forced_margin_pct = kwargs.get("max_forced_margin_pct", 20.0)
            margin_for_one = (entry_px * qm) / applied_leverage
            margin_cap = balance * (max_forced_margin_pct / 100.0)
            multiple = margin_for_one / margin if margin > 0 else float("inf")
            if margin_for_one <= margin_cap and multiple <= forced_size_max_multiple:
                size = 1
                forced_min_size = True
                olog(f"[ema_auto_trade] {symbol}: size_raw={size_raw:.3f}<1 при "
                     f"номинальной марже {margin:.2f}U — форсирую size=1 "
                     f"(нужно {margin_for_one:.2f}U маржи = {multiple:.1f}× номинала, потолок "
                     f"{margin_cap:.2f}U={max_forced_margin_pct}% депо, баланс {balance:.2f}U)")
            else:
                reason = (f"{margin_for_one:.2f}U > потолка {margin_cap:.2f}U ({max_forced_margin_pct}% депо)"
                          if margin_for_one > margin_cap else
                          f"нужно {multiple:.1f}× номинальной маржи > лимита {forced_size_max_multiple}×")
                raise RuntimeError(
                    f"Недостаточно средств: balance={balance:.2f} margin={margin:.2f} "
                    f"lev={applied_leverage}× ep={entry_px} qm={qm} → size={size_raw:.3f} < 1, "
                    f"даже 1 контракт требует {margin_for_one:.2f}U маржи — {reason} — пропуск"
                )
        notional = size * entry_px * qm
        olog(f"🔓 Открываем {direction.upper()} {symbol}: "
             f"balance={balance:.2f} × {position_pct}%(маржа) × {applied_leverage}×(плечо) → позиция~{notional:.2f}U | "
             f"риск={risk_pct}% | size={size} контр{' [форс-минимум]' if forced_min_size else ''} | "
             f"entry≈{_fmt_px(entry_px)} SL={_fmt_px(sl_px)} TP={_fmt_px(tp_px)}")

        _gate_cancel_orders(symbol)

        is_long = (direction == "long")
        try:
            _gate_req("POST", "/futures/usdt/orders", body={
                "contract": contract,
                "size":     size if is_long else -size,
                "price":    "0",
                "tif":      "ioc",
                "text":     f"t-{text_prefix}-open",
            }, _retries=1)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ReadTimeout) as _e_net:
            olog(f"⚠ Таймаут/сеть при отправке ордера на вход ({_e_net}) — "
                 f"проверяю на бирже, прошёл ли ордер, прежде чем считать "
                 f"попытку неудачной (чтобы не задвоить позицию повтором)")
            time.sleep(2.0)
            try:
                _check = _gate_get_position(contract)
            except Exception:
                _check = None
            if not _check:
                raise
            olog(f"✓ Ордер на вход всё же исполнился на бирже несмотря на "
                 f"таймаут ответа — продолжаю как при успехе, повтор не нужен")
        time.sleep(1.0)

        close_size = -size if is_long else size
        pos_info   = {"dir": direction, "entry": entry_px, "sl": sl_px, "tp": tp_px,
                      "size": size, "leverage": applied_leverage, "notional": round(notional, 2)}
        tp_sl_ok   = False

        def _place_tp_sl():
            _gate_place_protective_orders(symbol, direction, close_size, sl_px, tp_px, text_prefix)

        try:
            _place_tp_sl()
            tp_sl_ok = True
        except Exception as e_tp:
            olog(f"⚠ TP/SL попытка 1 упала: {e_tp} — повтор через 2с")
            time.sleep(2.0)
            try:
                _place_tp_sl()
                tp_sl_ok = True
                olog("✓ TP/SL выставлены со второй попытки")
            except Exception as e_tp2:
                olog(f"🚨 TP/SL НЕ выставлены после 2 попыток: {e_tp2}")
                _send_alert(
                    f"🚨 <b>{symbol}</b> — позиция открыта, но TP/SL НЕ выставлены!\n"
                    f"Причина: {e_tp2}\nВыставь стоп вручную! "
                    f"TP={_fmt_px(tp_px)} SL={_fmt_px(sl_px)}"
                )

        emoji = "🟢" if is_long else "🔴"
        status = "✅ открыт + TP/SL выставлены" if tp_sl_ok else "⚠️ открыт, TP/SL — см. выше"
        olog(f"{status}: {symbol} {direction.upper()} [{label}]")
        _send_alert(
            f"{emoji} <b>{symbol} {label}</b> — {direction.upper()}\n"
            f"Entry ≈ {_fmt_px(entry_px)} | TP {_fmt_px(tp_px)} | SL {_fmt_px(sl_px)}\n"
            f"Size {size} контр | ~{notional:.1f}U | {applied_leverage}×плечо"
            + ("" if tp_sl_ok else "\n⚠️ TP/SL не выставлены — проверь вручную!")
        )
        return pos_info
    except Exception as e:
        olog(f"⚠ gate_open_position {symbol}: {e}")
        _send_alert(f"🚨 <b>{symbol}</b> — ошибка открытия позиции:\n{e}")
        return None

def _load_gate_cfg():
    global GATE_KEY, GATE_SECRET
    try:
        with open(GATE_CFG_PATH) as f:
            cfg = json.load(f)
        GATE_KEY    = cfg.get("gate_key",    GATE_KEY)    or GATE_KEY
        GATE_SECRET = cfg.get("gate_secret", GATE_SECRET) or GATE_SECRET
    except: pass

def _save_gate_cfg():
    try:
        with open(GATE_CFG_PATH, "w") as f:
            json.dump({"gate_key": GATE_KEY, "gate_secret": GATE_SECRET}, f)
    except Exception as e:
        olog(f"⚠ Не удалось сохранить gate cfg: {e}")

def _ts():
    return time.strftime("[%H:%M:%S]")

def olog(msg):
    with _log_lock:
        opt_state["logs"].append({"ts": time.strftime("%H:%M:%S"), "msg": msg})
        if len(opt_state["logs"]) > 500:
            opt_state["logs"] = opt_state["logs"][-300:]
            opt_state["logs_dropped"] = opt_state.get("logs_dropped",0) + 200

# ─── Gate.io fetch ──────────────────────────────────────────────────────────
MAX_SPREAD_PCT = 0.15
MIN_VOLUME_24H_USD = 3_000_000

def _fetch_all_symbols():
    try:
        r = requests.get(f"{GATE_API}/futures/usdt/tickers", timeout=15)
        if r.status_code != 200: return []
        data = r.json()
        if not isinstance(data, list): return []
        valid = [t for t in data
                 if isinstance(t, dict) and "_USDT" in t.get("contract","")]
        valid.sort(key=lambda t: float(t.get("volume_24h_usd") or t.get("volume_24h_quote") or t.get("volume_24h") or 0), reverse=True)
        candidates = valid[:max(150, 100)]
        filtered = []
        skipped_spread = []
        skipped_volume = []
        for t in candidates:
            try:
                vol24 = float(t.get("volume_24h_usd") or t.get("volume_24h_quote") or t.get("volume_24h") or 0)
                if vol24 < MIN_VOLUME_24H_USD:
                    skipped_volume.append(f"{t.get('contract')}(${vol24:,.0f})")
                    continue
                bid = float(t.get("highest_bid") or 0)
                ask = float(t.get("lowest_ask") or 0)
                if bid <= 0 or ask <= 0 or ask < bid:
                    continue
                spread_pct = (ask - bid) / ((ask + bid) / 2) * 100
                if spread_pct <= MAX_SPREAD_PCT:
                    filtered.append(t)
                else:
                    skipped_spread.append(f"{t['contract']}({spread_pct:.2f}%)")
            except (TypeError, ValueError):
                continue
        top50 = [t["contract"] for t in filtered[:100]]
        olog(f"Топ-100 по объёму 24h (объём ≥${MIN_VOLUME_24H_USD:,.0f}, спред ≤{MAX_SPREAD_PCT}%): {', '.join(top50)}")
        if skipped_spread:
            olog(f"⚠ Отсеяно по широкому спреду ({len(skipped_spread)}): {', '.join(skipped_spread[:15])}"
                 + (" ..." if len(skipped_spread) > 15 else ""))
        if skipped_volume:
            olog(f"⚠ Отсеяно по низкому объёму <${MIN_VOLUME_24H_USD:,.0f} ({len(skipped_volume)}): "
                 f"{', '.join(skipped_volume[:15])}" + (" ..." if len(skipped_volume) > 15 else ""))
        return top50
    except Exception as e:
        olog(f"fetch_all_symbols error: {e}")
        return []

def _fetch_candles(symbol, tf, days, _stop_event=None, offset_days=0):
    interval_sec = TF_SECONDS.get(tf, 3600)
    now   = int(time.time()) - offset_days * 86400
    since = now - days * 86400
    LIMIT = 999
    all_candles = []
    current_from = since
    fail_count = 0
    MAX_FAILS = 5
    while current_from < now:
        if _stop_event and _stop_event.is_set(): return []
        try:
            r = requests.get(f"{GATE_API}/futures/usdt/candlesticks",
                params={"contract":symbol,"interval":tf,"from":current_from,"limit":LIMIT},
                timeout=15)
            if r.status_code != 200:
                fail_count += 1
                olog(f"⚠ Gate.io {r.status_code} для {symbol}: {r.text[:200]}")
                if fail_count >= MAX_FAILS:
                    olog(f"❌ {symbol}: {fail_count} ошибок подряд — контракт не существует "
                         f"или недоступен на Gate.io Futures, прерываю загрузку")
                    break
                sleep_t = 1 if _stop_event else 5
                if _stop_event:
                    _stop_event.wait(timeout=sleep_t)
                else:
                    time.sleep(sleep_t)
                continue
            fail_count = 0
            raw = r.json()
            if not raw: break
            batch = []
            for c in raw:
                t = int(c.get("t",0))
                batch.append({
                    "t": t, "open": float(c.get("o",0)),
                    "high": float(c.get("h",0)), "low": float(c.get("l",0)),
                    "close": float(c.get("c",0)), "vol": float(c.get("v",0))
                })
            if not batch: break
            all_candles.extend(batch)
            last_t = batch[-1]["t"]
            if last_t >= now - interval_sec: break
            current_from = last_t + interval_sec
            if _stop_event:
                if _stop_event.is_set(): return []
                _stop_event.wait(timeout=0.12)
            else:
                time.sleep(0.12)
        except Exception as e:
            fail_count += 1
            olog(f"fetch error: {e}")
            if fail_count >= MAX_FAILS:
                olog(f"❌ {symbol}: {fail_count} ошибок подряд, прерываю загрузку")
                break
            sleep_t = 1 if _stop_event else 5
            if _stop_event:
                _stop_event.wait(timeout=sleep_t)
            else:
                time.sleep(sleep_t)
    seen = set()
    result = []
    for c in sorted(all_candles, key=lambda x: x["t"]):
        if c["t"] not in seen:
            seen.add(c["t"]); result.append(c)
    result = [c for c in result if c["t"] + interval_sec <= now]
    return result

# ─── Индикаторы ─────────────────────────────────────────────────────────────
def _ema(arr, period):
    result = [None]*len(arr)
    if len(arr) < period: return result
    k = 2.0/(period+1)
    s = sum(arr[:period])/period
    result[period-1] = s
    for i in range(period, len(arr)):
        s = arr[i]*k + s*(1-k)
        result[i] = s
    return result

def _atr(candles, period=14):
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]; l = candles[i]["low"]; pc = candles[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    result = [None]*len(candles)
    if len(trs) < period: return result
    s = sum(trs[:period])/period
    result[period] = s
    for i in range(period+1, len(candles)):
        s = (s*(period-1) + trs[i-1])/period
        result[i] = s
    return result

def _rsi(closes, period=14):
    n = len(closes)
    result = [None]*n
    if n < period + 1:
        return result
    gains  = [max(closes[i]-closes[i-1], 0.0) for i in range(1, n)]
    losses = [max(closes[i-1]-closes[i], 0.0) for i in range(1, n)]
    avg_gain = sum(gains[:period])/period
    avg_loss = sum(losses[:period])/period
    def _val(ag, al):
        if al == 0: return 100.0
        rs = ag/al
        return 100.0 - 100.0/(1.0+rs)
    result[period] = round(_val(avg_gain, avg_loss), 2)
    for i in range(period+1, n):
        avg_gain = (avg_gain*(period-1) + gains[i-1])/period
        avg_loss = (avg_loss*(period-1) + losses[i-1])/period
        result[i] = round(_val(avg_gain, avg_loss), 2)
    return result

def _adx(candles, period=14):
    n = len(candles)
    adx_arr, plus_di_arr, minus_di_arr = [None]*n, [None]*n, [None]*n
    if n < period*2 + 1:
        return adx_arr, plus_di_arr, minus_di_arr
    tr, pdm, mdm = [None]*n, [None]*n, [None]*n
    for i in range(1, n):
        h, l = candles[i]["high"], candles[i]["low"]
        ph, pl, pc = candles[i-1]["high"], candles[i-1]["low"], candles[i-1]["close"]
        up_move, down_move = h - ph, pl - l
        pdm[i] = up_move if (up_move > down_move and up_move > 0) else 0.0
        mdm[i] = down_move if (down_move > up_move and down_move > 0) else 0.0
        tr[i]  = max(h-l, abs(h-pc), abs(l-pc))
    tr_s  = sum(tr[1:period+1])
    pdm_s = sum(pdm[1:period+1])
    mdm_s = sum(mdm[1:period+1])
    def _fill_di(i, tr_s, pdm_s, mdm_s):
        pdi = 100.0*pdm_s/tr_s if tr_s > 0 else 0.0
        mdi = 100.0*mdm_s/tr_s if tr_s > 0 else 0.0
        plus_di_arr[i]  = round(pdi, 2)
        minus_di_arr[i] = round(mdi, 2)
        return 100.0*abs(pdi-mdi)/(pdi+mdi) if (pdi+mdi) > 0 else 0.0
    dx_hist = [_fill_di(period, tr_s, pdm_s, mdm_s)]
    adx_prev = None
    for i in range(period+1, n):
        tr_s  = tr_s  - tr_s/period  + tr[i]
        pdm_s = pdm_s - pdm_s/period + pdm[i]
        mdm_s = mdm_s - mdm_s/period + mdm[i]
        dx = _fill_di(i, tr_s, pdm_s, mdm_s)
        dx_hist.append(dx)
        if len(dx_hist) == period:
            adx_prev = sum(dx_hist)/period
            adx_arr[i] = round(adx_prev, 2)
        elif len(dx_hist) > period:
            adx_prev = (adx_prev*(period-1) + dx)/period
            adx_arr[i] = round(adx_prev, 2)
    return adx_arr, plus_di_arr, minus_di_arr

def _vol_ratio(candles, i, lookback=20):
    if i < lookback or i >= len(candles):
        return None
    vols = [candles[j].get("vol") for j in range(i-lookback, i)]
    vols = [v for v in vols if v]
    if not vols:
        return None
    med = sorted(vols)[len(vols)//2]
    if med <= 0:
        return None
    cur = candles[i].get("vol") or 0
    return round(cur/med, 3)

def _atr_percentile(atr_arr, i, lookback=100):
    if i >= len(atr_arr) or atr_arr[i] is None:
        return None
    window = [v for v in atr_arr[max(0, i-lookback):i] if v is not None]
    if len(window) < 10:
        return None
    cur = atr_arr[i]
    rank = sum(1 for v in window if v <= cur)
    return round(100.0*rank/len(window), 1)

def _ribbon_spread_atr(ladder_vals, atr_v):
    vals = [v for v in ladder_vals if v is not None]
    if len(vals) < 2 or not atr_v:
        return None
    return round((max(vals)-min(vals))/atr_v, 3)

def _candle_reaction_pattern(candles, i):
    if i < 1 or i >= len(candles):
        return False, False
    c, p = candles[i], candles[i-1]
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    rng = h - l
    if rng <= 0:
        return False, False
    body = abs(cl-o)
    upper_wick = h - max(o, cl)
    lower_wick = min(o, cl) - l
    bullish_pin = lower_wick >= body*2 and lower_wick >= rng*0.5
    bearish_pin = upper_wick >= body*2 and upper_wick >= rng*0.5
    po, pcl = p["open"], p["close"]
    bullish_engulf = cl > o and pcl < po and cl >= po and o <= pcl
    bearish_engulf = cl < o and pcl > po and cl <= po and o >= pcl
    return (bullish_pin or bullish_engulf), (bearish_pin or bearish_engulf)

def _session_bucket(ts=None):
    h = time.gmtime(ts if ts is not None else time.time()).tm_hour
    if 0  <= h < 8:  return "asia"
    if 8  <= h < 14: return "europe"
    if 14 <= h < 22: return "us"
    return "asia"

# ─── EMA-bounce dossier engine ──────────────────────────────────────────────
EMA_TF_PERIODS = {
    "1m":  [5, 8, 13, 21],
    "5m":  [5, 8, 13, 21],
    "15m": [9, 20, 21],
    "1h":  [9, 20, 21],
    "4h":  [21, 50, 100],
    "1d":  [7, 14, 28],
    "1w":  [50, 100, 200],
}
EMA_DOSSIER_TFS  = ["1m", "5m", "15m", "1h", "4h", "1d"]
EMA_DOSSIER_DAYS = {"1m": 3, "5m": 6, "15m": 10, "1h": 30, "4h": 90, "1d": 400}
EMA_WEEKLY_PERIODS  = EMA_TF_PERIODS["1w"]
PUMP_LOOKBACK_DAYS  = 7
PUMP_THRESHOLD_PCT_DOSSIER  = 20.0   # rename: не путать с PUMP_THRESHOLD_PCT детектора ниже
EMA_DOSSIER_TOUCH_ATR    = 0.25
EMA_TOUCH_CLOSE_MAX_ATR  = 1.5
EMA_DOSSIER_REACT_ATR    = 0.5
EMA_DOSSIER_REACT_BARS   = 5
EMA_DOSSIER_MIN_TOUCHES  = 5
EMA_DOSSIER_FILE = os.path.expanduser("~/pumpradar_dossier_state.json")
EMA_SIGNAL_SL_ATR = 0.6
EMA_SIGNAL_RR      = 2.0
MAX_LEVERAGE = 20
EMA_HTF_TREND_TF        = "1d"
EMA_HTF_TREND_PERIOD    = 28
EMA_HTF_TREND_FILTER_TFS = {"1m", "5m", "15m", "1h", "4h"}

def _ladder_periods_for(tf, ema_period):
    return sorted(EMA_TF_PERIODS.get(tf, EMA_TF_PERIODS["1d"]))


def _ladder_order(vals):
    if any(v is None for v in vals):
        return None
    if all(vals[k] >= vals[k+1] for k in range(len(vals)-1)):
        return "up"
    if all(vals[k] <= vals[k+1] for k in range(len(vals)-1)):
        return "down"
    return None

def _detect_ema_bounces(candles, ema_period, tf="1d",
                         touch_atr=EMA_DOSSIER_TOUCH_ATR,
                         react_atr=EMA_DOSSIER_REACT_ATR,
                         react_bars=EMA_DOSSIER_REACT_BARS):
    closes = [c["close"] for c in candles]
    ema_arr = _ema(closes, ema_period)
    atr_arr = _atr(candles, 14)
    ladder_periods = _ladder_periods_for(tf, ema_period)
    ladder_arrs = [_ema(closes, p) for p in ladder_periods]
    n = len(candles)
    touches = 0
    bounces = 0
    breaks  = 0
    bounce_up = 0
    bounce_dn = 0
    i = ema_period
    while i < n - react_bars:
        ema_v = ema_arr[i]
        atr_v = atr_arr[i] or 0.0
        if ema_v is None or atr_v <= 0:
            i += 1
            continue
        lo, hi = candles[i]["low"], candles[i]["high"]
        tol = touch_atr * atr_v
        touched = ((lo - tol) <= ema_v <= (hi + tol) and
                   abs(closes[i] - ema_v) <= EMA_TOUCH_CLOSE_MAX_ATR * atr_v)
        prev_close = closes[i-1]
        if touched and abs(prev_close - ema_v) > tol:
            side_above = prev_close > ema_v
            expected = "up" if side_above else "down"
            ladder = _ladder_order([closes[i]] + [arr[i] for arr in ladder_arrs])
            if ladder is not None and ladder != expected:
                i += 1
                continue
            touches += 1
            fwd_closes = closes[i+1:i+1+react_bars]
            if side_above:
                moved_up   = max((c - ema_v) for c in fwd_closes) if fwd_closes else 0
                moved_down = max((ema_v - c) for c in fwd_closes) if fwd_closes else 0
                if moved_up >= react_atr * atr_v and moved_up > moved_down:
                    bounces += 1; bounce_up += 1
                elif moved_down >= react_atr * atr_v:
                    breaks += 1
            else:
                moved_down = max((ema_v - c) for c in fwd_closes) if fwd_closes else 0
                moved_up   = max((c - ema_v) for c in fwd_closes) if fwd_closes else 0
                if moved_down >= react_atr * atr_v and moved_down > moved_up:
                    bounces += 1; bounce_dn += 1
                elif moved_up >= react_atr * atr_v:
                    breaks += 1
            i += react_bars
        else:
            i += 1
    bounce_rate = (bounces / touches) if touches else 0.0
    return {
        "ema_period": ema_period, "touches": touches, "bounces": bounces,
        "breaks": breaks, "bounce_up": bounce_up, "bounce_dn": bounce_dn,
        "bounce_rate": round(bounce_rate, 4),
    }

def _resample_to_weekly(daily_candles):
    if not daily_candles: return []
    weekly = []
    bucket = []
    bucket_start = None
    for c in daily_candles:
        day_idx = c["t"] // 86400
        week_idx = day_idx // 7
        if bucket_start is None:
            bucket_start = week_idx
        if week_idx != bucket_start:
            if bucket:
                weekly.append({
                    "t": bucket[0]["t"],
                    "open": bucket[0]["open"], "close": bucket[-1]["close"],
                    "high": max(b["high"] for b in bucket),
                    "low":  min(b["low"] for b in bucket),
                    "vol":  sum(b.get("vol", 0) for b in bucket),
                })
            bucket = [c]; bucket_start = week_idx
        else:
            bucket.append(c)
    if bucket:
        weekly.append({
            "t": bucket[0]["t"],
            "open": bucket[0]["open"], "close": bucket[-1]["close"],
            "high": max(b["high"] for b in bucket),
            "low":  min(b["low"] for b in bucket),
            "vol":  sum(b.get("vol", 0) for b in bucket),
        })
    return weekly

def _detect_recent_pump(daily_candles, lookback_days=PUMP_LOOKBACK_DAYS, threshold_pct=PUMP_THRESHOLD_PCT_DOSSIER):
    if len(daily_candles) < lookback_days + 1:
        return False, 0.0
    prev = daily_candles[-lookback_days-1]["close"]
    last = daily_candles[-1]["close"]
    if prev <= 0: return False, 0.0
    pct = (last - prev) / prev * 100.0
    return (pct >= threshold_pct), round(pct, 2)

def _build_coin_dossier(symbol, timeframes=None):
    timeframes  = timeframes or EMA_DOSSIER_TFS
    dossier = {"symbol": symbol, "by_tf": {}, "generated_at": int(time.time()),
               "recent_pump": False, "pump_pct": 0.0}
    daily_candles_cache = None
    for tf in timeframes:
        days = EMA_DOSSIER_DAYS.get(tf, 30)
        candles = _fetch_candles(symbol, tf, days)
        if tf == "1d":
            daily_candles_cache = candles
        ema_periods = EMA_TF_PERIODS.get(tf, EMA_TF_PERIODS["1d"])
        if len(candles) < max(ema_periods) + EMA_DOSSIER_REACT_BARS + 10:
            dossier["by_tf"][tf] = {"error": "недостаточно данных", "emas": []}
            continue
        stats = [_detect_ema_bounces(candles, p, tf=tf) for p in ema_periods]
        reliable = [s for s in stats if s["touches"] >= EMA_DOSSIER_MIN_TOUCHES]
        best = max(reliable, key=lambda s: s["bounce_rate"]) if reliable else None
        dossier["by_tf"][tf] = {
            "emas": stats,
            "best_ema": best["ema_period"] if best else None,
            "best_bounce_rate": best["bounce_rate"] if best else None,
        }
    if daily_candles_cache is None:
        daily_candles_cache = _fetch_candles(symbol, "1d", EMA_DOSSIER_DAYS.get("1d", 400))
    if daily_candles_cache:
        weekly_candles = _resample_to_weekly(daily_candles_cache)
        w_periods = EMA_TF_PERIODS["1w"]
        if len(weekly_candles) >= max(w_periods) + EMA_DOSSIER_REACT_BARS + 10:
            w_stats = [_detect_ema_bounces(weekly_candles, p, tf="1w") for p in w_periods]
            w_reliable = [s for s in w_stats if s["touches"] >= EMA_DOSSIER_MIN_TOUCHES]
            w_best = max(w_reliable, key=lambda s: s["bounce_rate"]) if w_reliable else None
            dossier["by_tf"]["1w"] = {
                "emas": w_stats,
                "best_ema": w_best["ema_period"] if w_best else None,
                "best_bounce_rate": w_best["bounce_rate"] if w_best else None,
            }
        else:
            dossier["by_tf"]["1w"] = {"error": "недостаточно недель истории", "emas": []}
        is_pump, pct = _detect_recent_pump(daily_candles_cache)
        dossier["recent_pump"] = is_pump
        dossier["pump_pct"] = pct
    return dossier

def _load_ema_dossier_state():
    try:
        with open(EMA_DOSSIER_FILE, "r") as f: return json.load(f)
    except Exception:
        return {"status": "idle", "results": {}, "updated_at": 0}

def _save_ema_dossier_state(state):
    try:
        with open(EMA_DOSSIER_FILE, "w") as f: json.dump(state, f)
    except Exception as e:
        olog(f"ema_dossier save error: {e}")

def _run_ema_dossier_scan(top_n=50):
    symbols = _fetch_all_symbols()[:top_n]
    if len(symbols) < top_n:
        olog(f"⚠ [ema_dossier] после фильтра по спреду доступно только "
             f"{len(symbols)} монет из запрошенных {top_n} — список короче "
             f"ожидаемого (см. лог отсева выше)")
    state = {"status": "running", "results": {}, "total": len(symbols),
             "done": 0, "updated_at": int(time.time())}
    _save_ema_dossier_state(state)
    _before = _rss_mb()
    ex = _PoolExecutor(max_workers=NUM_WORKERS)
    olog(f"[ema_dossier] скан старт: {len(symbols)} монет, "
         f"{NUM_WORKERS} {'потоков' if _POOL_TYPE=='thread' else 'процессов'}"
         + (f", RSS {_before} МБ" if _before is not None else ""))
    try:
        futs = {ex.submit(_build_coin_dossier, sym): sym for sym in symbols}
        for fut in _as_completed(futs):
            sym = futs[fut]
            try:
                state["results"][sym] = fut.result()
            except Exception as e:
                state["results"][sym] = {"symbol": sym, "error": str(e)}
            state["done"] += 1
            state["updated_at"] = int(time.time())
            _save_ema_dossier_state(state)
    finally:
        _shutdown_pool_safely(ex)
    _after = _rss_mb()
    if _before is not None and _after is not None:
        olog(f"[ema_dossier] пул закрыт, RSS {_before}→{_after} МБ")
    state["status"] = "done"
    _save_ema_dossier_state(state)
    olog(f"[ema_dossier] скан завершён: {len(symbols)} монет")
    return state

# ─── Live EMA-сигналы ───────────────────────────────────────────────────────
EMA_LIVE_FILE      = os.path.expanduser("~/pumpradar_live_signals.json")
EMA_LIVE_POLL_SEC  = 15
_ema_live_lock  = threading.Lock()
_ema_live_state = {"signals": {}, "updated_at": 0}

EMA_HISTORY_FILE = os.path.expanduser("~/pumpradar_signal_history.json")
EMA_HISTORY_MAX  = 500
_ema_history_lock = threading.Lock()

def _load_ema_history():
    try:
        with open(EMA_HISTORY_FILE, "r") as f:
            state = json.load(f)
            state.setdefault("items", {})
            return state
    except Exception:
        return {"items": {}}

def _save_ema_history(state):
    try:
        tmp_path = EMA_HISTORY_FILE + f".tmp{os.getpid()}"
        with open(tmp_path, "w") as f:
            json.dump(state, f)
        os.replace(tmp_path, EMA_HISTORY_FILE)
    except Exception as e:
        olog(f"[ema_history] ошибка сохранения: {e}")

def _ema_history_add(sig):
    key = f"{sig['symbol']}|{sig['tf']}|{sig['ema_period']}|{sig['bar_t']}"
    with _ema_history_lock:
        state = _load_ema_history()
        if key in state["items"]:
            return key
        item = dict(sig)
        item.update(status="open", opened_at=int(time.time()),
                    closed_at=None, close_price=None)
        state["items"][key] = item
        if len(state["items"]) > EMA_HISTORY_MAX:
            oldest = sorted(state["items"].items(), key=lambda kv: kv[1]["opened_at"])
            for k, _ in oldest[:len(state["items"]) - EMA_HISTORY_MAX]:
                del state["items"][k]
        _save_ema_history(state)
        return key

def _ema_reconcile_live_positions():
    with _ema_history_lock:
        state = _load_ema_history()
    live_open = [(k, v) for k, v in state["items"].items()
                 if v.get("status") == "open" and v.get("live")]
    if not live_open:
        return
    updated = {}
    for key, item in live_open:
        contract = item["symbol"].replace("/", "_").upper()
        try:
            still_open = _gate_get_position(contract)
        except Exception as e:
            olog(f"[ema_reconcile] {item['symbol']}: не смог проверить биржу ({e}) — пропуск")
            continue
        if still_open:
            continue
        try:
            _gate_cancel_orders(contract)
        except Exception as e:
            olog(f"[ema_reconcile] {item['symbol']}: не смог снять старые ордера ({e})")
        pnl_info = _gate_get_last_pnl(item["symbol"], fallback_since_ts=item["opened_at"])
        close_p  = (pnl_info["close_price"] if pnl_info and pnl_info.get("close_price") is not None
                    else item.get("sl"))
        won      = bool(pnl_info and pnl_info["pnl"] >= 0)
        item["status"]            = "tp" if won else "sl"
        item["closed_at"]         = int(time.time())
        item["close_price"]       = close_p
        item["closed_externally"] = True
        item["diag_status"]       = "pending"
        if pnl_info:
            item["live_pnl"]     = pnl_info["pnl"]
            item["live_pnl_pct"] = pnl_info["pnl_pct"]
            item["live_pnl_fee"]   = pnl_info.get("pnl_fee")
            item["live_pnl_fund"]  = pnl_info.get("pnl_fund")
            item["live_pnl_price"] = pnl_info.get("pnl_price")
        olog(f"[ema_reconcile] {item['symbol']}: реально закрыта на бирже "
             f"внешним путём (не по нашему TP/SL), close={close_p} pnl={pnl_info}")
        updated[key] = item
    if updated:
        with _ema_history_lock:
            state = _load_ema_history()
            state["items"].update(updated)
            _save_ema_history(state)

def _ema_history_update_open():
    with _ema_history_lock:
        state = _load_ema_history()
    open_items = [(k, v) for k, v in state["items"].items() if v["status"] == "open"]
    if not open_items:
        return
    updated = {}
    for key, item in open_items:
        try:
            outcome, outcome_price = None, None
            live_price = _gate_get_price(item["symbol"])
            if live_price:
                if item["dir"] == "long":
                    if live_price <= item["sl"]: outcome, outcome_price = "sl", item["sl"]
                    elif live_price >= item["tp"]: outcome, outcome_price = "tp", item["tp"]
                else:
                    if live_price >= item["sl"]: outcome, outcome_price = "sl", item["sl"]
                    elif live_price <= item["tp"]: outcome, outcome_price = "tp", item["tp"]
            if not outcome:
                fetch_tf = "1d" if item["tf"] == "1w" else item["tf"]
                days = _days_for_live_check(item["tf"], item["ema_period"])
                raw = _fetch_candles(item["symbol"], fetch_tf, days)
                candles = _resample_to_weekly(raw) if item["tf"] == "1w" else raw
                fwd = [c for c in candles if c["t"] > item["bar_t"]]
                for c in fwd:
                    if item["dir"] == "long":
                        hit_tp, hit_sl = c["high"] >= item["tp"], c["low"] <= item["sl"]
                    else:
                        hit_tp, hit_sl = c["low"] <= item["tp"], c["high"] >= item["sl"]
                    if hit_sl:
                        outcome, outcome_price = "sl", item["sl"]; break
                    if hit_tp:
                        outcome, outcome_price = "tp", item["tp"]; break
            if outcome:
                item["status"] = outcome
                item["closed_at"] = int(time.time())
                item["close_price"] = outcome_price
                item["diag_status"] = "pending"
                if item.get("live"):
                    try:
                        pnl_info = _gate_get_last_pnl(item["symbol"], fallback_since_ts=item["opened_at"])
                    except Exception as e:
                        olog(f"[ema_history] {item['symbol']}: не смог получить live_pnl ({e})")
                        pnl_info = None
                    if pnl_info:
                        item["live_pnl"]       = pnl_info["pnl"]
                        item["live_pnl_pct"]   = pnl_info.get("pnl_pct")
                        item["live_pnl_fee"]   = pnl_info.get("pnl_fee")
                        item["live_pnl_fund"]  = pnl_info.get("pnl_fund")
                        item["live_pnl_price"] = pnl_info.get("pnl_price")
                updated[key] = item
        except Exception as e:
            olog(f"[ema_history] статус {item.get('symbol')} ошибка: {e}")
    if updated:
        with _ema_history_lock:
            state2 = _load_ema_history()
            for key, item in updated.items():
                state2["items"][key] = item
            _save_ema_history(state2)
        for item in updated.values():
            if item.get("live"):
                _ema_finalize_live_position(item)

# ─── реальная автоторговля по EMA-сигналам ─────────────────────────────────
EMA_AUTO_TRADE_CFG_FILE = os.path.expanduser("~/pumpradar_auto_trade_cfg.json")
ema_auto_trade_lock  = threading.Lock()
ema_auto_trade_state = {
    "enabled": False,
    "position_pct":   3.0,
    "risk_pct":       5.0,
    "max_concurrent": None,
    "max_forced_margin_pct": 20.0,
    "forced_size_max_multiple": 3.0,
    "last_error": "",
}

def _load_ema_auto_trade_cfg():
    try:
        with open(EMA_AUTO_TRADE_CFG_FILE) as f:
            saved = json.load(f)
        with ema_auto_trade_lock:
            for k in ("enabled", "position_pct", "risk_pct", "max_concurrent", "max_forced_margin_pct", "forced_size_max_multiple"):
                if k in saved:
                    ema_auto_trade_state[k] = saved[k]
    except Exception:
        pass

def _save_ema_auto_trade_cfg():
    try:
        with ema_auto_trade_lock:
            snapshot = {k: ema_auto_trade_state[k] for k in
                        ("enabled", "position_pct", "risk_pct", "max_concurrent", "max_forced_margin_pct", "forced_size_max_multiple")}
        with open(EMA_AUTO_TRADE_CFG_FILE, "w") as f:
            json.dump(snapshot, f)
    except Exception as e:
        olog(f"[ema_auto_trade] ошибка сохранения настроек: {e}")

def _ema_maybe_open_live_trade(symbol, sig, hist_key):
    with ema_auto_trade_lock:
        if not ema_auto_trade_state["enabled"]:
            return
        position_pct = ema_auto_trade_state["position_pct"]
        risk_pct     = ema_auto_trade_state["risk_pct"]
        max_c        = ema_auto_trade_state["max_concurrent"]
        max_forced_margin_pct = ema_auto_trade_state["max_forced_margin_pct"]
        forced_size_max_multiple = ema_auto_trade_state["forced_size_max_multiple"]
    with _ema_history_lock:
        state = _load_ema_history()
        live_items = [v for v in state["items"].values()
                      if v.get("status") == "open" and v.get("live")]
    if any(v["symbol"] == symbol for v in live_items):
        return
    if max_c and len(live_items) >= max_c:
        olog(f"[ema_auto_trade] лимит {max_c} одновр. позиций достигнут — пропуск {symbol}")
        return
    contract = symbol.replace("/", "_").upper()
    try:
        existing = _gate_get_position(contract)
    except Exception as e:
        olog(f"[ema_auto_trade] {symbol}: не смог проверить биржу перед входом ({e}) — пропуск на этот раз")
        return
    if existing:
        olog(f"[ema_auto_trade] {symbol}: на бирже уже есть позиция вне нашего учёта — пропуск")
        return
    pos_info = _gate_open_position(
        symbol, sig["dir"], sig["price"], sig["sl"], sig["tp"], risk_pct,
        position_pct=position_pct, label="EMA INVERT", text_prefix="emainv",
        max_forced_margin_pct=max_forced_margin_pct,
        forced_size_max_multiple=forced_size_max_multiple,
    )
    if not pos_info:
        return
    with _ema_history_lock:
        state = _load_ema_history()
        item = state["items"].get(hist_key)
        if item:
            item["live"] = True
            item["live_size"]     = pos_info.get("size")
            item["live_leverage"] = pos_info.get("leverage")
            item["live_notional"] = pos_info.get("notional")
            _save_ema_history(state)

def _ema_finalize_live_position(item):
    symbol = item["symbol"]
    contract = symbol.replace("/", "_").upper()
    try:
        still_open = _gate_get_position(contract)
    except Exception as e:
        olog(f"[ema_auto_trade] {symbol}: не смог проверить биржу после закрытия сигнала: {e}")
        return
    if not still_open:
        return
    olog(f"[ema_auto_trade] ⚠ {symbol}: сигнал закрыт ({item['status']}), но позиция "
         f"ещё висит на бирже — закрываю маркетом принудительно")
    try:
        _gate_close_position(symbol)
        _send_alert(f"⚠️ <b>{symbol} EMA INVERT</b> — TP/SL-ордер не сработал вовремя, "
                     f"позиция закрыта маркетом принудительно")
    except Exception as e:
        olog(f"[ema_auto_trade] {symbol}: ошибка принудительного закрытия: {e}")
        _send_alert(f"🚨 <b>{symbol} EMA INVERT</b> — не удалось закрыть позицию "
                     f"принудительно: {e}. ЗАКРОЙ ВРУЧНУЮ!")


EMA_DIAG_FILE          = os.path.expanduser("~/pumpradar_signal_diagnostics.jsonl")
EMA_DIAG_WAIT_BARS     = 20
EMA_DIAG_LOOKBACK_PAD  = 5
_ema_diag_lock = threading.Lock()

def _ema_diag_log_write(record):
    try:
        with _ema_diag_lock:
            with open(EMA_DIAG_FILE, "a") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        olog(f"[ema_diag] ошибка записи лога: {e}")

EMA_EVENTS_FILE = os.path.expanduser("~/pumpradar_events_diagnostics.jsonl")
_ema_events_lock = threading.Lock()

def _ema_event_log_write(event_type, **fields):
    record = {"ts": time.time(), "ts_h": time.strftime("%Y-%m-%d %H:%M:%S"), "type": event_type}
    record.update(fields)
    try:
        with _ema_events_lock:
            with open(EMA_EVENTS_FILE, "a") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        olog(f"[ema_events] ⚠ ошибка записи в {EMA_EVENTS_FILE}: {e}")

def _ema_diagnose_one(item):
    symbol, tf, ema_period = item["symbol"], item["tf"], item["ema_period"]
    direction = item["dir"]
    entry, tp = item["price"], item["tp"]
    sl_hit_price   = item["sl"]
    sl             = item.get("orig_sl", sl_hit_price)
    breakeven_hit  = bool(item.get("breakeven_done"))
    bar_t, closed_at = item["bar_t"], item["closed_at"]
    fetch_tf = "1d" if tf == "1w" else tf
    span_sec = max(1, int(time.time()) - bar_t)
    days = math.ceil(span_sec / 86400) + EMA_DIAG_LOOKBACK_PAD
    raw = _fetch_candles(symbol, fetch_tf, days)
    candles = _resample_to_weekly(raw) if tf == "1w" else raw
    if not candles:
        return None
    during = [c for c in candles if bar_t <= c["t"] < closed_at]
    after  = [c for c in candles if c["t"] >= closed_at]
    if len(after) < EMA_DIAG_WAIT_BARS:
        return None
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    if direction == "long":
        mfe = max([c["high"] for c in during], default=entry) - entry
        post_high = max(c["high"] for c in after[:EMA_DIAG_WAIT_BARS])
        post_low  = min(c["low"]  for c in after[:EMA_DIAG_WAIT_BARS])
        would_hit_tp   = post_high >= tp
        continued_down = post_low <= sl - risk
    else:
        mfe = entry - min([c["low"] for c in during], default=entry)
        post_high = max(c["high"] for c in after[:EMA_DIAG_WAIT_BARS])
        post_low  = min(c["low"]  for c in after[:EMA_DIAG_WAIT_BARS])
        would_hit_tp   = post_low <= tp
        continued_down = post_high >= sl + risk
    mfe_r_atr = round(mfe / item["atr_v"], 2) if item.get("atr_v") else None
    bars_to_sl = len(during)
    if breakeven_hit:
        verdict = ("закрылась в безубыток: SL был переставлен watchdog'ом "
                    "после достижения профита, реальный убыток — только "
                    "комиссия/буфер, не полноценный стоп")
    elif would_hit_tp:
        verdict = ("стоп преждевременный: цена выбила SL, но в следующие "
                    f"{EMA_DIAG_WAIT_BARS} баров всё же дошла до уровня TP — "
                    "направление было угадано верно, не хватило запаса по стопу")
    elif continued_down:
        verdict = ("сетап был неверным: после SL цена продолжила движение "
                    "против сигнала ещё как минимум на такой же риск — "
                    "отскока от EMA не было, уровень был пробит трендом")
    elif mfe_r_atr is not None and mfe_r_atr < 0.15:
        verdict = ("шум/боковик: цена почти не двигалась в сторону сигнала "
                    "ни до, ни после стопа — касание EMA было случайным, "
                    "не реальным отскоком")
    else:
        verdict = ("смешанная картина: небольшое движение в пользу сигнала "
                    "было, но недостаточное ни для TP, ни для явного "
                    "продолжения тренда против — типичный шумовой стоп")
    record = {
        "ts": int(time.time()), "symbol": symbol, "tf": tf,
        "ema_period": ema_period, "dir": direction,
        "entry": entry, "sl": sl, "sl_hit_price": sl_hit_price, "tp": tp,
        "breakeven_hit": breakeven_hit,
        "bounce_rate": item.get("bounce_rate"), "touches": item.get("touches"),
        "dist_atr_at_entry": item.get("dist_atr"), "ladder_at_entry": item.get("ladder"),
        "bars_to_sl": bars_to_sl,
        "mfe_atr_before_sl": mfe_r_atr,
        "post_sl_hit_tp": would_hit_tp,
        "post_sl_continued_against": continued_down,
        "verdict": verdict,
        "rsi_at_entry": item.get("rsi"), "adx_at_entry": item.get("adx"),
        "plus_di_at_entry": item.get("plus_di"), "minus_di_at_entry": item.get("minus_di"),
        "vol_ratio_at_entry": item.get("vol_ratio"),
        "atr_percentile_at_entry": item.get("atr_percentile"),
        "ribbon_spread_atr_at_entry": item.get("ribbon_spread_atr"),
        "candle_pattern_at_entry": item.get("candle_pattern"),
        "session_at_entry": item.get("session"),
        "htf_trend_at_entry": item.get("htf_trend"),
    }
    return record

def _ema_run_diagnostics():
    with _ema_history_lock:
        state = _load_ema_history()
    pending = [(k, v) for k, v in state["items"].items()
               if v.get("status") == "sl" and v.get("diag_status") == "pending"]
    if not pending:
        return
    done = {}
    for key, item in pending:
        tf_sec = TF_SECONDS.get("1d" if item["tf"] == "1w" else item["tf"], 3600)
        if int(time.time()) - item["closed_at"] < EMA_DIAG_WAIT_BARS * tf_sec:
            continue
        try:
            record = _ema_diagnose_one(item)
        except Exception as e:
            olog(f"[ema_diag] {item.get('symbol')} ошибка разбора: {e}")
            continue
        if record is None:
            continue
        _ema_diag_log_write(record)
        olog(f"[ema_diag] {record['symbol']} {record['tf']} EMA{record['ema_period']} "
             f"{record['dir']} SL → {record['verdict']}")
        item["diag_status"] = "done"
        done[key] = item
    if done:
        with _ema_history_lock:
            state2 = _load_ema_history()
            for key, item in done.items():
                state2["items"][key] = item
            _save_ema_history(state2)

EMA_LIVE_TFS = {"1m", "5m", "15m", "1h"}

def _pick_best_ema_for_symbol(dossier_entry):
    best = None
    for tf, d in (dossier_entry.get("by_tf") or {}).items():
        if tf not in EMA_LIVE_TFS:
            continue
        for s in d.get("emas", []):
            touches = s.get("touches", 0)
            if touches < EMA_DOSSIER_MIN_TOUCHES:
                continue
            break_rate = s.get("breaks", 0) / touches
            if best is None or break_rate > best["break_rate"]:
                best = {"tf": tf, "ema_period": s["ema_period"],
                        "bounce_rate": s["bounce_rate"],
                        "break_rate": round(break_rate, 4),
                        "touches": touches}
    return best

def _days_for_live_check(tf, ema_period):
    bars_needed = ema_period + 260
    src_tf = "1d" if tf == "1w" else tf
    days = math.ceil(bars_needed * (TF_SECONDS.get(src_tf, 3600) if tf != "1w" else 86400*7) / 86400)
    return max(3, days)

_ema_touch_state = {}
EMA_TOUCH_EXIT_MULT = 1.8
_ema_touch_pending  = {}
EMA_TOUCH_CONFIRM_SEC = 15
_ema_touch_confirm_stats = {"confirmed": 0, "aborted": 0}

_ema_open_keys_cache = {"at": 0, "keys": set()}
EMA_OPEN_KEYS_TTL = 10

def _ema_has_open_signal(symbol, tf, ema_period):
    now = time.time()
    if now - _ema_open_keys_cache["at"] > EMA_OPEN_KEYS_TTL:
        with _ema_history_lock:
            state = _load_ema_history()
        _ema_open_keys_cache["keys"] = {
            f"{v['symbol']}|{v['tf']}|{v['ema_period']}"
            for v in state["items"].values() if v.get("status") == "open"
        }
        _ema_open_keys_cache["at"] = now
    return f"{symbol}|{tf}|{ema_period}" in _ema_open_keys_cache["keys"]
_ema_ctx_cache   = {}

def _round_price(v):
    if v is None or v == 0:
        return v
    magnitude = math.floor(math.log10(abs(v)))
    decimals = max(0, 6 - magnitude - 1)
    return round(v, decimals)

def _ema_live_value(prev_ema, live_price, period):
    if prev_ema is None: return None
    k = 2.0 / (period + 1)
    return live_price * k + prev_ema * (1 - k)

def _ema_get_closed_ctx(symbol, tf, ema_period):
    key = f"{symbol}|{tf}|{ema_period}"
    now = time.time()
    cached = _ema_ctx_cache.get(key)
    if cached and now < cached["next_close_t"]:
        return cached
    days = _days_for_live_check(tf, ema_period)
    fetch_tf = "1d" if tf == "1w" else tf
    raw = _fetch_candles(symbol, fetch_tf, days)
    candles = _resample_to_weekly(raw) if tf == "1w" else raw
    if len(candles) < ema_period + 5:
        return None
    closes = [c["close"] for c in candles]
    ema_arr = _ema(closes, ema_period)
    atr_arr = _atr(candles, 14)
    i = len(candles) - 1
    ema_closed, atr_v = ema_arr[i], atr_arr[i]
    if ema_closed is None or not atr_v:
        return None
    ladder_periods = _ladder_periods_for(tf, ema_period)
    ladder_ema_closed = [_ema(closes, p)[i] for p in ladder_periods]
    interval_sec = TF_SECONDS.get("1d" if tf == "1w" else tf, 3600)
    next_close_t = candles[i]["t"] + interval_sec + 2
    rsi_arr = _rsi(closes, 14)
    adx_arr, plus_di_arr, minus_di_arr = _adx(candles, 14)
    bullish_reaction, bearish_reaction = _candle_reaction_pattern(candles, i)
    ctx = {
        "close_i": closes[i], "ema_closed": ema_closed, "atr_v": atr_v,
        "ladder_periods": ladder_periods, "ladder_ema_closed": ladder_ema_closed,
        "next_close_t": next_close_t,
        "rsi": rsi_arr[i], "adx": adx_arr[i],
        "plus_di": plus_di_arr[i], "minus_di": minus_di_arr[i],
        "vol_ratio": _vol_ratio(candles, i),
        "atr_percentile": _atr_percentile(atr_arr, i),
        "ribbon_spread_atr": _ribbon_spread_atr(ladder_ema_closed, atr_v),
        "bullish_reaction": bullish_reaction, "bearish_reaction": bearish_reaction,
    }
    _ema_ctx_cache[key] = ctx
    return ctx

def _get_htf_trend(symbol):
    ctx = _ema_get_closed_ctx(symbol, EMA_HTF_TREND_TF, EMA_HTF_TREND_PERIOD)
    if ctx is None:
        return None
    return "up" if ctx["close_i"] > ctx["ema_closed"] else "down"

def _ema_check_symbol_signal(symbol, pick):
    tf, ema_period = pick["tf"], pick["ema_period"]
    ctx = _ema_get_closed_ctx(symbol, tf, ema_period)
    if ctx is None: return None
    ema_closed, atr_v = ctx["ema_closed"], ctx["atr_v"]
    live_price = _gate_get_price(symbol)
    if not live_price: return None
    ema_v = _ema_live_value(ema_closed, live_price, ema_period)
    tol = EMA_DOSSIER_TOUCH_ATR * atr_v
    dist = abs(live_price - ema_v)
    key = f"{symbol}|{tf}|{ema_period}"
    state = _ema_touch_state.get(key, "out")
    side_above = ctx["close_i"] > ema_closed
    direction = "long" if side_above else "short"
    touched_now = dist <= tol if state == "out" else dist <= tol * EMA_TOUCH_EXIT_MULT
    if not touched_now:
        if state == "pending":
            _ema_touch_confirm_stats["aborted"] += 1
            waited = time.time() - _ema_touch_pending.get(key, time.time())
            olog(f"[ema_touch] {symbol} {tf} EMA{ema_period}: касание отменено — "
                 f"цена пробила уровень насквозь за {waited:.0f}с без подтверждения "
                 f"(всего отменено={_ema_touch_confirm_stats['aborted']}, "
                 f"подтверждено={_ema_touch_confirm_stats['confirmed']})")
            _ema_event_log_write("touch_aborted", symbol=symbol, tf=tf,
                                  ema_period=ema_period, waited_sec=round(waited, 1),
                                  aborted_total=_ema_touch_confirm_stats["aborted"],
                                  confirmed_total=_ema_touch_confirm_stats["confirmed"])
        _ema_touch_state[key] = "out"
        _ema_touch_pending.pop(key, None)
        return None
    if state == "out":
        _ema_touch_state[key] = "pending"
        _ema_touch_pending[key] = time.time()
        return None
    if state == "pending":
        if time.time() - _ema_touch_pending.get(key, 0) < EMA_TOUCH_CONFIRM_SEC:
            return None
        _ema_touch_state[key] = "done"
        _ema_touch_confirm_stats["confirmed"] += 1
        _ema_event_log_write("touch_confirmed", symbol=symbol, tf=tf,
                              ema_period=ema_period,
                              waited_sec=round(time.time() - _ema_touch_pending.get(key, time.time()), 1),
                              aborted_total=_ema_touch_confirm_stats["aborted"],
                              confirmed_total=_ema_touch_confirm_stats["confirmed"])
    else:
        return None
    if _ema_has_open_signal(symbol, tf, ema_period):
        return None
    ladder_vals_live = [_ema_live_value(v, live_price, p)
                         for v, p in zip(ctx["ladder_ema_closed"], ctx["ladder_periods"])]
    ladder = _ladder_order([live_price] + ladder_vals_live)
    expected = "up" if side_above else "down"
    if ladder is not None and ladder != expected:
        return None
    htf_trend = _get_htf_trend(symbol)
    if tf in EMA_HTF_TREND_FILTER_TFS and htf_trend is not None:
        if direction == "long" and htf_trend == "up":
            return None
        if direction == "short" and htf_trend == "down":
            return None
    price = live_price
    sl_buf = EMA_SIGNAL_SL_ATR * atr_v
    level = (ema_v - sl_buf) if direction == "long" else (ema_v + sl_buf)
    trade_dir = "short" if direction == "long" else "long"
    tp = level
    if trade_dir == "long":
        risk_to_tp = tp - price
        safety_sl = price - EMA_INVERT_SAFETY_RR * risk_to_tp
    else:
        risk_to_tp = price - tp
        safety_sl = price + EMA_INVERT_SAFETY_RR * risk_to_tp
    if risk_to_tp <= 0:
        return None
    if trade_dir == "long" and not (safety_sl < price < tp):
        return None
    if trade_dir == "short" and not (tp < price < safety_sl):
        return None
    ema_r, tp_r, safety_sl_r = _round_price(ema_v), _round_price(tp), _round_price(safety_sl)
    if trade_dir == "long" and not (safety_sl_r < price < tp_r):
        return None
    if trade_dir == "short" and not (tp_r < price < safety_sl_r):
        return None
    dist_atr = round(abs(live_price - ema_v) / atr_v, 3) if atr_v else None
    ladder_snapshot = {str(p): _round_price(v) for p, v in
                        zip(ctx["ladder_periods"], ladder_vals_live)}
    if direction == "long":
        candle_pattern = "confirmed" if ctx["bullish_reaction"] else "absent"
    else:
        candle_pattern = "confirmed" if ctx["bearish_reaction"] else "absent"

    if ctx["vol_ratio"] is not None and ctx["vol_ratio"] > EMA_INVERT_MAX_VOL_RATIO:
        _ema_invert_filter_stats["vol_ratio"] += 1
        _ema_event_log_write("invert_filter_rejected", symbol=symbol, tf=tf,
                              ema_period=ema_period, reason="vol_ratio",
                              value=ctx["vol_ratio"], threshold=EMA_INVERT_MAX_VOL_RATIO)
        return None
    if EMA_INVERT_REJECT_CONFIRMED_PATTERN and candle_pattern == "confirmed":
        _ema_invert_filter_stats["candle_pattern"] += 1
        _ema_event_log_write("invert_filter_rejected", symbol=symbol, tf=tf,
                              ema_period=ema_period, reason="candle_pattern")
        return None

    return {
        "symbol": symbol, "tf": tf, "ema_period": ema_period, "dir": trade_dir,
        "bounce_dir": direction,
        "price": price, "ema_value": ema_r,
        "sl": safety_sl_r, "tp": tp_r, "rr": None,
        "time_limit_sec": _ema_invert_time_limit_sec(tf),
        "bounce_rate": pick["bounce_rate"], "break_rate": pick.get("break_rate"),
        "bar_t": int(time.time()),
        "touches": pick.get("touches"), "atr_v": _round_price(atr_v),
        "dist_atr": dist_atr, "ladder": ladder_snapshot,
        "rsi": ctx["rsi"], "adx": ctx["adx"],
        "plus_di": ctx["plus_di"], "minus_di": ctx["minus_di"],
        "vol_ratio": ctx["vol_ratio"], "atr_percentile": ctx["atr_percentile"],
        "ribbon_spread_atr": ctx["ribbon_spread_atr"],
        "candle_pattern": candle_pattern,
        "session": _session_bucket(),
        "htf_trend": htf_trend,
    }

def _ema_signal_loop():
    time.sleep(30)
    _iter = 0
    while True:
        try:
            state = _load_ema_dossier_state()
            results = state.get("results") or {}
            if not results:
                time.sleep(EMA_LIVE_POLL_SEC); continue
            for symbol, entry in results.items():
                if "error" in entry: continue
                pick = _pick_best_ema_for_symbol(entry)
                if not pick: continue
                sig = _ema_check_symbol_signal(symbol, pick)
                if not sig: continue
                with _ema_live_lock:
                    _ema_live_state["signals"][symbol] = sig
                    _ema_live_state["updated_at"] = int(time.time())
                hist_key = _ema_history_add(sig)
                arrow = "🟢 LONG" if sig["dir"] == "long" else "🔴 SHORT"
                mins_limit = sig["time_limit_sec"] / 60.0
                msg = (f"📊 <b>{symbol}</b> {sig['tf']} — {arrow} (INVERT: пробой EMA{sig['ema_period']}, "
                       f"против bounce_rate {sig['bounce_rate']*100:.0f}%)\n"
                       f"Цена: {sig['price']} | EMA: {sig['ema_value']}\n"
                       f"Safety-SL: {sig['sl']} | TP: {sig['tp']} | time-stop: {mins_limit:.0f}мин")
                _send_alert(msg)
                olog(f"[ema_live] новый сигнал {symbol} {sig['tf']} EMA{sig['ema_period']} {sig['dir']}")
                _ema_maybe_open_live_trade(symbol, sig, hist_key)
            _ema_reconcile_live_positions()
            _ema_rearm_missing_protection()
            _ema_invert_timestop_watchdog()
            _ema_history_update_open()
            _ema_invert_run_diagnostics()
        except Exception as e:
            olog(f"[ema_live] ошибка цикла: {e}")
        _iter += 1
        if _iter % 120 == 0:
            _rss = _rss_mb()
            if _rss is not None:
                olog(f"[ema_live] RSS {_rss} МБ (итерация {_iter}, ctx_cache={len(_ema_ctx_cache)})")
            gc.collect()
        time.sleep(EMA_LIVE_POLL_SEC)
EMA_HTML_PAGE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pump Radar</title>
<style>
body{background:#0d1117;color:#c9d1d9;font-family:system-ui,sans-serif;margin:0;padding:16px}
h1{font-size:20px}
h3{font-size:15px;margin:0}
button{background:#238636;color:#fff;border:0;padding:10px 16px;border-radius:6px;font-size:14px;margin:6px 6px 6px 0;cursor:pointer}
table{width:100%;border-collapse:collapse;margin-top:0;font-size:13px}
th,td{padding:6px 8px;text-align:left;border-bottom:1px solid #21262d;white-space:nowrap}
th{color:#8b949e;position:sticky;top:0;background:#161b22}
tbody tr:hover{background:#1c2128}
tr.row-tp{background:rgba(63,185,80,.06)}
tr.row-sl{background:rgba(248,81,73,.06)}
.long{color:#3fb950}.short{color:#f85149}
.tp{color:#3fb950}.sl{color:#f85149}.open{color:#8b949e}
#status{color:#8b949e;font-size:13px;margin-top:8px}
.livebadge{background:#1f6feb;color:#fff;font-size:10px;padding:1px 5px;border-radius:3px;margin-left:4px}
#atBox{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:10px 14px;margin-top:10px;font-size:13px}
#atModal,#alertModal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:50;align-items:center;justify-content:center}
#atModal .card,#alertModal .card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:20px;width:min(420px,92vw)}
#atModal label,#alertModal label{display:block;margin-top:12px;font-size:13px;color:#8b949e}
#atModal input[type=number],#alertModal input[type=number],#atModal input[type=text],#alertModal input[type=text]{width:100%;background:#0d1117;color:#c9d1d9;border:1px solid #30363d;border-radius:6px;padding:8px;margin-top:4px;font-size:14px;box-sizing:border-box}
#atModal .row,#alertModal .row{display:flex;justify-content:space-between;align-items:center;margin-top:12px}
#atModal hr{border:0;border-top:1px solid #30363d;margin:16px 0 4px}
.switch{position:relative;width:44px;height:24px}
.switch input{opacity:0;width:0;height:0}
.slider{position:absolute;inset:0;background:#30363d;border-radius:24px;cursor:pointer;transition:.2s}
.slider:before{content:"";position:absolute;height:18px;width:18px;left:3px;bottom:3px;background:#fff;border-radius:50%;transition:.2s}
input:checked + .slider{background:#238636}
input:checked + .slider:before{transform:translateX(20px)}
.section-card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px;margin-top:14px}
.section-head{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
.btn-danger-sm{background:#3a1414;border:1px solid #f85149;color:#f85149;padding:4px 10px;font-size:12px;border-radius:6px;margin:0}
.summary-line{font-size:13px;color:#8b949e;margin-top:6px}
.summary-line b{color:#c9d1d9}
.filter-row{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap}
.filter-btn{background:#21262d;border:1px solid #30363d;color:#8b949e;padding:4px 12px;font-size:12px;margin:0;border-radius:14px;cursor:pointer}
.filter-btn.active{background:#1f6feb;border-color:#1f6feb;color:#fff}
.table-scroll{overflow-x:auto;margin-top:10px;border-radius:6px}
.status-pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;white-space:nowrap}
.status-pill.tp{background:#0d2818;color:#3fb950;border:1px solid #23643a}
.status-pill.sl{background:#2a0f0f;color:#f85149;border:1px solid #6e2323}
.status-pill.open{background:#2a2410;color:#d29922;border:1px solid #6e5a1f}
.dur{font-size:11px;color:#6e7681;margin-top:2px}
.pager{display:flex;align-items:center;justify-content:center;gap:10px;margin-top:10px;font-size:12px;color:#8b949e}
.pager button{background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:4px 12px;font-size:12px;margin:0;border-radius:6px}
.pager button:disabled{opacity:.35;cursor:default}
details summary{cursor:pointer;font-size:15px;list-style:none}
details summary::-webkit-details-marker{display:none}
details summary:before{content:"▸ ";color:#8b949e}
details[open] summary:before{content:"▾ "}
.log-box{background:#010409;border:1px solid #21262d;border-radius:4px;height:200px;overflow-y:auto;padding:6px;font-size:11px;font-family:monospace}
.log-line{padding:1px 0;border-bottom:1px solid #21262d}
@media (max-width:480px){
  body{padding:8px;font-size:13px}
  h1{font-size:17px}
  h3{font-size:14px}
  button{padding:8px 12px;font-size:13px;margin:4px 4px 4px 0}
  th,td{padding:4px 6px;font-size:12px}
  .section-card{padding:10px;margin-top:10px}
  #atBox{padding:8px 10px;font-size:12px}
  .filter-btn{padding:3px 9px;font-size:11px}
  .status-pill{padding:1px 6px;font-size:10px}
}
</style></head><body>
<h1>&#128225; Pump Radar <span style="font-size:12px;color:#8b949e;font-weight:normal">v__APP_VERSION__</span></h1>
<button onclick="startScan()">Запустить скан EMA-досье (топ-50)</button>
<button onclick="openSettings()" style="background:#21262d;border:1px solid #30363d">&#9881;&#65039; Автоторговля</button>
<button onclick="openAlertSettings()" style="background:#21262d;border:1px solid #30363d">&#128276; Алерты</button>
<a href="/pump_match" style="text-decoration:none"><button style="background:#8250df">&#127919; Pump Match (подбор параметров)</button></a>
<div id="status"></div>
<div id="atBox"></div>

<div id="atModal">
  <div class="card">
    <h3 style="margin-top:0">&#9881;&#65039; Автоторговля EMA (Gate.io)</h3>
    <div class="row">
      <span>Включена</span>
      <label class="switch"><input type="checkbox" id="atEnabled"><span class="slider"></span></label>
    </div>
    <label>Маржа на ОДИН вход, % от депозита</label>
    <input type="number" id="atPositionPct" step="0.5" min="0.1" max="100">
    <label>Risk %, влияет на плечо (плечо = risk% / SL%)</label>
    <input type="number" id="atRiskPct" step="0.5" min="0.1" max="100">
    <label>Максимум одновременных позиций (пусто = без ограничений)</label>
    <input type="number" id="atMaxConcurrent" step="1" min="1">
    <label>Потолок маржи при форс-минимуме (% депозита)</label>
    <input type="number" id="atMaxForcedMarginPct" step="1" min="0.1" max="100">
    <label>Потолок форс-минимума по кратности номинала (×)</label>
    <input type="number" id="atForcedSizeMaxMultiple" step="0.5" min="1" max="20">
    <p style="font-size:12px;color:#8b949e;margin:2px 0 0">Если 1 мин. контракт требует больше этого потолка ИЛИ больше чем в N раз номинальной маржи — сигнал пропускается, а не открывается ценой почти всего депозита.</p>
    <hr>
    <label style="color:#c9d1d9;font-weight:bold">&#128273; Gate.io API ключи</label>
    <p style="font-size:12px;color:#8b949e;margin:2px 0 0">Нужны для реальных ордеров. Оставьте пустыми, чтобы не менять сохранённые. Оба поля заполняются вместе.</p>
    <label>API key</label>
    <input type="text" id="atGateKey" placeholder="">
    <label>API secret</label>
    <input type="text" id="atGateSecret" placeholder="">
    <div class="row">
      <button onclick="closeSettings()" style="background:#21262d;border:1px solid #30363d">Отмена</button>
      <button onclick="saveSettings()">Сохранить</button>
    </div>
    <div id="atMsg" style="margin-top:8px;font-size:12px;color:#f85149"></div>
  </div>
</div>

<div id="alertModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:50;align-items:center;justify-content:center">
  <div class="card">
    <h3 style="margin-top:0">&#128276; Алерты (Telegram / ntfy)</h3>
    <label>Telegram bot token</label>
    <input type="text" id="alTgToken" placeholder="123456:AAаа...">
    <label>Telegram chat id</label>
    <input type="text" id="alTgChat" placeholder="-1001234567890">
    <label>ntfy.sh URL (необязательно, запасной канал)</label>
    <input type="text" id="alNtfyUrl" placeholder="https://ntfy.sh/my-topic">
    <div class="row">
      <button onclick="closeAlertSettings()" style="background:#21262d;border:1px solid #30363d">Отмена</button>
      <div>
        <button onclick="testAlert()" style="background:#1f6feb">Тест</button>
        <button onclick="saveAlertSettings()">Сохранить</button>
      </div>
    </div>
    <div id="alertMsg" style="margin-top:8px;font-size:12px;color:#f85149"></div>
  </div>
</div>

<div class="section-card">
  <details id="logDetails">
    <summary><h3 style="display:inline">&#128221; Логи</h3></summary>
    <div class="card log-box" id="logBox" style="margin-top:8px"></div>
  </details>
</div>

<div class="section-card">
  <h3>&#128293; Живые пампы (Pump Radar)</h3>
  <div id="pumpSummary" class="summary-line">пока не было срабатываний</div>
  <div class="table-scroll">
    <table id="pumps"><thead><tr><th>Монета</th><th>%</th><th>База → сейчас</th><th>Когда</th></tr></thead><tbody></tbody></table>
  </div>
</div>

<div class="section-card">
  <h3>&#128276; Живые EMA-инверт сигналы</h3>
  <div class="table-scroll">
    <table id="live"><thead><tr><th>Монета</th><th>ТФ</th><th>EMA</th><th>Направление</th><th>Цена</th><th>SL</th><th>TP</th><th>Bounce rate</th></tr></thead><tbody></tbody></table>
  </div>
</div>

<div class="section-card">
  <div class="section-head">
    <h3>&#128203; История EMA-сигналов</h3>
    <button onclick="clearHistory()" class="btn-danger-sm">Очистить историю</button>
  </div>
  <div id="histSummary" class="summary-line"></div>
  <div class="filter-row" id="histFilterRow">
    <button class="filter-btn active" data-f="all" onclick="setHistFilter('all')">Все</button>
    <button class="filter-btn" data-f="open" onclick="setHistFilter('open')">&#8987; Открытые</button>
    <button class="filter-btn" data-f="tp" onclick="setHistFilter('tp')">&#9989; TP</button>
    <button class="filter-btn" data-f="sl" onclick="setHistFilter('sl')">&#10060; SL</button>
  </div>
  <div class="table-scroll">
    <table id="history"><thead><tr><th>Монета</th><th>ТФ</th><th>EMA</th><th>Направление</th><th>Вход</th><th>SL</th><th>TP</th><th>Статус</th><th>Когда</th></tr></thead><tbody></tbody></table>
  </div>
  <div class="pager" id="historyPager"></div>
</div>

<details id="dossierBlock" class="section-card">
<summary>Досье по монетам (лучшая EMA на каждом ТФ, включая недельный) — <span id="dossierCount">0</span> строк</summary>
<div class="table-scroll">
  <table id="dossier"><thead><tr><th>Монета</th><th>Взлёт</th><th>ТФ</th><th>Лучшая EMA</th><th>Bounce rate</th></tr></thead><tbody></tbody></table>
</div>
<div class="pager" id="dossierPager"></div>
</details>
<script>
async function startScan(){
  const r = await fetch('/ema_dossier_start', {method:'POST'}); const d = await r.json();
  document.getElementById('status').innerText = d.msg;
}
async function clearHistory(){
  if(!confirm('Точно очистить всю историю сигналов? Открытые сделки тоже сотрутся, отменить нельзя.')) return;
  await fetch('/ema_signal_history_clear', {method:'POST'});
  refresh();
}
async function openSettings(){
  document.getElementById('atModal').style.display='flex';
  document.getElementById('atEnabled').disabled = true;
  await loadSettingsIntoModal();
  document.getElementById('atEnabled').disabled = false;
}
function closeSettings(){ document.getElementById('atModal').style.display='none'; }
async function loadSettingsIntoModal(){
  try{
    const r = await fetch('/ema_auto_trade_status'); const d = await r.json();
    document.getElementById('atEnabled').checked = !!d.enabled;
    document.getElementById('atPositionPct').value = d.position_pct;
    document.getElementById('atRiskPct').value = d.risk_pct;
    document.getElementById('atMaxConcurrent').value = d.max_concurrent ?? '';
    document.getElementById('atMaxForcedMarginPct').value = d.max_forced_margin_pct ?? 20;
    document.getElementById('atForcedSizeMaxMultiple').value = d.forced_size_max_multiple ?? 3;
  }catch(e){}
  try{
    const rg = await fetch('/gate_cfg'); const dg = await rg.json();
    document.getElementById('atGateKey').value = '';
    document.getElementById('atGateSecret').value = '';
    document.getElementById('atGateKey').placeholder = dg.has_key ? (dg.gate_key + ' (сохранён)') : 'API key';
    document.getElementById('atGateSecret').placeholder = dg.has_key ? '*** (сохранён)' : 'API secret';
  }catch(e){}
}
async function saveSettings(){
  const msg = document.getElementById('atMsg'); msg.innerText = '';
  const gateKey = document.getElementById('atGateKey').value.trim();
  const gateSecret = document.getElementById('atGateSecret').value.trim();
  if((gateKey && !gateSecret) || (!gateKey && gateSecret)){
    msg.innerText = 'Введите и ключ, и секрет Gate.io вместе (или оставьте оба пустыми)';
    return;
  }
  if(gateKey && gateSecret){
    const rg = await fetch('/gate_cfg', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({gate_key:gateKey, gate_secret:gateSecret})});
    const dg = await rg.json();
    if(!dg.ok){ msg.innerText = 'Ошибка сохранения ключей Gate.io'; return; }
  }
  const payload = {
    enabled: document.getElementById('atEnabled').checked,
    position_pct: parseFloat(document.getElementById('atPositionPct').value),
    risk_pct: parseFloat(document.getElementById('atRiskPct').value),
    max_concurrent: document.getElementById('atMaxConcurrent').value || null,
    max_forced_margin_pct: parseFloat(document.getElementById('atMaxForcedMarginPct').value),
    forced_size_max_multiple: parseFloat(document.getElementById('atForcedSizeMaxMultiple').value),
  };
  const r = await fetch('/ema_auto_trade_settings', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
  const d = await r.json();
  if(!d.ok){ msg.innerText = d.msg || 'Ошибка сохранения'; return; }
  closeSettings();
  refreshAutoTradeBox();
}
async function closeLivePosition(sym){
  if(!confirm(`Закрыть реальную позицию ${sym} маркетом прямо сейчас?`)) return;
  await fetch('/ema_auto_trade_close', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({symbol: sym})});
  refreshAutoTradeBox();
}

function openAlertSettings(){ document.getElementById('alertModal').style.display='flex'; loadAlertSettings(); }
function closeAlertSettings(){ document.getElementById('alertModal').style.display='none'; }
async function loadAlertSettings(){
  try{
    const r = await fetch('/alert_cfg'); const d = await r.json();
    document.getElementById('alTgToken').value = d.tg_token || '';
    document.getElementById('alTgChat').value  = d.tg_chat  || '';
    document.getElementById('alNtfyUrl').value = d.ntfy_url || '';
  }catch(e){}
}
async function saveAlertSettings(){
  const msg = document.getElementById('alertMsg'); msg.innerText = '';
  const payload = {
    tg_token: document.getElementById('alTgToken').value.trim(),
    tg_chat:  document.getElementById('alTgChat').value.trim(),
    ntfy_url: document.getElementById('alNtfyUrl').value.trim(),
  };
  const r = await fetch('/alert_cfg', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
  const d = await r.json();
  if(!d.ok){ msg.innerText = d.msg || 'Ошибка сохранения'; return; }
  closeAlertSettings();
}
async function saveAlertSettingsSync(){
  const payload = {
    tg_token: document.getElementById('alTgToken').value.trim(),
    tg_chat:  document.getElementById('alTgChat').value.trim(),
    ntfy_url: document.getElementById('alNtfyUrl').value.trim(),
  };
  const r = await fetch('/alert_cfg', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
  return r.json();
}
async function testAlert(){
  const msg = document.getElementById('alertMsg'); msg.style.color = '#8b949e'; msg.innerText = 'Отправляю...';
  const saved = await saveAlertSettingsSync();
  if(!saved.ok){ msg.style.color = '#f85149'; msg.innerText = saved.msg || 'Ошибка сохранения'; return; }
  const r = await fetch('/alert_test', {method:'POST'}); const d = await r.json();
  msg.style.color = d.ok ? '#3fb950' : '#f85149';
  msg.innerText = d.ok ? '✅ Тестовое сообщение отправлено' : ('Ошибка: ' + (d.error||''));
}

async function refreshAutoTradeBox(){
  try{
    const r = await fetch('/ema_auto_trade_status'); const d = await r.json();
    const box = document.getElementById('atBox');
    const stateTxt = d.enabled ? '🟢 включена' : '⚪ выключена';
    let rows = (d.live_positions||[]).map(p => {
      const cls = p.dir === 'long' ? 'long' : 'short';
      return `<tr><td>${p.symbol}</td><td class="${cls}">${p.dir.toUpperCase()}</td><td>${p.entry}</td><td>${p.sl}</td><td>${p.tp}</td><td>${p.size ?? ''}</td><td>${p.leverage ?? ''}×</td><td>${p.notional ?? ''}USDT</td>`
        + `<td><button style="margin:0;padding:3px 8px;font-size:11px;background:#3a1414;border:1px solid #f85149;color:#f85149" onclick="closeLivePosition('${p.symbol}')">Закрыть</button></td></tr>`;
    }).join('');
    box.innerHTML = `<b>Автоторговля:</b> ${stateTxt} &nbsp;|&nbsp; `
      + `маржа/вход ${d.position_pct}% &nbsp;|&nbsp; risk ${d.risk_pct}% &nbsp;|&nbsp; `
      + `потолок форс-маржи ${d.max_forced_margin_pct}% &nbsp;|&nbsp; `
      + `лимит позиций: ${d.max_concurrent ?? 'без ограничений'} &nbsp;|&nbsp; `
      + `открыто сейчас: <b>${d.live_count}</b>`
      + (d.balance != null ? ` &nbsp;|&nbsp; баланс: ${d.balance.toFixed(2)}USDT` : '')
      + (!d.gate_configured ? ` <span style="color:#f85149">— Gate.io ключи не настроены (/gate_cfg)</span>` : '')
      + (rows ? `<div class="table-scroll"><table style="margin-top:8px"><thead><tr><th>Монета</th><th>Напр.</th><th>Вход</th><th>SL</th><th>TP</th><th>Size</th><th>Плечо</th><th>~USDT</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>` : '');
  }catch(e){}
}
function fmtAgo(ts){
  const s = Math.floor(Date.now()/1000) - ts;
  if(s < 3600) return Math.floor(s/60)+'м назад';
  if(s < 86400) return Math.floor(s/3600)+'ч назад';
  return Math.floor(s/86400)+'д назад';
}
function fmtDuration(s){
  if(s < 60) return s+'с';
  if(s < 3600) return Math.floor(s/60)+'м';
  if(s < 86400) return Math.floor(s/3600)+'ч '+Math.floor((s%3600)/60)+'м';
  return Math.floor(s/86400)+'д '+Math.floor((s%86400)/3600)+'ч';
}
function renderPager(elId, page, totalPages, onChange){
  const el = document.getElementById(elId);
  if(totalPages <= 1){ el.innerHTML = ''; return; }
  el.innerHTML = `<button id="${elId}_prev" ${page<=0?'disabled':''}>&#8249; Назад</button>`
    + `<span>стр. ${page+1} из ${totalPages}</span>`
    + `<button id="${elId}_next" ${page>=totalPages-1?'disabled':''}>Вперёд &#8250;</button>`;
  document.getElementById(elId+'_prev').onclick = () => onChange(page-1);
  document.getElementById(elId+'_next').onclick = () => onChange(page+1);
}

const HIST_PAGE_SIZE = 15;
let histFilter = 'all', histPage = 0, histAllItems = [];
function setHistFilter(f){
  histFilter = f; histPage = 0;
  document.querySelectorAll('#histFilterRow .filter-btn').forEach(b => b.classList.toggle('active', b.dataset.f === f));
  renderHistoryTable();
}
function renderHistoryTable(){
  const filtered = histFilter === 'all' ? histAllItems : histAllItems.filter(it => it.status === histFilter);
  const totalPages = Math.max(1, Math.ceil(filtered.length / HIST_PAGE_SIZE));
  if(histPage >= totalPages) histPage = totalPages - 1;
  if(histPage < 0) histPage = 0;
  const pageItems = filtered.slice(histPage*HIST_PAGE_SIZE, histPage*HIST_PAGE_SIZE + HIST_PAGE_SIZE);
  const tbody3 = document.querySelector('#history tbody'); tbody3.innerHTML = '';
  const statusLabel = {open:'&#9203; Открыт', tp:'&#9989; TP', sl:'&#10060; SL'};
  for(const it of pageItems){
    const tr = document.createElement('tr');
    const dcls = it.dir === 'long' ? 'long' : 'short';
    const liveBadge = it.live ? '<span class="livebadge">LIVE</span>' : '';
    let pctStr = '', whenCell = fmtAgo(it.opened_at);
    if((it.status === 'tp' || it.status === 'sl') && it.close_price != null){
      const pct = it.dir === 'long'
        ? (it.close_price - it.price) / it.price * 100
        : (it.price - it.close_price) / it.price * 100;
      pctStr = ' ' + (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
      tr.classList.add('row-' + it.status);
      if(it.closed_at) whenCell += `<div class="dur">длилась ${fmtDuration(it.closed_at - it.opened_at)}</div>`;
    }
    const badge = `<span class="status-pill ${it.status}">${statusLabel[it.status]||it.status}${pctStr}</span>${liveBadge}`;
    tr.innerHTML = `<td>${it.symbol}</td><td>${it.tf}</td><td>EMA${it.ema_period}</td><td class="${dcls}">${it.dir.toUpperCase()}</td><td>${it.price}</td><td>${it.sl}</td><td>${it.tp}</td><td>${badge}</td><td>${whenCell}</td>`;
    tbody3.appendChild(tr);
  }
  renderPager('historyPager', histPage, totalPages, p => { histPage = p; renderHistoryTable(); });
}

const DOSSIER_PAGE_SIZE = 20;
let dossierPage = 0, dossierAllRows = [];
function renderDossierTable(){
  const totalPages = Math.max(1, Math.ceil(dossierAllRows.length / DOSSIER_PAGE_SIZE));
  if(dossierPage >= totalPages) dossierPage = totalPages - 1;
  if(dossierPage < 0) dossierPage = 0;
  const pageRows = dossierAllRows.slice(dossierPage*DOSSIER_PAGE_SIZE, dossierPage*DOSSIER_PAGE_SIZE + DOSSIER_PAGE_SIZE);
  const tbody = document.querySelector('#dossier tbody'); tbody.innerHTML = '';
  for(const row of pageRows){
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${row.sym}</td><td>${row.pumpLabel}</td><td>${row.tf}</td><td>EMA${row.ema}</td><td>${(row.rate*100).toFixed(0)}%</td>`;
    tbody.appendChild(tr);
  }
  renderPager('dossierPager', dossierPage, totalPages, p => { dossierPage = p; renderDossierTable(); });
}

async function refreshPumps(){
  try{
    const r = await fetch('/pump_status'); const d = await r.json();
    const tbody = document.querySelector('#pumps tbody'); tbody.innerHTML = '';
    const items = (d.recent || []).slice().reverse();
    document.getElementById('pumpSummary').innerHTML = items.length
      ? `отслеживается монет: <b>${d.tracked||0}</b> &nbsp;·&nbsp; сработало пампов: <b>${items.length}</b>`
      : `отслеживается монет: <b>${d.tracked||0}</b> &nbsp;·&nbsp; пока не было срабатываний`;
    for(const it of items.slice(0,30)){
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${it.symbol}</td><td class="long">+${it.pct}%</td><td>${it.base_price} -> ${it.last_price}</td><td>${fmtAgo(it.ts)}</td>`;
      tbody.appendChild(tr);
    }
  }catch(e){}
}

async function refresh(){
  try{
    const r2 = await fetch('/ema_live_signals'); const d2 = await r2.json();
    const tbody2 = document.querySelector('#live tbody'); tbody2.innerHTML = '';
    for(const [sym, sig] of Object.entries(d2.signals||{})){
      const tr = document.createElement('tr');
      const cls = sig.dir === 'long' ? 'long' : 'short';
      tr.innerHTML = `<td>${sym}</td><td>${sig.tf}</td><td>EMA${sig.ema_period}</td><td class="${cls}">${sig.dir.toUpperCase()}</td><td>${sig.price}</td><td>${sig.sl}</td><td>${sig.tp}</td><td>${(sig.bounce_rate*100).toFixed(0)}%</td>`;
      tbody2.appendChild(tr);
    }

    const r3 = await fetch('/ema_signal_history'); const d3 = await r3.json();
    document.getElementById('histSummary').innerHTML = d3.closed
      ? `закрыто <b>${d3.closed}</b> &nbsp;·&nbsp; <span class="tp">&#9989; TP ${d3.tp}</span> &nbsp;·&nbsp; <span class="sl">&#10060; SL ${d3.sl}</span> &nbsp;·&nbsp; винрейт <b>${d3.winrate}%</b>`
      : 'пока нет закрытых сигналов';
    histAllItems = d3.items || [];
    renderHistoryTable();

    const r = await fetch('/ema_dossier_status'); const d = await r.json();
    document.getElementById('status').innerText =
      `EMA-досье: ${d.status||'idle'}  |  ${d.done||0}/${d.total||0} монет`;
    const rows = [];
    for(const [sym, entry] of Object.entries(d.results||{})){
      if(entry.error){ continue; }
      const pumpLabel = entry.recent_pump ? ('&#128293; +'+entry.pump_pct+'%') : '';
      for(const [tf, tfd] of Object.entries(entry.by_tf||{})){
        if(!tfd.best_ema) continue;
        rows.push({sym, pumpLabel, tf, ema: tfd.best_ema, rate: tfd.best_bounce_rate});
      }
    }
    rows.sort((a,b) => b.rate - a.rate);
    document.getElementById('dossierCount').innerText = rows.length;
    dossierAllRows = rows;
    renderDossierTable();
  }catch(e){}
}
let emaLogsTotal = 0;
async function pollEmaLogs(){
  try{
    const r = await fetch('/ema_logs', {cache:'no-store'});
    const d = await r.json();
    const dropped = d.logs_dropped || 0;
    const totalNow = dropped + (d.logs||[]).length;
    const newFrom = Math.max(0, emaLogsTotal - dropped);
    const newLogs = (d.logs||[]).slice(newFrom);
    const lb = document.getElementById('logBox');
    const wasOpen = document.getElementById('logDetails').open;
    const atBottom = !wasOpen || (lb.scrollTop + lb.clientHeight >= lb.scrollHeight - 4);
    newLogs.forEach(l => {
      const div = document.createElement('div');
      div.className = 'log-line';
      div.innerHTML = `<span style="color:#555">[${l.ts}]</span> ${l.msg}`;
      lb.appendChild(div);
    });
    while(lb.children.length > 300) lb.removeChild(lb.firstChild);
    if(newLogs.length && atBottom) lb.scrollTop = lb.scrollHeight;
    emaLogsTotal = totalNow;
  }catch(e){}
}

pollEmaLogs(); setInterval(pollEmaLogs, 4000);
refresh(); setInterval(refresh, 5000);
refreshAutoTradeBox(); setInterval(refreshAutoTradeBox, 5000);
refreshPumps(); setInterval(refreshPumps, 8000);
</script></body></html>"""
# ─── конец live EMA-сигналов ────────────────────────────────────────────────
# ─── конец EMA-bounce dossier engine ────────────────────────────────────────
# ─── Telegram / ntfy ────────────────────────────────────────────────────────
def _send_alert(msg):
    """Отправка алерта в Telegram и/или ntfy. Запускается в фоновом потоке.
    При сетевой ошибке — 3 попытки с паузой 5с."""
    def _do_send():
        for attempt in range(1, 4):
            sent = False
            if TG_TOKEN and TG_CHAT:
                try:
                    r = requests.post(
                        f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                        json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
                        timeout=8)
                    if r.ok:
                        sent = True
                    else:
                        olog(f"⚠ TG алерт HTTP {r.status_code} (попытка {attempt}/3)")
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout) as e:
                    olog(f"⚠ TG алерт сеть (попытка {attempt}/3): {e}")
                except Exception as e:
                    olog(f"⚠ TG алерт: {e}")
                    break
            if NTFY_URL:
                try:
                    requests.post(NTFY_URL, data=msg.encode(), timeout=8)
                    sent = True
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout) as e:
                    olog(f"⚠ ntfy алерт сеть (попытка {attempt}/3): {e}")
                except Exception as e:
                    olog(f"⚠ ntfy алерт: {e}")
                    break
            if sent or attempt == 3:
                break
            time.sleep(5)
    threading.Thread(target=_do_send, daemon=True).start()

def _render_signal_chart_png(candles, sig, sym, tf):
    """Скриншот сделки (EMA-сигнал) для Telegram — своя картинка, отдельная
    от Pump Radar (_render_pump_chart_png ниже)."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        return None
    try:
        if not candles:
            return None
        entry_i = sig.get("entry_i")
        if entry_i is None or not (0 <= entry_i < len(candles)):
            entry_i = len(candles) - 1
        lo = max(0, entry_i - 50)
        hi = min(len(candles), entry_i + 16)
        window = candles[lo:hi]
        if len(window) < 2:
            return None

        def _g(c, *keys):
            for k in keys:
                if k in c: return c[k]
            return 0

        highs  = [_g(c, "high", "h") for c in window]
        lows   = [_g(c, "low", "l") for c in window]
        entry, sl, tp = sig.get("entry"), sig.get("sl"), sig.get("tp")
        vals = highs + lows + [v for v in (entry, sl, tp) if v]
        vmax, vmin = max(vals), min(vals)
        if vmax <= vmin:
            vmax = vmin + max(abs(vmin) * 0.001, 1e-9)

        W, H = 960, 540
        pad_l, pad_r, pad_t, pad_b = 16, 90, 46, 30
        bg, grid, txt = (13, 13, 15), (40, 40, 44), (220, 220, 220)
        green, red, white = (8, 153, 129), (242, 54, 69), (235, 235, 235)

        img = Image.new("RGB", (W, H), bg)
        d = ImageDraw.Draw(img)
        try:
            font = ImageFont.load_default()
        except Exception:
            font = None

        def y_of(v):
            return pad_t + (vmax - v) / (vmax - vmin) * (H - pad_t - pad_b)

        n = len(window)
        cw = (W - pad_l - pad_r) / n

        for gy in range(5):
            yy = pad_t + gy * (H - pad_t - pad_b) / 4
            d.line([(pad_l, yy), (W - pad_r, yy)], fill=grid, width=1)

        for i, c in enumerate(window):
            o, h, l, cl = _g(c, "open", "o"), _g(c, "high", "h"), _g(c, "low", "l"), _g(c, "close", "c")
            x = pad_l + i * cw
            w = max(cw * 0.6, 1)
            color = green if cl >= o else red
            xc = x + w / 2
            d.line([(xc, y_of(h)), (xc, y_of(l))], fill=color, width=2)
            ytop, ybot = sorted([y_of(o), y_of(cl)])
            if ybot - ytop < 1.5:
                ybot = ytop + 1.5
            d.rectangle([x, ytop, x + w, ybot], fill=color)

        def hline(v, color, label):
            if not v:
                return
            yy = y_of(v)
            xseg = pad_l
            while xseg < W - pad_r:
                d.line([(xseg, yy), (min(xseg + 8, W - pad_r), yy)], fill=color, width=1)
                xseg += 14
            d.text((W - pad_r + 4, yy - 6), label, fill=color, font=font)

        hline(entry, white, f"E {entry:.6g}" if entry else "")
        hline(sl,    red,   f"SL {sl:.6g}" if sl else "")
        hline(tp,    green, f"TP {tp:.6g}" if tp else "")

        title = f"{sym} {tf} — {str(sig.get('dir','')).upper()}"
        d.text((10, 8), title, fill=txt, font=font)

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        olog(f"⚠ Рендер скриншота сигнала: {e}")
        return None

def _send_alert_photo(png_bytes, caption):
    """Как _send_alert, но с картинкой (Telegram sendPhoto). Если png_bytes
    нет — тихо откатывается на обычный текстовый _send_alert."""
    if not png_bytes:
        _send_alert(caption)
        return
    def _do_send():
        if TG_TOKEN and TG_CHAT:
            try:
                r = requests.post(
                    f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto",
                    data={"chat_id": TG_CHAT, "caption": caption, "parse_mode": "HTML"},
                    files={"photo": ("signal.png", png_bytes, "image/png")},
                    timeout=20)
                if not r.ok:
                    olog(f"⚠ TG фото HTTP {r.status_code} — шлю текстом")
                    _send_alert(caption)
            except Exception as e:
                olog(f"⚠ TG фото: {e} — шлю текстом")
                _send_alert(caption)
        else:
            _send_alert(caption)
    threading.Thread(target=_do_send, daemon=True).start()


# ─── v0.1.11: Live Pump Detector ────────────────────────────────────────────
# Отдельный, НЕЗАВИСИМЫЙ от EMA-инверт-сигналов детектор. Не ищет вход —
# только ловит резкий рост цены за короткое окно и шлёт в Telegram картинку
# в стиле стороннего скринера, который прислали как референс: чёрная линия
# цены с точками + красно-зелёная гистограмма объёма на заднем плане
# (зелёная свеча = объём зелёным, красная = красным), сплошная красная
# горизонтальная линия — базовая цена, от которой считается % пампа,
# синие штрихованные линии — сетка цены. Это первый этап будущего пайплайна
# "памп → откат → вход по EMA" — сам вход по EMA сюда сознательно не
# подключён, это отдельная следующая задача.
#
# Дешёвый источник цен: ОДИН запрос /futures/usdt/tickers на весь топ
# (как в _fetch_all_symbols) раз в PUMP_DETECT_POLL_SEC, а не по одному
# тикеру на монету — иначе скан топ-100 каждые 60с означал бы 100
# HTTP-запросов в минуту только на детектор, поверх всего остального.
# Скользящая история цен по каждой монете держится в памяти
# (_pump_price_history), 1m-свечи для самой картинки запрашиваются только
# В МОМЕНТ срабатывания алерта — не для всех монет постоянно.

PUMP_DETECT_TF            = "1m"
PUMP_CHART_WINDOW_MIN     = 120   # сколько минут истории показываем на картинке (как на референсе ~11:30-13:30)
PUMP_MOVE_WINDOW_MIN      = 20    # окно, по которому считаем % пампа: (last - min_in_window)/min_in_window
PUMP_THRESHOLD_PCT        = 5.0   # порог срабатывания
PUMP_DETECT_POLL_SEC      = 60    # как часто обновляем цены (один общий /tickers запрос)
PUMP_DETECT_COOLDOWN_SEC  = 30 * 60   # не повторять алерт по той же монете чаще, пока памп длится
PUMP_DETECT_TOP_N         = 100   # сколько монет по объёму отслеживаем (см. _fetch_all_symbols)
PUMP_DETECT_HISTORY_FILE  = os.path.expanduser("~/pumpradar_pump_history.json")  # необязательный лог сработавших пампов

_pump_detect_lock   = threading.Lock()
_pump_price_history = {}   # symbol -> [(ts, price), ...] по возрастанию времени
_pump_detect_state  = {}   # symbol -> {"last_alert_ts": float, "last_pct": float}


def _pump_fetch_tickers_snapshot():
    """Один запрос ко всем тикерам Gate.io Futures USDT — даёт last price
    и 24h-объём разом по всем контрактам. Так же дёшево, как один вызов
    _fetch_all_symbols, но нам тут нужна ещё и сама цена (last), которую
    _fetch_all_symbols не возвращает наружу."""
    try:
        r = requests.get(f"{GATE_API}/futures/usdt/tickers", timeout=15)
        if r.status_code != 200:
            return {}
        data = r.json()
        if not isinstance(data, list):
            return {}
        out = {}
        for t in data:
            if not isinstance(t, dict):
                continue
            contract = t.get("contract", "")
            if "_USDT" not in contract:
                continue
            try:
                px = float(t.get("last") or t.get("mark_price") or 0)
                vol = float(t.get("volume_24h_usd") or t.get("volume_24h_quote")
                             or t.get("volume_24h") or 0)
            except (TypeError, ValueError):
                continue
            if px <= 0:
                continue
            out[contract] = {"price": px, "vol24": vol}
        return out
    except Exception as e:
        olog(f"[pump_detect] ошибка запроса tickers: {e}")
        return {}


def _pump_update_history(snapshot, top_symbols):
    """Дописывает текущую цену в скользящую историю каждой отслеживаемой
    монеты и обрезает всё, что старше PUMP_CHART_WINDOW_MIN (+запас).
    Монеты, выпавшие из топа по объёму, из истории удаляются — иначе
    словарь рос бы неограниченно по мере ротации топа за дни/недели."""
    now = time.time()
    cutoff = now - (PUMP_CHART_WINDOW_MIN + 10) * 60
    top_set = set(top_symbols)
    with _pump_detect_lock:
        for sym in top_symbols:
            info = snapshot.get(sym)
            if not info:
                continue
            hist = _pump_price_history.setdefault(sym, [])
            hist.append((now, info["price"]))
            while hist and hist[0][0] < cutoff:
                hist.pop(0)
        stale = [s for s in _pump_price_history if s not in top_set]
        for s in stale:
            del _pump_price_history[s]


def _pump_detect_check(symbol):
    """Возвращает {"base_price":..,"last_price":..,"pct":..} если за
    последние PUMP_MOVE_WINDOW_MIN минут цена выросла от своего минимума
    в этом окне на PUMP_THRESHOLD_PCT% и больше — иначе None. Базовая цена
    намеренно берётся как МИНИМУМ в коротком недавнем окне (а не самая
    старая точка всего графика) — так же, как на референсном скрине: %
    считается от цены прямо перед разгоном, а не от начала всего показанного
    двухчасового окна (оно там чисто для визуального контекста)."""
    now = time.time()
    with _pump_detect_lock:
        hist = list(_pump_price_history.get(symbol, ()))
    if len(hist) < 3:
        return None
    move_cutoff = now - PUMP_MOVE_WINDOW_MIN * 60
    window = [(t, p) for t, p in hist if t >= move_cutoff]
    if len(window) < 3:
        return None
    base_t, base_p = min(window, key=lambda tp: tp[1])
    last_t, last_p = hist[-1]
    if base_p <= 0:
        return None
    pct = (last_p - base_p) / base_p * 100.0
    if pct < PUMP_THRESHOLD_PCT:
        return None
    return {"base_price": base_p, "last_price": last_p, "pct": round(pct, 2)}


PUMP_RENDER_PARAMS_FILE = os.path.expanduser("~/pumpradar_render_params.json")
# v0.2.0: параметры стиля отрисовки гистограммы теперь настраиваются, а не
# зашиты намертво — подбираются через /pump_match (см. секцию Pump Match
# ниже) и применяются здесь же, к РЕАЛЬНЫМ алертам, без перезапуска процесса.
_pump_render_params = {"volume_mode": "directional", "ema_period": 3, "floor_frac": 0.15}

def _load_pump_render_params():
    global _pump_render_params
    try:
        with open(PUMP_RENDER_PARAMS_FILE) as f:
            saved = json.load(f)
        _pump_render_params.update({k: saved[k] for k in
                                     ("volume_mode", "ema_period", "floor_frac") if k in saved})
        olog(f"[pump_match] загружены сохранённые параметры рендера: {_pump_render_params}")
    except Exception:
        pass  # первый запуск — файла ещё нет, живём с дефолтами выше

def _save_pump_render_params(params):
    global _pump_render_params
    _pump_render_params.update({k: params[k] for k in
                                 ("volume_mode", "ema_period", "floor_frac") if k in params})
    try:
        with open(PUMP_RENDER_PARAMS_FILE, "w") as f:
            json.dump(_pump_render_params, f)
        olog(f"[pump_match] новые параметры рендера сохранены и применены: {_pump_render_params}")
    except Exception as e:
        olog(f"[pump_match] ⚠ не смог сохранить параметры рендера: {e}")


def _pm_ema(arr, period):
    """EMA как везде в файле, но period<=1 означает 'без сглаживания'
    (просто вернуть исходный массив) — используется сеткой перебора, где
    period=1 это одна из проверяемых гипотез."""
    if not arr:
        return []
    if period <= 1:
        return list(arr)
    k = 2.0 / (period + 1)
    out = [arr[0]]
    for v in arr[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def _render_pump_chart_variant(candles, base_price, volume_mode="directional",
                                ema_period=3, floor_frac=0.15):
    """Параметризованная версия рендера — используется и живыми алертами
    (через _render_pump_chart_png ниже, с сохранёнными параметрами), и
    перебором /pump_match (с разными параметрами на кандидата). Требует
    Pillow — если не установлен, возвращает None.
      volume_mode: "directional" — объём разбит на зелёный/красный по
                   направлению свечи (bull/bear), "raw" — весь объём в
                   обоих цветах поровну (без разбивки по направлению).
      ema_period:  сглаживание объёма перед отрисовкой (1 = без сглаживания).
      floor_frac:  ось объёма НЕ от нуля — начинается с floor_frac*vmax,
                   поэтому заливка не пропадает полностью в затишье
                   (0 = ось от нуля, как в самой первой версии).
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        return None
    if not candles or len(candles) < 3:
        return None

    W, H = 960, 540
    pad_l, pad_r, pad_t, pad_b = 60, 60, 24, 40
    bg           = (255, 255, 255)
    grid_color   = (70, 70, 150)
    line_color   = (15, 15, 15)
    green        = (46, 160, 90)
    red          = (214, 58, 58)
    baseline_col = (206, 28, 28)
    text_color   = (50, 50, 50)

    closes = [c["close"] for c in candles]
    opens  = [c["open"] for c in candles]
    vols   = [c.get("vol", 0) or 0 for c in candles]
    times  = [c["t"] for c in candles]
    n = len(candles)

    pmin = min(closes + [base_price])
    pmax = max(closes + [base_price])
    if pmax <= pmin:
        pmax = pmin + max(abs(pmin) * 0.001, 1e-9)

    if volume_mode == "raw":
        green_raw = list(vols)
        red_raw   = list(vols)
    else:  # "directional"
        green_raw = [vols[i] if closes[i] >= opens[i] else 0.0 for i in range(n)]
        red_raw   = [vols[i] if closes[i] <  opens[i] else 0.0 for i in range(n)]
    green_s = _pm_ema(green_raw, ema_period)
    red_s   = _pm_ema(red_raw, ema_period)
    vmax = max(max(green_s, default=1.0), max(red_s, default=1.0)) or 1.0
    axis_min = vmax * max(0.0, min(0.9, floor_frac))

    img = Image.new("RGB", (W, H), bg)
    try:
        font = ImageFont.load_default()
    except Exception:
        font = None

    plot_w = W - pad_l - pad_r
    plot_h = H - pad_t - pad_b

    def x_of(i):
        return pad_l + (i / (n - 1) * plot_w if n > 1 else 0)

    def y_price(v):
        return pad_t + (pmax - v) / (pmax - pmin) * plot_h

    def y_vol(v):
        vv = max(v, axis_min)
        denom = (vmax - axis_min) or 1.0
        return pad_t + (1 - (vv - axis_min) / denom) * plot_h

    floor_y = pad_t + plot_h

    d = ImageDraw.Draw(img)
    for k in range(5):
        v = pmin + (pmax - pmin) * k / 4.0
        yy = y_price(v)
        xx = pad_l
        while xx < W - pad_r:
            d.line([(xx, yy), (min(xx + 6, W - pad_r), yy)], fill=grid_color, width=1)
            xx += 10
        d.text((4, yy - 6), f"{v:.6g}", fill=text_color, font=font)

    overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    for series, color in ((red_s, red), (green_s, green)):
        pts = [(x_of(i), y_vol(series[i])) for i in range(n)]
        poly = [(x_of(0), floor_y)] + pts + [(x_of(n - 1), floor_y)]
        od.polygon(poly, fill=color + (95,))
        od.line(pts, fill=color + (230,), width=2)
    img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
    d = ImageDraw.Draw(img)

    by = y_price(base_price)
    d.line([(pad_l, by), (W - pad_r, by)], fill=baseline_col, width=2)
    d.text((W - pad_r + 4, by - 6), f"{base_price:.6g}", fill=baseline_col, font=font)

    pts = [(x_of(i), y_price(closes[i])) for i in range(n)]
    d.line(pts, fill=line_color, width=2)
    for (xx, yy) in pts:
        d.ellipse([xx - 2, yy - 2, xx + 2, yy + 2], fill=line_color)

    step = max(1, n // 10)
    for i in range(0, n, step):
        xx = x_of(i)
        lbl = time.strftime("%H:%M", time.localtime(times[i]))
        d.text((xx - 14, H - pad_b + 6), lbl, fill=text_color, font=font)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _render_pump_chart_png(candles, base_price):
    """Обёртка для живых алертов — рисует текущими сохранёнными параметрами
    (см. _pump_render_params / /pump_match). Сигнатура не меняется, чтобы
    не трогать _pump_fire_alert."""
    p = _pump_render_params
    return _render_pump_chart_variant(candles, base_price,
                                       volume_mode=p.get("volume_mode", "directional"),
                                       ema_period=p.get("ema_period", 3),
                                       floor_frac=p.get("floor_frac", 0.15))


def _pump_fire_alert(symbol, res):
    """Тянет 1m-свечи за последнее PUMP_CHART_WINDOW_MIN окно, рисует
    картинку и шлёт в Telegram/ntfy (через уже существующий
    _send_alert_photo — если Pillow недоступен/рендер не удался, тот сам
    откатится на обычный текстовый _send_alert, ничего не потеряется)."""
    candles = []
    try:
        days = max(1, math.ceil(PUMP_CHART_WINDOW_MIN * 60 / 86400) + 1)
        raw = _fetch_candles(symbol, PUMP_DETECT_TF, days)
        cutoff = time.time() - PUMP_CHART_WINDOW_MIN * 60
        candles = [c for c in raw if c["t"] >= cutoff]
    except Exception as e:
        olog(f"[pump_detect] {symbol}: не смог получить свечи для графика: {e}")

    png = None
    try:
        png = _render_pump_chart_png(candles, res["base_price"])
    except Exception as e:
        olog(f"[pump_detect] {symbol}: ошибка рендера графика: {e}")

    caption = (f"<b>{symbol}</b>\n"
               f"Pump: {res['pct']}%\n"
               f"{_fmt_px(res['base_price'])} -> {_fmt_px(res['last_price'])}")
    _send_alert_photo(png, caption)
    olog(f"[pump_detect] 🔥 {symbol}: памп {res['pct']}% "
         f"({res['base_price']:.6g} → {res['last_price']:.6g}) — алерт отправлен")

    # необязательный лёгкий лог сработавших пампов на диск — не критично,
    # ошибки записи не должны валить основной цикл детектора
    try:
        rec = {"ts": int(time.time()), "symbol": symbol, "pct": res["pct"],
               "base_price": res["base_price"], "last_price": res["last_price"]}
        hist = []
        if os.path.exists(PUMP_DETECT_HISTORY_FILE):
            with open(PUMP_DETECT_HISTORY_FILE) as f:
                hist = json.load(f)
        hist.append(rec)
        hist = hist[-200:]   # не растим файл бесконечно
        with open(PUMP_DETECT_HISTORY_FILE, "w") as f:
            json.dump(hist, f)
    except Exception as e:
        olog(f"[pump_detect] ⚠ не смог записать {PUMP_DETECT_HISTORY_FILE}: {e}")


def _pump_detect_loop():
    """Фоновый цикл: раз в PUMP_DETECT_POLL_SEC — один запрос /tickers,
    обновление скользящей истории цен по топ-N монетам, проверка каждой на
    памп, алерт с картинкой при первом срабатывании и затем не чаще, чем
    раз в PUMP_DETECT_COOLDOWN_SEC по той же монете (пока памп продолжается,
    иначе спамило бы каждую минуту, пока цена не откатится вниз за базу)."""
    time.sleep(20)
    while True:
        try:
            snapshot = _pump_fetch_tickers_snapshot()
            if snapshot:
                top_symbols = sorted(snapshot.keys(),
                                      key=lambda s: snapshot[s]["vol24"],
                                      reverse=True)[:PUMP_DETECT_TOP_N]
                _pump_update_history(snapshot, top_symbols)
                now = time.time()
                for sym in top_symbols:
                    res = _pump_detect_check(sym)
                    if not res:
                        continue
                    with _pump_detect_lock:
                        last_alert = _pump_detect_state.get(sym, {}).get("last_alert_ts", 0)
                    if now - last_alert < PUMP_DETECT_COOLDOWN_SEC:
                        continue
                    _pump_fire_alert(sym, res)
                    with _pump_detect_lock:
                        _pump_detect_state[sym] = {"last_alert_ts": now, "last_pct": res["pct"]}
        except Exception as e:
            olog(f"[pump_detect] ошибка цикла: {e}")
        time.sleep(PUMP_DETECT_POLL_SEC)
# ─── конец Live Pump Detector ───────────────────────────────────────────────

def _load_alert_cfg():
    """Подхватывает сохранённые TG/ntfy настройки из файла (приоритет над env)."""
    global TG_TOKEN, TG_CHAT, NTFY_URL, WATCHDOG_ENABLED, WATCHDOG_TIMEOUT_MIN, HC_URL
    try:
        with open(ALERT_CFG_PATH, "r") as f:
            cfg = json.load(f)
        TG_TOKEN = cfg.get("tg_token", TG_TOKEN) or TG_TOKEN
        TG_CHAT  = cfg.get("tg_chat",  TG_CHAT)  or TG_CHAT
        NTFY_URL = cfg.get("ntfy_url", NTFY_URL) or NTFY_URL
        HC_URL   = cfg.get("hc_url",   HC_URL)   or HC_URL
        if "watchdog_enabled" in cfg:
            WATCHDOG_ENABLED = bool(cfg.get("watchdog_enabled"))
        if "watchdog_timeout_min" in cfg:
            try:
                WATCHDOG_TIMEOUT_MIN = max(5, min(1440, int(cfg.get("watchdog_timeout_min"))))
            except (TypeError, ValueError):
                pass
    except FileNotFoundError:
        pass
    except Exception as e:
        olog(f"⚠ Не удалось прочитать {ALERT_CFG_PATH}: {e}")

def _save_alert_cfg():
    try:
        with open(ALERT_CFG_PATH, "w") as f:
            json.dump({"tg_token": TG_TOKEN, "tg_chat": TG_CHAT, "ntfy_url": NTFY_URL,
                       "hc_url": HC_URL,
                       "watchdog_enabled": WATCHDOG_ENABLED,
                       "watchdog_timeout_min": WATCHDOG_TIMEOUT_MIN}, f)
    except Exception as e:
        olog(f"⚠ Не удалось сохранить {ALERT_CFG_PATH}: {e}")

def _test_alert():
    """Шлёт тестовое уведомление и честно проверяет, дошло ли оно."""
    if not ((TG_TOKEN and TG_CHAT) or NTFY_URL):
        return False, "Не заданы TG_TOKEN+TG_CHAT или NTFY_URL"
    msg = "✅ Pump Radar: тестовое уведомление. Если ты это видишь — алерты настроены верно."
    ok_any, errs = False, []
    if TG_TOKEN and TG_CHAT:
        try:
            r = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id":TG_CHAT,"text":msg,"parse_mode":"HTML"}, timeout=8)
            j = r.json()
            if j.get("ok"):
                ok_any = True
            else:
                errs.append("Telegram: " + j.get("description","неизвестная ошибка"))
        except Exception as e:
            errs.append(f"Telegram: {e}")
    if NTFY_URL:
        try:
            r = requests.post(NTFY_URL, data=msg.encode(), timeout=8)
            if r.status_code < 300:
                ok_any = True
            else:
                errs.append(f"ntfy: HTTP {r.status_code}")
        except Exception as e:
            errs.append(f"ntfy: {e}")
    return ok_any, ("; ".join(errs) if errs else None)

# ─── Watchdog пропажи интернета ──────────────────────────────────────────────
_watchdog_lock       = threading.Lock()
_watchdog_down_since = None
_watchdog_alerted    = False
_watchdog_last_alert = None
WATCHDOG_CHECK_SEC   = 60

def _check_connectivity():
    try:
        r = requests.get(f"{GATE_API}/futures/usdt/contracts/BTC_USDT", timeout=6)
        return r.ok
    except Exception:
        return False

def _watchdog_loop():
    global _watchdog_down_since, _watchdog_alerted, _watchdog_last_alert
    while True:
        time.sleep(WATCHDOG_CHECK_SEC)
        if not WATCHDOG_ENABLED:
            continue
        online = _check_connectivity()
        now = time.time()
        with _watchdog_lock:
            if online:
                if _watchdog_alerted and _watchdog_down_since:
                    down_min = max(1, int((now - _watchdog_down_since) / 60))
                    _send_alert(f"🟢 Связь восстановлена. Не было интернета/Gate.io ~{down_min} мин.")
                    olog(f"🟢 watchdog: связь восстановлена, простой ~{down_min} мин")
                _watchdog_down_since = None
                _watchdog_alerted    = False
                _watchdog_last_alert = None
            else:
                if _watchdog_down_since is None:
                    _watchdog_down_since = now
                    olog("⚠ watchdog: нет связи с интернетом/Gate.io — таймер запущен")
                    continue
                elapsed_min = (now - _watchdog_down_since) / 60
                if elapsed_min < WATCHDOG_TIMEOUT_MIN:
                    continue
                if (not _watchdog_alerted) or \
                   (now - (_watchdog_last_alert or 0) >= WATCHDOG_TIMEOUT_MIN * 60):
                    _send_alert(f"🔴 Нет связи с интернетом/Gate.io уже {int(elapsed_min)} мин.")
                    _watchdog_alerted    = True
                    _watchdog_last_alert = now

# ─── Heartbeat healthchecks.io ────────────────────────────────────────────────
HEARTBEAT_INTERVAL_SEC = 300

def _heartbeat_loop():
    while True:
        time.sleep(HEARTBEAT_INTERVAL_SEC)
        if not HC_URL:
            continue
        try:
            requests.get(HC_URL, timeout=10)
        except Exception:
            pass


def _fmt_px(v):
    if v is None: return "—"
    if v >= 100:  return f"{v:.2f}"
    if v >= 1:    return f"{v:.4f}"
    return f"{v:.6f}".rstrip("0").rstrip(".")

def _rss_mb():
    """Текущий RSS процесса в МБ."""
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    except Exception:
        return None

def _shutdown_pool_safely(pool):
    """Явный terminate() всех ещё живых дочерних процессов после shutdown,
    плюс gc.collect() (только для ProcessPool)."""
    try:
        pool.shutdown(wait=True, cancel_futures=True)
    except Exception as e:
        olog(f"⚠ pool.shutdown: {e}")
    if _POOL_TYPE == "process":
        try:
            for proc in (getattr(pool, "_processes", {}) or {}).values():
                if proc.is_alive():
                    olog(f"⚠ pool respawn: воркер pid={proc.pid} не завершился штатно — terminate()")
                    proc.terminate()
                    proc.join(timeout=3)
        except Exception as e:
            olog(f"⚠ pool cleanup: {e}")
    import gc
    gc.collect()

# ─── Pump Match: авто-подбор параметров рендера по скрину-эталону ─────────
# Веб-страница /pump_match: загружаешь скрин чужого бота + монету/дату/окно
# времени → тянем РЕАЛЬНЫЕ свечи с Gate.io за это окно → перебираем сетку
# параметров рендера (_render_pump_chart_variant) → каждый вариант сверяем
# со скрином по цветовому профилю (HSV, устойчиво к сжатию JPEG и блёклым
# полупрозрачным заливкам) → лучший вариант СРАЗУ сохраняется в
# PUMP_RENDER_PARAMS_FILE и с этого момента используется в реальных алертах
# (см. _render_pump_chart_png выше — она читает эти же сохранённые параметры).

PUMP_MATCH_GRID_VOLUME_MODES = ["directional", "raw"]
PUMP_MATCH_GRID_EMA_PERIODS  = [1, 2, 3, 5, 8]     # 1 = без сглаживания
PUMP_MATCH_GRID_FLOOR_FRACS  = [0.0, 0.10, 0.15, 0.25]


def _pm_parse_multipart(body_bytes, content_type_header):
    """Разбирает multipart/form-data через email-парсер стандартной
    библиотеки (оборачиваем тело в псевдо-письмо с тем же Content-Type) —
    надёжнее, чем ручной разбор boundary руками, и не тянет отдельных
    зависимостей. Возвращает {field_name: (raw_bytes, filename_or_None)}."""
    import email
    from email import policy as _email_policy
    from email.parser import BytesParser
    header = f"Content-Type: {content_type_header}\r\nMIME-Version: 1.0\r\n\r\n".encode()
    msg = BytesParser(policy=_email_policy.default).parsebytes(header + body_bytes)
    fields = {}
    if not msg.is_multipart():
        return fields
    for part in msg.iter_parts():
        cd = part.get("Content-Disposition", "") or ""
        name = None
        filename = None
        for piece in cd.split(";"):
            piece = piece.strip()
            if piece.startswith("name="):
                name = piece.split("=", 1)[1].strip('"')
            elif piece.startswith("filename="):
                filename = piece.split("=", 1)[1].strip('"')
        if name is None:
            continue
        payload = part.get_payload(decode=True)
        fields[name] = (payload, filename)
    return fields


def _pm_autocrop_chart(img, bright_thresh=200, row_frac_thresh=0.5, col_frac_thresh=0.5):
    """Находит ограничивающий прямоугольник самой большой яркой (белой)
    прямоугольной области на скрине — это и есть карточка с графиком на
    тёмном фоне Telegram. Возвращает None, если ничего похожего не нашлось
    (тогда вызывающий код сравнивает по всему присланному изображению)."""
    img = img.convert("RGB")
    W, H = img.size
    px = img.load()
    step = max(1, min(W, H) // 300)

    bright_rows = []
    for y in range(0, H, step):
        cnt = 0
        tot = 0
        for x in range(0, W, step):
            r, g, b = px[x, y]
            if (r + g + b) / 3 >= bright_thresh:
                cnt += 1
            tot += 1
        if tot and cnt / tot >= row_frac_thresh:
            bright_rows.append(y)
    if not bright_rows:
        return None
    top, bottom = min(bright_rows), max(bright_rows)

    bright_cols = []
    for x in range(0, W, step):
        cnt = 0
        tot = 0
        for y in range(top, bottom + 1, step):
            r, g, b = px[x, y]
            if (r + g + b) / 3 >= bright_thresh:
                cnt += 1
            tot += 1
        if tot and cnt / tot >= col_frac_thresh:
            bright_cols.append(x)
    if not bright_cols:
        return None
    left, right = min(bright_cols), max(bright_cols)

    if (right - left) * (bottom - top) / (W * H) < 0.05:
        return None
    return img.crop((left, top, right + 1, bottom + 1))


def _pm_color_profile(img, w=200, h=100, sat_thresh=0.05):
    """Для каждого столбца изображения (после ресайза до w×h) считает долю
    зеленоватых и красноватых пикселей по HSV (оттенок+насыщенность, а не
    сырые RGB-пороги) — устойчиво к бледным полупрозрачным заливкам и
    JPEG-артефактам реальных скринов (проверено на живом скрине: чистые
    RGB-пороги давали почти нулевой сигнал на бледной зелёной заливке,
    HSV с низким порогом насыщенности — нет)."""
    import colorsys
    img = img.convert("RGB").resize((w, h))
    px = img.load()
    green = [0.0] * w
    red = [0.0] * w
    for x in range(w):
        gc = 0
        rc = 0
        for y in range(h):
            r, g, b = px[x, y]
            hh, ss, vv = colorsys.rgb_to_hsv(r / 255, g / 255, b / 255)
            if ss < sat_thresh or vv < 0.20:
                continue
            deg = hh * 360
            if 60 <= deg <= 170:
                gc += 1
            elif deg <= 30 or deg >= 335:
                rc += 1
        green[x] = gc / h
        red[x] = rc / h
    return green, red


def _pm_pearson(a, b):
    n = len(a)
    if n == 0 or n != len(b):
        return 0.0
    ma = sum(a) / n
    mb = sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return 0.0
    return cov / (va * vb) ** 0.5


def _pm_fetch_candles_window(symbol, start_ts, end_ts, interval="1m"):
    """Тянет свечи Gate.io Futures в явном окне [start_ts, end_ts] (unix-
    секунды) — отдельная от боевой _fetch_candles функция (та заточена под
    'от now назад на days дней', тут нужно произвольное окно в прошлом),
    чтобы не рисковать стабильностью живого сканера/автотрейда."""
    interval_sec = TF_SECONDS.get(interval, 60)
    all_candles = []
    current_from = int(start_ts)
    end_ts = int(end_ts)
    fail_count = 0
    while current_from < end_ts:
        try:
            r = requests.get(
                f"{GATE_API}/futures/usdt/candlesticks",
                params={"contract": symbol, "interval": interval,
                        "from": current_from, "limit": 999},
                timeout=15,
            )
            if r.status_code != 200:
                fail_count += 1
                if fail_count >= 5:
                    break
                time.sleep(1)
                continue
            fail_count = 0
            raw = r.json()
            if not raw:
                break
            batch = []
            for c in raw:
                t = int(c.get("t", 0))
                if t > end_ts:
                    continue
                batch.append({
                    "t": t, "open": float(c.get("o", 0)), "high": float(c.get("h", 0)),
                    "low": float(c.get("l", 0)), "close": float(c.get("c", 0)),
                    "vol": float(c.get("v", 0)),
                })
            if not batch:
                break
            all_candles.extend(batch)
            last_t = batch[-1]["t"]
            if last_t >= end_ts - interval_sec:
                break
            current_from = last_t + interval_sec
            time.sleep(0.1)
        except Exception as e:
            fail_count += 1
            olog(f"[pump_match] fetch ошибка: {e}")
            if fail_count >= 5:
                break
            time.sleep(1)
    seen = set()
    out = []
    for c in sorted(all_candles, key=lambda x: x["t"]):
        if c["t"] not in seen:
            seen.add(c["t"])
            out.append(c)
    return out


def _pm_png_to_b64(png_bytes):
    return "data:image/png;base64," + base64.b64encode(png_bytes).decode("ascii")


def _pump_match_run(image_bytes, symbol, start_ts, end_ts, base_price_override=None):
    """Основной пайплайн подбора: обрезать эталон → стянуть реальные свечи
    → перебрать сетку параметров рендера → оценить каждый вариант по
    корреляции цветового профиля с эталоном → сохранить и вернуть лучшие."""
    from PIL import Image
    try:
        ref_img = Image.open(io.BytesIO(image_bytes))
    except Exception as e:
        return {"ok": False, "msg": f"Не смог открыть картинку: {e}"}

    cropped = _pm_autocrop_chart(ref_img) or ref_img.convert("RGB")
    ref_g, ref_r = _pm_color_profile(cropped)

    candles = _pm_fetch_candles_window(symbol, start_ts, end_ts)
    if not candles or len(candles) < 3:
        return {"ok": False,
                "msg": f"Не удалось получить свечи для '{symbol}' в этом окне "
                       f"(получено {len(candles) if candles else 0}). Контракт может "
                       f"называться иначе на Gate.io (для монет с крошечной ценой часто "
                       f"есть множитель в имени, напр. 1000{symbol}) — либо не совпадает "
                       f"окно времени."}

    base_price = base_price_override if base_price_override else min(c["close"] for c in candles)
    last_price = candles[-1]["close"]
    pct = (last_price - base_price) / base_price * 100.0 if base_price else 0.0

    results = []
    for vm in PUMP_MATCH_GRID_VOLUME_MODES:
        for ep in PUMP_MATCH_GRID_EMA_PERIODS:
            for ff in PUMP_MATCH_GRID_FLOOR_FRACS:
                try:
                    png = _render_pump_chart_variant(candles, base_price, volume_mode=vm,
                                                      ema_period=ep, floor_frac=ff)
                    if not png:
                        continue
                    cand_img = Image.open(io.BytesIO(png))
                    cg, cr = _pm_color_profile(cand_img)
                    sg = _pm_pearson(ref_g, cg)
                    sr = _pm_pearson(ref_r, cr)
                except Exception as e:
                    olog(f"[pump_match] вариант vm={vm} ep={ep} ff={ff} упал: {e}")
                    continue
                results.append({
                    "score": (sg + sr) / 2, "score_g": sg, "score_r": sr,
                    "volume_mode": vm, "ema_period": ep, "floor_frac": ff, "png": png,
                })

    if not results:
        return {"ok": False, "msg": "Ни один вариант рендера не удалось построить (нет Pillow?)"}

    results.sort(key=lambda r: r["score"], reverse=True)
    top = results[:6]
    best = top[0]
    _save_pump_render_params({"volume_mode": best["volume_mode"],
                               "ema_period": best["ema_period"],
                               "floor_frac": best["floor_frac"]})
    olog(f"[pump_match] {symbol}: лучший вариант score={best['score']:.3f} "
         f"vm={best['volume_mode']} ema={best['ema_period']} floor={best['floor_frac']} "
         f"— сохранён как рабочий")

    return {
        "ok": True,
        "candles_count": len(candles),
        "base_price": base_price, "last_price": last_price, "pct": round(pct, 2),
        "cropped_preview_b64": _pm_png_to_b64(_pil_to_png_bytes(cropped)),
        "top": [{
            "score": round(r["score"], 3), "score_g": round(r["score_g"], 3),
            "score_r": round(r["score_r"], 3), "volume_mode": r["volume_mode"],
            "ema_period": r["ema_period"], "floor_frac": r["floor_frac"],
            "png_b64": _pm_png_to_b64(r["png"]),
        } for r in top],
    }


def _pil_to_png_bytes(img):
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG")
    return buf.getvalue()


PUMP_MATCH_HTML_PAGE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pump Match</title>
<style>
body{background:#0d1117;color:#c9d1d9;font-family:system-ui,sans-serif;margin:0;padding:16px}
h1{font-size:20px}
label{display:block;margin-top:12px;font-size:13px;color:#8b949e}
input{width:100%;background:#161b22;color:#c9d1d9;border:1px solid #30363d;border-radius:6px;padding:8px;margin-top:4px;font-size:14px;box-sizing:border-box}
button{background:#238636;color:#fff;border:0;padding:10px 16px;border-radius:6px;font-size:14px;margin-top:14px;cursor:pointer}
.card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:16px;margin-top:14px;max-width:480px}
.cand{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:10px;margin-top:12px}
.cand.best{border-color:#3fb950;box-shadow:0 0 0 1px #3fb950}
.cand img{width:100%;border-radius:6px;margin-top:6px}
.cand .params{font-size:12px;color:#8b949e;margin-top:6px}
.score{font-size:16px;font-weight:bold;color:#3fb950}
#status{color:#8b949e;font-size:13px;margin-top:10px}
#previewCard img{max-width:100%;border-radius:6px}
</style></head><body>
<h1>&#127919; Pump Match — авто-подбор параметров по скрину</h1>
<p style="color:#8b949e;font-size:13px;max-width:480px">Загрузи скрин чужого бота (пампа), укажи монету и окно времени с самого скрина — перебираем варианты отрисовки на РЕАЛЬНЫХ свечах Gate.io и подбираем ближайший. Лучший вариант сразу становится рабочим для всех новых алертов.</p>

<div class="card">
  <label>Скрин-эталон</label>
  <input type="file" id="pmImage" accept="image/*">
  <label>Символ (контракт Gate.io)</label>
  <input type="text" id="pmSymbol" placeholder="XEC_USDT" value="XEC_USDT">
  <label>Дата (ГГГГ-ММ-ДД)</label>
  <input type="date" id="pmDate">
  <label>Начало окна (время на скрине)</label>
  <input type="time" id="pmStart" value="07:00">
  <label>Конец окна (время на скрине)</label>
  <input type="time" id="pmEnd" value="09:10">
  <label>Базовая цена (необязательно — по умолчанию минимум окна)</label>
  <input type="text" id="pmBase" placeholder="напр. 5.801e-6">
  <button onclick="runMatch()">&#128269; Подобрать параметры</button>
  <div id="status"></div>
</div>

<div id="previewCard" class="card" style="display:none">
  <b>Область скрина, с которой сравниваем</b>
  <img id="previewImg">
</div>

<div id="results"></div>

<script>
document.getElementById('pmDate').valueAsDate = new Date();

async function runMatch(){
  const statusEl = document.getElementById('status');
  const fileInput = document.getElementById('pmImage');
  if(!fileInput.files.length){ statusEl.innerText = 'Выбери файл скрина'; return; }
  const symbol = document.getElementById('pmSymbol').value.trim();
  const date = document.getElementById('pmDate').value;
  const start = document.getElementById('pmStart').value;
  const end = document.getElementById('pmEnd').value;
  const base = document.getElementById('pmBase').value.trim();
  if(!symbol || !date || !start || !end){ statusEl.innerText = 'Заполни символ/дату/окно времени'; return; }

  const fd = new FormData();
  fd.append('image', fileInput.files[0]);
  fd.append('symbol', symbol);
  fd.append('date', date);
  fd.append('start', start);
  fd.append('end', end);
  if(base) fd.append('base_price', base);

  statusEl.innerText = 'Тяну свечи и перебираю варианты (может занять ~10-20с)...';
  document.getElementById('results').innerHTML = '';
  document.getElementById('previewCard').style.display = 'none';

  try{
    const r = await fetch('/pump_match_run', {method:'POST', body: fd});
    const d = await r.json();
    if(!d.ok){ statusEl.innerText = '❌ ' + d.msg; return; }

    statusEl.innerText = `Свечей: ${d.candles_count} | База: ${d.base_price} → Последняя: ${d.last_price} (${d.pct>=0?'+':''}${d.pct}%)`;

    document.getElementById('previewImg').src = d.cropped_preview_b64;
    document.getElementById('previewCard').style.display = 'block';

    const resDiv = document.getElementById('results');
    d.top.forEach((c, i) => {
      const div = document.createElement('div');
      div.className = 'cand' + (i===0 ? ' best' : '');
      div.innerHTML = `<div class="score">${i===0?'★ ЛУЧШИЙ — ':''}score ${c.score}</div>`
        + `<div class="params">volume_mode=${c.volume_mode} | ema_period=${c.ema_period} | floor_frac=${c.floor_frac} | (g=${c.score_g} r=${c.score_r})</div>`
        + `<img src="${c.png_b64}">`;
      resDiv.appendChild(div);
    });
    if(d.top.length){
      statusEl.innerText += ' — лучшие параметры сохранены и уже применяются в реальных алертах';
    }
  }catch(e){
    statusEl.innerText = '❌ ошибка запроса: ' + e;
  }
}
</script></body></html>"""



class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",len(body))
        self.send_header("Cache-Control","no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma","no-cache")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
      try:
        if self.path == "/" or self.path == "/index.html":
            self.send_response(302)
            self.send_header("Location", "/ema")
            self.end_headers()
        elif self.path == "/ema" or self.path == "/ema.html":
            body = EMA_HTML_PAGE.replace("__APP_VERSION__", APP_VERSION).encode()
            self.send_response(200)
            self.send_header("Content-Type","text/html; charset=utf-8")
            self.send_header("Content-Length",len(body))
            self.send_header("Cache-Control","no-store, no-cache, must-revalidate, max-age=0")
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/ema_logs":
            with opt_lock:
                self._json({"logs": opt_state["logs"], "logs_dropped": opt_state.get("logs_dropped", 0)})
        elif self.path == "/ema_dossier_status":
            self._json(_load_ema_dossier_state())
        elif self.path == "/ema_live_signals":
            with _ema_live_lock:
                self._json(dict(_ema_live_state))
        elif self.path == "/ema_signal_history":
            with _ema_history_lock:
                state = _load_ema_history()
            items = sorted(state["items"].values(), key=lambda v: v["opened_at"], reverse=True)
            closed = [v for v in items if v["status"] in ("tp", "sl")]
            tp_n = sum(1 for v in closed if v["status"] == "tp")
            winrate = round(tp_n / len(closed) * 100, 1) if closed else None
            self._json({"items": items, "closed": len(closed), "tp": tp_n,
                         "sl": len(closed) - tp_n, "winrate": winrate})
        elif self.path == "/ema_auto_trade_status":
            with ema_auto_trade_lock:
                cfg = dict(ema_auto_trade_state)
            with _ema_history_lock:
                state = _load_ema_history()
            live_items = [v for v in state["items"].values()
                          if v.get("status") == "open" and v.get("live")]
            balance = None
            try:
                balance = _gate_get_balance()
            except Exception:
                pass
            cfg["live_count"] = len(live_items)
            cfg["live_positions"] = [
                {"symbol": v["symbol"], "dir": v["dir"], "entry": v["price"],
                 "sl": v["sl"], "tp": v["tp"], "size": v.get("live_size"),
                 "leverage": v.get("live_leverage"), "notional": v.get("live_notional"),
                 "opened_at": v["opened_at"]}
                for v in live_items
            ]
            cfg["balance"] = balance
            cfg["gate_configured"] = bool(GATE_KEY and GATE_SECRET)
            self._json(cfg)
        elif self.path == "/gate_cfg":
            self._json({"gate_key": GATE_KEY[:4]+"***" if GATE_KEY else "",
                        "gate_secret": "***" if GATE_SECRET else "",
                        "has_key": bool(GATE_KEY and GATE_SECRET)})
        elif self.path == "/pump_status":
            # v0.1.0: статус Live Pump Detector — сколько монет отслеживается
            # прямо сейчас и последние сработавшие пампы (из истории на диске).
            with _pump_detect_lock:
                tracked = len(_pump_price_history)
            recent = []
            try:
                if os.path.exists(PUMP_DETECT_HISTORY_FILE):
                    with open(PUMP_DETECT_HISTORY_FILE) as f:
                        recent = json.load(f)
            except Exception:
                recent = []
            self._json({"tracked": tracked, "recent": recent[-100:]})
        elif self.path == "/alert_cfg":
            self._json({"tg_token": TG_TOKEN, "tg_chat": TG_CHAT, "ntfy_url": NTFY_URL,
                        "hc_url": HC_URL,
                        "watchdog_enabled": WATCHDOG_ENABLED,
                        "watchdog_timeout_min": WATCHDOG_TIMEOUT_MIN})
        elif self.path == "/pump_match" or self.path == "/pump_match.html":
            body = PUMP_MATCH_HTML_PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()
      except Exception as e:
        try:
            self.send_response(500); self.end_headers()
        except Exception: pass

    def do_POST(self):
        if self.path == "/pump_match_run":
            self._handle_pump_match_run()
            return
        try:
            length = int(self.headers.get("Content-Length",0))
            body   = json.loads(self.rfile.read(length)) if length else {}
        except Exception as e:
            self._json({"ok":False,"msg":f"bad request: {e}"}); return

        if self.path == "/alert_cfg":
            global TG_TOKEN, TG_CHAT, NTFY_URL, WATCHDOG_ENABLED, WATCHDOG_TIMEOUT_MIN, HC_URL
            TG_TOKEN = (body.get("tg_token") or "").strip()
            TG_CHAT  = (body.get("tg_chat")  or "").strip()
            NTFY_URL = (body.get("ntfy_url") or "").strip()
            if "hc_url" in body:
                HC_URL = (body.get("hc_url") or "").strip()
            if "watchdog_enabled" in body:
                WATCHDOG_ENABLED = bool(body.get("watchdog_enabled"))
            if "watchdog_timeout_min" in body:
                try:
                    WATCHDOG_TIMEOUT_MIN = max(5, min(1440, int(body.get("watchdog_timeout_min"))))
                except (TypeError, ValueError):
                    pass
            _save_alert_cfg()
            self._json({"ok": True})

        elif self.path == "/alert_test":
            ok, err = _test_alert()
            self._json({"ok": ok, "error": err})

        elif self.path == "/ema_signal_history_clear":
            with _ema_history_lock:
                _save_ema_history({"items": {}})
            olog("[ema_history] история сигналов очищена вручную")
            self._json({"ok": True})

        elif self.path == "/ema_dossier_start":
            with _ema_dossier_lock:
                if _ema_dossier_running["v"]:
                    self._json({"ok": False, "msg": "Скан уже идёт"})
                else:
                    _ema_dossier_running["v"] = True
                    def _run():
                        try:
                            _run_ema_dossier_scan(50)
                        finally:
                            with _ema_dossier_lock:
                                _ema_dossier_running["v"] = False
                    threading.Thread(target=_run, daemon=True).start()
                    self._json({"ok": True, "msg": "Скан запущен (топ-50, 3 ТФ x 4 EMA) — статус на /ema_dossier_status"})

        elif self.path == "/gate_cfg":
            global GATE_KEY, GATE_SECRET
            GATE_KEY    = (body.get("gate_key")    or "").strip()
            GATE_SECRET = (body.get("gate_secret") or "").strip()
            _save_gate_cfg()
            self._json({"ok": True})

        elif self.path == "/ema_auto_trade_settings":
            try:
                if "enabled" in body:
                    enabled = bool(body["enabled"])
                    if enabled and not (GATE_KEY and GATE_SECRET):
                        self._json({"ok": False, "msg": "Не настроены Gate.io ключи (/gate_cfg)"}); return
                    with ema_auto_trade_lock:
                        ema_auto_trade_state["enabled"] = enabled
                if "position_pct" in body:
                    pct = float(body["position_pct"])
                    assert 0.1 <= pct <= 100.0, f"position_pct вне диапазона: {pct}"
                    with ema_auto_trade_lock:
                        ema_auto_trade_state["position_pct"] = pct
                if "risk_pct" in body:
                    rp = float(body["risk_pct"])
                    assert 0.1 <= rp <= 100.0, f"risk_pct вне диапазона: {rp}"
                    with ema_auto_trade_lock:
                        ema_auto_trade_state["risk_pct"] = rp
                if "max_concurrent" in body:
                    mc = body["max_concurrent"]
                    mc = int(mc) if mc not in (None, "", "null") else None
                    with ema_auto_trade_lock:
                        ema_auto_trade_state["max_concurrent"] = mc
                if "max_forced_margin_pct" in body:
                    mfp = float(body["max_forced_margin_pct"])
                    assert 0.1 <= mfp <= 100.0, f"max_forced_margin_pct вне диапазона: {mfp}"
                    with ema_auto_trade_lock:
                        ema_auto_trade_state["max_forced_margin_pct"] = mfp
                if "forced_size_max_multiple" in body:
                    fsm = float(body["forced_size_max_multiple"])
                    assert 1.0 <= fsm <= 20.0, f"forced_size_max_multiple вне диапазона: {fsm}"
                    with ema_auto_trade_lock:
                        ema_auto_trade_state["forced_size_max_multiple"] = fsm
            except Exception as e:
                self._json({"ok": False, "msg": f"Некорректные параметры: {e}"}); return
            _save_ema_auto_trade_cfg()
            with ema_auto_trade_lock:
                olog(f"[ema_auto_trade] настройки обновлены: {dict(ema_auto_trade_state)}")
            self._json({"ok": True})

        elif self.path == "/ema_auto_trade_close":
            sym = (body.get("symbol") or "").strip()
            if not sym:
                self._json({"ok": False, "msg": "Не указан symbol"}); return
            try:
                _gate_close_position(sym)
                self._json({"ok": True})
            except Exception as e:
                self._json({"ok": False, "msg": str(e)})

        else:
            self.send_response(404); self.end_headers()

    def _handle_pump_match_run(self):
        """multipart/form-data: image + symbol + date + start + end +
        необязательный base_price. Отдельный путь от общего do_POST, т.к.
        тело — не JSON, а файл с полями."""
        try:
            content_type = self.headers.get("Content-Type", "")
            length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(length) if length else b""
            if "multipart/form-data" not in content_type:
                self._json({"ok": False, "msg": "Ожидался multipart/form-data"}); return
            fields = _pm_parse_multipart(raw_body, content_type)
            if "image" not in fields or not fields["image"][0]:
                self._json({"ok": False, "msg": "Файл картинки не пришёл"}); return
            image_bytes = fields["image"][0]

            def _f(name, default=""):
                v = fields.get(name)
                return v[0].decode("utf-8", "ignore").strip() if v else default

            symbol = _f("symbol")
            date_s = _f("date")
            start_s = _f("start")
            end_s = _f("end")
            base_s = _f("base_price")
            if not (symbol and date_s and start_s and end_s):
                self._json({"ok": False, "msg": "Не хватает полей (symbol/date/start/end)"}); return

            import datetime as _dt
            try:
                start_dt = _dt.datetime.strptime(f"{date_s} {start_s}", "%Y-%m-%d %H:%M")
                end_dt = _dt.datetime.strptime(f"{date_s} {end_s}", "%Y-%m-%d %H:%M")
                start_ts = time.mktime(start_dt.timetuple())
                end_ts = time.mktime(end_dt.timetuple())
            except Exception as e:
                self._json({"ok": False, "msg": f"Не смог разобрать дату/время: {e}"}); return
            if end_ts <= start_ts:
                self._json({"ok": False, "msg": "Конец окна должен быть позже начала"}); return

            base_price_override = None
            if base_s:
                try:
                    base_price_override = float(base_s)
                except ValueError:
                    pass

            result = _pump_match_run(image_bytes, symbol.upper(), start_ts, end_ts,
                                      base_price_override=base_price_override)
            self._json(result)
        except Exception as e:
            olog(f"[pump_match] ⚠ ошибка обработки запроса: {e}")
            try:
                self._json({"ok": False, "msg": f"Внутренняя ошибка: {e}"})
            except Exception:
                pass

def main():
    global TG_TOKEN, TG_CHAT, NTFY_URL
    TG_TOKEN  = os.environ.get("TG_TOKEN", TG_TOKEN)
    TG_CHAT   = os.environ.get("TG_CHAT",  TG_CHAT)
    NTFY_URL  = os.environ.get("NTFY_URL", NTFY_URL)
    _load_alert_cfg()
    _load_gate_cfg()
    _load_ema_auto_trade_cfg()
    _load_pump_render_params()
    threading.Thread(target=_watchdog_loop, daemon=True).start()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    threading.Thread(target=_ema_signal_loop, daemon=True).start()
    threading.Thread(target=_pump_detect_loop, daemon=True).start()

    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"{_C_GRN}Pump Radar v{APP_VERSION} — http://0.0.0.0:{PORT}{_C_RST}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nЗавершено"); server.shutdown()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass
    main()
