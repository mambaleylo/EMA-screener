# EMA Invert Experiment — патч v0.1.10

Проверил v0.1.5–v0.1.9 (это и есть последние 5 апдейтов с утра, раз до ночи
система была на 0.1.4) — по всем трём пунктам:

- **п.3 (выборка EMA / фильтры по vol_ratio, candle_pattern)** — не трогалось
  вообще, ни разу. Ниже полностью новый код.
- **п.4 (RR / time-stop)** — `EMA_INVERT_SAFETY_RR` последний раз трогали в
  v0.1.4 (2.0→1.4, ещё ДО ночных пяти апдейтов). `EMA_INVERT_TIME_STOP_BARS`
  не трогали никогда с момента форка. Обе правки актуальны, ниже.
- **п.5 (видимость PnL)** — `max_age_sec` подняли 180→1800 в v0.1.8, это уже
  сделано, править число ещё раз вслепую смысла нет. НО нашёл кое-что похуже:
  `_ema_history_update_open()` (детект TP/SL по живой цене — самый частый
  путь закрытия, не через reconcile/time_stop) вообще НИКОГДА не вызывает
  `_gate_get_last_pnl` — там просто нет вызова. Из 71 закрытой live-сделки в
  вашем архиве это давало долю "молчащих" по PnL закрытий. Плюс отдельно —
  6 закрытий через reconcile и 8 через time_stop, где `_gate_get_last_pnl`
  честно вызывался, но вернул `None` даже с max_age_sec=1800. Ниже — фикс
  обоих случаев: (a) вызов PnL добавлен в сам путь детекта TP/SL по цене,
  (b) fallback через `/futures/usdt/account_book`, когда `/position_close`
  ничего не дал.

Ниже — точные блоки "было/стало" с версионным комментарием в вашем стиле,
чтобы вставить прямо в файл.

---

## Пункт 3 — разворот критерия отбора EMA + входные фильтры

### 3a. `_pick_best_ema_for_symbol` — bounce_rate max → break_rate max

**Было:**
```python
def _pick_best_ema_for_symbol(dossier_entry):
    """Из досье монеты выбирает одну (tf, ema_period) пару с максимальным
    bounce_rate среди надёжных (touches >= EMA_DOSSIER_MIN_TOUCHES).
    v3.6.14: "1d"/"1w" из досье игнорируются — см. EMA_LIVE_TFS выше, сами
    по себе в досье они по-прежнему считаются и отображаются."""
    best = None
    for tf, d in (dossier_entry.get("by_tf") or {}).items():
        if tf not in EMA_LIVE_TFS:
            continue
        for s in d.get("emas", []):
            if s["touches"] < EMA_DOSSIER_MIN_TOUCHES: continue
            if best is None or s["bounce_rate"] > best["bounce_rate"]:
                best = {"tf": tf, "ema_period": s["ema_period"],
                        "bounce_rate": s["bounce_rate"], "touches": s["touches"]}
    return best
```

**Стало:**
```python
def _pick_best_ema_for_symbol(dossier_entry):
    """v0.1.10: КРИТИЧНЫЙ разворот критерия отбора под инверт-логику. Раньше
    выбирался (tf, ema_period) с МАКСИМАЛЬНЫМ bounce_rate — то есть система
    искала уровень, который лучше всего исторически УДЕРЖИВАЕТ цену (реальный
    support/resistance). Но эта стратегия торгует ПРОТИВ отскока — ставит на
    то, что уровень будет ПРОБИТ. Разбор диагностики (100 закрытых сделок,
    ema_invert_diagnostics.jsonl от 13.07) подтвердил разворот эмпирически
    с трёх независимых сторон:
      - corr(bounce_rate, live_pnl_pct) = -0.31 (n=54 live) — чем выше
        историческая надёжность уровня, тем хуже исход инверт-сделки;
      - candle_pattern_at_entry=="confirmed" (видна настоящая реакция-отскок
        на касании) -> winrate 45.5% против 61.5% при "absent";
      - vol_ratio_at_entry>1.5 (сильный объём на касании, обычно = сильная
        реакция) -> winrate 42% против 66% при vol_ratio<0.7.
    Все три сигнала об одном: чем убедительнее уровень СЕЙЧАС или ИСТОРИЧЕСКИ
    держит цену — тем хуже для ставки на пробой. Критерий отбора развёрнут:
    теперь берём (tf, ema_period) с МАКСИМАЛЬНЫМ break_rate = breaks/touches
    (доля касаний, закончившихся явным пробоем, а не отскоком/шумом) —
    прямая мера того, что нужно именно этой стратегии. Поле "breaks" уже
    считалось и сохранялось в _detect_ema_bounces (просто не использовалось
    для отбора) — новых полей в досье не требуется, старые сохранённые
    ema_invert_dossier_state.json продолжат работать как есть, breaks там
    уже лежит. "1d"/"1w" по-прежнему игнорируются для live (EMA_LIVE_TFS)."""
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
```

### 3b. Новые константы фильтра (положить рядом с `EMA_INVERT_SAFETY_RR`)

```python
# v0.1.10: входные фильтры по диагностике (100 сделок, 13.07) — обе метрики
# независимо показали, что "убедительная" реакция на касании (объём/паттерн
# свечи) означает, что уровень скорее УДЕРЖИТ цену, а не будет пробит — для
# инверт-стратегии это сигнал ПРОТИВ входа, не за. Пороги — по границам
# сегментов, где эффект был наиболее выражен, не по медиане (см. коммент
# у _pick_best_ema_for_symbol с цифрами по каждому сегменту).
EMA_INVERT_MAX_VOL_RATIO = 1.5               # выше — отклоняем (winrate 42% в этом сегменте)
EMA_INVERT_REJECT_CONFIRMED_PATTERN = True   # candle_pattern=="confirmed" — отклоняем (45.5% vs 61.5%)
_ema_invert_filter_stats = {"vol_ratio": 0, "candle_pattern": 0}
```

### 3c. Сам фильтр в `_ema_check_symbol_signal`

Вставить **после** блока, где считается `candle_pattern`, и **до** финального
`return {...}` (в самом конце функции):

**Было (конец функции):**
```python
    if direction == "long":
        candle_pattern = "confirmed" if ctx["bullish_reaction"] else "absent"
    else:
        candle_pattern = "confirmed" if ctx["bearish_reaction"] else "absent"
    return {
        "symbol": symbol, "tf": tf, "ema_period": ema_period, "dir": trade_dir,
        "bounce_dir": direction,
        "price": price, "ema_value": ema_r,
        "sl": safety_sl_r, "tp": tp_r, "rr": None,
        "time_limit_sec": _ema_invert_time_limit_sec(tf),
        "bounce_rate": pick["bounce_rate"], "bar_t": int(time.time()),
        "touches": pick.get("touches"), "atr_v": _round_price(atr_v),
        ...
```

**Стало:**
```python
    if direction == "long":
        candle_pattern = "confirmed" if ctx["bullish_reaction"] else "absent"
    else:
        candle_pattern = "confirmed" if ctx["bearish_reaction"] else "absent"

    # v0.1.10: фильтр по силе реакции на касании (см. EMA_INVERT_MAX_VOL_RATIO/
    # EMA_INVERT_REJECT_CONFIRMED_PATTERN выше и коммент у _pick_best_ema_for_symbol
    # с разбором по диагностике). Считается ДО построения sig-dict, чтобы
    # отклонённый подход не тратил место в истории/алертах — как и touch_aborted.
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
        ... # остальные поля без изменений
```

Только добавлены `"break_rate": pick.get("break_rate"),` в возвращаемый dict
(для будущей самопроверки — увидите break_rate прямо в истории сигналов) и
блок фильтра выше. Остальные поля этого return не трогаются.

Учтите: `EMA_INVERT_MAX_VOL_RATIO`/паттерн-фильтр считались на n=100, сегменты
местами по 17-22 сделки — это не железная закономерность, а рабочая гипотеза.
Стоит погонять ещё сотню-другую сделок с новым критерием отбора и проверить,
что break_rate-selection + фильтры действительно поднимают winrate, а не
просто режут количество сигналов.

---

## Пункт 4 — RR и время удержания

**Было:**
```python
EMA_INVERT_TIME_STOP_BARS = {"1m": 4, "5m": 4, "15m": 3, "1h": 2}
EMA_INVERT_TIME_STOP_DEFAULT_BARS = 3   #fallback для ТФ вне словаря выше
...
EMA_INVERT_SAFETY_RR = 1.4
```

**Стало:**
```python
# v0.1.10: разбор ema_invert_diagnostics.jsonl (100 сделок, 13.07) — 18 сделок
# закрылись по time_stop, и у 14 из них (78%!) post_timestop_would_hit_tp==True
# — то есть цена ДОШЛА БЫ до TP в ближайшие бары ПОСЛЕ принудительного
# закрытия. Медианный MFE у time_stop-сделок к моменту закрытия — 0.686R к
# TP (не 0, не отрицательный — сделка реально шла куда надо, просто не
# успевала). Лимит баров поднят примерно в 1.5 раза — сознательно не до
# EMA_INVERT_DIAG_WAIT_BARS=10 (это разрушило бы саму идею форка — "сделки
# должны быть быстрыми", см. шапку файла), просто чтобы медианная сделка
# успевала докатиться своим ходом, а не резалась почти у цели.
EMA_INVERT_TIME_STOP_BARS = {"1m": 6, "5m": 6, "15m": 5, "1h": 3}
EMA_INVERT_TIME_STOP_DEFAULT_BARS = 5   #fallback для ТФ вне словаря выше
...
# v0.1.10: было 1.4 (не трогали с v0.1.4, т.е. ещё ДО последних 5 апдейтов).
# Матчасть: при наблюдаемом винрейте 58% (100 сделок) safety_sl шире TP в
# 1.4 раза даёт expectancy = 0.58×0.72% − 0.42×1.02% ≈ −0.01%/сделку — ровно
# то, что и получилось на балансе (avg −0.012%/сделку по факту). Это не
# просадка от неудачи, это встроенная в константу математика "в среднем в
# ноль или чуть хуже" даже при рабочем винрейте. Порог безубытка при 58%
# винрейте — RR≈0.72 (0.42/0.58); 1.1 даёт содержательный запас над
# безубытком (expectancy ≈ +0.12%/сделку по тем же цифрам), не срезая RR
# до самого края — совсем тесный SL (близко к 0.72) будет чаще выбивать
# safety_sl шумом раньше времени, а он и так уже не "редкий предохранитель",
# как задумано в шапке файла, а 33% исходов (33/100) — сужать его дальше
# без роста winrate от фильтров п.3 может это только усугубить. Смотреть
# вместе с изменением time-stop выше — обе правки тянут expectancy в одну
# сторону с разных концов, вместе их эффект надо мерить заново, а не порознь.
EMA_INVERT_SAFETY_RR = 1.1
```

Не забудьте поднять `APP_VERSION = "0.1.10"` и добавить абзац в докстринг в
начале файла (как вы делаете для каждой версии) — я написал текст выше в
вашем стиле, можно брать как есть для комментов у констант, а для шапки
файла коротко: `EMA Invert Experiment v0.1.10 — разворот критерия отбора EMA
(break_rate вместо bounce_rate) + фильтры по vol_ratio/candle_pattern,
пересмотрены EMA_INVERT_SAFETY_RR (1.4→1.1) и EMA_INVERT_TIME_STOP_BARS —
по разбору 100 сделок из ema_invert_diagnostics.jsonl.`

---

## Пункт 5 — видимость PnL: fallback + фикс дыры в самом частом пути закрытия

Из 71 закрытой live-сделки в вашем архиве `live_pnl` не был записан у 17.
Разбивка по причине:

| путь закрытия | closed_externally | без live_pnl |
|---|---|---|
| time_stop | False (свой watchdog) | 8 — `_gate_get_last_pnl` вызывался, вернул None |
| sl | True (reconcile) | 6 — `_gate_get_last_pnl` вызывался, вернул None |
| tp/sl | None (внутренний детект по цене) | 3 — **вызова `_gate_get_last_pnl` вообще нет в этом коде** |

Третья строка — самая частая по идее ветка (обычное закрытие по TP/SL без
необходимости лезть в reconcile) — `_ema_history_update_open()` детектит
исход и сохраняет status/close_price, но `_gate_get_last_pnl` не вызывает
вовсе. Первые две строки — max_age_sec=1800 (уже поднятый в v0.1.8) всё
равно иногда не успевает.

### 5a. Новая функция — fallback через account_book

Добавить рядом с `_gate_get_last_pnl`:

```python
def _gate_get_pnl_from_account_book(symbol, since_ts, until_ts=None, max_lookback_sec=3600):
    """v0.1.10: fallback, когда _gate_get_last_pnl_from_position_close не смог
    сматчить закрытие (см. диагностику — 17 из 71 live-закрытий остались без
    live_pnl даже с max_age_sec=1800). Использует /futures/usdt/account_book —
    общий леджер счёта (изменения баланса: pnl/комиссии/фандинг) как ВТОРОЙ,
    независимый от /position_close источник результата закрытия. Ищет записи
    type="pnl" по этому контракту в окне [since_ts, until_ts] и суммирует
    найденное. Это НЕ полная замена /position_close (нет entry/close price,
    только денежный итог) — но лучше, чем полное отсутствие данных о
    результате сделки. ВАЖНО: точную схему ответа account_book у Gate.io
    стоит сверить на первой реально сработавшей записи в логе — сделано по
    документации, живьём не тестировалось."""
    contract = symbol.replace("/", "_").upper()
    until_ts = until_ts or (since_ts + max_lookback_sec)
    try:
        entries = _gate_req("GET", "/futures/usdt/account_book", params={
            "contract": contract,
            "from": int(since_ts) - 30,   # запас на рассинхрон часов
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
```

### 5b. `_gate_get_last_pnl` — переименовать старое тело, добавить обёртку

**Было:** сигнатура `def _gate_get_last_pnl(symbol, max_age_sec=1800):` и всё
тело функции как есть.

**Стало:**
```python
def _gate_get_last_pnl(symbol, max_age_sec=1800, fallback_since_ts=None):
    """v0.1.10: теперь обёртка. Основная логика (как раньше) — в
    _gate_get_last_pnl_from_position_close. Если она вернула None
    (запись старше max_age_sec или /position_close вообще ничего не отдал)
    И передан fallback_since_ts (обычно item["opened_at"] вызывающего кода) —
    пробуем _gate_get_pnl_from_account_book как второй источник, вместо
    того чтобы молча терять $-результат сделки (см. таблицу в комментарии к
    патчу v0.1.10 — было 17/71 закрытий без live_pnl)."""
    result = _gate_get_last_pnl_from_position_close(symbol, max_age_sec)
    if result is not None:
        return result
    if fallback_since_ts:
        return _gate_get_pnl_from_account_book(symbol, fallback_since_ts)
    return None

def _gate_get_last_pnl_from_position_close(symbol, max_age_sec=1800):
    """Возвращает PnL последней закрытой позиции по символу (USDT).
    ... # ВЕСЬ остальной докстринг и тело — БЕЗ ИЗМЕНЕНИЙ, просто переименована функция
```

То есть: тело функции остаётся ровно таким, как в вашем текущем файле —
меняется только имя (`_gate_get_last_pnl` → `_gate_get_last_pnl_from_position_close`)
и сигнатура (`max_age_sec=1800` — без изменений). Новая маленькая функция
`_gate_get_last_pnl` сверху — единственная логика, которую нужно добавить.

### 5c. Прокинуть `fallback_since_ts` в местах вызова

Везде, где сейчас `_gate_get_last_pnl(symbol)` / `_gate_get_last_pnl(item["symbol"])`,
и рядом есть `item`/`opened_at` — добавить `fallback_since_ts=item["opened_at"]`:

**`_ema_invert_timestop_watchdog`:**
```python
pnl_info = _gate_get_last_pnl(symbol, fallback_since_ts=opened_at)
```
(`opened_at` там уже есть в локальной переменной чуть выше по функции)

**`_ema_reconcile_live_positions`:**
```python
pnl_info = _gate_get_last_pnl(item["symbol"], fallback_since_ts=item["opened_at"])
```

### 5d. Фикс дыры в самом частом пути — `_ema_history_update_open`

**Было** (конец функции, после того как `outcome` найден):
```python
            if outcome:
                item["status"] = outcome
                item["closed_at"] = int(time.time())
                item["close_price"] = outcome_price
                item["diag_status"] = "pending"
                updated[key] = item
```

**Стало:**
```python
            if outcome:
                item["status"] = outcome
                item["closed_at"] = int(time.time())
                item["close_price"] = outcome_price
                item["diag_status"] = "pending"
                # v0.1.10: раньше для этого (самого частого) пути закрытия
                # live_pnl вообще не запрашивался — см. таблицу в патче
                # v0.1.10, 3 из 17 "молчащих" закрытий были отсюда. Только
                # для live=True (реальных сделок), чтобы не дёргать Gate API
                # на пустом месте по бумажным сигналам.
                if item.get("live"):
                    pnl_info = _gate_get_last_pnl(item["symbol"], fallback_since_ts=item["opened_at"])
                    if pnl_info:
                        item["live_pnl"]       = pnl_info["pnl"]
                        item["live_pnl_pct"]   = pnl_info.get("pnl_pct")
                        item["live_pnl_fee"]   = pnl_info.get("pnl_fee")
                        item["live_pnl_fund"]  = pnl_info.get("pnl_fund")
                        item["live_pnl_price"] = pnl_info.get("pnl_price")
                updated[key] = item
```

Это добавляет один сетевой вызов на каждое реальное (live) закрытие в этом
пути — как и везде в файле, при сбое `_gate_get_last_pnl` просто вернёт
`None`, `pnl_info` останется `None`, ничего не падает.

---

## Что дальше

Все три пункта независимы, но 3 и 4 меняют, по сути, одни и те же 100
сделок с разных сторон — после накопления новой статистики (рекомендую
минимум 50-80 новых закрытых сделок) стоит заново прогнать ту же диагностику
и посмотреть на связку: winrate, exit_reason-распределение (ждём меньше
time_stop / больше tp, если п.4 сработал) и corr(break_rate, pnl) (должна
стать положительной, если п.3 сработал).

Отдельно всё ещё не тронуто (не входило в 3/4/5, но осталось с прошлого
раза): `max_forced_margin_pct`/`forced_size_max_multiple` у вас сейчас 85%,
это отключает оба предохранителя форс-минимума размера позиции — раз
профит уже разбираем на уровне механики TP/SL, эти два стоит вернуть к
разумным значениям (20% / 3×) до следующего прогона, иначе шум от размера
позиции забьёт эффект от правок 3/4.
