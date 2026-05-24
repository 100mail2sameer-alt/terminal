from __future__ import annotations

import asyncio
import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

try:
    from neo_api_client import NeoAPI
except Exception as import_error:  # noqa: BLE001
    NeoAPI = None
    SDK_IMPORT_ERROR = str(import_error)
else:
    SDK_IMPORT_ERROR = None

from kotak_execution_engine import Index, StrategyEngine, parse_payload

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("kotak-telegram-bot")

IST = ZoneInfo("Asia/Kolkata")
STRATEGY_STORE_PATH = Path("strategies.json")
ENV_PATH = Path(".venv/.kotak.env")

AUTH_CLIENT: NeoAPI | None = None
CLIENT_LOCK = threading.Lock()
EXECUTION_REGISTRY: dict[str, dict[str, Any]] = {}
EXECUTION_REGISTRY_LOCK = threading.Lock()
LAST_QUOTES: dict[str, dict[str, float | None]] = {
    "NIFTY": {"ltp": None, "prev": None},
    "BANKNIFTY": {"ltp": None, "prev": None},
}


def load_env() -> dict[str, str]:
    if not ENV_PATH.exists():
        raise ValueError(f"Missing {ENV_PATH}")
    data = dotenv_values(ENV_PATH)
    return {k: (v.strip() if isinstance(v, str) else "") for k, v in data.items()}


def allowed_chat_ids() -> set[int]:
    raw = load_env().get("TELEGRAM_ALLOWED_CHAT_IDS", "")
    out: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if token.isdigit():
            out.add(int(token))
    return out


def is_allowed(update: Update) -> bool:
    chat_id = update.effective_chat.id if update.effective_chat else None
    return bool(chat_id and chat_id in allowed_chat_ids())


def now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")


def load_strategies() -> list[dict[str, Any]]:
    if not STRATEGY_STORE_PATH.exists():
        return []
    try:
        data = json.loads(STRATEGY_STORE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    except Exception:
        pass
    return []


def save_strategies(rows: list[dict[str, Any]]) -> None:
    STRATEGY_STORE_PATH.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def set_strategy_enabled(strategy_id: str, enabled: bool) -> bool:
    rows = load_strategies()
    changed = False
    for row in rows:
        if str(row.get("strategy_id")) == strategy_id:
            row["execution_enabled"] = enabled
            payload = row.get("payload")
            if isinstance(payload, dict):
                payload["execution_enabled"] = enabled
                row["payload"] = payload
            changed = True
            break
    if changed:
        save_strategies(rows)
    return changed


def validate_credentials_env() -> tuple[str, str, str, str]:
    env = load_env()
    consumer_key = env.get("KOTAK_CONSUMER_KEY", "")
    mobile = env.get("KOTAK_MOBILE_NUMBER", "")
    ucc = env.get("KOTAK_UCC", "")
    mpin = env.get("KOTAK_MPIN", "")
    if not all([consumer_key, mobile, ucc, mpin]):
        raise ValueError("Missing Kotak credentials in .kotak.env")
    return consumer_key, mobile, ucc, mpin


def ensure_client() -> NeoAPI:
    if AUTH_CLIENT is None:
        raise RuntimeError("Kotak session is not active. Use /login <totp> first.")
    return AUTH_CLIENT


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "").strip())
        except Exception:
            return None
    return None


def _collect_items(resp: Any) -> list[dict[str, Any]]:
    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)]
    if isinstance(resp, dict):
        for key in ("data", "message", "result", "quotes"):
            v = resp.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _extract_ltp_prev(item: dict[str, Any]) -> tuple[float | None, float | None]:
    ltp = _to_float(item.get("ltp") or item.get("LTP") or item.get("last_traded_price"))
    prev = _to_float(item.get("close") or item.get("previous_close") or item.get("prevClose"))
    if ltp is None and isinstance(item.get("ohlc"), dict):
        o = item["ohlc"]
        ltp = _to_float(o.get("ltp") or o.get("close"))
        prev = prev if prev is not None else _to_float(o.get("prev_close") or o.get("close"))
    return ltp, prev


class NeoSessionExecutionClient:
    def __init__(self, client: NeoAPI):
        self._client = client
        self._lock = threading.Lock()

    def get_index_ltp(self, index: Index) -> float:
        token_map = {"NIFTY": "26000", "BANKNIFTY": "26009", "SENSEX": "1"}
        token = token_map.get(index.value, "26000")
        with self._lock:
            quote = self._client.quotes(
                instrument_tokens=[{"instrument_token": token, "exchange_segment": "nse_cm"}],
                quote_type="ltp",
                isIndex=True,
            )
        items = _collect_items(quote)
        if not items:
            raise RuntimeError(f"Quote unavailable for {index.value}")
        ltp, _ = _extract_ltp_prev(items[0])
        if ltp is None:
            raise RuntimeError(f"LTP unavailable for {index.value}")
        return float(ltp)

    def find_option_token(self, index: Index, expiry_date: str, strike: int, option_type: Any):
        suffix = "CE" if str(getattr(option_type, "value", option_type)).lower().startswith("call") else "PE"
        symbol = f"{index.value}{expiry_date}{strike}{suffix}"
        with self._lock:
            resp = self._client.search_scrip(exchange_segment="nfo", symbol=symbol)
        rows = resp.get("data", []) if isinstance(resp, dict) else []
        if not rows:
            raise ValueError(f"Option contract not found: {symbol}")
        row = rows[0]
        return row.get("trading_symbol", symbol), str(row.get("pSymbol") or row.get("token") or "")

    def search_scrip(self, exchange_segment: str, symbol: str) -> dict[str, Any]:
        with self._lock:
            resp = self._client.search_scrip(exchange_segment=exchange_segment, symbol=symbol)
        return resp if isinstance(resp, dict) else {}

    def get_ltp(self, exchange: str, trading_symbol: str, token: str) -> float:
        with self._lock:
            quote = self._client.quotes(
                instrument_tokens=[{"instrument_token": token, "exchange_segment": exchange}],
                quote_type="ltp",
            )
        items = _collect_items(quote)
        if not items:
            raise RuntimeError(f"LTP unavailable: {trading_symbol}")
        ltp, _ = _extract_ltp_prev(items[0])
        if ltp is None:
            raise RuntimeError(f"LTP unavailable: {trading_symbol}")
        return float(ltp)

    def place_order(
        self,
        trading_symbol: str,
        token: str,
        exchange: str,
        transaction_type: str,
        quantity: int,
        order_type: str = "MKT",
        price: float = 0.0,
        trigger_price: float = 0.0,
        product: str = "NRML",
        tag: str = "",
    ) -> str:
        with self._lock:
            resp = self._client.place_order(
                exchange_segment=exchange,
                product=product,
                price=str(price),
                order_type=order_type,
                quantity=str(quantity),
                validity="DAY",
                trading_symbol=trading_symbol,
                transaction_type=transaction_type,
                amo="NO",
                disclosed_quantity="0",
                market_protection="0",
                pf="N",
                trigger_price=str(trigger_price),
                tag=tag,
            )
        if not isinstance(resp, dict):
            raise RuntimeError("Empty order response")
        return str(resp.get("nOrdNo") or "")

    def cancel_order(self, order_id: str, is_amo: bool = False) -> None:
        with self._lock:
            self._client.cancel_order(order_id=order_id, isAMO=is_amo)

    def get_order_status(self, order_id: str) -> dict[str, Any]:
        with self._lock:
            rep = self._client.order_report()
        rows = rep.get("data", []) if isinstance(rep, dict) else []
        for row in rows:
            if isinstance(row, dict) and str(row.get("nOrdNo")) == str(order_id):
                return row
        return {}

    def get_positions(self):
        with self._lock:
            pos = self._client.positions()
        if isinstance(pos, dict):
            return pos.get("data", []) or pos.get("message", []) or []
        if isinstance(pos, list):
            return pos
        return []


def _worker(payload: dict[str, Any], strategy_id: str, stop_event: threading.Event) -> None:
    try:
        client = ensure_client()
        cfg = parse_payload(payload)
        adapter = NeoSessionExecutionClient(client)
        engine = StrategyEngine(adapter, cfg)

        with EXECUTION_REGISTRY_LOCK:
            EXECUTION_REGISTRY.setdefault(strategy_id, {}).setdefault("status", {}).update(
                {
                    "running": True,
                    "strategy_name": cfg.strategy_name,
                    "started_at": now_ist(),
                    "stopped_at": None,
                    "last_error": None,
                    "last_message": "Execution started.",
                }
            )

        async def run_with_stop() -> None:
            task = asyncio.create_task(engine.run())
            while not task.done():
                if stop_event.is_set():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    return
                await asyncio.sleep(1)
            await task

        asyncio.run(run_with_stop())
        with EXECUTION_REGISTRY_LOCK:
            EXECUTION_REGISTRY.setdefault(strategy_id, {}).setdefault("status", {}).update(
                {"running": False, "stopped_at": now_ist(), "last_message": "Execution stopped."}
            )
    except Exception as error:  # noqa: BLE001
        with EXECUTION_REGISTRY_LOCK:
            EXECUTION_REGISTRY.setdefault(strategy_id, {}).setdefault("status", {}).update(
                {
                    "running": False,
                    "stopped_at": now_ist(),
                    "last_error": str(error),
                    "last_message": "Execution failed.",
                }
            )


def start_strategy(strategy_id: str) -> tuple[bool, str]:
    rows = load_strategies()
    selected = next((r for r in rows if str(r.get("strategy_id")) == strategy_id), None)
    if not selected:
        return False, "Strategy not found."

    payload = selected.get("payload") if isinstance(selected.get("payload"), dict) else {}
    if not payload:
        return False, "Strategy payload missing."
    try:
        cfg = parse_payload(payload)
    except Exception as error:  # noqa: BLE001
        return False, f"Invalid strategy payload: {error}"
    if not cfg.legs:
        return False, "No legs configured."

    with EXECUTION_REGISTRY_LOCK:
        entry = dict(EXECUTION_REGISTRY.get(strategy_id, {}))
    t = entry.get("thread")
    if isinstance(t, threading.Thread) and t.is_alive():
        set_strategy_enabled(strategy_id, True)
        return True, "Already running."

    stop_event = threading.Event()
    t = threading.Thread(target=_worker, args=(payload, strategy_id, stop_event), daemon=True, name=f"tg-strat-{strategy_id[:8]}")
    with EXECUTION_REGISTRY_LOCK:
        EXECUTION_REGISTRY[strategy_id] = {
            "thread": t,
            "stop_event": stop_event,
            "status": {"running": True, "strategy_name": cfg.strategy_name, "last_message": "Execution starting..."},
        }
    set_strategy_enabled(strategy_id, True)
    t.start()
    return True, "Execution started."


def stop_strategy(strategy_id: str) -> tuple[bool, str]:
    with EXECUTION_REGISTRY_LOCK:
        entry = dict(EXECUTION_REGISTRY.get(strategy_id, {}))
    if not entry:
        set_strategy_enabled(strategy_id, False)
        return True, "Already stopped."
    e = entry.get("stop_event")
    t = entry.get("thread")
    if isinstance(e, threading.Event):
        e.set()
    if isinstance(t, threading.Thread) and t.is_alive():
        t.join(timeout=3.0)
    set_strategy_enabled(strategy_id, False)
    return True, "Execution stopped."


def running_ids() -> list[str]:
    out: list[str] = []
    with EXECUTION_REGISTRY_LOCK:
        items = list(EXECUTION_REGISTRY.items())
    for sid, entry in items:
        t = entry.get("thread")
        if isinstance(t, threading.Thread) and t.is_alive():
            out.append(sid)
    return out


def status_for(strategy_id: str) -> dict[str, Any]:
    with EXECUTION_REGISTRY_LOCK:
        entry = dict(EXECUTION_REGISTRY.get(strategy_id, {}))
    t = entry.get("thread")
    s = dict(entry.get("status") or {})
    s["running"] = bool(isinstance(t, threading.Thread) and t.is_alive())
    return s


def fetch_quotes() -> str:
    client = ensure_client()
    env = load_env()
    n_token = env.get("KOTAK_NIFTY_TOKEN", "") or "26000"
    b_token = env.get("KOTAK_BANKNIFTY_TOKEN", "") or "26009"
    with CLIENT_LOCK:
        quote = client.quotes(
            instrument_tokens=[
                {"instrument_token": n_token, "exchange_segment": "nse_cm"},
                {"instrument_token": b_token, "exchange_segment": "nse_cm"},
            ],
            quote_type="ohlc",
            isIndex=True,
        )
    items = _collect_items(quote)
    if len(items) < 2:
        return "Quotes unavailable."

    n_ltp, n_prev = _extract_ltp_prev(items[0])
    b_ltp, b_prev = _extract_ltp_prev(items[1])
    LAST_QUOTES["NIFTY"] = {"ltp": n_ltp, "prev": n_prev}
    LAST_QUOTES["BANKNIFTY"] = {"ltp": b_ltp, "prev": b_prev}

    def fmt(name: str, ltp: float | None, prev: float | None) -> str:
        if ltp is None:
            return f"{name}: -- (--%)"
        if prev in (None, 0):
            return f"{name}: {ltp:,.2f} (--%)"
        pct = ((ltp - prev) / prev) * 100
        sign = "+" if pct >= 0 else ""
        return f"{name}: {ltp:,.2f} ({sign}{pct:.2f}%)"

    return fmt("NIFTY", n_ltp, n_prev) + "\n" + fmt("BANKNIFTY", b_ltp, b_prev)


def fetch_portfolio_snapshot() -> str:
    client = ensure_client()
    with CLIENT_LOCK:
        positions = client.positions()
        limits = client.limits()
    rows = positions.get("data", []) if isinstance(positions, dict) else []
    mtm = 0.0
    for r in rows:
        if isinstance(r, dict):
            mtm += _to_float(r.get("mtm") or r.get("pnl") or r.get("dayMtm")) or 0.0
    margin = "--"
    if isinstance(limits, dict):
        obj = limits.get("data") if isinstance(limits.get("data"), dict) else limits
        margin = (
            obj.get("margin_available")
            or obj.get("available_margin")
            or obj.get("availableMargin")
            or obj.get("cash_available")
            or obj.get("cashAvailable")
            or "--"
        )
    return f"Positions: {len(rows)}\nMTM: {mtm:,.2f}\nMargin Available: {margin}"


def strategy_keyboard(rows: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    keys = []
    running = set(running_ids())
    for row in rows:
        sid = str(row.get("strategy_id") or "")
        name = str(row.get("strategy_name") or "Unnamed")
        if not sid:
            continue
        is_on = bool(row.get("execution_enabled")) or (sid in running)
        btn = InlineKeyboardButton(("🟢 ON " if is_on else "⚪ OFF ") + name[:30], callback_data=f"toggle:{sid}:{0 if is_on else 1}")
        keys.append([btn])
    keys.append([InlineKeyboardButton("Refresh", callback_data="refresh:list")])
    return InlineKeyboardMarkup(keys)


async def require_access(update: Update) -> bool:
    if not is_allowed(update):
        if update.effective_message:
            await update.effective_message.reply_text("Unauthorized chat.")
        return False
    return True


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    await update.message.reply_text(
        "Kotak Control Bot\n\n"
        "/login <totp>\n"
        "/logout\n"
        "/strategies\n"
        "/run <strategy_id>\n"
        "/stop <strategy_id>\n"
        "/stopall\n"
        "/status [strategy_id]\n"
        "/quotes\n"
        "/portfolio"
    )


async def cmd_login(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    if NeoAPI is None:
        await update.message.reply_text(f"Kotak SDK import failed: {SDK_IMPORT_ERROR}")
        return
    if not context.args:
        await update.message.reply_text("Usage: /login <6-digit totp>")
        return
    totp = context.args[0].strip()
    if not (totp.isdigit() and len(totp) == 6):
        await update.message.reply_text("Invalid TOTP format.")
        return
    try:
        consumer_key, mobile, ucc, mpin = validate_credentials_env()
        client = NeoAPI(environment="prod", access_token=None, neo_fin_key=None, consumer_key=consumer_key)
        with CLIENT_LOCK:
            client.totp_login(mobile_number=mobile, ucc=ucc, totp=totp)
            client.totp_validate(mpin=mpin)
        if not (client.configuration.edit_token and client.configuration.edit_sid):
            await update.message.reply_text("Login failed: Invalid TOTP/MPIN.")
            return
        global AUTH_CLIENT
        AUTH_CLIENT = client
        await update.message.reply_text("Kotak session active.")
    except Exception as error:  # noqa: BLE001
        await update.message.reply_text(f"Login failed: {error}")


async def cmd_logout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    global AUTH_CLIENT
    AUTH_CLIENT = None
    with EXECUTION_REGISTRY_LOCK:
        entries = [dict(x) for x in EXECUTION_REGISTRY.values()]
    for e in entries:
        ev = e.get("stop_event")
        if isinstance(ev, threading.Event):
            ev.set()
    for e in entries:
        t = e.get("thread")
        if isinstance(t, threading.Thread) and t.is_alive():
            t.join(timeout=2.0)
    await update.message.reply_text("Logged out and all strategy threads stopped.")


async def cmd_strategies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    rows = load_strategies()
    if not rows:
        await update.message.reply_text("No saved strategies.")
        return
    await update.message.reply_text("Strategies (toggle ON/OFF):", reply_markup=strategy_keyboard(rows))


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /run <strategy_id>")
        return
    ok, msg = start_strategy(context.args[0].strip())
    await update.message.reply_text(msg if ok else f"Failed: {msg}")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /stop <strategy_id>")
        return
    ok, msg = stop_strategy(context.args[0].strip())
    await update.message.reply_text(msg if ok else f"Failed: {msg}")


async def cmd_stopall(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    rows = load_strategies()
    for row in rows:
        sid = str(row.get("strategy_id") or "")
        if sid:
            stop_strategy(sid)
    await update.message.reply_text("Stop requested for all strategies.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    if context.args:
        sid = context.args[0].strip()
        st = status_for(sid)
        await update.message.reply_text(
            f"{sid}\n"
            f"running: {st.get('running')}\n"
            f"strategy: {st.get('strategy_name')}\n"
            f"started: {st.get('started_at')}\n"
            f"stopped: {st.get('stopped_at')}\n"
            f"message: {st.get('last_message')}\n"
            f"error: {st.get('last_error')}"
        )
        return
    rids = running_ids()
    await update.message.reply_text("Running: " + (", ".join(rids) if rids else "none"))


async def cmd_quotes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    try:
        text = fetch_quotes()
    except Exception as error:  # noqa: BLE001
        text = f"Quote error: {error}"
    await update.message.reply_text(text)


async def cmd_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    try:
        text = fetch_portfolio_snapshot()
    except Exception as error:  # noqa: BLE001
        text = f"Portfolio error: {error}"
    await update.message.reply_text(text)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_access(update):
        return
    q = update.callback_query
    if q is None:
        return
    await q.answer()
    data = q.data or ""
    if data.startswith("toggle:"):
        _, sid, enabled_str = data.split(":", 2)
        enabled = enabled_str == "1"
        ok, msg = start_strategy(sid) if enabled else stop_strategy(sid)
        rows = load_strategies()
        await q.edit_message_text(f"{msg if ok else 'Failed: ' + msg}\n\nStrategies:", reply_markup=strategy_keyboard(rows))
        return
    if data == "refresh:list":
        rows = load_strategies()
        await q.edit_message_text("Strategies (toggle ON/OFF):", reply_markup=strategy_keyboard(rows))


def build_application() -> Application:
    env = load_env()
    token = env.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise ValueError("Missing TELEGRAM_BOT_TOKEN in .kotak.env")
    if not allowed_chat_ids():
        raise ValueError("Set TELEGRAM_ALLOWED_CHAT_IDS in .kotak.env")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("login", cmd_login))
    app.add_handler(CommandHandler("logout", cmd_logout))
    app.add_handler(CommandHandler("strategies", cmd_strategies))
    app.add_handler(CommandHandler("run", cmd_run))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("stopall", cmd_stopall))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("quotes", cmd_quotes))
    app.add_handler(CommandHandler("portfolio", cmd_portfolio))
    app.add_handler(CallbackQueryHandler(on_callback))
    return app


def run_bot() -> None:
    app = build_application()
    log.info("Telegram bot started.")
    app.run_polling(close_loop=False)


def start_bot_background() -> threading.Thread | None:
    try:
        _ = build_application()
    except Exception as error:  # noqa: BLE001
        log.warning("Telegram bot disabled: %s", error)
        return None
    t = threading.Thread(target=run_bot, daemon=True, name="telegram-bot")
    t.start()
    return t


if __name__ == "__main__":
    run_bot()
