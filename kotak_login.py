from __future__ import annotations

import sys
import json
import asyncio
import threading
import uuid
import os
from pathlib import Path
from typing import Any
from datetime import datetime, time
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from flask import Flask, jsonify, redirect, render_template, request, send_from_directory, session, url_for

try:
    import six  # noqa: F401
except Exception:  # noqa: BLE001
    try:
        from pip._vendor import six as pip_six

        sys.modules["six"] = pip_six
    except Exception:  # noqa: BLE001
        pass

try:
    from neo_api_client import NeoAPI
    SDK_IMPORT_ERROR = None
except Exception as import_error:  # noqa: BLE001
    NeoAPI = None
    SDK_IMPORT_ERROR = str(import_error)

from kotak_execution_engine import Index, StrategyEngine, parse_payload
from telegram_bot import start_bot_background

app = Flask(__name__, template_folder='.')
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'kotak-neo-local-secret')
app.config['SESSION_PERMANENT'] = False

AUTH_CLIENT: NeoAPI | None = None
DEFAULT_NIFTY_TOKEN = '26000'
DEFAULT_BANKNIFTY_TOKEN = '26009'
DEFAULT_SENSEX_TOKEN = '1'
IST = ZoneInfo('Asia/Kolkata')

LIVE_QUOTES = {
    'nifty': {'ltp': None, 'prev_close': None},
    'banknifty': {'ltp': None, 'prev_close': None},
}
STRATEGY_STORE_PATH = Path('strategies.json')
EXECUTION_REGISTRY: dict[str, dict[str, Any]] = {}
EXECUTION_REGISTRY_LOCK = threading.Lock()


def _load_strategies() -> list[dict[str, Any]]:
    if not STRATEGY_STORE_PATH.exists():
        return []
    try:
        data = json.loads(STRATEGY_STORE_PATH.read_text(encoding='utf-8'))
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
    except Exception:
        return []
    return []


def _save_strategies(rows: list[dict[str, Any]]) -> None:
    STRATEGY_STORE_PATH.write_text(json.dumps(rows, indent=2), encoding='utf-8')


def _set_strategy_enabled(strategy_id: str, enabled: bool) -> bool:
    rows = _load_strategies()
    updated = False
    for row in rows:
        if str(row.get('strategy_id')) == str(strategy_id):
            row['execution_enabled'] = bool(enabled)
            payload = row.get('payload')
            if isinstance(payload, dict):
                payload['execution_enabled'] = bool(enabled)
                row['payload'] = payload
            updated = True
            break
    if updated:
        _save_strategies(rows)
    return updated


def _upsert_strategy(payload: dict[str, Any]) -> dict[str, Any]:
    strategies = _load_strategies()
    sid = str(payload.get('strategy_id') or '').strip() or str(uuid.uuid4())
    now = datetime.now(IST).isoformat()
    parsed = parse_payload(payload)
    record = {
        'strategy_id': sid,
        'strategy_name': parsed.strategy_name,
        'strategy_description': parsed.strategy_description,
        'saved_at': now,
        'last_deployed': payload.get('last_deployed'),
        'today_pnl': payload.get('today_pnl', 'Rs0.00'),
        'execution_enabled': bool(payload.get('execution_enabled', False)),
        'payload': payload | {'strategy_id': sid},
    }
    replaced = False
    for i, row in enumerate(strategies):
        if str(row.get('strategy_id')) == sid:
            old_last_deployed = row.get('last_deployed')
            old_enabled = bool(row.get('execution_enabled', False))
            if old_last_deployed and not record.get('last_deployed'):
                record['last_deployed'] = old_last_deployed
            if 'execution_enabled' not in payload:
                record['execution_enabled'] = old_enabled
            strategies[i] = record
            replaced = True
            break
    if not replaced:
        strategies.append(record)
    _save_strategies(strategies)
    return record


def is_market_open_ist() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    start = time(9, 15)
    end = time(15, 30)
    return start <= now.time() <= end


def load_kotak_env() -> dict[str, str]:
    env_path = Path('.venv/.kotak.env')
    if not env_path.exists():
        raise ValueError('Missing .venv/.kotak.env file.')

    data = dotenv_values(env_path)
    return {k: (v.strip() if isinstance(v, str) else '') for k, v in data.items()}


def validate_mobile(mobile_number: str) -> None:
    normalized = mobile_number.replace(' ', '')
    if not normalized.startswith('+') or not normalized[1:].isdigit():
        raise ValueError('KOTAK_MOBILE_NUMBER must include country code, e.g. +919876543210.')


def validate_env_credentials() -> tuple[str, str, str, str, str, str]:
    env_data = load_kotak_env()
    consumer_key = env_data.get('KOTAK_CONSUMER_KEY', '')
    mobile_number = env_data.get('KOTAK_MOBILE_NUMBER', '')
    ucc = env_data.get('KOTAK_UCC', '')
    mpin = env_data.get('KOTAK_MPIN', '')
    nifty_token = env_data.get('KOTAK_NIFTY_TOKEN', '') or DEFAULT_NIFTY_TOKEN
    banknifty_token = env_data.get('KOTAK_BANKNIFTY_TOKEN', '') or DEFAULT_BANKNIFTY_TOKEN

    if not consumer_key:
        raise ValueError('Missing KOTAK_CONSUMER_KEY in .venv/.kotak.env')
    if not mobile_number:
        raise ValueError('Missing KOTAK_MOBILE_NUMBER in .venv/.kotak.env')
    if not ucc:
        raise ValueError('Missing KOTAK_UCC in .venv/.kotak.env')
    if not mpin:
        raise ValueError('Missing KOTAK_MPIN in .venv/.kotak.env')
    if not mpin.isdigit():
        raise ValueError('KOTAK_MPIN must be numeric.')

    validate_mobile(mobile_number)
    return consumer_key, mobile_number, ucc, mpin, nifty_token, banknifty_token


def _env_float(key: str) -> float | None:
    try:
        return float(load_kotak_env().get(key, '').replace(',', '').strip())
    except Exception:
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(',', '').strip()
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _extract_quote_fields(item: dict[str, Any]) -> tuple[float | None, float | None]:
    ltp = _to_float(item.get('ltp') or item.get('LTP') or item.get('last_traded_price'))
    prev_close = _to_float(item.get('close') or item.get('previous_close') or item.get('prevClose'))

    if ltp is None and isinstance(item.get('ohlc'), dict):
        ohlc = item['ohlc']
        ltp = _to_float(ohlc.get('ltp') or ohlc.get('close'))
        prev_close = _to_float(ohlc.get('prev_close') or ohlc.get('close'))

    return ltp, prev_close


def _collect_quote_items(quote_response: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if isinstance(quote_response, list):
        return [x for x in quote_response if isinstance(x, dict)]
    if not isinstance(quote_response, dict):
        return items

    for key in ('data', 'message', 'result', 'quotes'):
        value = quote_response.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            nested_list = value.get('data') or value.get('message')
            if isinstance(nested_list, list):
                return [x for x in nested_list if isinstance(x, dict)]
    return items


def _format_index(
    ltp: float | None, prev_close: float | None, market_open: bool
) -> tuple[str, str, str]:
    if not market_open and prev_close is not None:
        return f'{prev_close:,.2f}', '(CLOSE)', 'neutral'
    if ltp is None and prev_close is not None:
        return f'{prev_close:,.2f}', '(+0.00%)', 'neutral'
    if ltp is None:
        return '--', '(--%)', 'neutral'

    if prev_close in (None, 0):
        return f'{ltp:,.2f}', '(--%)', 'neutral'

    pct = ((ltp - prev_close) / prev_close) * 100
    sign = '+' if pct >= 0 else ''
    tone = 'pos' if pct >= 0 else 'neg'
    return f'{ltp:,.2f}', f'({sign}{pct:.2f}%)', tone


def _atm_from_price(price: float | None, step: int) -> str:
    if price is None:
        return '--'
    try:
        return f'{int(round(float(price) / step) * step)}'
    except Exception:
        return '--'


def get_live_indices() -> dict[str, str]:
    market_open = is_market_open_ist()
    nifty_prev_env = _env_float('KOTAK_NIFTY_PREV_CLOSE')
    banknifty_prev_env = _env_float('KOTAK_BANKNIFTY_PREV_CLOSE')
    default_data = {
        'nifty_value': f'{nifty_prev_env:,.2f}' if nifty_prev_env else '--',
        'nifty_change': '(CLOSE)' if (nifty_prev_env and not market_open) else ('(+0.00%)' if nifty_prev_env else '(--%)'),
        'nifty_tone': 'neutral',
        'nifty_atm': _atm_from_price(nifty_prev_env, 50),
        'banknifty_value': f'{banknifty_prev_env:,.2f}' if banknifty_prev_env else '--',
        'banknifty_change': '(CLOSE)' if (banknifty_prev_env and not market_open) else ('(+0.00%)' if banknifty_prev_env else '(--%)'),
        'banknifty_tone': 'neutral',
        'banknifty_atm': _atm_from_price(banknifty_prev_env, 100),
    }

    if AUTH_CLIENT is None:
        return default_data

    try:
        _, _, _, _, nifty_token, banknifty_token = validate_env_credentials()
        if not nifty_token or not banknifty_token:
            return default_data

        instrument_tokens = [
            {'instrument_token': nifty_token, 'exchange_segment': 'nse_cm'},
            {'instrument_token': banknifty_token, 'exchange_segment': 'nse_cm'},
        ]

        items: list[dict[str, Any]] = []
        for quote_type, is_index in (
            ('ltp', True),
            ('ohlc', True),
            ('ltp', False),
            ('ohlc', False),
        ):
            try:
                quote_response = AUTH_CLIENT.quotes(
                    instrument_tokens=instrument_tokens,
                    quote_type=quote_type,
                    isIndex=is_index,
                )
                items = _collect_quote_items(quote_response)
                if len(items) >= 2:
                    break
            except Exception:
                continue

        if len(items) >= 2:
            token_map: dict[str, tuple[float | None, float | None]] = {}
            for item in items:
                tk, ltp_s, prev_s = _extract_stream_fields(item)
                ltp_q, prev_q = _extract_quote_fields(item)
                token = str(tk or item.get('instrument_token') or item.get('tk') or '')
                token_map[token] = (ltp_s if ltp_s is not None else ltp_q, prev_s if prev_s is not None else prev_q)

            n_ltp, n_prev = token_map.get(str(nifty_token), (None, None))
            b_ltp, b_prev = token_map.get(str(banknifty_token), (None, None))
            if n_ltp is None or b_ltp is None:
                # fallback to order if token keys are absent
                n_ltp, n_prev = _extract_quote_fields(items[0])
                b_ltp, b_prev = _extract_quote_fields(items[1])
            nv, nc, nt = _format_index(n_ltp, n_prev, market_open=market_open)
            bv, bc, bt = _format_index(b_ltp, b_prev, market_open=market_open)
            data = {
                'nifty_value': nv,
                'nifty_change': nc,
                'nifty_tone': nt,
                'nifty_atm': _atm_from_price(n_ltp if n_ltp is not None else n_prev, 50),
                'banknifty_value': bv,
                'banknifty_change': bc,
                'banknifty_tone': bt,
                'banknifty_atm': _atm_from_price(b_ltp if b_ltp is not None else b_prev, 100),
            }
            return data
    except Exception:
        pass

    return default_data


def get_index_spot_and_atm(index_name: str) -> dict[str, Any]:
    index_key = (index_name or '').strip().upper()
    token_map = {
        'NIFTY': DEFAULT_NIFTY_TOKEN,
        'BANKNIFTY': DEFAULT_BANKNIFTY_TOKEN,
        'SENSEX': DEFAULT_SENSEX_TOKEN,
    }
    step_map = {
        'NIFTY': 50,
        'BANKNIFTY': 100,
        'SENSEX': 100,
    }
    if index_key not in token_map:
        return {'ok': False, 'message': 'Unsupported index.'}

    if AUTH_CLIENT is None:
        return {'ok': False, 'message': 'Session is not active.'}

    try:
        env_data = load_kotak_env()
        token = env_data.get(f'KOTAK_{index_key}_TOKEN', '') or token_map[index_key]
        quote = AUTH_CLIENT.quotes(
            instrument_tokens=[{'instrument_token': token, 'exchange_segment': 'nse_cm'}],
            quote_type='ltp',
            isIndex=True,
        )
        items = _collect_quote_items(quote)
        if not items:
            quote = AUTH_CLIENT.quotes(
                instrument_tokens=[{'instrument_token': token, 'exchange_segment': 'nse_cm'}],
                quote_type='ohlc',
                isIndex=True,
            )
            items = _collect_quote_items(quote)
        if not items:
            return {'ok': False, 'message': 'Index quote unavailable.'}

        ltp, _prev = _extract_quote_fields(items[0])
        if ltp is None:
            tk, ltp2, _ = _extract_stream_fields(items[0])
            ltp = ltp2
        if ltp is None:
            return {'ok': False, 'message': 'Index LTP unavailable.'}

        step = step_map[index_key]
        atm = round(float(ltp) / step) * step
        return {'ok': True, 'index': index_key, 'spot': float(ltp), 'atm_strike': int(atm), 'step': step}
    except Exception as error:  # noqa: BLE001
        return {'ok': False, 'message': f'Failed to fetch index quote: {error}'}




def _extract_stream_fields(message: Any) -> tuple[str | None, float | None, float | None]:
    if not isinstance(message, dict):
        return None, None, None

    token = str(message.get('tk') or message.get('token') or message.get('instrument_token') or '')
    ltp = _to_float(message.get('ltp') or message.get('LTP') or message.get('lp') or message.get('last_traded_price'))
    prev_close = _to_float(
        message.get('close') or message.get('c') or message.get('previous_close') or message.get('prevClose')
    )

    if ltp is None and isinstance(message.get('ohlc'), dict):
        ohlc = message['ohlc']
        ltp = _to_float(ohlc.get('ltp') or ohlc.get('close') or ohlc.get('c'))
        if prev_close is None:
            prev_close = _to_float(ohlc.get('prev_close') or ohlc.get('close'))

    if not token and isinstance(message.get('data'), dict):
        nested = message['data']
        token = str(nested.get('tk') or nested.get('token') or nested.get('instrument_token') or '')
        if ltp is None:
            ltp = _to_float(nested.get('ltp') or nested.get('LTP') or nested.get('lp'))
        if prev_close is None:
            prev_close = _to_float(nested.get('close') or nested.get('previous_close') or nested.get('prevClose'))

    return token or None, ltp, prev_close


def start_quote_stream(client: NeoAPI, nifty_token: str, banknifty_token: str) -> None:
    def on_message(message: Any) -> None:
        token, ltp, prev_close = _extract_stream_fields(message)
        if token is None:
            return

        if token == str(nifty_token):
            if ltp is not None:
                LIVE_QUOTES['nifty']['ltp'] = ltp
            if prev_close is not None:
                LIVE_QUOTES['nifty']['prev_close'] = prev_close
        elif token == str(banknifty_token):
            if ltp is not None:
                LIVE_QUOTES['banknifty']['ltp'] = ltp
            if prev_close is not None:
                LIVE_QUOTES['banknifty']['prev_close'] = prev_close

    client.on_message = on_message
    client.on_error = lambda message: None
    client.on_close = lambda message: None
    client.subscribe(
        instrument_tokens=[
            {'instrument_token': nifty_token, 'exchange_segment': 'nse_cm'},
            {'instrument_token': banknifty_token, 'exchange_segment': 'nse_cm'},
        ],
        isIndex=True,
        isDepth=False,
    )

def _safe_number(value: Any) -> float:
    parsed = _to_float(value)
    return parsed if parsed is not None else 0.0


def get_portfolio_analytics() -> dict[str, Any]:
    data: dict[str, Any] = {
        'portfolio_error': None,
        'holdings': [],
        'positions': [],
        'limits': {},
        'total_holding_value': 0.0,
        'total_position_mtm': 0.0,
    }

    if AUTH_CLIENT is None:
        data['portfolio_error'] = 'Session is not active. Please login again.'
        return data

    try:
        holdings_resp = AUTH_CLIENT.holdings()
        positions_resp = AUTH_CLIENT.positions()
        limits_resp = AUTH_CLIENT.limits()
    except Exception as error:  # noqa: BLE001
        data['portfolio_error'] = f'Failed to fetch portfolio data: {error}'
        return data

    holdings = []
    if isinstance(holdings_resp, list):
        holdings = holdings_resp
    elif isinstance(holdings_resp, dict):
        if isinstance(holdings_resp.get('data'), list):
            holdings = holdings_resp['data']
        elif isinstance(holdings_resp.get('message'), list):
            holdings = holdings_resp['message']

    positions = []
    if isinstance(positions_resp, list):
        positions = positions_resp
    elif isinstance(positions_resp, dict):
        if isinstance(positions_resp.get('data'), list):
            positions = positions_resp['data']
        elif isinstance(positions_resp.get('message'), list):
            positions = positions_resp['message']

    limits: dict[str, Any] = {}
    if isinstance(limits_resp, dict):
        if isinstance(limits_resp.get('data'), dict):
            limits = limits_resp['data']
        elif isinstance(limits_resp.get('message'), dict):
            limits = limits_resp['message']
        else:
            limits = limits_resp

    total_holding_value = 0.0
    for row in holdings:
        if not isinstance(row, dict):
            continue
        qty = _safe_number(row.get('quantity') or row.get('qty') or row.get('holdingQuantity'))
        ltp = _safe_number(row.get('ltp') or row.get('lastPrice') or row.get('last_traded_price'))
        total_holding_value += qty * ltp

    total_position_mtm = 0.0
    for row in positions:
        if not isinstance(row, dict):
            continue
        total_position_mtm += _safe_number(
            row.get('mtm') or row.get('pnl') or row.get('pnl_mtm') or row.get('dayMtm')
        )

    data['holdings'] = [row for row in holdings if isinstance(row, dict)]
    data['positions'] = [row for row in positions if isinstance(row, dict)]
    data['limits'] = limits
    data['total_holding_value'] = total_holding_value
    data['total_position_mtm'] = total_position_mtm
    return data


def kotak_login_with_totp(totp: str) -> tuple[bool, str]:
    global AUTH_CLIENT

    if NeoAPI is None:
        return False, (
            'Kotak SDK is not available in this environment. '
            f'Import error: {SDK_IMPORT_ERROR}'
        )

    try:
        consumer_key, mobile_number, ucc, mpin, _, _ = validate_env_credentials()
    except ValueError as error:
        return False, str(error)

    try:
        client = NeoAPI(
            environment='prod',
            access_token=None,
            neo_fin_key=None,
            consumer_key=consumer_key,
        )
        login_resp = client.totp_login(mobile_number=mobile_number, ucc=ucc, totp=totp)
        _validate_resp = client.totp_validate(mpin=mpin)

        # SDK sets these only on successful totp_validate (2xx).
        if not (client.configuration.edit_token and client.configuration.edit_sid):
            # Keep message clear for UI while still covering MPIN/session failures.
            return False, 'Invalid TOTP. Please enter the correct current TOTP.'

        _, _, _, _, nifty_token, banknifty_token = validate_env_credentials()
        AUTH_CLIENT = client
        start_quote_stream(client, nifty_token, banknifty_token)
        return True, 'Login successful. Kotak session is active.'
    except Exception as error:  # noqa: BLE001
        err = str(error).lower()
        if any(x in err for x in ['totp', 'otp', 'invalid', 'expired', 'wrong', 'mismatch']):
            return False, 'Invalid TOTP. Please enter the correct current TOTP.'
        return False, f'Login failed: {error}'


class NeoSessionExecutionClient:
    """Adapter that makes authenticated NeoAPI session compatible with StrategyEngine."""

    def __init__(self, client: NeoAPI):
        self._client = client
        self._lock = threading.Lock()

    def get_index_ltp(self, index: Index) -> float:
        token_map = {'NIFTY': '26000', 'BANKNIFTY': '26009', 'SENSEX': '1'}
        token = token_map.get(index.value, '26000')
        with self._lock:
            quote = self._client.quotes(
                instrument_tokens=[{'instrument_token': token, 'exchange_segment': 'nse_cm'}],
                quote_type='ltp',
            )
        items = _collect_quote_items(quote)
        if not items:
            raise RuntimeError(f'Failed to fetch {index.value} quote.')
        ltp, _ = _extract_quote_fields(items[0])
        if ltp is None:
            raise RuntimeError(f'{index.value} LTP is unavailable.')
        return float(ltp)

    def find_option_token(self, index: Index, expiry_date: str, strike: int, option_type: Any):
        suffix = 'CE' if str(option_type.value if hasattr(option_type, 'value') else option_type).lower().startswith('call') else 'PE'
        symbol = f'{index.value}{expiry_date}{strike}{suffix}'
        with self._lock:
            resp = self._client.search_scrip(exchange_segment='nfo', symbol=symbol)
        rows = resp.get('data', []) if isinstance(resp, dict) else []
        if not rows:
            raise ValueError(f'Option contract not found: {symbol}')
        row = rows[0]
        return row.get('trading_symbol', symbol), str(row.get('pSymbol') or row.get('token') or '')

    def get_ltp(self, exchange: str, trading_symbol: str, token: str) -> float:
        with self._lock:
            quote = self._client.quotes(
                instrument_tokens=[{'instrument_token': token, 'exchange_segment': exchange}],
                quote_type='ltp',
            )
        items = _collect_quote_items(quote)
        if not items:
            raise RuntimeError(f'LTP not found for {trading_symbol}')
        ltp, _ = _extract_quote_fields(items[0])
        if ltp is None:
            raise RuntimeError(f'LTP unavailable for {trading_symbol}')
        return float(ltp)

    def place_order(self, trading_symbol: str, token: str, exchange: str, transaction_type: str, quantity: int, order_type: str = 'MKT', price: float = 0.0, trigger_price: float = 0.0, product: str = 'NRML', tag: str = '') -> str:
        with self._lock:
            resp = self._client.place_order(
                exchange_segment=exchange,
                product=product,
                price=str(price),
                order_type=order_type,
                quantity=str(quantity),
                validity='DAY',
                trading_symbol=trading_symbol,
                transaction_type=transaction_type,
                amo='NO',
                disclosed_quantity='0',
                market_protection='0',
                pf='N',
                trigger_price=str(trigger_price),
                tag=tag,
            )
        if not isinstance(resp, dict):
            raise RuntimeError('Order placement failed with empty response.')
        return str(resp.get('nOrdNo') or '')

    def cancel_order(self, order_id: str, is_amo: bool = False) -> None:
        with self._lock:
            self._client.cancel_order(order_id=order_id, isAMO=is_amo)

    def get_order_status(self, order_id: str) -> dict[str, Any]:
        with self._lock:
            report = self._client.order_report()
        rows = report.get('data', []) if isinstance(report, dict) else []
        for row in rows:
            if isinstance(row, dict) and str(row.get('nOrdNo')) == str(order_id):
                return row
        return {}

    def get_positions(self):
        with self._lock:
            positions = self._client.positions()
        if isinstance(positions, dict):
            return positions.get('data', []) or positions.get('message', []) or []
        if isinstance(positions, list):
            return positions
        return []

    def search_scrip(self, exchange_segment: str, symbol: str) -> dict[str, Any]:
        with self._lock:
            resp = self._client.search_scrip(exchange_segment=exchange_segment, symbol=symbol)
        return resp if isinstance(resp, dict) else {}


def _write_strategy_payload(payload: dict[str, Any]) -> Path:
    path = Path('strategy.json')
    path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    return path


def _write_strategy_payload_by_id(strategy_id: str, payload: dict[str, Any]) -> Path:
    path = Path('strategy.json')
    path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    return path


def _delete_strategy_payload_files(strategy_id: str) -> None:
    root_strategy = Path('strategy.json')
    if root_strategy.exists():
        try:
            raw = json.loads(root_strategy.read_text(encoding='utf-8'))
            if str(raw.get('strategy_id') or '') == strategy_id:
                root_strategy.unlink(missing_ok=True)
        except Exception:
            pass


def _running_strategy_ids() -> list[str]:
    ids: list[str] = []
    with EXECUTION_REGISTRY_LOCK:
        items = list(EXECUTION_REGISTRY.items())
    for sid, entry in items:
        thread = entry.get('thread')
        if isinstance(thread, threading.Thread) and thread.is_alive():
            ids.append(sid)
    return ids


def _strategy_status(strategy_id: str) -> dict[str, Any]:
    with EXECUTION_REGISTRY_LOCK:
        entry = dict(EXECUTION_REGISTRY.get(strategy_id, {}))
    thread = entry.get('thread')
    status = dict(entry.get('status') or {})
    status['running'] = bool(isinstance(thread, threading.Thread) and thread.is_alive())
    return status


def _execution_worker(payload: dict[str, Any], strategy_id: str, stop_event: threading.Event) -> None:
    try:
        if AUTH_CLIENT is None:
            raise RuntimeError('Kotak session is not active. Login again.')

        cfg = parse_payload(payload)
        engine = StrategyEngine(NeoSessionExecutionClient(AUTH_CLIENT), cfg)
        with EXECUTION_REGISTRY_LOCK:
            status = EXECUTION_REGISTRY.setdefault(strategy_id, {}).setdefault('status', {})
        status.update({
            'running': True,
            'strategy_name': cfg.strategy_name,
            'started_at': datetime.now(IST).isoformat(),
            'stopped_at': None,
            'last_error': None,
            'last_message': 'Execution started.',
        })
        strategies = _load_strategies()
        for row in strategies:
            if str(row.get('strategy_id')) == str(strategy_id):
                row['last_deployed'] = datetime.now(IST).isoformat()
                break
        _save_strategies(strategies)

        async def _runner() -> None:
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

        asyncio.run(_runner())
        status.update({
            'running': False,
            'stopped_at': datetime.now(IST).isoformat(),
            'last_message': 'Execution stopped.',
        })
    except Exception as error:  # noqa: BLE001
        with EXECUTION_REGISTRY_LOCK:
            status = EXECUTION_REGISTRY.setdefault(strategy_id, {}).setdefault('status', {})
        status.update({
            'running': False,
            'stopped_at': datetime.now(IST).isoformat(),
            'last_error': str(error),
            'last_message': 'Execution failed.',
        })


@app.after_request
def disable_page_cache(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.get('/')
def home():
    if session.get('logged_in'):
        return redirect(url_for('dashboard'))
    return render_template('login.html', error=None, success=None)


@app.get('/home')
def dashboard():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        session.clear()
        return redirect(url_for('home'))

    quote_data = get_live_indices()
    strategies = _load_strategies()
    return render_template('home.html', strategies=strategies, running_strategy_ids=_running_strategy_ids(), **quote_data)


@app.get('/strategy-builder')
def strategy_builder():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        session.clear()
        return redirect(url_for('home'))

    quote_data = get_live_indices()
    return render_template('strategy_builder.html', **quote_data)


@app.get('/backtest-builder')
def backtest_builder():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        session.clear()
        return redirect(url_for('home'))

    quote_data = get_live_indices()
    strategy_name = request.args.get('strategy_name', '').strip()
    strategy_description = request.args.get('strategy_description', '').strip()
    strategy_id = request.args.get('strategy_id', '').strip()
    force_new = request.args.get('new', '').strip() == '1'
    strategy_payload = None
    if strategy_id and not force_new:
        rows = _load_strategies()
        found = next((r for r in rows if str(r.get('strategy_id')) == strategy_id), None)
        if found and isinstance(found.get('payload'), dict):
            strategy_payload = found.get('payload')
            strategy_name = strategy_payload.get('strategyName', strategy_name)
            strategy_description = strategy_payload.get('strategyDescription', strategy_description)
    return render_template(
        'backtest_builder.html',
        strategy_name=strategy_name,
        strategy_description=strategy_description,
        strategy_id='' if force_new else (strategy_id or (strategy_payload or {}).get('strategy_id', '')),
        strategy_payload=strategy_payload,
        is_new_strategy=force_new,
        **quote_data,
    )


@app.get('/portfolio-analytics')
def portfolio_analytics():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        session.clear()
        return redirect(url_for('home'))

    quote_data = get_live_indices()
    portfolio_data = get_portfolio_analytics()
    return render_template('portfolio_analytics.html', **quote_data, **portfolio_data)




@app.get('/quotes-live')
def quotes_live():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        return jsonify({'error': 'not_logged_in'}), 401

    market_open = is_market_open_ist()

    n_ltp = LIVE_QUOTES['nifty']['ltp']
    n_prev = LIVE_QUOTES['nifty']['prev_close'] or _env_float('KOTAK_NIFTY_PREV_CLOSE')
    b_ltp = LIVE_QUOTES['banknifty']['ltp']
    b_prev = LIVE_QUOTES['banknifty']['prev_close'] or _env_float('KOTAK_BANKNIFTY_PREV_CLOSE')

    # If websocket values are not yet available, fallback to direct quote fetch.
    if n_ltp is None or b_ltp is None:
        direct = get_live_indices()
        if direct.get('nifty_value') != '--' or direct.get('banknifty_value') != '--':
            return jsonify(direct)

    nv, nc, nt = _format_index(n_ltp, n_prev, market_open=market_open)
    bv, bc, bt = _format_index(b_ltp, b_prev, market_open=market_open)

    return jsonify({
        'nifty_value': nv,
        'nifty_change': nc,
        'nifty_tone': nt,
        'nifty_atm': _atm_from_price(n_ltp if n_ltp is not None else n_prev, 50),
        'banknifty_value': bv,
        'banknifty_change': bc,
        'banknifty_tone': bt,
        'banknifty_atm': _atm_from_price(b_ltp if b_ltp is not None else b_prev, 100),
    })


@app.get('/api/index-strike')
def index_strike():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        return jsonify({'ok': False, 'message': 'Session expired. Login again.'}), 401
    index_name = request.args.get('index', 'NIFTY')
    result = get_index_spot_and_atm(index_name)
    status = 200 if result.get('ok') else 400
    return jsonify(result), status


@app.post('/api/strategy/save')
def save_strategy():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        return jsonify({'ok': False, 'message': 'Session expired. Login again.'}), 401
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({'ok': False, 'message': 'Invalid payload.'}), 400
    strategy_name = str(payload.get('strategyName') or '').strip()
    if not strategy_name or strategy_name.lower() == 'backtest builder':
        return jsonify({'ok': False, 'message': 'Strategy name is required.'}), 400
    try:
        parse_payload(payload)
        saved = _upsert_strategy(payload)
        _write_strategy_payload(saved['payload'])
        _write_strategy_payload_by_id(saved['strategy_id'], saved['payload'])
        return jsonify({
            'ok': True,
            'message': 'Strategy saved.',
            'strategy_id': saved['strategy_id'],
            'saved_at': saved['saved_at'],
        })
    except Exception as error:  # noqa: BLE001
        return jsonify({'ok': False, 'message': f'Save failed: {error}'}), 400


@app.post('/api/execution/start')
def execution_start():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        return jsonify({'ok': False, 'message': 'Session expired. Login again.'}), 401

    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({'ok': False, 'message': 'Invalid payload.'}), 400
    strategy_name = str(payload.get('strategyName') or '').strip()
    if not strategy_name or strategy_name.lower() == 'backtest builder':
        return jsonify({'ok': False, 'message': 'Strategy name is required before starting execution.'}), 400

    try:
        parsed = parse_payload(payload)
        if not parsed.legs:
            return jsonify({'ok': False, 'message': 'Add at least one leg before starting execution.'}), 400
        _write_strategy_payload(payload)
    except Exception as error:  # noqa: BLE001
        return jsonify({'ok': False, 'message': f'Invalid strategy: {error}'}), 400

    strategy_id = str(payload.get('strategy_id') or '').strip() or f'adhoc-{uuid.uuid4()}'
    with EXECUTION_REGISTRY_LOCK:
        existing = dict(EXECUTION_REGISTRY.get(strategy_id, {}))
    existing_thread = existing.get('thread')
    if isinstance(existing_thread, threading.Thread) and existing_thread.is_alive():
        return jsonify({'ok': False, 'message': 'Execution already running for this strategy.'}), 409

    stop_event = threading.Event()
    thread = threading.Thread(
        target=_execution_worker,
        args=(payload, strategy_id, stop_event),
        daemon=True,
        name=f'strategy-exec-{strategy_id[:8]}',
    )
    with EXECUTION_REGISTRY_LOCK:
        EXECUTION_REGISTRY[strategy_id] = {'thread': thread, 'stop_event': stop_event, 'status': {'running': True, 'last_message': 'Execution starting...'}}
    thread.start()
    return jsonify({'ok': True, 'message': 'Execution started.'})


@app.post('/api/execution/stop')
def execution_stop():
    body = request.get_json(silent=True) or {}
    strategy_id = str(body.get('strategy_id') or '').strip()
    if strategy_id:
        with EXECUTION_REGISTRY_LOCK:
            entry = dict(EXECUTION_REGISTRY.get(strategy_id, {}))
        if not entry:
            return jsonify({'ok': True, 'message': 'Execution already stopped.'})
        stop_event = entry.get('stop_event')
        thread = entry.get('thread')
        if isinstance(stop_event, threading.Event):
            stop_event.set()
        if isinstance(thread, threading.Thread) and thread.is_alive():
            thread.join(timeout=3.0)
        return jsonify({'ok': True, 'message': 'Stop requested.'})

    with EXECUTION_REGISTRY_LOCK:
        entries = [dict(x) for x in EXECUTION_REGISTRY.values()]
    for entry in entries:
        stop_event = entry.get('stop_event')
        if isinstance(stop_event, threading.Event):
            stop_event.set()
    for entry in entries:
        thread = entry.get('thread')
        if isinstance(thread, threading.Thread) and thread.is_alive():
            thread.join(timeout=3.0)
    return jsonify({'ok': True, 'message': 'Stop requested for all running strategies.'})


@app.get('/api/strategy/list')
def strategy_list():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        return jsonify({'ok': False, 'message': 'Session expired. Login again.'}), 401
    rows = _load_strategies()
    running_ids = _running_strategy_ids()
    enabled_ids = [str(r.get('strategy_id')) for r in rows if bool(r.get('execution_enabled'))]
    return jsonify({
        'ok': True,
        'strategies': rows,
        'running_strategy_ids': running_ids,
        'enabled_strategy_ids': enabled_ids,
        'running': bool(running_ids),
    })


@app.post('/api/strategy/toggle')
def strategy_toggle():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        return jsonify({'ok': False, 'message': 'Session expired. Login again.'}), 401

    body = request.get_json(silent=True) or {}
    strategy_id = str(body.get('strategy_id') or '').strip()
    enabled = bool(body.get('enabled'))
    if not strategy_id:
        return jsonify({'ok': False, 'message': 'strategy_id is required.'}), 400

    rows = _load_strategies()
    selected = next((r for r in rows if str(r.get('strategy_id')) == strategy_id), None)
    if not selected:
        return jsonify({'ok': False, 'message': 'Strategy not found.'}), 404

    if enabled:
        _set_strategy_enabled(strategy_id, True)
        with EXECUTION_REGISTRY_LOCK:
            existing = dict(EXECUTION_REGISTRY.get(strategy_id, {}))
        existing_thread = existing.get('thread')
        if isinstance(existing_thread, threading.Thread) and existing_thread.is_alive():
            return jsonify({'ok': True, 'message': 'Strategy execution already running.'})
        payload = selected.get('payload') or {}
        strategy_name = str(payload.get('strategyName') or '').strip()
        if not strategy_name or strategy_name.lower() == 'backtest builder':
            return jsonify({'ok': False, 'message': 'Strategy name is required before execution.'}), 400
        try:
            parse_payload(payload)
        except Exception as error:  # noqa: BLE001
            return jsonify({'ok': False, 'message': f'Invalid strategy payload: {error}'}), 400
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_execution_worker,
            args=(payload, strategy_id, stop_event),
            daemon=True,
            name=f'strategy-exec-{strategy_id[:8]}',
        )
        with EXECUTION_REGISTRY_LOCK:
            EXECUTION_REGISTRY[strategy_id] = {'thread': thread, 'stop_event': stop_event, 'status': {'running': True, 'last_message': 'Execution starting...'}}
        thread.start()
        return jsonify({'ok': True, 'message': 'Strategy execution started.'})

    _set_strategy_enabled(strategy_id, False)
    with EXECUTION_REGISTRY_LOCK:
        entry = dict(EXECUTION_REGISTRY.get(strategy_id, {}))
    if not entry:
        return jsonify({'ok': True, 'message': 'Strategy already stopped.'})
    stop_event = entry.get('stop_event')
    thread = entry.get('thread')
    if isinstance(stop_event, threading.Event):
        stop_event.set()
    if isinstance(thread, threading.Thread) and thread.is_alive():
        thread.join(timeout=3.0)
    return jsonify({'ok': True, 'message': 'Strategy execution stopped.'})


@app.post('/api/strategy/edit')
def strategy_edit():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        return jsonify({'ok': False, 'message': 'Session expired. Login again.'}), 401

    body = request.get_json(silent=True) or {}
    strategy_id = str(body.get('strategy_id') or '').strip()
    strategy_name = str(body.get('strategy_name') or '').strip()
    strategy_description = str(body.get('strategy_description') or '').strip()

    if not strategy_id:
        return jsonify({'ok': False, 'message': 'strategy_id is required.'}), 400
    if not strategy_name:
        return jsonify({'ok': False, 'message': 'Strategy name is required.'}), 400

    rows = _load_strategies()
    updated = False
    for row in rows:
        if str(row.get('strategy_id')) == strategy_id:
            row['strategy_name'] = strategy_name
            row['strategy_description'] = strategy_description
            payload = row.get('payload') or {}
            if isinstance(payload, dict):
                payload['strategyName'] = strategy_name
                payload['strategyDescription'] = strategy_description
                row['payload'] = payload
            updated = True
            break

    if not updated:
        return jsonify({'ok': False, 'message': 'Strategy not found.'}), 404

    _save_strategies(rows)
    return jsonify({'ok': True, 'message': 'Strategy updated.'})


@app.post('/api/strategy/delete')
def strategy_delete():
    if not session.get('logged_in') or AUTH_CLIENT is None:
        return jsonify({'ok': False, 'message': 'Session expired. Login again.'}), 401
    body = request.get_json(silent=True) or {}
    strategy_id = str(body.get('strategy_id') or '').strip()
    if not strategy_id:
        return jsonify({'ok': False, 'message': 'strategy_id is required.'}), 400

    with EXECUTION_REGISTRY_LOCK:
        entry = dict(EXECUTION_REGISTRY.get(strategy_id, {}))
    thread = entry.get('thread') if isinstance(entry, dict) else None
    if isinstance(thread, threading.Thread) and thread.is_alive():
        return jsonify({'ok': False, 'message': 'Stop running strategy before deleting.'}), 409

    rows = _load_strategies()
    new_rows = [r for r in rows if str(r.get('strategy_id')) != strategy_id]
    if len(new_rows) == len(rows):
        return jsonify({'ok': False, 'message': 'Strategy not found.'}), 404
    _save_strategies(new_rows)
    _delete_strategy_payload_files(strategy_id)
    return jsonify({'ok': True, 'message': 'Strategy deleted.'})


@app.get('/api/execution/status')
def execution_status():
    strategy_id = request.args.get('strategy_id', '').strip()
    if strategy_id:
        status = _strategy_status(strategy_id)
        status['strategy_id'] = strategy_id
        return jsonify(status)

    running_ids = _running_strategy_ids()
    with EXECUTION_REGISTRY_LOCK:
        keys = list(EXECUTION_REGISTRY.keys())
    statuses = {sid: _strategy_status(sid) for sid in keys}
    return jsonify({
        'running': bool(running_ids),
        'running_strategy_ids': running_ids,
        'statuses': statuses,
    })


@app.get('/style.css')
def style():
    return send_from_directory('.', 'style.css')


@app.post('/login')
def login():
    totp = request.form.get('totp', '').strip()
    if not totp:
        return render_template('login.html', error='Please enter TOTP.', success=None)
    if not (totp.isdigit() and len(totp) == 6):
        return render_template('login.html', error='TOTP must be a 6-digit number.', success=None)

    ok, message = kotak_login_with_totp(totp)
    if ok:
        session.clear()
        session['logged_in'] = True
        return redirect(url_for('dashboard'))
    return render_template('login.html', error=message, success=None)


@app.get('/logout')
def logout():
    global AUTH_CLIENT
    with EXECUTION_REGISTRY_LOCK:
        entries = [dict(x) for x in EXECUTION_REGISTRY.values()]
    for entry in entries:
        stop_event = entry.get('stop_event')
        if isinstance(stop_event, threading.Event):
            stop_event.set()
    for entry in entries:
        thread = entry.get('thread')
        if isinstance(thread, threading.Thread) and thread.is_alive():
            thread.join(timeout=2.0)
    AUTH_CLIENT = None
    session.clear()
    return redirect(url_for('home'))


if __name__ == '__main__':
    start_bot_background()
    app.run(debug=False)
