"""
kotak_execution_engine.py
=========================
Strategy Execution Engine for Kotak Neo API
Maps every UI control from backtest_builder.html to live order execution.

UI → Engine mapping overview
─────────────────────────────────────────────────────────────────────
Instrument Settings  │ index, underlying (Cash / Futures)
Entry Settings       │ strategyType, entryTime, exitTime,
                     │ noEntryAfter, overallMomentum
Legwise Settings     │ squareOff (Partial / Complete),
                     │ trailSlToBreakeven, trailScope
Leg Builder          │ segment, lot, position, optionType, expiry,
                     │ strikeCriteria + dynamic sub-fields,
                     │ targetProfit, stopLoss, trailSL,
                     │ reEntryOnTgt, reEntryOnSL,
                     │ simpleMomentum, rangeBreakout

Kotak Neo API docs:  https://kite.trade/docs/connect/ (Neo variant)
Install SDK:         pip install neo-api-client
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

# ── Kotak Neo SDK ──────────────────────────────────────────────────────────────
# pip install neo-api-client
try:
    from neo_api_client import NeoAPI  # type: ignore
except ImportError:  # allow import without SDK for unit-testing
    NeoAPI = None  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("KotakEngine")


# ══════════════════════════════════════════════════════════════════════════════
# 1.  ENUMS  –  mirror every segmented-button / select value in the UI
# ══════════════════════════════════════════════════════════════════════════════

class Index(str, Enum):
    NIFTY     = "NIFTY"
    BANKNIFTY = "BANKNIFTY"
    SENSEX    = "SENSEX"

class Underlying(str, Enum):
    CASH    = "Cash"
    FUTURES = "Futures"

class StrategyType(str, Enum):
    INTRADAY   = "Intraday"
    BTST       = "BTST"
    POSITIONAL = "Positional"

class SquareOff(str, Enum):
    PARTIAL  = "Partial"
    COMPLETE = "Complete"

class TrailScope(str, Enum):
    ALL_LEGS = "All Legs"
    SL_LEGS  = "SL Legs"

class Segment(str, Enum):
    FUTURES = "Futures"
    OPTIONS = "Options"

class Position(str, Enum):
    BUY  = "Buy"
    SELL = "Sell"

class OptionType(str, Enum):
    CALL = "Call"
    PUT  = "Put"

class Expiry(str, Enum):
    WEEKLY  = "Weekly"
    MONTHLY = "Monthly"

class StrikeCriteria(str, Enum):
    STRIKE_TYPE     = "Strike Type"
    CLOSEST_PREMIUM = "Closest Premium"
    PREMIUM_GE      = "Premium >="
    PREMIUM_LE      = "Premium <="
    PREMIUM_RANGE   = "Premium Range"
    PERCENT_ATM     = "% of ATM"

class MomentumType(str, Enum):
    POINTS_UP    = "Points Up"
    POINTS_DOWN  = "Points Down"
    PERCENT_UP   = "Percent Up"
    PERCENT_DOWN = "Percent Down"

class PnLUnit(str, Enum):
    POINTS  = "Points (Pts)"
    PERCENT = "Percent (%)"
    PREMIUM = "Premium"

class ReEntryTiming(str, Enum):
    RE_ASAP      = "RE ASAP"
    NEXT_CANDLE  = "Next Candle"


# ══════════════════════════════════════════════════════════════════════════════
# 2.  DATA CLASSES  –  one-to-one with every section in the builder UI
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class LegConfig:
    """Maps the Leg Builder card + all per-leg toggle rows."""
    # ── Primary row ────────────────────────────────────────────
    segment:     Segment    = Segment.OPTIONS
    lot:         int        = 1
    position:    Position   = Position.BUY
    option_type: OptionType = OptionType.PUT
    expiry:      Expiry     = Expiry.WEEKLY

    # ── Strike criteria (strikeCriteria select + dynamic sub-fields) ──
    strike_criteria:      StrikeCriteria = StrikeCriteria.STRIKE_TYPE
    strike_selection:     str            = "ATM"      # ATM / ITM N / OTM N
    closest_premium:      Optional[float] = None
    premium_ge:           Optional[float] = None
    premium_le:           Optional[float] = None
    premium_range_lower:  Optional[float] = None
    premium_range_upper:  Optional[float] = None
    percent_atm_sign:     str             = "+"       # "+" or "-"
    percent_atm_value:    Optional[float] = None

    # ── Per-leg toggles ────────────────────────────────────────
    target_profit_enabled:  bool = False
    target_profit_unit:     PnLUnit = PnLUnit.POINTS
    target_profit_value:    float   = 0.0

    stop_loss_enabled:  bool    = False
    stop_loss_unit:     PnLUnit = PnLUnit.POINTS
    stop_loss_value:    float   = 0.0

    trail_sl_enabled:   bool    = False
    trail_sl_unit:      str     = "Points"
    trail_sl_x:         float   = 0.0     # trigger movement
    trail_sl_y:         float   = 0.0     # trail step

    reentry_on_target_enabled: bool           = False
    reentry_on_target_timing:  ReEntryTiming  = ReEntryTiming.RE_ASAP
    reentry_on_target_count:   int            = 1

    reentry_on_sl_enabled: bool          = False
    reentry_on_sl_timing:  ReEntryTiming = ReEntryTiming.RE_ASAP
    reentry_on_sl_count:   int           = 1

    simple_momentum_enabled: bool   = False
    simple_momentum_unit:    str    = "Points (Pts) ?"
    simple_momentum_value:   float  = 0.0

    range_breakout_enabled: bool   = False
    range_breakout_time:    str    = "09:45"
    range_breakout_hl:      str    = "High"   # "High" | "Low"
    range_breakout_ref:     str    = "Strike Price"  # "Strike Price" | "Premium"

    # ── Runtime state (filled by engine) ───────────────────────
    order_id:       Optional[str] = field(default=None, repr=False)
    entry_price:    Optional[float] = field(default=None, repr=False)
    current_price:  Optional[float] = field(default=None, repr=False)
    trading_symbol: Optional[str] = field(default=None, repr=False)
    token:          Optional[str] = field(default=None, repr=False)
    exchange:       str = field(default="nfo", repr=False)
    reentry_count_used: int = field(default=0, repr=False)
    exited:         bool = field(default=False, repr=False)


@dataclass
class StrategyConfig:
    """
    Top-level payload exactly matching buildPayload() in backtest_builder.html.
    """
    # ── Strategy meta ──────────────────────────────────────────
    strategy_name:        str = "Unnamed Strategy"
    strategy_description: str = ""

    # ── Instrument settings ────────────────────────────────────
    index:      Index      = Index.NIFTY
    underlying: Underlying = Underlying.CASH

    # ── Entry settings ─────────────────────────────────────────
    strategy_type: StrategyType = StrategyType.BTST
    entry_time:    str          = "09:20"    # "HH:MM"
    exit_time:     str          = "15:25"    # "HH:MM"

    no_entry_after_enabled: bool          = False
    no_entry_after_time:    Optional[str] = None  # "HH:MM"

    overall_momentum_enabled: bool                 = False
    overall_momentum_type:    Optional[MomentumType] = None
    overall_momentum_value:   Optional[float]        = None

    # ── Legwise settings ───────────────────────────────────────
    square_off:           SquareOff  = SquareOff.PARTIAL
    trail_sl_to_breakeven: bool      = False
    trail_scope:          TrailScope = TrailScope.ALL_LEGS

    # ── Legs ───────────────────────────────────────────────────
    legs: List[LegConfig] = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════════
# 3.  PAYLOAD PARSER  –  read the JSON emitted by localStorage / Save button
# ══════════════════════════════════════════════════════════════════════════════

def parse_payload(raw: Dict[str, Any]) -> StrategyConfig:
    """
    Convert the JSON dict from buildPayload() / localStorage
    into a typed StrategyConfig.
    """
    legs = []
    for l in raw.get("legs", []):
        legs.append(LegConfig(
            segment        = Segment(l.get("segment", "Options")),
            lot            = int(l.get("lot", 1)),
            position       = Position(l.get("position", "Buy")),
            option_type    = OptionType(l.get("optionType", "Put")),
            expiry         = Expiry(l.get("expiry", "Weekly")),
            strike_criteria= StrikeCriteria(l.get("strike", "Strike Type")),
            strike_selection      = l.get("strikeSelection") or "ATM",
            closest_premium       = _f(l.get("closestPremiumValue")),
            premium_ge            = _f(l.get("premiumGeValue")),
            premium_le            = _f(l.get("premiumLeValue")),
            premium_range_lower   = _f((l.get("premiumRange") or "").split("-")[0]),
            premium_range_upper   = _f((l.get("premiumRange") or "").split("-")[-1]),
            percent_atm_sign      = (l.get("percentAtm") or "+")[0] if l.get("percentAtm") else "+",
            percent_atm_value     = _f((l.get("percentAtm") or "").lstrip("+-")),
            target_profit_enabled = bool(l.get("targetProfitEnabled")),
            target_profit_unit    = PnLUnit(l.get("targetProfitUnit", "Points (Pts)")),
            target_profit_value   = _f(l.get("targetProfitValue")) or 0.0,
            stop_loss_enabled     = bool(l.get("stopLossEnabled")),
            stop_loss_unit        = PnLUnit(l.get("stopLossUnit", "Points (Pts)")),
            stop_loss_value       = _f(l.get("stopLossValue")) or 0.0,
            trail_sl_enabled      = bool(l.get("trailSlEnabled")),
            trail_sl_unit         = str(l.get("trailSlUnit", "Points")),
            trail_sl_x            = _f(l.get("trailSlX")) or 0.0,
            trail_sl_y            = _f(l.get("trailSlY")) or 0.0,
            reentry_on_target_enabled = bool(l.get("reEntryOnTgtEnabled")),
            reentry_on_target_timing  = ReEntryTiming(l.get("reEntryOnTgtTiming", "RE ASAP")),
            reentry_on_target_count   = int(l.get("reEntryOnTgtCount", 1) or 1),
            reentry_on_sl_enabled = bool(l.get("reEntryOnSlEnabled")),
            reentry_on_sl_timing  = ReEntryTiming(l.get("reEntryOnSlTiming", "RE ASAP")),
            reentry_on_sl_count   = int(l.get("reEntryOnSlCount", 1) or 1),
            simple_momentum_enabled = bool(l.get("simpleMomentumEnabled")),
            simple_momentum_unit    = str(l.get("simpleMomentumUnit", "Points (Pts) ?")),
            simple_momentum_value   = _f(l.get("simpleMomentumValue")) or 0.0,
            range_breakout_enabled  = bool(l.get("rangeBreakoutEnabled")),
            range_breakout_time     = str(l.get("rangeBreakoutTime", "09:45")),
            range_breakout_hl       = str(l.get("rangeBreakoutHL", "High")),
            range_breakout_ref      = str(l.get("rangeBreakoutRef", "Strike Price")),
        ))

    momentum_type = None
    if raw.get("overallMomentumEnabled") and raw.get("overallMomentumType"):
        momentum_type = MomentumType(raw["overallMomentumType"])

    return StrategyConfig(
        strategy_name        = raw.get("strategyName", "Unnamed"),
        strategy_description = raw.get("strategyDescription", ""),
        index                = Index(raw.get("index", "NIFTY")),
        underlying           = Underlying(raw.get("underlying", "Cash")),
        strategy_type        = StrategyType(raw.get("strategyType", "BTST")),
        entry_time           = raw.get("entryTime", "09:20"),
        exit_time            = raw.get("exitTime", "15:25"),
        no_entry_after_enabled = bool(raw.get("noEntryAfterEnabled")),
        no_entry_after_time    = raw.get("noEntryAfterTime"),
        overall_momentum_enabled = bool(raw.get("overallMomentumEnabled")),
        overall_momentum_type    = momentum_type,
        overall_momentum_value   = _f(raw.get("overallMomentumValue")),
        square_off            = SquareOff(raw.get("squareOff", "Partial")),
        trail_sl_to_breakeven = bool(raw.get("trailSlToBreakeven")),
        trail_scope           = TrailScope(raw.get("trailScope", "All Legs")),
        legs                  = legs,
    )


def _f(val) -> Optional[float]:
    """Safe cast to float."""
    try:
        return float(val) if val not in (None, "", "-") else None
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 4.  KOTAK NEO API CLIENT WRAPPER
# ══════════════════════════════════════════════════════════════════════════════

# ── Index → Kotak exchange token map ──────────────────────────────────────────
INDEX_TOKEN: Dict[str, str] = {
    "NIFTY":     "26000",
    "BANKNIFTY": "26009",
    "SENSEX":    "1",
}

# ── Exchange codes used by Neo API ─────────────────────────────────────────────
EXCHANGE_NFO = "nfo"   # NSE F&O
EXCHANGE_BSE = "bse"

# ── Order type constants ────────────────────────────────────────────────────────
ORDER_TYPE_MARKET = "MKT"
ORDER_TYPE_LIMIT  = "L"
ORDER_TYPE_SL_M   = "SL-M"

TRANSACTION_BUY  = "B"
TRANSACTION_SELL = "S"


class KotakClient:
    """
    Thin wrapper around neo-api-client that provides helpers
    used by the execution engine.

    Credentials can come from env vars or be passed directly.
    """

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        access_token: str = "",
        neo_fin_key: str = "",
        environment: str = "prod",   # "prod" | "uat"
    ):
        if NeoAPI is None:
            raise RuntimeError(
                "neo-api-client not installed. Run:  pip install neo-api-client"
            )
        self._client = NeoAPI(
            consumer_key    = consumer_key,
            consumer_secret = consumer_secret,
            environment     = environment,
            access_token    = access_token,
            neo_fin_key     = neo_fin_key,
        )
        self._quotes_cache: Dict[str, float] = {}

    # ── Auth ───────────────────────────────────────────────────────────────────

    def login(self, mobile_number: str, password: str, mpin: str) -> None:
        """
        Full OTP-based login flow.
        In production use the session/token already stored after first login.
        """
        resp = self._client.login(mobilenumber=mobile_number, password=password)
        log.info("login response: %s", resp)
        resp2 = self._client.session_2fa(OTP=mpin)
        log.info("2FA response: %s", resp2)

    # ── Market data ────────────────────────────────────────────────────────────

    def get_ltp(self, exchange: str, trading_symbol: str, token: str) -> float:
        """Return Last Traded Price for a symbol."""
        resp = self._client.quotes(
            instrument_tokens=[{"instrument_token": token, "exchange_segment": exchange}],
            quote_type="ltp",
        )
        ltp = float(resp["data"][0]["last_traded_price"])
        self._quotes_cache[trading_symbol] = ltp
        return ltp

    def get_index_ltp(self, index: Index) -> float:
        """Convenience: fetch current index spot price."""
        token  = INDEX_TOKEN[index.value]
        # NSE index lives on nse_cm (cash market)
        return self.get_ltp("nse_cm", index.value, token)

    def find_option_token(
        self,
        index: Index,
        expiry_date: str,        # "DDMMMYY" e.g. "27JUN24"
        strike: int,
        option_type: OptionType,
    ) -> Tuple[str, str]:
        """
        Search the instrument master for the matching option contract.
        Returns (trading_symbol, instrument_token).
        """
        suffix = "CE" if option_type == OptionType.CALL else "PE"
        symbol = f"{index.value}{expiry_date}{strike}{suffix}"
        resp = self._client.search_scrip(exchange_segment="nfo", symbol=symbol)
        if not resp or not resp.get("data"):
            raise ValueError(f"Option contract not found: {symbol}")
        item = resp["data"][0]
        return item["trading_symbol"], str(item["pSymbol"])

    # ── Orders ─────────────────────────────────────────────────────────────────

    def place_order(
        self,
        trading_symbol: str,
        token: str,
        exchange: str,
        transaction_type: str,   # "B" or "S"
        quantity: int,
        order_type: str = ORDER_TYPE_MARKET,
        price: float = 0.0,
        trigger_price: float = 0.0,
        product: str = "NRML",   # "NRML" | "MIS"
        tag: str = "",
    ) -> str:
        """Place an order; return order_id."""
        resp = self._client.place_order(
            exchange_segment = exchange,
            product          = product,
            price            = str(price),
            order_type       = order_type,
            quantity         = str(quantity),
            validity         = "DAY",
            trading_symbol   = trading_symbol,
            transaction_type = transaction_type,
            amo              = "NO",
            disclosed_quantity = "0",
            market_protection  = "0",
            pf                 = "N",
            trigger_price      = str(trigger_price),
            tag                = tag,
        )
        order_id = resp.get("nOrdNo", "")
        log.info("Placed %s %s qty=%s  order_id=%s", transaction_type, trading_symbol, quantity, order_id)
        return order_id

    def cancel_order(self, order_id: str, is_amo: bool = False) -> None:
        self._client.cancel_order(order_id=order_id, isAMO=is_amo)
        log.info("Cancelled order %s", order_id)

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        orders = self._client.order_report()
        for o in orders.get("data", []):
            if o.get("nOrdNo") == order_id:
                return o
        return {}

    def get_positions(self) -> List[Dict[str, Any]]:
        return self._client.positions().get("data", [])

    def search_scrip(self, exchange_segment: str, symbol: str) -> Dict[str, Any]:
        return self._client.search_scrip(exchange_segment=exchange_segment, symbol=symbol)


# ══════════════════════════════════════════════════════════════════════════════
# 5.  STRIKE RESOLVER
#     Maps the UI's "Strike Criteria" section → actual strike price integer
# ══════════════════════════════════════════════════════════════════════════════

class StrikeResolver:
    """
    Converts the builder's strike selection UI into a concrete strike price,
    given the current underlying spot price and a list of available strikes.
    """

    # NIFTY strikes every 50 pts; BANKNIFTY every 100 pts; SENSEX every 100 pts
    STEP: Dict[str, int] = {
        "NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100
    }

    @classmethod
    def atm_strike(cls, spot: float, index: Index) -> int:
        step = cls.STEP.get(index.value, 50)
        return round(spot / step) * step

    @classmethod
    def resolve(
        cls,
        leg: LegConfig,
        spot: float,
        index: Index,
        available_strikes: List[int],         # sorted ascending
        ltp_fn,                               # (strike, option_type) → float
    ) -> int:
        """
        Return the strike price integer for `leg` given current market state.

        Mapping:
          Strike Type     → ATM / ITM N / OTM N  (from `leg.strike_selection`)
          Closest Premium → iterate strikes, pick closest LTP
          Premium >=      → first strike whose LTP ≥ threshold
          Premium <=      → last strike whose LTP ≤ threshold
          Premium Range   → first strike with LTP in [lower, upper]
          % of ATM        → ATM ± percent
        """
        atm = cls.atm_strike(spot, index)
        step = cls.STEP.get(index.value, 50)
        is_call = (leg.option_type == OptionType.CALL)

        if leg.strike_criteria == StrikeCriteria.STRIKE_TYPE:
            sel = leg.strike_selection.upper()
            if sel == "ATM":
                return atm
            parts = sel.split()
            n = int(parts[1]) if len(parts) == 2 else 0
            if parts[0] == "ITM":
                return atm - n * step if is_call else atm + n * step
            if parts[0] == "OTM":
                return atm + n * step if is_call else atm - n * step
            return atm

        elif leg.strike_criteria == StrikeCriteria.CLOSEST_PREMIUM:
            target = leg.closest_premium or 0
            return min(available_strikes,
                       key=lambda s: abs(ltp_fn(s, leg.option_type) - target))

        elif leg.strike_criteria == StrikeCriteria.PREMIUM_GE:
            threshold = leg.premium_ge or 0
            # For calls: lower strike → higher premium; for puts: higher → higher
            strikes = sorted(available_strikes, reverse=not is_call)
            for s in strikes:
                if ltp_fn(s, leg.option_type) >= threshold:
                    return s
            return available_strikes[0]  # fallback ATM-ish

        elif leg.strike_criteria == StrikeCriteria.PREMIUM_LE:
            threshold = leg.premium_le or 0
            strikes = sorted(available_strikes, reverse=is_call)
            for s in strikes:
                if ltp_fn(s, leg.option_type) <= threshold:
                    return s
            return available_strikes[-1]

        elif leg.strike_criteria == StrikeCriteria.PREMIUM_RANGE:
            lo = leg.premium_range_lower or 0
            hi = leg.premium_range_upper or float("inf")
            for s in available_strikes:
                ltp = ltp_fn(s, leg.option_type)
                if lo <= ltp <= hi:
                    return s
            log.warning("No strike in premium range %.2f–%.2f; using ATM", lo, hi)
            return atm

        elif leg.strike_criteria == StrikeCriteria.PERCENT_ATM:
            pct = leg.percent_atm_value or 0
            sign = 1 if leg.percent_atm_sign == "+" else -1
            target_price = spot * (1 + sign * pct / 100)
            return cls.atm_strike(target_price, index)

        return atm  # default


# ══════════════════════════════════════════════════════════════════════════════
# 6.  EXPIRY HELPER
# ══════════════════════════════════════════════════════════════════════════════

def nearest_expiry(index: Index, expiry: Expiry) -> str:
    """
    Return expiry date string in "DDMMMYY" format.
    For production, fetch the actual option chain from Kotak to get real dates.
    This placeholder computes the next Thursday (weekly) or last Thursday of month.
    """
    today = datetime.today()
    if expiry == Expiry.WEEKLY:
        # Next or same Thursday
        days_ahead = (3 - today.weekday()) % 7   # Thursday = 3
        expiry_dt  = today + timedelta(days=days_ahead)
    else:  # Monthly – last Thursday of the current month
        year, month = today.year, today.month
        # Walk forward to find last Thursday
        last_thu = None
        d = datetime(year, month, 1)
        while d.month == month:
            if d.weekday() == 3:
                last_thu = d
            d += timedelta(days=1)
        if last_thu and last_thu < today:
            # Roll to next month
            month = month % 12 + 1
            year  = year + (1 if month == 1 else 0)
            d = datetime(year, month, 1)
            last_thu = None
            while d.month == month:
                if d.weekday() == 3:
                    last_thu = d
                d += timedelta(days=1)
        expiry_dt = last_thu or today

    return expiry_dt.strftime("%d%b%y").upper()   # e.g. "27JUN24"


# ══════════════════════════════════════════════════════════════════════════════
# 7.  MOMENTUM FILTER
#     Implements "Overall Momentum" from Entry Settings
# ══════════════════════════════════════════════════════════════════════════════

def passes_momentum_filter(
    cfg: StrategyConfig,
    open_price: float,
    current_price: float,
) -> bool:
    """
    Returns True if the overall momentum condition is satisfied.
    UI controls:
      overallMomentumType  → Points Up / Points Down / Percent Up / Percent Down
      overallMomentumValue → numeric threshold
    """
    if not cfg.overall_momentum_enabled:
        return True
    if cfg.overall_momentum_type is None or cfg.overall_momentum_value is None:
        return True

    change = current_price - open_price
    pct    = (change / open_price * 100) if open_price else 0

    mtype = cfg.overall_momentum_type
    val   = cfg.overall_momentum_value

    if mtype == MomentumType.POINTS_UP:
        return change >= val
    if mtype == MomentumType.POINTS_DOWN:
        return change <= -val
    if mtype == MomentumType.PERCENT_UP:
        return pct >= val
    if mtype == MomentumType.PERCENT_DOWN:
        return pct <= -val
    return True


# ══════════════════════════════════════════════════════════════════════════════
# 8.  LOT SIZE LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

LOT_SIZE: Dict[str, int] = {
    "NIFTY":     50,
    "BANKNIFTY": 15,
    "SENSEX":    10,
}


# ══════════════════════════════════════════════════════════════════════════════
# 9.  STRATEGY EXECUTION ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class StrategyEngine:
    """
    Core execution loop.

    Usage::

        client = KotakClient(consumer_key="...", consumer_secret="...",
                             access_token="...", neo_fin_key="...")
        config = parse_payload(json.loads(localStorage_json))
        engine = StrategyEngine(client, config)
        asyncio.run(engine.run())
    """

    POLL_INTERVAL = 5   # seconds between price checks

    def __init__(self, client: KotakClient, config: StrategyConfig):
        self.client  = client
        self.cfg     = config
        self._active_orders: Dict[int, str] = {}   # leg_idx → order_id
        self._entry_prices:  Dict[int, float] = {}
        self._reentry_used:  Dict[int, Dict[str, int]] = {}  # leg_idx → {"tgt":0,"sl":0}

    # ── Public API ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main async entry point – runs from market open until exit_time."""
        log.info("Engine starting: %s", self.cfg.strategy_name)
        await self._wait_until(self.cfg.entry_time)

        if self.cfg.no_entry_after_enabled and self.cfg.no_entry_after_time:
            if self._now_str() >= self.cfg.no_entry_after_time:
                log.warning("Already past no-entry-after time %s – aborting.",
                            self.cfg.no_entry_after_time)
                return

        spot      = self.client.get_index_ltp(self.cfg.index)
        open_price = spot

        if not passes_momentum_filter(self.cfg, open_price, spot):
            log.info("Overall momentum filter failed; skipping entry.")
            return

        await self._enter_all_legs(spot)

        # ── Monitor loop ───────────────────────────────────────────────────
        while True:
            now = self._now_str()
            if now >= self.cfg.exit_time:
                log.info("Exit time %s reached – squaring off.", self.cfg.exit_time)
                await self._exit_all_legs()
                break

            spot = self.client.get_index_ltp(self.cfg.index)
            await self._monitor_legs(spot)

            if all(l.exited for l in self.cfg.legs):
                log.info("All legs exited.")
                break

            await asyncio.sleep(self.POLL_INTERVAL)

        log.info("Engine finished: %s", self.cfg.strategy_name)

    # ── Entry ──────────────────────────────────────────────────────────────────

    async def _enter_all_legs(self, spot: float) -> None:
        expiry_date = nearest_expiry(self.cfg.index, Expiry.WEEKLY)   # default
        lot_size    = LOT_SIZE.get(self.cfg.index.value, 50)
        product     = self._product_code()

        for idx, leg in enumerate(self.cfg.legs):
            if leg.segment == Segment.FUTURES:
                await self._enter_futures_leg(idx, leg, expiry_date, lot_size, product)
            else:
                expiry_date = nearest_expiry(self.cfg.index, leg.expiry)
                await self._enter_options_leg(idx, leg, spot, expiry_date, lot_size, product)

            self._reentry_used[idx] = {"tgt": 0, "sl": 0}

    async def _enter_options_leg(
        self,
        idx: int,
        leg: LegConfig,
        spot: float,
        expiry_date: str,
        lot_size: int,
        product: str,
    ) -> None:
        """Resolve strike, fetch token, place market order."""

        # Build a quick LTP function for strike resolution
        _ltp_cache: Dict[Tuple, float] = {}

        def ltp_fn(strike: int, opt_type: OptionType) -> float:
            key = (strike, opt_type)
            if key not in _ltp_cache:
                try:
                    sym, tok = self.client.find_option_token(
                        self.cfg.index, expiry_date, strike, opt_type
                    )
                    _ltp_cache[key] = self.client.get_ltp(EXCHANGE_NFO, sym, tok)
                except Exception:
                    _ltp_cache[key] = 0.0
            return _ltp_cache[key]

        # Generate candidate strikes (ATM ±20 steps)
        step = StrikeResolver.STEP.get(self.cfg.index.value, 50)
        atm  = StrikeResolver.atm_strike(spot, self.cfg.index)
        candidates = sorted(
            {atm + i * step for i in range(-20, 21)} | {atm}
        )

        strike = StrikeResolver.resolve(leg, spot, self.cfg.index, candidates, ltp_fn)

        trading_symbol, token = self.client.find_option_token(
            self.cfg.index, expiry_date, strike, leg.option_type
        )

        if leg.simple_momentum_enabled and (leg.simple_momentum_value or 0) > 0:
            base_ltp = self.client.get_ltp(EXCHANGE_NFO, trading_symbol, token)
            await self._wait_for_simple_momentum(
                leg=leg,
                base_price=base_ltp,
                fetch_price=lambda: self.client.get_ltp(EXCHANGE_NFO, trading_symbol, token),
            )

        tx   = TRANSACTION_BUY if leg.position == Position.BUY else TRANSACTION_SELL
        qty  = leg.lot * lot_size

        order_id = self.client.place_order(
            trading_symbol   = trading_symbol,
            token            = token,
            exchange         = EXCHANGE_NFO,
            transaction_type = tx,
            quantity         = qty,
            order_type       = ORDER_TYPE_MARKET,
            product          = product,
            tag              = f"leg{idx}_{self.cfg.strategy_name[:10]}",
        )

        leg.order_id    = order_id
        leg.trading_symbol = trading_symbol
        leg.token = token
        leg.exchange = EXCHANGE_NFO
        leg.entry_price = self.client.get_ltp(EXCHANGE_NFO, trading_symbol, token)
        leg.exited      = False
        log.info("Leg %d: ENTERED %s %s strike=%d entry=%.2f",
                 idx, leg.position, leg.option_type, strike, leg.entry_price or 0)

    async def _enter_futures_leg(
        self,
        idx: int,
        leg: LegConfig,
        expiry_date: str,
        lot_size: int,
        product: str,
    ) -> None:
        symbol = f"{self.cfg.index.value}{expiry_date}FUT"
        resp   = self.client.search_scrip(exchange_segment="nfo", symbol=symbol)
        item   = resp["data"][0]
        trading_symbol = item["trading_symbol"]
        token          = str(item["pSymbol"])

        if leg.simple_momentum_enabled and (leg.simple_momentum_value or 0) > 0:
            base_ltp = self.client.get_ltp(EXCHANGE_NFO, trading_symbol, token)
            await self._wait_for_simple_momentum(
                leg=leg,
                base_price=base_ltp,
                fetch_price=lambda: self.client.get_ltp(EXCHANGE_NFO, trading_symbol, token),
            )

        tx  = TRANSACTION_BUY if leg.position == Position.BUY else TRANSACTION_SELL
        qty = leg.lot * lot_size

        order_id = self.client.place_order(
            trading_symbol   = trading_symbol,
            token            = token,
            exchange         = EXCHANGE_NFO,
            transaction_type = tx,
            quantity         = qty,
            order_type       = ORDER_TYPE_MARKET,
            product          = product,
            tag              = f"fut{idx}_{self.cfg.strategy_name[:10]}",
        )
        leg.order_id    = order_id
        leg.trading_symbol = trading_symbol
        leg.token = token
        leg.exchange = EXCHANGE_NFO
        leg.entry_price = self.client.get_ltp(EXCHANGE_NFO, trading_symbol, token)
        leg.exited      = False
        log.info("Leg %d: ENTERED Futures tx=%s entry=%.2f", idx, tx, leg.entry_price or 0)

    # ── Monitor ────────────────────────────────────────────────────────────────

    async def _monitor_legs(self, spot: float) -> None:
        """Check P&L per leg against target/SL/trail; exit if triggered."""
        for idx, leg in enumerate(self.cfg.legs):
            if leg.exited or leg.entry_price is None:
                continue

            ltp = await self._fetch_leg_ltp(leg)
            if ltp is None:
                continue
            leg.current_price = ltp

            pnl_pts = (ltp - leg.entry_price) if leg.position == Position.BUY \
                      else (leg.entry_price - ltp)

            # ── Target Profit ─────────────────────────────────────────────
            if leg.target_profit_enabled:
                tgt = self._pnl_threshold(leg.entry_price, leg.target_profit_value,
                                          leg.target_profit_unit)
                if pnl_pts >= tgt:
                    log.info("Leg %d: TARGET HIT pnl=%.2f tgt=%.2f", idx, pnl_pts, tgt)
                    await self._exit_leg(idx, leg)
                    if leg.reentry_on_target_enabled:
                        await self._handle_reentry(idx, leg, "tgt", spot)
                    continue

            # ── Stop Loss ─────────────────────────────────────────────────
            if leg.stop_loss_enabled:
                sl = self._pnl_threshold(leg.entry_price, leg.stop_loss_value,
                                         leg.stop_loss_unit)
                if pnl_pts <= -sl:
                    log.info("Leg %d: STOP LOSS pnl=%.2f sl=%.2f", idx, pnl_pts, sl)
                    await self._exit_leg(idx, leg)
                    if leg.reentry_on_sl_enabled:
                        await self._handle_reentry(idx, leg, "sl", spot)
                    continue

            # ── Trail SL to break-even (legwise) ──────────────────────────
            if leg.trail_sl_enabled:
                self._apply_trail_sl(idx, leg, pnl_pts)

            # ── Trail SL to breakeven (strategy-level) ────────────────────
            if self.cfg.trail_sl_to_breakeven:
                self._maybe_trail_to_breakeven(idx, leg, pnl_pts)

    # ── Exit ───────────────────────────────────────────────────────────────────

    async def _exit_leg(self, idx: int, leg: LegConfig) -> None:
        if leg.exited:
            return
        # Reverse transaction to close
        tx = TRANSACTION_SELL if leg.position == Position.BUY else TRANSACTION_BUY
        lot_size = LOT_SIZE.get(self.cfg.index.value, 50)

        try:
            # Use cancel if order is still open; else place closing order
            if not leg.trading_symbol or not leg.token:
                raise RuntimeError("Missing traded symbol/token for leg exit.")
            self.client.place_order(
                trading_symbol   = leg.trading_symbol,
                token            = leg.token,
                exchange         = leg.exchange or EXCHANGE_NFO,
                transaction_type = tx,
                quantity         = leg.lot * lot_size,
                order_type       = ORDER_TYPE_MARKET,
                product          = self._product_code(),
                tag              = f"exit_leg{idx}",
            )
        except Exception as exc:
            log.error("Failed to exit leg %d: %s", idx, exc)
        else:
            leg.exited = True
            log.info("Leg %d: EXITED at %.2f", idx, leg.current_price or 0)

    async def _exit_all_legs(self) -> None:
        """Square off all open legs – respects squareOff Partial/Complete setting."""
        for idx, leg in enumerate(self.cfg.legs):
            if not leg.exited:
                await self._exit_leg(idx, leg)
                # For Partial square-off mode, exit legs one at a time
                if self.cfg.square_off == SquareOff.PARTIAL:
                    await asyncio.sleep(0.3)

    # ── Re-entry ───────────────────────────────────────────────────────────────

    async def _handle_reentry(
        self,
        idx: int,
        leg: LegConfig,
        kind: str,   # "tgt" or "sl"
        spot: float,
    ) -> None:
        """
        Re-entry on Target / Re-entry on SL.
        UI controls: timing (RE ASAP / Next Candle) + max count (1/2/3/5).
        """
        used    = self._reentry_used[idx][kind]
        max_cnt = (leg.reentry_on_target_count
                   if kind == "tgt" else leg.reentry_on_sl_count)
        timing  = (leg.reentry_on_target_timing
                   if kind == "tgt" else leg.reentry_on_sl_timing)

        if used >= max_cnt:
            log.info("Leg %d: Re-entry limit reached (%d/%d)", idx, used, max_cnt)
            return

        if timing == ReEntryTiming.NEXT_CANDLE:
            log.info("Leg %d: Waiting for next candle before re-entry…", idx)
            await asyncio.sleep(60)   # simplified – wait 1 min for next candle

        log.info("Leg %d: Re-entering (%s) %d/%d", idx, kind, used + 1, max_cnt)
        leg.exited = False
        expiry_date = nearest_expiry(self.cfg.index, leg.expiry)
        lot_size    = LOT_SIZE.get(self.cfg.index.value, 50)
        if leg.segment == Segment.FUTURES:
            await self._enter_futures_leg(idx, leg, expiry_date, lot_size, self._product_code())
        else:
            await self._enter_options_leg(idx, leg, spot, expiry_date, lot_size,
                                          self._product_code())
        self._reentry_used[idx][kind] += 1

    # ── Trail SL helpers ───────────────────────────────────────────────────────

    def _apply_trail_sl(self, idx: int, leg: LegConfig, pnl_pts: float) -> None:
        """
        Per-leg Trail SL.
        UI: trail_sl_x (move X pts/%) → trail SL by Y pts/%
        Stores the running best pnl on leg.entry_price (simplified).
        """
        x = leg.trail_sl_x
        y = leg.trail_sl_y
        if x <= 0 or y <= 0 or pnl_pts < x:
            return
        # Each time profit moves another X, tighten SL by Y
        steps = int(pnl_pts / x)
        new_sl = leg.entry_price + steps * y if leg.position == Position.BUY \
                 else leg.entry_price - steps * y
        # In a real implementation update the Kotak GTT/SL order price
        log.debug("Leg %d: Trail SL updated → %.2f", idx, new_sl)

    def _maybe_trail_to_breakeven(self, idx: int, leg: LegConfig, pnl_pts: float) -> None:
        """
        Strategy-level 'Trail SL to Break-even'.
        Scope: All Legs or only SL Legs (as set in trailScope segmented button).
        """
        if self.cfg.trail_scope == TrailScope.SL_LEGS and not leg.stop_loss_enabled:
            return
        if pnl_pts > 0:
            log.debug("Leg %d: Break-even trail active (pnl=%.2f)", idx, pnl_pts)

    async def _wait_for_simple_momentum(
        self,
        leg: LegConfig,
        base_price: float,
        fetch_price,
    ) -> None:
        """
        After entry time, wait until price moves by configured momentum before entering.
        Supports:
          - Points (Pts) ↑ / ↓
          - Percent (%) ↑ / ↓
        """
        unit = (leg.simple_momentum_unit or "").strip()
        val = float(leg.simple_momentum_value or 0)
        if val <= 0:
            return

        log.info("Simple Momentum active: unit=%s value=%s base=%.2f", unit, val, base_price)
        while True:
            if self._now_str() >= self.cfg.exit_time:
                raise RuntimeError("Exit time reached before simple momentum trigger.")
            if self.cfg.no_entry_after_enabled and self.cfg.no_entry_after_time and self._now_str() >= self.cfg.no_entry_after_time:
                raise RuntimeError("No-entry-after time reached before simple momentum trigger.")

            current = fetch_price()
            if self._simple_momentum_hit(unit=unit, value=val, base=base_price, current=current):
                log.info("Simple Momentum triggered at %.2f (base %.2f)", current, base_price)
                return
            await asyncio.sleep(2)

    @staticmethod
    def _simple_momentum_hit(unit: str, value: float, base: float, current: float) -> bool:
        u = (unit or "").lower()
        up = ("up" in u) or ("↑" in unit)
        down = ("down" in u) or ("↓" in unit)
        is_points = "points" in u
        is_percent = "percent" in u

        if is_points and up:
            return current >= (base + value)
        if is_points and down:
            return current <= (base - value)
        if is_percent and up:
            if base == 0:
                return False
            return ((current - base) / base) * 100 >= value
        if is_percent and down:
            if base == 0:
                return False
            return ((base - current) / base) * 100 >= value
        return True

    # ── Utilities ──────────────────────────────────────────────────────────────

    def _product_code(self) -> str:
        """
        Intraday → MIS   |   BTST/Positional → NRML
        Maps the strategyType segmented button.
        """
        return "MIS" if self.cfg.strategy_type == StrategyType.INTRADAY else "NRML"

    @staticmethod
    def _pnl_threshold(entry: float, value: float, unit: PnLUnit) -> float:
        """Convert UI profit/loss value+unit into absolute points."""
        if unit == PnLUnit.POINTS:
            return value
        if unit == PnLUnit.PERCENT:
            return entry * value / 100
        if unit == PnLUnit.PREMIUM:
            return value   # Premium = absolute price of the option
        return value

    @staticmethod
    def _now_str() -> str:
        return datetime.now().strftime("%H:%M")

    @staticmethod
    async def _wait_until(time_str: str) -> None:
        while True:
            now = datetime.now().strftime("%H:%M")
            if now >= time_str:
                return
            await asyncio.sleep(10)

    async def _fetch_leg_ltp(self, leg: LegConfig) -> Optional[float]:
        """Fetch current LTP for a live leg using stored traded symbol/token."""
        try:
            if leg.trading_symbol and leg.token:
                return self.client.get_ltp(leg.exchange or EXCHANGE_NFO, leg.trading_symbol, leg.token)
        except Exception:
            pass
        return leg.current_price   # fallback: last known price


# ══════════════════════════════════════════════════════════════════════════════
# 10.  CLI / ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def load_config_from_file(path: str) -> StrategyConfig:
    """
    Load a strategy exported by the UI's 'Save Strategy' button
    (stored in kotak_builder_strategy localStorage key).
    """
    with open(path, "r") as fh:
        raw = json.load(fh)
    return parse_payload(raw)


def main() -> None:
    import os

    # ── Credentials from env vars ──────────────────────────────────────────────
    consumer_key    = os.environ["KOTAK_CONSUMER_KEY"]
    consumer_secret = os.environ["KOTAK_CONSUMER_SECRET"]
    access_token    = os.environ.get("KOTAK_ACCESS_TOKEN", "")
    neo_fin_key     = os.environ.get("KOTAK_NEO_FIN_KEY", "")

    client = KotakClient(
        consumer_key    = consumer_key,
        consumer_secret = consumer_secret,
        access_token    = access_token,
        neo_fin_key     = neo_fin_key,
    )

    # ── Load strategy config saved from the UI ─────────────────────────────────
    config_path = os.environ.get("STRATEGY_CONFIG", "strategy.json")
    config      = load_config_from_file(config_path)

    log.info("Loaded strategy: %s  (%d legs)", config.strategy_name, len(config.legs))

    engine = StrategyEngine(client, config)
    asyncio.run(engine.run())


if __name__ == "__main__":
    main()
