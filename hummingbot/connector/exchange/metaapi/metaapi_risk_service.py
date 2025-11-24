import asyncio
import importlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _coerce_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


@dataclass
class HighProbabilityConfig:
    min_win_rate: Decimal
    min_trades: int
    lookback_days: int
    cache_ttl: float = 300.0  # seconds


class MetaAPIRiskService:
    """
    Lightweight wrapper around MetaAPI's RiskManagement equity/balance stream.
    """

    def __init__(
        self,
        token: str,
        account_id: str,
        domain: str,
        startup_timeout: float = 5.0,
    ):
        self._token = token
        self._account_id = account_id
        self._domain = domain
        self._startup_timeout = startup_timeout
        self._risk_management = None
        self._risk_api = None
        self._listener = None
        self._listener_id = None
        self._latest_equity: Optional[Decimal] = None
        self._latest_balance: Optional[Decimal] = None
        self._equity_ready = asyncio.Event()
        self._start_lock = asyncio.Lock()
        self._risk_management_cls = None
        self._equity_listener_cls = None

    @property
    def latest_equity(self) -> Optional[Decimal]:
        return self._latest_equity

    async def ensure_started(self):
        if self._listener_id is not None:
            return
        async with self._start_lock:
            if self._listener_id is not None:
                return
            risk_cls, listener_cls = self._import_sdk_classes()
            if self._risk_management is None:
                self._risk_management = risk_cls(self._token, {"domain": self._domain})
                self._risk_api = self._risk_management.risk_management_api
            self._listener = self._build_listener(listener_cls)
            self._listener_id = await self._risk_api.add_equity_balance_listener(self._listener, self._account_id)

    async def wait_for_first_sample(self, timeout: Optional[float] = None):
        await asyncio.wait_for(self._equity_ready.wait(), timeout=timeout)

    async def stop(self):
        if self._risk_api and self._listener_id is not None:
            try:
                self._risk_api.remove_equity_balance_listener(self._listener_id)
            except Exception:
                logger.exception("Failed to remove MetaAPI equity balance listener")
        self._listener_id = None
        self._listener = None

    async def handle_equity_update(self, payload: Dict[str, Any]):
        equity = _coerce_decimal(payload.get("equity"))
        balance = _coerce_decimal(payload.get("balance"))
        self._latest_equity = equity
        self._latest_balance = balance
        if not self._equity_ready.is_set():
            self._equity_ready.set()

    async def handle_error(self, error: Exception):
        logger.warning("MetaAPI risk listener error: %s", error)

    def _import_sdk_classes(self):
        if self._risk_management_cls is None or self._equity_listener_cls is None:
            try:
                sdk = importlib.import_module("metaapi_cloud_sdk")
            except ModuleNotFoundError as exc:  # pragma: no cover - dependency missing
                raise ImportError(
                    "MetaAPI SDK not installed. Install metaapi-cloud-sdk to enable risk management."
                ) from exc
            self._risk_management_cls = getattr(sdk, "RiskManagement")
            self._equity_listener_cls = getattr(sdk, "EquityBalanceListener")
        return self._risk_management_cls, self._equity_listener_cls

    def _build_listener(self, base_cls):
        owner = self
        account_id = self._account_id

        class EquityListener(base_cls):  # type: ignore[misc]
            def __init__(self):
                super().__init__(account_id)

            async def on_equity_or_balance_updated(self, equity_balance_data):
                await owner.handle_equity_update(equity_balance_data)

            async def on_connected(self):
                logger.debug("MetaAPI risk listener connected")

            async def on_disconnected(self):
                logger.debug("MetaAPI risk listener disconnected")

            async def on_error(self, error: Exception):
                await owner.handle_error(error)

        return EquityListener()


class MetaAPIProbabilityService:
    """
    Pulls recent trade statistics via MetaStats and decides if the account is on a high-probability streak.
    """

    def __init__(
        self,
        token: str,
        account_id: str,
        config: HighProbabilityConfig,
        meta_stats_factory=None,
    ):
        self._enabled = bool(token and account_id)
        self._token = token
        self._account_id = account_id
        self._config = config
        self._meta_stats_factory = meta_stats_factory
        self._meta_stats_cls = None
        self._meta_stats = None
        self._lock = asyncio.Lock()
        self._last_refresh: float = 0.0
        self._cached_result = False
        self._cached_trade_count = 0

    async def allows_high_probability_override(self) -> bool:
        if not self._enabled:
            return False
        async with self._lock:
            now = time.time()
            if now - self._last_refresh >= self._config.cache_ttl or self._meta_stats is None:
                await self._refresh_cache()
            return self._cached_result

    async def _refresh_cache(self):
        if self._meta_stats is None:
            try:
                self._meta_stats = self._get_meta_stats_cls()(self._token)
            except ImportError as exc:  # pragma: no cover - dependency missing
                logger.warning("%s", exc)
                self._enabled = False
                self._cached_result = False
                self._cached_trade_count = 0
                self._last_refresh = time.time()
                return
        start, end = self._time_window()
        try:
            trades = await self._meta_stats.get_account_trades(self._account_id, start, end)
        except Exception as exc:  # pragma: no cover - network failures
            logger.warning("Unable to refresh MetaAPI trade stats: %s", exc)
            self._cached_result = False
            self._cached_trade_count = 0
            self._last_refresh = time.time()
            return

        total_trades = len(trades)
        winning_trades = 0
        for trade in trades:
            pnl = trade.get("gain")
            if pnl is None:
                pnl = trade.get("profit")
            if pnl is None:
                pnl = trade.get("equity")
            pnl_value = _coerce_decimal(pnl)
            if pnl_value > 0:
                winning_trades += 1

        win_rate = (Decimal(winning_trades) / Decimal(total_trades)) if total_trades > 0 else Decimal("0")
        self._cached_trade_count = total_trades
        meets_threshold = (
            total_trades >= self._config.min_trades and win_rate >= self._config.min_win_rate
        )
        self._cached_result = meets_threshold
        self._last_refresh = time.time()
        logger.debug(
            "MetaAPI win-rate window: trades=%s wins=%s win_rate=%.3f threshold=%.3f result=%s",
            total_trades,
            winning_trades,
            float(win_rate),
            float(self._config.min_win_rate),
            meets_threshold,
        )

    def _time_window(self):
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=self._config.lookback_days)
        return (
            self._format_ts(start_dt),
            self._format_ts(end_dt),
        )

    @staticmethod
    def _format_ts(dt: datetime) -> str:
        # MetaStats expects millisecond precision timestamps.
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _get_meta_stats_cls(self):
        if self._meta_stats_factory is not None:
            return self._meta_stats_factory
        if self._meta_stats_cls is None:
            try:
                sdk = importlib.import_module("metaapi_cloud_sdk")
            except ModuleNotFoundError as exc:  # pragma: no cover - dependency missing
                raise ImportError(
                    "MetaAPI SDK not installed. Install metaapi-cloud-sdk to enable MetaStats-based overrides."
                ) from exc
            self._meta_stats_cls = getattr(sdk, "MetaStats")
        return self._meta_stats_cls


class MetaAPIRiskManager:
    """
    Coordinates the MetaAPI equity feed and the optional high-probability override logic.
    """

    def __init__(
        self,
        token: Optional[str],
        account_id: Optional[str],
        domain: str,
        equity_floor: Decimal,
        allow_high_probability_override: bool,
        probability_config: Optional[HighProbabilityConfig],
        startup_sample_timeout: float = 5.0,
        risk_service: Optional[MetaAPIRiskService] = None,
        probability_service: Optional[MetaAPIProbabilityService] = None,
    ):
        self._token = token
        self._account_id = account_id
        self._domain = domain
        self._equity_floor = equity_floor
        self._allow_override = allow_high_probability_override
        self._startup_sample_timeout = startup_sample_timeout
        self._probability_config = probability_config
        self._risk_service = risk_service
        self._probability_service = probability_service
        self._enabled = (
            self._token is not None
            and self._account_id is not None
            and self._equity_floor is not None
            and self._equity_floor > 0
        )
        self._initialize_services()

    def _initialize_services(self):
        if not self._enabled:
            logger.info("MetaAPI risk manager disabled (missing token/account id or equity floor <= 0).")
            return
        if self._risk_service is None:
            try:
                self._risk_service = MetaAPIRiskService(
                    token=self._token,
                    account_id=self._account_id,
                    domain=self._domain,
                    startup_timeout=self._startup_sample_timeout,
                )
            except ImportError as exc:
                logger.warning("%s", exc)
                self._enabled = False
                self._risk_service = None
                return
        if self._allow_override and self._probability_service is None and self._probability_config is not None:
            try:
                self._probability_service = MetaAPIProbabilityService(
                    token=self._token,
                    account_id=self._account_id,
                    config=self._probability_config,
                )
            except ImportError as exc:
                logger.warning("%s", exc)
                self._probability_service = None

    @property
    def enabled(self) -> bool:
        return self._enabled and self._risk_service is not None

    @property
    def equity_floor(self) -> Decimal:
        return self._equity_floor

    @property
    def latest_equity(self) -> Optional[Decimal]:
        if self._risk_service is None:
            return None
        return self._risk_service.latest_equity

    async def ensure_monitoring(self):
        if not self.enabled:
            return
        await self._risk_service.ensure_started()

    async def ensure_can_place_order(self):
        if not self.enabled:
            return
        await self.ensure_monitoring()
        try:
            await self._risk_service.wait_for_first_sample(timeout=self._startup_sample_timeout)
        except asyncio.TimeoutError:
            logger.warning("Timed out waiting for MetaAPI equity sample; continuing without enforcement.")
            return
        equity = self._risk_service.latest_equity
        if equity is None:
            return
        if equity >= self._equity_floor:
            return
        if self._probability_service and await self._probability_service.allows_high_probability_override():
            logger.info(
                "Equity %.2f breached floor %.2f but high-probability override approved order.",
                float(equity),
                float(self._equity_floor),
            )
            return
        raise RuntimeError(
            f"MetaAPI equity {equity} is below the configured floor {self._equity_floor}. "
            "Halting new order submissions."
        )

    async def stop(self):
        if self._risk_service is not None:
            await self._risk_service.stop()
