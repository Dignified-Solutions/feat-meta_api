import asyncio
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from hummingbot.client.config.security import Security
from hummingbot.connector.exchange.metaapi import metaapi_constants as META_CONSTANTS
from hummingbot.connector.exchange.metaapi.metaapi_utils import convert_trading_pair_to_metaapi_format
from hummingbot.data_feed.metaapi.metaapi_historical_data import MetaAPIHistoricalDataClient, MetaAPIStatsClient
from hummingbot.data_feed.metaapi.metaapi_streamer import MetaAPIStreamingManager
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.controllers.metaapi_data_config import MetaAPIDataConfig


class MetaAPIDataHook:
    """
    Lazily loads MetaAPI credentials from the secured connector config and exposes helper calls for controllers.
    """

    _logger: HummingbotLogger = HummingbotLogger(__name__)

    def __init__(self, config: MetaAPIDataConfig):
        self._config = config
        self._historical_client: Optional[MetaAPIHistoricalDataClient] = None
        self._stats_client: Optional[MetaAPIStatsClient] = None
        self._streaming_manager: Optional[MetaAPIStreamingManager] = None
        self._token: Optional[str] = None
        self._account_id: Optional[str] = None
        self._domain: str = META_CONSTANTS.DEFAULT_DOMAIN
        self._initialization_lock = asyncio.Lock()
        self._historical_ready = False
        self._historical_snapshot: Dict[str, Any] = {}

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    @property
    def ready_for_trading(self) -> bool:
        historical_ready = self._historical_ready
        streaming_ready = True
        if self._config.streaming_enabled:
            streaming_ready = self._streaming_manager is not None and self._streaming_manager.warmup_ready
        return historical_ready and streaming_ready

    async def fetch_snapshot(self, default_symbol: Optional[str]) -> Dict[str, Any]:
        """
        Returns a snapshot containing candles/ticks/stats depending on the config.
        """
        if not self.enabled:
            return {}
        ready = await self._ensure_clients()
        if not ready:
            return {}

        symbols = self._resolve_symbols(default_symbol)
        if not symbols:
            return {}

        snapshot: Dict[str, Any] = {}
        if not self._historical_ready:
            await self._fetch_initial_history(symbols)
        if self._historical_snapshot:
            snapshot["history"] = self._historical_snapshot

        stream_symbols = self._resolve_streaming_symbols(default_symbol)
        if self._config.streaming_enabled and stream_symbols and self._streaming_manager is not None:
            await self._streaming_manager.ensure_running(stream_symbols)
            snapshot["streaming"] = await self._streaming_manager.snapshot()

        if self._config.fetch_stats:
            stats = await self._fetch_stats_snapshot()
            if stats:
                snapshot["stats"] = stats

        return snapshot

    async def stop(self):
        """Stop any active background tasks and release resources."""
        try:
            if self._streaming_manager is not None:
                await self._streaming_manager.stop()
        except Exception as exc:  # pragma: no cover - best effort cleanup
            self._logger.warning("MetaAPI streaming manager shutdown errored: %s", exc)
        finally:
            self._streaming_manager = None

        # Reset cached state to force reinitialization on next fetch.
        self._historical_ready = False
        self._historical_snapshot = {}
        self._historical_client = None
        self._stats_client = None

    async def _ensure_clients(self) -> bool:
        if (
            self._historical_client is not None
            and (self._config.fetch_stats is False or self._stats_client is not None)
            and (self._config.streaming_enabled is False or self._streaming_manager is not None)
        ):
            return True
        async with self._initialization_lock:
            if (
                self._historical_client is not None
                and (self._config.fetch_stats is False or self._stats_client is not None)
                and (self._config.streaming_enabled is False or self._streaming_manager is not None)
            ):
                return True
            self._load_credentials()
            if not self._token or not self._account_id:
                self._logger.debug("MetaAPI controller hook not initialized (missing credentials).")
                return False
            if self._historical_client is None:
                self._historical_client = MetaAPIHistoricalDataClient(
                    token=self._token,
                    account_id=self._account_id,
                    domain=self._domain,
                )
            if self._config.fetch_stats and self._stats_client is None:
                self._stats_client = MetaAPIStatsClient(
                    token=self._token,
                    account_id=self._account_id,
                )
            if self._config.streaming_enabled and self._streaming_manager is None:
                try:
                    self._streaming_manager = MetaAPIStreamingManager(
                        token=self._token,
                        account_id=self._account_id,
                        domain=self._domain,
                        warmup_minutes=self._config.streaming_warmup_minutes,
                        buffer_size=self._config.streaming_buffer,
                    )
                except ImportError as exc:
                    self._logger.warning("%s", exc)
                    self._config.streaming_enabled = False
        return True

    def _load_credentials(self):
        if self._token and self._account_id:
            return
        adapter = Security.decrypted_value("metaapi")
        if adapter is None:
            return
        token_field = getattr(adapter, "metaapi_token", None)
        account_id = getattr(adapter, "metaapi_account_id", None)
        if token_field:
            self._token = token_field.get_secret_value() if hasattr(token_field, "get_secret_value") else token_field
        if account_id:
            self._account_id = account_id
        if not self._account_id:
            mt_login = getattr(adapter, "mt_login", None)
            self._account_id = mt_login
        # Allow overriding domain via env/adapter if ever exposed; default remains constant.

    def _resolve_symbols(self, default_symbol: Optional[str]) -> List[str]:
        symbols = self._config.symbols or []
        if not symbols and default_symbol:
            symbols = [default_symbol]
        return self._normalize_symbols(symbols)

    def _resolve_streaming_symbols(self, default_symbol: Optional[str]) -> List[str]:
        if self._config.streaming_symbols:
            return self._normalize_symbols(self._config.streaming_symbols)
        return self._resolve_symbols(default_symbol)

    def _normalize_symbols(self, symbols: List[str]) -> List[str]:
        resolved = []
        for symbol in symbols:
            if symbol is None:
                continue
            meta_symbol = convert_trading_pair_to_metaapi_format(symbol) if "-" in symbol else symbol.replace("-", "")
            resolved.append(meta_symbol.upper())
        seen = set()
        unique = []
        for symbol in resolved:
            if symbol and symbol not in seen:
                seen.add(symbol)
                unique.append(symbol)
        return unique

    async def _fetch_stats_snapshot(self) -> Dict[str, Any]:
        if self._stats_client is None:
            return {}
        try:
            now = datetime.utcnow()
            start = now - timedelta(days=self._config.stats_lookback_days)
            start_str = start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            end_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            metrics = await self._stats_client.get_metrics()
            trades = await self._stats_client.get_trades(start_str, end_str)
            open_trades = await self._stats_client.get_open_trades()
            return {
                "metrics": metrics,
                "trades": trades,
                "open_trades": open_trades,
                "window": {"start": start_str, "end": end_str},
            }
        except Exception as exc:
            self._logger.warning("MetaAPI stats fetch failed: %s", exc)
            return {}

    async def _fetch_initial_history(self, symbols: List[str]):
        if self._historical_client is None:
            return
        candles_payload: Dict[str, List[Dict[str, Any]]] = {}
        ticks_payload: Dict[str, List[Dict[str, Any]]] = {}
        pages = max(self._config.candle_pages, self._estimate_history_pages(self._config.timeframe))
        for symbol in symbols:
            try:
                candles_payload[symbol] = await self._historical_client.fetch_candles(
                    symbol=symbol,
                    timeframe=self._config.timeframe,
                    pages=pages,
                )
            except Exception as exc:
                self._logger.warning("MetaAPI candle history fetch failed for %s: %s", symbol, exc)
            if self._config.fetch_ticks and self._config.tick_pages > 0:
                try:
                    ticks_payload[symbol] = await self._historical_client.fetch_ticks(
                        symbol=symbol,
                        pages=self._config.tick_pages,
                        lookback_days=max(self._config.history_lookback_days, self._config.tick_lookback_days),
                    )
                except Exception as exc:
                    self._logger.warning("MetaAPI tick history fetch failed for %s: %s", symbol, exc)

        self._historical_snapshot = {
            "timeframe": self._config.timeframe,
            "lookback_days": self._config.history_lookback_days,
            "candles": candles_payload,
        }
        if ticks_payload:
            self._historical_snapshot["ticks"] = ticks_payload
        self._historical_ready = bool(candles_payload)

    def _estimate_history_pages(self, timeframe: str) -> int:
        minutes = self._timeframe_to_minutes(timeframe)
        if minutes <= 0:
            return self._config.candle_pages or 1
        candles_needed = (self._config.history_lookback_days * 24 * 60) / minutes
        return max(1, math.ceil(candles_needed / 1000))

    @staticmethod
    def _timeframe_to_minutes(timeframe: str) -> int:
        if timeframe.endswith("m"):
            return int(timeframe[:-1])
        if timeframe.endswith("h"):
            return int(timeframe[:-1]) * 60
        if timeframe.endswith("d"):
            return int(timeframe[:-1]) * 24 * 60
        return 1
