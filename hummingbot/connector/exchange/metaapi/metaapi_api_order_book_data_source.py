import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List

from hummingbot.connector.exchange.metaapi.metaapi_constants import DEFAULT_DOMAIN
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource

if TYPE_CHECKING:
    from hummingbot.connector.exchange.metaapi.metaapi_exchange import MetaAPIExchange


class MetaAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """Order book data source that simulates depth snapshots from MetaAPI streaming prices."""

    SNAPSHOT_INTERVAL = 5.0
    DIFF_INTERVAL = 1.0

    def __init__(self, trading_pairs: List[str], connector: "MetaAPIExchange", domain: str = DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        streaming_connection = self._connector._streaming_connection
        if streaming_connection is None:
            return prices

        for trading_pair in trading_pairs:
            symbol = trading_pair.replace("-", "")
            price_data = streaming_connection.terminal_state.price(symbol)
            if price_data:
                bid = price_data.get("bid", 0)
                ask = price_data.get("ask", 0)
                if bid > 0 and ask > 0:
                    prices[trading_pair] = (bid + ask) / 2
        return prices

    async def get_order_book_data(self, trading_pair: str) -> Dict[str, Any]:
        streaming_connection = self._connector._streaming_connection
        if streaming_connection is None:
            return {"bids": [], "asks": [], "timestamp": time.time() * 1000}

        symbol = trading_pair.replace("-", "")
        price_data = streaming_connection.terminal_state.price(symbol)
        if not price_data:
            return {"bids": [], "asks": [], "timestamp": time.time() * 1000}

        bid = price_data.get("bid", 0)
        ask = price_data.get("ask", 0)
        if bid <= 0 or ask <= 0:
            return {"bids": [], "asks": [], "timestamp": time.time() * 1000}

        spread = max(ask - bid, 1e-5)
        levels = 5
        volume_base = 1000
        bids = []
        asks = []

        for idx in range(levels):
            distance = spread * (idx + 1) * 0.1
            reduction = max(1 - idx * 0.1, 0.2)
            bids.append([bid - distance, volume_base * reduction])
            asks.append([ask + distance, volume_base * reduction])

        return {
            "bids": bids,
            "asks": asks,
            "timestamp": price_data.get("time", time.time() * 1000),
        }

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    data = await self.get_order_book_data(trading_pair)
                    if data["bids"] or data["asks"]:
                        message = self._build_order_book_message(
                            trading_pair=trading_pair,
                            data=data,
                            message_type=OrderBookMessageType.DIFF,
                        )
                        output.put_nowait(message)
                await asyncio.sleep(self.DIFF_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error in MetaAPI diff listener", exc_info=True)
                await asyncio.sleep(5.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    snapshot = await self._order_book_snapshot(trading_pair)
                    if snapshot is not None:
                        output.put_nowait(snapshot)
                await asyncio.sleep(self.SNAPSHOT_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error in MetaAPI snapshot listener", exc_info=True)
                await asyncio.sleep(5.0)

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        while True:
            try:
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                raise

    async def listen_for_subscriptions(self):
        while True:
            try:
                await asyncio.sleep(60.0)
            except asyncio.CancelledError:
                raise

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        data = await self.get_order_book_data(trading_pair)
        if not data["bids"] and not data["asks"]:
            raise RuntimeError(f"No market data available for {trading_pair}")
        return self._build_order_book_message(trading_pair, data, OrderBookMessageType.SNAPSHOT)

    def _build_order_book_message(
        self,
        trading_pair: str,
        data: Dict[str, Any],
        message_type: OrderBookMessageType,
    ) -> OrderBookMessage:
        update_id = int(data.get("timestamp", time.time() * 1000))
        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": data.get("bids", []),
            "asks": data.get("asks", []),
        }
        if message_type is OrderBookMessageType.DIFF:
            content["first_update_id"] = update_id
        timestamp = update_id / 1000
        return OrderBookMessage(message_type=message_type, content=content, timestamp=timestamp)
