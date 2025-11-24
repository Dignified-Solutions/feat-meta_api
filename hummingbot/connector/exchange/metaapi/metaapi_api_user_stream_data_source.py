import asyncio
from typing import TYPE_CHECKING, Dict, List, Optional

from hummingbot.connector.exchange.metaapi.metaapi_constants import DEFAULT_DOMAIN
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.exchange.metaapi.metaapi_auth import MetaAPIAuth
    from hummingbot.connector.exchange.metaapi.metaapi_exchange import MetaAPIExchange


class MetaAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """Pushes account/order/position events derived from the MetaAPI streaming connection."""

    def __init__(
        self,
        auth: "MetaAPIAuth",
        trading_pairs: List[str],
        connector: "MetaAPIExchange",
        api_factory: WebAssistantsFactory,
        domain: str = DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._last_deal_id: Optional[int] = None
        self._last_order_ids: Dict[str, str] = {}

    async def listen_for_user_stream(self, output: asyncio.Queue):
        while True:
            try:
                stream = self._connector._streaming_connection
                if stream is None:
                    await asyncio.sleep(1.0)
                    continue

                await self._process_account_updates(stream, output)
                await self._process_order_updates(stream, output)
                await self._process_position_updates(stream, output)
                await self._process_trade_updates(stream, output)
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._connector.logger().exception("Error in MetaAPI user stream listener", exc_info=True)
                await asyncio.sleep(5.0)

    async def _process_account_updates(self, stream, output: asyncio.Queue):
        account_info = stream.terminal_state.account_information
        if not account_info:
            return
        event = {
            "event_type": "balance_update",
            "account_info": account_info,
            "timestamp": self._connector.current_timestamp,
        }
        output.put_nowait(event)

    async def _process_order_updates(self, stream, output: asyncio.Queue):
        orders = stream.terminal_state.orders or []
        for order in orders:
            order_id = str(order.get("id"))
            if self._last_order_ids.get(order_id) == order.get("state"):
                continue
            self._last_order_ids[order_id] = order.get("state")
            event = {
                "event_type": "order_update",
                "order": order,
                "timestamp": self._connector.current_timestamp,
            }
            output.put_nowait(event)

    async def _process_position_updates(self, stream, output: asyncio.Queue):
        positions = stream.terminal_state.positions or []
        for position in positions:
            event = {
                "event_type": "position_update",
                "position": position,
                "timestamp": self._connector.current_timestamp,
            }
            output.put_nowait(event)

    async def _process_trade_updates(self, stream, output: asyncio.Queue):
        history_storage = stream.history_storage
        if not history_storage:
            return
        deals = history_storage.deals or []
        for deal in deals[-20:]:
            raw_id = deal.get("id")
            if raw_id is None:
                continue
            try:
                deal_id = int(str(raw_id))
            except (TypeError, ValueError):
                self._connector.logger().debug(
                    "Skipping MetaAPI deal with non-numeric id: %s", raw_id
                )
                continue
            if self._last_deal_id is not None and deal_id <= self._last_deal_id:
                continue
            event = {
                "event_type": "trade_update",
                "deal": deal,
                "timestamp": self._connector.current_timestamp,
            }
            output.put_nowait(event)
            self._last_deal_id = deal_id
