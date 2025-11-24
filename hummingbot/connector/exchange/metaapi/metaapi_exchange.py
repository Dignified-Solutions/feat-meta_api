import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.metaapi import metaapi_constants as CONSTANTS
from hummingbot.connector.exchange.metaapi.metaapi_api_order_book_data_source import MetaAPIOrderBookDataSource
from hummingbot.connector.exchange.metaapi.metaapi_api_user_stream_data_source import MetaAPIUserStreamDataSource
from hummingbot.connector.exchange.metaapi.metaapi_auth import MetaAPIAuth
from hummingbot.connector.exchange.metaapi.metaapi_risk_service import HighProbabilityConfig, MetaAPIRiskManager
from hummingbot.connector.exchange.metaapi.metaapi_utils import (
    DEFAULT_FEES,
    MetaAPIConfigMap,
    convert_trading_pair_from_metaapi_format,
    convert_trading_pair_to_metaapi_format,
    parse_metaapi_account_info,
    parse_metaapi_deal,
    parse_metaapi_order,
    validate_trading_pair,
)
from hummingbot.connector.exchange.metaapi.metaapi_web_utils import MetaAPIWebUtils
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

try:
    from metaapi_cloud_sdk import MetaApi
    from metaapi_cloud_sdk.clients.method_access_exception import MethodAccessException
except ImportError:  # pragma: no cover - dependency missing only during tests without extras
    MetaApi = None
    MethodAccessException = Exception


class MetaAPIExchange(ExchangePyBase):
    """Hummingbot connector backed by MetaAPI's MT4/MT5 infrastructure."""

    def __init__(
        self,
        metaapi_token: str,
        mt_login: str,
        mt_password: str,
        mt_server: str,
        metaapi_account_id: Optional[str] = None,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        platform: str = "mt5",
        connector_configuration: Optional[MetaAPIConfigMap] = None,
        equity_floor: Optional[Decimal] = None,
        allow_high_probability_override: Optional[bool] = None,
        high_probability_win_rate: Optional[Decimal] = None,
        high_probability_min_trades: Optional[int] = None,
        high_probability_lookback_days: Optional[int] = None,
    ):
        if MetaApi is None:
            raise ImportError("MetaAPI SDK not installed. Please install with: pip install metaapi-cloud-sdk")

        self.metaapi_token = metaapi_token
        self.mt_login = mt_login
        self.mt_password = mt_password
        self.mt_server = mt_server
        self.metaapi_account_id = metaapi_account_id
        self._domain = domain
        self._platform = platform.lower()
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []

        self._metaapi_client: Optional[MetaApi] = None
        self._mt_account = None
        self._rpc_connection = None
        self._streaming_connection = None
        self._account_deployed = False
        self._order_id_map: Dict[str, str] = {}
        self._latest_deal_id: Optional[str] = None
        self._connector_configuration = connector_configuration

        (
            self._equity_floor,
            self._allow_high_probability_override,
            self._high_probability_win_rate,
            self._high_probability_min_trades,
            self._high_probability_lookback_days,
        ) = self._derive_risk_settings(
            connector_configuration=connector_configuration,
            equity_floor=equity_floor,
            allow_high_probability_override=allow_high_probability_override,
            high_probability_win_rate=high_probability_win_rate,
            high_probability_min_trades=high_probability_min_trades,
            high_probability_lookback_days=high_probability_lookback_days,
        )
        probability_config = None
        if self._allow_high_probability_override and self._high_probability_win_rate is not None:
            probability_config = HighProbabilityConfig(
                min_win_rate=self._high_probability_win_rate,
                min_trades=self._high_probability_min_trades or 0,
                lookback_days=self._high_probability_lookback_days or 1,
            )
        self._risk_manager = MetaAPIRiskManager(
            token=self.metaapi_token,
            account_id=self.metaapi_account_id,
            domain=self._domain,
            equity_floor=self._equity_floor,
            allow_high_probability_override=self._allow_high_probability_override,
            probability_config=probability_config,
        )

        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ---------------------------------------------------------------------
    # Connector metadata
    # ---------------------------------------------------------------------
    @property
    def authenticator(self) -> MetaAPIAuth:
        return MetaAPIAuth(
            metaapi_token=self.metaapi_token,
            mt_login=self.mt_login,
            mt_password=self.mt_password,
            mt_server=self.mt_server,
            domain=self._domain,
            platform=self._platform,
        )

    @property
    def name(self) -> str:
        return f"metaapi_{self._platform}"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return ""

    @property
    def trading_pairs_request_path(self):
        return ""

    @property
    def check_network_request_path(self):
        return ""

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.MARKET, OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        return []

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return "order not found" in str(status_update_exception).lower()

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return "order not found" in str(cancelation_exception).lower()

    # ---------------------------------------------------------------------
    # Factory hooks
    # ---------------------------------------------------------------------
    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return MetaAPIWebUtils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return MetaAPIOrderBookDataSource(trading_pairs=self._trading_pairs, connector=self, domain=self.domain)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return MetaAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    # ---------------------------------------------------------------------
    # Fees and order submission
    # ---------------------------------------------------------------------
    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> AddedToCostTradeFee:
        percent = (
            DEFAULT_FEES.maker_percent_fee_decimal if is_maker else DEFAULT_FEES.taker_percent_fee_decimal
        )
        return AddedToCostTradeFee(percent=percent)

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        await self._risk_manager.ensure_can_place_order()
        await self._ensure_metaapi_connections()
        if not self._rpc_connection:
            raise ConnectionError("RPC connection not established")

        order_type_str = self.metaapi_order_type(order_type, trade_type)
        symbol = convert_trading_pair_to_metaapi_format(trading_pair)
        order_params = {
            "symbol": symbol,
            "type": order_type_str,
            "volume": float(amount),
            "clientId": order_id,
        }
        if order_type in {OrderType.LIMIT, OrderType.LIMIT_MAKER} and price != s_decimal_NaN:
            order_params["price"] = float(price)
        if "comment" in kwargs:
            order_params["comment"] = kwargs["comment"]

        submit_fn = None
        if order_type == OrderType.MARKET:
            submit_fn = (
                self._rpc_connection.create_market_buy_order
                if trade_type == TradeType.BUY
                else self._rpc_connection.create_market_sell_order
            )
        else:
            submit_fn = (
                self._rpc_connection.create_limit_buy_order
                if trade_type == TradeType.BUY
                else self._rpc_connection.create_limit_sell_order
            )

        result = await submit_fn(**order_params)
        exchange_order_id = str(result.get("orderId") or result.get("id") or result.get("ticket"))
        transact_time = self.current_timestamp
        self._order_id_map[exchange_order_id] = order_id
        return exchange_order_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        await self._ensure_metaapi_connections()
        if not self._rpc_connection:
            raise ConnectionError("RPC connection not established")

        target_id = tracked_order.exchange_order_id or order_id
        try:
            await self._rpc_connection.cancel_order(target_id)
            return True
        except Exception:
            self.logger().exception(f"Failed to cancel order {target_id}")
            return False

    # ---------------------------------------------------------------------
    # Trading rules & fees polling
    # ---------------------------------------------------------------------
    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        trading_rules: List[TradingRule] = []
        specifications = exchange_info_dict.get("specifications", {}) if exchange_info_dict else {}
        for symbol, spec in specifications.items():
            trading_pair = convert_trading_pair_from_metaapi_format(symbol)
            if not validate_trading_pair(trading_pair):
                continue
            try:
                min_volume = Decimal(str(spec.get("minVolume", 0.01)))
                volume_step = Decimal(str(spec.get("volumeStep", 0.01)))
                min_price = Decimal(str(spec.get("minPrice", 0.00001)))
                price_step = Decimal(str(spec.get("priceStep", 0.00001)))
                trading_rules.append(
                    TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=min_volume,
                        min_base_amount_increment=volume_step,
                        min_price_increment=price_step,
                        min_notional_size=min_volume * min_price,
                    )
                )
            except Exception:
                self.logger().warning(f"Failed to parse trading rule for {symbol}", exc_info=True)
        return trading_rules

    async def _update_trading_fees(self):
        # Fees are broker specific; defaults provided via DEFAULT_FEES
        pass

    async def _make_trading_rules_request(self) -> Dict[str, Any]:
        await self._ensure_metaapi_connections()
        specifications = getattr(self._streaming_connection.terminal_state, "specifications", {}) if self._streaming_connection else {}
        return {"specifications": specifications}

    async def _make_trading_pairs_request(self) -> Dict[str, Any]:
        return await self._make_trading_rules_request()

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        if self._trading_pairs:
            return
        specifications = exchange_info.get("specifications", {}) if exchange_info else {}
        discovered: List[str] = []
        for symbol in specifications.keys():
            trading_pair = convert_trading_pair_from_metaapi_format(symbol)
            if validate_trading_pair(trading_pair):
                discovered.append(trading_pair)
        if discovered:
            self._trading_pairs.extend(discovered)

    # ---------------------------------------------------------------------
    # User stream handling
    # ---------------------------------------------------------------------
    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("event_type")
                if event_type == "balance_update":
                    self._handle_balance_event(event_message)
                elif event_type == "order_update":
                    await self._handle_order_event(event_message)
                elif event_type == "trade_update":
                    await self._handle_trade_event(event_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop")
                await self._sleep(5.0)

    def _handle_balance_event(self, event: Dict[str, Any]):
        parsed = parse_metaapi_account_info(event.get("account_info", {}))
        currency = parsed["currency"]
        self._account_balances[currency] = Decimal(str(parsed["balance"]))
        self._account_available_balances[currency] = Decimal(str(parsed["free_margin"]))

    async def _handle_order_event(self, event: Dict[str, Any]):
        order_data = parse_metaapi_order(event.get("order", {}))
        symbol = order_data.get("symbol")
        trading_pair = convert_trading_pair_from_metaapi_format(symbol) if symbol else None
        if not trading_pair:
            return
        client_order_id = order_data.get("client_order_id") or self._order_id_map.get(order_data.get("id"))
        tracked_order = self._order_tracker.fetch_order(client_order_id=client_order_id, exchange_order_id=order_data.get("id"))
        if tracked_order is None:
            return
        new_state = CONSTANTS.ORDER_STATE.get(order_data.get("state"), tracked_order.current_state)
        order_update = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=event.get("timestamp", self.current_timestamp),
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id or order_data.get("id"),
        )
        self._order_tracker.process_order_update(order_update)

    async def _handle_trade_event(self, event: Dict[str, Any]):
        deal = parse_metaapi_deal(event.get("deal", {}))
        exchange_order_id = deal.get("order_id")
        client_order_id = self._order_id_map.get(exchange_order_id)
        tracked_order = self._order_tracker.fetch_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)
        if tracked_order is None:
            return
        trade_update = self._build_trade_update_from_deal(deal, tracked_order.trading_pair, client_order_id=tracked_order.client_order_id, exchange_order_id=exchange_order_id)
        if trade_update:
            self._order_tracker.process_trade_update(trade_update)
            self._latest_deal_id = deal.get("id")

    def _build_trade_update_from_deal(
        self,
        deal: Dict[str, Any],
        trading_pair: str,
        client_order_id: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
    ) -> Optional[TradeUpdate]:
        if not deal:
            return None
        base, quote = trading_pair.split("-")
        fill_timestamp = float(deal.get("time", 0)) / 1000 if deal.get("time") else self.current_timestamp
        fill_price = Decimal(str(deal.get("price", "0")))
        fill_base_amount = Decimal(str(deal.get("volume", "0")))
        fill_quote_amount = fill_base_amount * fill_price
        fee_amount = Decimal(str(abs(deal.get("commission", 0))))
        fee = AddedToCostTradeFee(flat_fees=[(quote, fee_amount)])
        trade_update = TradeUpdate(
            trade_id=deal.get("id", ""),
            client_order_id=client_order_id or self._order_id_map.get(deal.get("order_id"), ""),
            exchange_order_id=exchange_order_id or str(deal.get("order_id", "")),
            trading_pair=trading_pair,
            fill_timestamp=fill_timestamp,
            fill_price=fill_price,
            fill_base_amount=fill_base_amount,
            fill_quote_amount=fill_quote_amount,
            fee=fee,
        )
        return trade_update

    # ---------------------------------------------------------------------
    # Balances / status polling
    # ---------------------------------------------------------------------
    async def _update_balances(self):
        await self._ensure_metaapi_connections()
        if not self._rpc_connection:
            return
        account_info = await self._rpc_connection.get_account_information()
        parsed = parse_metaapi_account_info(account_info)
        currency = parsed["currency"]
        self._account_balances[currency] = Decimal(str(parsed["balance"]))
        self._account_available_balances[currency] = Decimal(str(parsed["free_margin"]))

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if not self._streaming_connection:
            return []
        history_storage = self._streaming_connection.history_storage
        if not history_storage:
            return []
        updates: List[TradeUpdate] = []
        for deal in history_storage.deals or []:
            if str(deal.get("orderId")) == (order.exchange_order_id or ""):
                parsed_deal = parse_metaapi_deal(deal)
                trade_update = self._build_trade_update_from_deal(
                    parsed_deal,
                    order.trading_pair,
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                )
                if trade_update:
                    updates.append(trade_update)
        return updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        order = None
        if self._streaming_connection:
            for candidate in self._streaming_connection.terminal_state.orders or []:
                if str(candidate.get("id")) == (tracked_order.exchange_order_id or ""):
                    order = candidate
                    break
        if order is None:
            raise ValueError(f"Order {tracked_order.client_order_id} not found")
        order_data = parse_metaapi_order(order)
        new_state = CONSTANTS.ORDER_STATE.get(order_data.get("state"), tracked_order.current_state)
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
        )

    # ---------------------------------------------------------------------
    # Network lifecycle
    # ---------------------------------------------------------------------
    async def start_network(self):
        await super().start_network()
        await self._ensure_metaapi_connections()

    async def stop_network(self):
        if self._rpc_connection:
            await self._rpc_connection.close()
            self._rpc_connection = None
        if self._streaming_connection:
            await self._streaming_connection.close()
            self._streaming_connection = None
        if self._mt_account and self._account_deployed:
            await self._mt_account.undeploy()
            self._account_deployed = False
            self._mt_account = None
        await self._risk_manager.stop()
        await super().stop_network()

    async def _make_network_check_request(self):
        await self._ensure_metaapi_connections()
        if self._rpc_connection:
            await self._rpc_connection.get_account_information()

    # ---------------------------------------------------------------------
    # MetaAPI helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def metaapi_order_type(order_type: OrderType, trade_type: TradeType) -> str:
        if order_type == OrderType.MARKET:
            return CONSTANTS.ORDER_TYPE_BUY if trade_type == TradeType.BUY else CONSTANTS.ORDER_TYPE_SELL
        if order_type in {OrderType.LIMIT, OrderType.LIMIT_MAKER}:
            return CONSTANTS.ORDER_TYPE_BUY_LIMIT if trade_type == TradeType.BUY else CONSTANTS.ORDER_TYPE_SELL_LIMIT
        raise ValueError(f"Unsupported order type: {order_type}")

    @staticmethod
    def to_hb_order_type(metaapi_type: str) -> OrderType:
        type_mapping = {
            CONSTANTS.ORDER_TYPE_BUY: OrderType.MARKET,
            CONSTANTS.ORDER_TYPE_SELL: OrderType.MARKET,
            CONSTANTS.ORDER_TYPE_BUY_LIMIT: OrderType.LIMIT,
            CONSTANTS.ORDER_TYPE_SELL_LIMIT: OrderType.LIMIT,
            CONSTANTS.ORDER_TYPE_BUY_STOP: OrderType.LIMIT,
            CONSTANTS.ORDER_TYPE_SELL_STOP: OrderType.LIMIT,
        }
        return type_mapping.get(metaapi_type, OrderType.LIMIT)

    async def _ensure_metaapi_connections(self):
        if self._metaapi_client is None:
            self._metaapi_client = MetaApi(token=self.metaapi_token, domain=self._domain)
        if self._mt_account is None or not self._account_deployed:
            await self._setup_mt_account()

    async def _setup_mt_account(self):
        account = None
        if self.metaapi_account_id:
            try:
                account = await self._metaapi_client.metatrader_account_api.get_account(self.metaapi_account_id)
            except Exception as exc:
                raise ValueError(
                    f"MetaAPI account id '{self.metaapi_account_id}' is not accessible with the provided token."
                ) from exc
        else:
            try:
                accounts = await self._metaapi_client.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()
            except MethodAccessException as exc:
                raise PermissionError(
                    "MetaAPI token cannot list accounts. Provide an API access token or set metaapi_account_id "
                    "in conf/connectors/metaapi.yml."
                ) from exc
            for acc in accounts:
                if acc.login == self.mt_login and acc.type.startswith("cloud"):
                    account = acc
                    break
            if account is None:
                account_config = self._auth.get_mt_account_config()
                account_config["platform"] = self._platform
                try:
                    account = await self._metaapi_client.metatrader_account_api.create_account(account_config)
                except MethodAccessException as exc:
                    raise PermissionError(
                        "MetaAPI token cannot create accounts. Provide an API access token or supply an existing "
                        "metaapi_account_id in your connector configuration."
                    ) from exc
        self._mt_account = account
        await self._mt_account.deploy()
        self._account_deployed = True
        await self._mt_account.wait_connected()

        self._rpc_connection = self._mt_account.get_rpc_connection()
        await self._rpc_connection.connect()
        await self._rpc_connection.wait_synchronized()

        self._streaming_connection = self._mt_account.get_streaming_connection()
        await self._streaming_connection.connect()
        await self._streaming_connection.wait_synchronized()
        await self._risk_manager.ensure_monitoring()

    def _derive_risk_settings(
        self,
        connector_configuration: Optional[MetaAPIConfigMap],
        equity_floor: Optional[Decimal],
        allow_high_probability_override: Optional[bool],
        high_probability_win_rate: Optional[Decimal],
        high_probability_min_trades: Optional[int],
        high_probability_lookback_days: Optional[int],
    ):
        config_equity_floor = (connector_configuration.equity_floor if connector_configuration else None)
        config_allow_override = (
            connector_configuration.allow_high_probability_override if connector_configuration else None
        )
        config_win_rate = connector_configuration.high_probability_win_rate if connector_configuration else None
        config_min_trades = connector_configuration.high_probability_min_trades if connector_configuration else None
        config_lookback_days = (
            connector_configuration.high_probability_lookback_days if connector_configuration else None
        )

        if equity_floor is not None:
            resolved_equity_floor = Decimal(str(equity_floor))
        elif config_equity_floor is not None:
            resolved_equity_floor = config_equity_floor
        else:
            resolved_equity_floor = Decimal("0")

        if allow_high_probability_override is not None:
            resolved_allow_override = allow_high_probability_override
        elif config_allow_override is not None:
            resolved_allow_override = config_allow_override
        else:
            resolved_allow_override = False

        if high_probability_win_rate is not None:
            resolved_win_rate = Decimal(str(high_probability_win_rate))
        else:
            resolved_win_rate = config_win_rate

        if high_probability_min_trades is not None:
            resolved_min_trades = int(high_probability_min_trades)
        else:
            resolved_min_trades = config_min_trades

        if high_probability_lookback_days is not None:
            resolved_lookback_days = int(high_probability_lookback_days)
        else:
            resolved_lookback_days = config_lookback_days
        return (
            resolved_equity_floor,
            resolved_allow_override,
            resolved_win_rate,
            resolved_min_trades,
            resolved_lookback_days,
        )


class MetaapiExchange(MetaAPIExchange):
    """
    Backwards-compatible alias that matches the class name inferred from connector metadata.
    Hummingbot's dynamic loader capitalizes module segments (metaapi_exchange -> MetaapiExchange),
    so we expose this alias to satisfy the loader while keeping the canonical MetaAPIExchange name.
    """

    pass
