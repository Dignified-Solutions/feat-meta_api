"""Utility helpers and connector metadata for the MetaAPI exchange adapter."""

from decimal import Decimal
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field, SecretStr, field_validator

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "EUR-USD"
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0002"),
)
SUPPORTED_CURRENCIES = {
    "AUD",
    "CAD",
    "CHF",
    "EUR",
    "GBP",
    "JPY",
    "NZD",
    "USD",
}


def is_exchange_information_valid(_exchange_info: Dict[str, Any]) -> bool:
    """MetaAPI does not expose a conventional exchange-info payload but we keep the hook for parity."""
    return True


def convert_trading_pair_to_metaapi_format(trading_pair: str) -> str:
    """Convert Hummingbot format (EUR-USD) into MetaAPI format (EURUSD)."""
    return trading_pair.replace("-", "")


def convert_trading_pair_from_metaapi_format(symbol: str) -> str:
    """Convert MetaAPI symbols (EURUSD) into Hummingbot format (EUR-USD)."""
    if len(symbol) == 6:
        return f"{symbol[:3]}-{symbol[3:]}"
    return symbol


def extract_symbol_from_trading_pair(trading_pair: str) -> str:
    """Return the MetaAPI symbol for the provided trading pair."""
    return convert_trading_pair_to_metaapi_format(trading_pair)


def format_metaapi_order_params(order_params: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce order params (volume, price) into the types expected by MetaAPI's RPC layer."""
    formatted_params = order_params.copy()
    if "volume" in formatted_params:
        formatted_params["volume"] = float(formatted_params["volume"])
    if "price" in formatted_params and formatted_params["price"] is not None:
        formatted_params["price"] = float(formatted_params["price"])
    if "symbol" in formatted_params:
        formatted_params["symbol"] = convert_trading_pair_to_metaapi_format(formatted_params["symbol"])
    return formatted_params


def parse_metaapi_account_info(account_info: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the subset of account info fields used by the connector."""
    equity = float(account_info.get("equity", 0))
    margin = float(account_info.get("margin", 0))
    return {
        "balance": float(account_info.get("balance", 0)),
        "equity": equity,
        "margin": margin,
        "free_margin": equity - margin,
        "currency": account_info.get("currency", "USD"),
        "leverage": int(account_info.get("leverage", 100)),
        "profit": float(account_info.get("profit", 0)),
    }


def parse_metaapi_position(position: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize position payloads so downstream code can rely on common keys."""
    return {
        "id": position.get("id"),
        "symbol": position.get("symbol"),
        "type": position.get("type"),
        "volume": float(position.get("volume", 0)),
        "price": float(position.get("price", 0)),
        "profit": float(position.get("profit", 0)),
        "swap": float(position.get("swap", 0)),
        "commission": float(position.get("commission", 0)),
        "magic": position.get("magic"),
        "comment": position.get("comment"),
        "time": position.get("time"),
    }


def parse_metaapi_order(order: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize order metadata."""
    return {
        "id": str(order.get("id")),
        "client_order_id": order.get("clientOrderId"),
        "symbol": order.get("symbol"),
        "type": order.get("type"),
        "state": order.get("state"),
        "volume": float(order.get("volume", 0)),
        "price": float(order.get("price", 0)),
        "stop_loss": float(order.get("stopLoss", 0)) if order.get("stopLoss") else None,
        "take_profit": float(order.get("takeProfit", 0)) if order.get("takeProfit") else None,
        "time": order.get("time"),
        "expiration": order.get("expiration"),
        "comment": order.get("comment"),
    }


def parse_metaapi_deal(deal: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize executed trade details."""
    return {
        "id": str(deal.get("id")),
        "order_id": str(deal.get("orderId")),
        "position_id": deal.get("positionId"),
        "symbol": deal.get("symbol"),
        "type": deal.get("type"),
        "volume": float(deal.get("volume", 0)),
        "price": float(deal.get("price", 0)),
        "profit": float(deal.get("profit", 0)),
        "swap": float(deal.get("swap", 0)),
        "commission": float(deal.get("commission", 0)),
        "time": deal.get("time"),
        "entry_type": deal.get("entryType"),
        "magic": deal.get("magic"),
        "comment": deal.get("comment"),
    }


def calculate_lot_size(volume: float, _symbol: str = "", _account_currency: str = "USD") -> float:
    """Return the supplied volume unchanged; MetaAPI expects lots and the strategy drives the sizing."""
    return volume


def validate_trading_pair(trading_pair: str) -> bool:
    if "-" not in trading_pair:
        return False
    base, quote = trading_pair.split("-", 1)
    return len(base) == 3 and len(quote) == 3 and {base.upper(), quote.upper()} <= SUPPORTED_CURRENCIES


def _lower(value: str) -> str:
    return value.lower() if isinstance(value, str) else value


class MetaAPIConfigMap(BaseConnectorConfigMap):
    """Connector level configuration prompts for the MetaAPI SDK adapter."""

    connector: str = "metaapi"
    receive_connector_configuration: bool = Field(default=True)

    metaapi_token: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MetaAPI token",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    metaapi_account_id: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your MetaAPI account id (optional, required for account-scoped tokens)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    mt_login: str = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MT4/MT5 account login",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    mt_password: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MT4/MT5 account password",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    mt_server: str = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MT4/MT5 server name (e.g., 'ICMarkets-Demo')",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    platform: str = Field(
        default="mt5",
        json_schema_extra={
            "prompt": "Enter trading platform (mt4 or mt5)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": True,
        },
    )
    equity_floor: Decimal = Field(
        default=Decimal("20"),
        ge=Decimal("0"),
        description="Minimum equity (in account currency) required before placing new orders.",
        json_schema_extra={
            "prompt": "Enter the minimum equity you are willing to keep before new orders are allowed (e.g., 20)",
            "prompt_on_new": True,
        },
    )
    allow_high_probability_override: bool = Field(
        default=True,
        description="Allow AI/high-probability logic to override the equity floor when confidence is high.",
        json_schema_extra={
            "prompt": "Allow high-probability trades to override the equity floor? (true/false)",
            "prompt_on_new": True,
        },
    )
    high_probability_win_rate: Decimal = Field(
        default=Decimal("0.65"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Minimum historical win rate required to override the equity floor.",
        json_schema_extra={
            "prompt": "Enter the minimum win-rate (0-1) required to override the equity floor",
            "prompt_on_new": True,
        },
    )
    high_probability_min_trades: int = Field(
        default=25,
        ge=1,
        description="Minimum number of recent trades evaluated when computing the win rate.",
        json_schema_extra={
            "prompt": "Enter how many recent trades must be analyzed before overrides are considered",
            "prompt_on_new": True,
        },
    )
    high_probability_lookback_days: int = Field(
        default=7,
        ge=1,
        description="Number of days to consider when calculating the recent win rate.",
        json_schema_extra={
            "prompt": "Enter the lookback window (in days) for win-rate calculations",
            "prompt_on_new": True,
        },
    )

    model_config = ConfigDict(title="metaapi")

    @field_validator("platform")
    @classmethod
    def normalize_platform(cls, value: str) -> str:
        normalized = _lower(value)
        if normalized not in {"mt4", "mt5"}:
            raise ValueError("platform must be either 'mt4' or 'mt5'")
        return normalized


KEYS = MetaAPIConfigMap.model_construct()
