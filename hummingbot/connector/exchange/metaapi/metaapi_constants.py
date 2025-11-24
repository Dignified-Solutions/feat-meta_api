from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

# MetaAPI Configuration
DEFAULT_DOMAIN = "agiliumtrade.agiliumtrade.ai"
METAAPI_API_VERSION = "1"

# Authentication
HBOT_ORDER_ID_PREFIX = "x-MA"
MAX_ORDER_ID_LEN = 32

# MetaAPI doesn't have traditional REST endpoints like exchanges
# Instead it uses RPC connections and streaming
# These are placeholder URLs for compatibility
REST_URL = "https://mt-client-api-v1.{}/"
WSS_URL = "wss://mt-client-api-v1.{}/"

# MetaAPI Order Types (MT4/MT5)
ORDER_TYPE_BUY = "ORDER_TYPE_BUY"
ORDER_TYPE_SELL = "ORDER_TYPE_SELL"
ORDER_TYPE_BUY_LIMIT = "ORDER_TYPE_BUY_LIMIT"
ORDER_TYPE_SELL_LIMIT = "ORDER_TYPE_SELL_LIMIT"
ORDER_TYPE_BUY_STOP = "ORDER_TYPE_BUY_STOP"
ORDER_TYPE_SELL_STOP = "ORDER_TYPE_SELL_STOP"
ORDER_TYPE_BUY_STOP_LIMIT = "ORDER_TYPE_BUY_STOP_LIMIT"
ORDER_TYPE_SELL_STOP_LIMIT = "ORDER_TYPE_SELL_STOP_LIMIT"

# Time in Force
ORDER_TIME_GTC = "ORDER_TIME_GTC"  # Good till cancelled
ORDER_TIME_DAY = "ORDER_TIME_DAY"  # Good for day
ORDER_TIME_SPECIFIED = "ORDER_TIME_SPECIFIED"  # Until specified time

# Order States Mapping (MetaAPI to Hummingbot)
ORDER_STATE = {
    "ORDER_STATE_PLACED": OrderState.PENDING_CREATE,
    "ORDER_STATE_PENDING": OrderState.OPEN,
    "ORDER_STATE_PARTIAL": OrderState.PARTIALLY_FILLED,
    "ORDER_STATE_FILLED": OrderState.FILLED,
    "ORDER_STATE_CANCELED": OrderState.CANCELED,
    "ORDER_STATE_REJECTED": OrderState.FAILED,
    "ORDER_STATE_EXPIRED": OrderState.FAILED,
}

# Deal Types (for historical data)
DEAL_TYPE_BUY = "DEAL_TYPE_BUY"
DEAL_TYPE_SELL = "DEAL_TYPE_SELL"

# Position Types
POSITION_TYPE_BUY = "POSITION_TYPE_BUY"
POSITION_TYPE_SELL = "POSITION_TYPE_SELL"

# Rate Limits (MetaAPI has different rate limiting than traditional exchanges)
# These are conservative defaults - MetaAPI rate limits are more flexible
RATE_LIMITS = [
    RateLimit(limit_id="rpc_requests", limit=100, time_interval=60),  # 100 requests per minute
    RateLimit(limit_id="streaming_connections", limit=10, time_interval=60),  # 10 connections per minute
    RateLimit(limit_id="historical_data", limit=50, time_interval=60),  # 50 historical requests per minute
]

# Error Codes
ERROR_ACCOUNT_NOT_FOUND = "E_ACCOUNT_NOT_FOUND"
ERROR_TRADE_REJECTED = "E_TRADE_REJECTED"
ERROR_INVALID_SYMBOL = "E_INVALID_SYMBOL"
ERROR_INSUFFICIENT_FUNDS = "E_INSUFFICIENT_FUNDS"

# Connection Settings
CONNECTION_TIMEOUT = 30  # seconds
RECONNECT_DELAY = 5  # seconds
MAX_RECONNECT_ATTEMPTS = 5

# Synchronization Settings
TERMINAL_SYNC_TIMEOUT = 300  # 5 minutes for terminal state sync
ACCOUNT_DEPLOY_TIMEOUT = 120  # 2 minutes for account deployment

# Risk Management Defaults
DEFAULT_DRAWDOWN_THRESHOLD = 5.0  # 5% drawdown threshold
DEFAULT_PERIOD = "day"  # Daily risk monitoring