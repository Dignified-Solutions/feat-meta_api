from typing import Any, Dict

from hummingbot.core.web_assistant.auth import AuthBase


class MetaAPIAuth(AuthBase):
    """
    MetaAPI authentication handler.

    MetaAPI uses token-based authentication for API access, combined with
    MT4/MT5 broker credentials for trading account access.
    """

    def __init__(
        self,
        metaapi_token: str,
        mt_login: str,
        mt_password: str,
        mt_server: str,
        domain: str = "agiliumtrade.agiliumtrade.ai",
        platform: str = "mt5",
    ):
        """
        Initialize MetaAPI authentication.

        Args:
            metaapi_token: MetaAPI access token
            mt_login: MT4/MT5 account login
            mt_password: MT4/MT5 account password
            mt_server: MT4/MT5 server name (e.g., "ICMarkets-Demo")
            domain: MetaAPI domain
        """
        super().__init__()
        self.metaapi_token = metaapi_token
        self.mt_login = mt_login
        self.mt_password = mt_password
        self.mt_server = mt_server
        self.domain = domain
        self.platform = platform

    async def rest_authenticate(self, request: Any) -> Any:
        """
        Add authentication headers for REST API requests.

        MetaAPI uses Bearer token authentication for REST endpoints.
        """
        headers = request.headers or {}
        headers["Authorization"] = f"Bearer {self.metaapi_token}"
        headers["Content-Type"] = "application/json"
        return request.clone(headers=headers)

    async def ws_authenticate(self, request: Any) -> Any:
        """
        Add authentication for WebSocket connections.

        MetaAPI WebSocket connections use token-based authentication.
        """
        headers = request.headers or {}
        headers["Authorization"] = f"Bearer {self.metaapi_token}"
        return request.clone(headers=headers)

    def get_mt_account_config(self) -> Dict[str, Any]:
        """
        Get MT4/MT5 account configuration for MetaAPI account creation.

        Returns:
            Dictionary with account configuration parameters
        """
        return {
            "name": f"MT Account {self.mt_login}",
            "type": "cloud",
            "login": self.mt_login,
            "password": self.mt_password,
            "server": self.mt_server,
            "platform": self.platform,
            "application": "MetaApi",
            "magic": 1000,
        }

    def get_metaapi_config(self) -> Dict[str, str]:
        """
        Get MetaAPI client configuration.

        Returns:
            Dictionary with MetaAPI configuration
        """
        return {"domain": self.domain, "token": self.metaapi_token}
