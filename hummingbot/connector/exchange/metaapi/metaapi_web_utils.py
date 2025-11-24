from typing import Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class MetaAPIWebUtils:
    """
    Web utilities for MetaAPI connector.

    MetaAPI uses a different architecture than traditional exchanges,
    so this provides compatibility with Hummingbot's web assistant framework.
    """

    @staticmethod
    def build_api_factory(
        throttler: AsyncThrottler,
        time_synchronizer: TimeSynchronizer,
        _domain: str,
        auth: Optional["MetaAPIAuth"] = None
    ) -> WebAssistantsFactory:
        """
        Build web assistants factory for MetaAPI.

        Since MetaAPI uses SDK-based communication rather than REST/WebSocket,
        this factory provides compatibility with Hummingbot's expectations.
        """
        # MetaAPI leverages SDK connections; however, Hummingbot expects a WebAssistantsFactory instance.
        # We construct a minimal factory with the provided throttler and optional auth.
        api_factory = WebAssistantsFactory(
            throttler=throttler,
            auth=auth,
        )
        return api_factory

    @staticmethod
    def public_rest_url(path_url: str, domain: str) -> str:
        """
        Generate public REST URL for MetaAPI.

        MetaAPI doesn't have traditional public REST endpoints,
        but this provides compatibility.
        """
        return f"https://mt-client-api-v1.{domain}{path_url}"

    @staticmethod
    def private_rest_url(path_url: str, domain: str) -> str:
        """
        Generate private REST URL for MetaAPI.

        MetaAPI doesn't have traditional private REST endpoints,
        but this provides compatibility.
        """
        return f"https://mt-client-api-v1.{domain}{path_url}"