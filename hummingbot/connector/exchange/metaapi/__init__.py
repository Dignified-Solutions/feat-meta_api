__all__ = ["MetaAPIExchange"]


def __getattr__(name):
    if name == "MetaAPIExchange":
        from .metaapi_exchange import MetaAPIExchange as _MetaAPIExchange
        return _MetaAPIExchange
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
