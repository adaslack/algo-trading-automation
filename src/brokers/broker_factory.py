"""
Broker Factory — Dynamic Broker Selection
============================================
Creates the correct broker instance based on BROKER_TYPE in .env.
Provides a single get_broker() call for all pipeline modules.

Usage:
    from broker_factory import get_broker
    broker = get_broker()
    broker.connect()
    account = broker.get_account()
"""
import os
import sys
import logging
import asyncio
from dotenv import load_dotenv

# Setup default event loop globally for Python 3.14+ asyncio compatibility
try:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
except Exception:
    pass

# Ensure the brokers directory is on the path
_BROKERS_DIR = os.path.dirname(os.path.abspath(__file__))
if _BROKERS_DIR not in sys.path:
    sys.path.insert(0, _BROKERS_DIR)

from base_broker import BaseBroker

log = logging.getLogger("BrokerFactory")

load_dotenv(os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    '.env'
))

_broker_instance: BaseBroker | None = None


def create_broker(broker_type: str = None) -> BaseBroker:
    """
    Create a new broker instance based on the given type.

    Parameters
    ----------
    broker_type : 'ibkr'. Defaults to BROKER_TYPE env var.

    Returns
    -------
    BaseBroker implementation instance (not yet connected).
    """
    bt = (broker_type or os.getenv('BROKER_TYPE', 'ibkr')).lower().strip()

    if bt == 'ibkr':
        from ibkr_broker import IBKRBroker
        host = os.getenv('IB_HOST', '127.0.0.1')
        port = int(os.getenv('IB_PORT', 7497))
        client_id = int(os.getenv('IB_CLIENT_ID', 1))
        log.info(f"Creating IBKR broker (host={host}, port={port}, client_id={client_id})")
        return IBKRBroker(host=host, port=port, client_id=client_id)

    else:
        raise ValueError(
            f"Unknown BROKER_TYPE '{bt}'. Supported: 'ibkr'. "
            f"Set BROKER_TYPE in your .env file."
        )


def get_broker(auto_connect: bool = True) -> BaseBroker:
    """
    Get the singleton broker instance, creating and connecting if needed.

    Parameters
    ----------
    auto_connect : If True, automatically calls connect() on first creation.

    Returns
    -------
    Connected BaseBroker instance.
    """
    global _broker_instance

    if _broker_instance is None:
        _broker_instance = create_broker()
        if auto_connect:
            success = _broker_instance.connect()
            if not success:
                log.warning(
                    f"Broker '{_broker_instance.name}' failed to connect. "
                    f"Pipeline will run in simulation/fallback mode."
                )

    return _broker_instance


def reset_broker():
    """Reset the singleton broker (useful for testing or broker switching)."""
    global _broker_instance
    if _broker_instance is not None:
        try:
            _broker_instance.disconnect()
        except Exception:
            pass
    _broker_instance = None
