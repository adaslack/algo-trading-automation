"""
Event Bus — Lightweight Pub/Sub Architecture (V5 Upgrade - Optimized)
=====================================================================
Decouples wheels from direct SQLite coupling by providing a centralized
publish/subscribe event system.

Optimized with a centralized ThreadPoolExecutor for high-frequency async dispatch.
"""
import json
import time
import threading
import concurrent.futures
from collections import defaultdict
from typing import Callable, Any, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime
from logger import get_logger

log = get_logger("EventBus")

# Reusable thread pool for async event dispatching (Phase-2 Latency Optimization)
_EVENT_BUS_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=5, thread_name_prefix="EventBusPool")

@dataclass
class Event:
    """Typed event contract for the event bus."""
    source: str                     # Which wheel published this
    event_type: str = ""           # e.g. "market.update", "signal.buy"
    data: dict = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    correlation_id: str = ""       # For tracing event chains

    def to_json(self) -> str:
        def default_serializer(o):
            import datetime
            if isinstance(o, (datetime.datetime, datetime.date)):
                return o.isoformat()
            raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")
        return json.dumps(asdict(self), default=default_serializer)

    @classmethod
    def from_json(cls, raw: str) -> 'Event':
        return cls(**json.loads(raw))


class InMemoryBus:
    """
    Thread-safe in-memory pub/sub event bus.
    Suitable for single-process multi-threaded pipelines.
    """

    def __init__(self):
        self._subscribers: dict[str, list[Callable]] = defaultdict(list)
        self._lock = threading.Lock()
        self._event_log: list[Event] = []
        self._max_log_size = 10000
        self.stats = {
            'published': 0,
            'delivered': 0,
            'errors': 0,
        }

    def subscribe(self, topic: str, handler: Callable[[Event], None]):
        """Subscribe a handler to a topic."""
        with self._lock:
            self._subscribers[topic].append(handler)
            log.info(f"Subscribed to '{topic}' ({len(self._subscribers[topic])} listeners)")

    def unsubscribe(self, topic: str, handler: Callable):
        """Remove a handler from a topic."""
        with self._lock:
            if topic in self._subscribers:
                self._subscribers[topic] = [h for h in self._subscribers[topic] if h != handler]

    def publish(self, topic: str, event: Event):
        """Publish an event to all subscribers of a topic."""
        event.event_type = topic
        self.stats['published'] += 1

        # Log event
        if len(self._event_log) < self._max_log_size:
            self._event_log.append(event)

        with self._lock:
            handlers = list(self._subscribers.get(topic, []))

        for handler in handlers:
            try:
                handler(event)
                self.stats['delivered'] += 1
            except Exception as e:
                self.stats['errors'] += 1
                log.error(f"Event handler error on '{topic}': {e}")

    def publish_async(self, topic: str, event: Event):
        """Publish an event asynchronously using asyncio loop tasks if available, else thread pool."""
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Schedule as concurrent coroutine task on the active loop
                loop.create_task(self._async_publish_wrapper(topic, event))
                return
        except Exception:
            pass
            
        # Background fallback thread pool submit
        _EVENT_BUS_POOL.submit(self.publish, topic, event)

    async def _async_publish_wrapper(self, topic: str, event: Event):
        self.publish(topic, event)

    def get_stats(self) -> dict:
        return {**self.stats, 'topics': len(self._subscribers)}

    def get_recent_events(self, topic: Optional[str] = None, limit: int = 50) -> list[dict]:
        """Get recent events for debugging/monitoring."""
        events = self._event_log
        if topic:
            events = [e for e in events if e.event_type == topic]
        return [asdict(e) for e in events[-limit:]]


class RedisBus:
    """
    Redis Streams-backed event bus for production.
    Leverages lock-free Redis Streams (XADD, XREADGROUP) to provide 
    consumer group load balancing, backpressure, and strict event ordering.
    """

    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        try:
            import redis
            self._redis = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True,
                socket_connect_timeout=2.0,
                socket_timeout=2.0
            )
            self._redis.ping()
            self._handlers: dict[str, list[Callable]] = defaultdict(list)
            self._consumer_group = "quant_group"
            self._active_streams: dict[str, str] = {}  # {stream_key: group_name}
            self._listener_threads: list[threading.Thread] = []
            self._stop_event = threading.Event()
            self.stats = {'published': 0, 'delivered': 0, 'errors': 0}
            log.info(f"RedisBus connected to Redis Streams at {host}:{port}")
        except Exception as e:
            log.warning(f"Redis unavailable ({e}), falling back to InMemoryBus")
            raise

    def subscribe(self, topic: str, handler: Callable[[Event], None]):
        self._handlers[topic].append(handler)
        stream_key = f"stream:{topic}"
        group_name = self._consumer_group
        
        # Ensure consumer group exists
        try:
            self._redis.xgroup_create(stream_key, group_name, id="0", mkstream=True)
            log.info(f"Created Redis Stream consumer group '{group_name}' for stream '{stream_key}'")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                log.warning(f"Note on xgroup_create for {stream_key}: {e}")

        # Start background consumer thread if not active
        if stream_key not in self._active_streams:
            self._active_streams[stream_key] = group_name
            t = threading.Thread(
                target=self._consume_loop,
                args=(topic, stream_key, group_name),
                daemon=True,
                name=f"RedisStreamConsumer-{topic}"
            )
            t.start()
            self._listener_threads.append(t)
            log.info(f"Started Redis Stream consumer loop for stream '{stream_key}'")

    def unsubscribe(self, topic: str, handler: Callable):
        """Remove a handler from a topic in Redis Streams."""
        if topic in self._handlers:
            self._handlers[topic] = [h for h in self._handlers[topic] if h != handler]
        log.info(f"Redis unsubscribed from stream '{topic}'")

    def _consume_loop(self, topic: str, stream_key: str, group_name: str):
        consumer_name = f"consumer_{threading.get_ident()}_{int(time.time())}"
        log.info(f"Redis Stream consumer '{consumer_name}' active for {stream_key}")
        
        while not self._stop_event.is_set():
            try:
                # Read from group, block for up to 100ms
                messages = self._redis.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_key: ">"},
                    count=10,
                    block=100
                )
                
                if not messages:
                    continue
                
                for stream, msg_list in messages:
                    for msg_id, payload in msg_list:
                        raw_event = payload.get("event")
                        if raw_event:
                            try:
                                event = Event.from_json(raw_event)
                                for handler in list(self._handlers.get(topic, [])):
                                    try:
                                        handler(event)
                                        self.stats['delivered'] += 1
                                    except Exception as eh:
                                        self.stats['errors'] += 1
                                        log.error(f"Error in Redis Stream handler: {eh}")
                            except Exception as ed:
                                log.error(f"Failed to deserialize stream event: {ed}")
                        
                        # Acknowledge the message
                        self._redis.xack(stream_key, group_name, msg_id)
            except Exception as e:
                # Sleep briefly to avoid tight loop on Redis connection loss
                time.sleep(1.0)
                log.error(f"Redis Stream consume error on '{stream_key}': {e}")

    def publish(self, topic: str, event: Event):
        event.event_type = topic
        stream_key = f"stream:{topic}"
        # Publish to Redis Stream with maxlen to prevent infinite memory growth
        self._redis.xadd(stream_key, {"event": event.to_json()}, maxlen=10000, approximate=True)
        self.stats['published'] += 1

    def publish_async(self, topic: str, event: Event):
        self.publish(topic, event)  # Redis Stream write is lock-free & non-blocking


# ========== FACTORY ==========

_bus_instance = None

def get_bus(use_redis: bool = True) -> InMemoryBus | RedisBus:
    """
    Get the singleton event bus instance.
    """
    global _bus_instance
    if _bus_instance is None:
        if use_redis:
            try:
                import os
                host = os.getenv('REDIS_HOST', 'localhost')
                port = int(os.getenv('REDIS_PORT', 6379))
                db = int(os.getenv('REDIS_DB', 0))
                
                _bus_instance = RedisBus(host=host, port=port, db=db)
            except Exception as e:
                log.warning(f"Redis connection failed, falling back to InMemoryBus: {e}")
                _bus_instance = InMemoryBus()
        else:
            _bus_instance = InMemoryBus()
    return _bus_instance


# ========== STANDARD TOPICS ==========

class Topics:
    """Standard event topic names for the pipeline."""
    MARKET_UPDATE = "market.update"
    ALT_DATA_UPDATE = "alt_data.update"
    SIGNAL_GENERATED = "signal.generated"
    ORDER_APPROVED = "order.approved"
    ORDER_REJECTED = "order.rejected"
    ORDER_FILLED = "order.filled"
    EXIT_TRIGGERED = "exit.triggered"
    RISK_ALERT = "risk.alert"
    CIRCUIT_BREAKER = "risk.circuit_breaker"
    PORTFOLIO_UPDATE = "portfolio.update"
    STRATEGY_REBALANCE = "strategy.rebalance"

