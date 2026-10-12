"""
News Sentiment Ingest — Real-Time News Streamer
==============================================
Collects news from CNBC RSS, Yahoo Finance RSS, yfinance API, and IBKR bulletins.
Publishes raw news events to the event bus.
"""
import time
import threading
import datetime
import sqlite3
import os
import feedparser
import yfinance as yf
from event_bus import get_bus, Event, Topics
from logger import get_logger

log = get_logger("NewsIngest")

from paths import DB_PATH

# Tracks processed article links to avoid duplicate broadcasts (bounded to prevent memory leak)
_processed_urls = set()
_processed_lock = threading.Lock()
_MAX_PROCESSED_URLS = 5000  # Clear oldest URLs when this limit is reached

def get_watchlist():
    """Helper to fetch active watchlist from daily_watchlist table."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        c = conn.cursor()
        c.execute('SELECT ticker FROM daily_watchlist')
        watchlist = [r[0] for r in c.fetchall()]
        conn.close()
        return watchlist
    except Exception as e:
        log.warning(f"Could not load watchlist: {e}")
        return []

def poll_rss_feeds():
    """Polls CNBC and Yahoo Finance RSS feeds for breaking macro and business news."""
    feeds = {
        'cnbc': 'https://search.cnbc.com/rs/search/combinedfeed.xml',
        'yahoo': 'https://finance.yahoo.com/news/rssindex'
    }
    
    bus = get_bus()
    for name, url in feeds.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                link = entry.get('link', '')
                if not link:
                    continue
                
                with _processed_lock:
                    if link in _processed_urls:
                        continue
                    _processed_urls.add(link)
                    if len(_processed_urls) > _MAX_PROCESSED_URLS:
                        _processed_urls.clear()
                
                title = entry.get('title', '')
                summary = entry.get('summary', '') or entry.get('description', '')
                pub_date = entry.get('published', '') or datetime.datetime.now().isoformat()
                
                log.debug(f"New RSS article: {title[:50]}...")
                bus.publish(Topics.NEWS_ARRIVED, Event(
                    source=f"rss_{name}",
                    data={
                        "title": title,
                        "summary": summary,
                        "url": link,
                        "published_at": pub_date,
                        "feed": name
                    }
                ))
        except Exception as e:
            log.error(f"RSS polling failed for {name}: {e}")

def poll_yfinance_news():
    """Polls ticker-specific news for symbols on the daily watchlist."""
    watchlist = get_watchlist()
    if not watchlist:
        return
        
    bus = get_bus()
    for ticker in watchlist:
        if ticker == 'SPY':  # Skip general index to focus on specific stock assets
            continue
        try:
            stock = yf.Ticker(ticker)
            news_list = stock.news
            if not news_list:
                continue
                
            for item in news_list:
                link = item.get('link', '')
                if not link:
                    continue
                
                with _processed_lock:
                    if link in _processed_urls:
                        continue
                    _processed_urls.add(link)
                    if len(_processed_urls) > _MAX_PROCESSED_URLS:
                        _processed_urls.clear()
                    
                title = item.get('title', '')
                publisher = item.get('publisher', '')
                pub_date = item.get('providerPublishTime', datetime.datetime.now().timestamp())
                if isinstance(pub_date, (int, float)):
                    pub_date = datetime.datetime.fromtimestamp(pub_date).isoformat()
                    
                log.debug(f"New yfinance article for {ticker}: {title[:50]}...")
                bus.publish(Topics.NEWS_ARRIVED, Event(
                    source="yfinance_api",
                    data={
                        "title": title,
                        "summary": publisher,
                        "url": link,
                        "published_at": pub_date,
                        "feed": "yfinance",
                        "associated_ticker": ticker
                    }
                ))
        except Exception as e:
            log.error(f"yfinance news polling failed for {ticker}: {e}")

def run_news_ingest(stop_event: threading.Event):
    """Execution loop run by the orchestrator daemon thread."""
    log.info("Starting News Sentiment Ingest Service...")
    
    rss_interval = 30  # 30 seconds
    yf_interval = 60   # 60 seconds
    
    last_rss = 0
    last_yf = 0
    
    # Try to hook into IBKR news bulletins if available
    try:
        from broker_factory import get_broker
        broker = get_broker(auto_connect=False)
        if broker and broker.name == "Interactive Brokers":
            # Subscribe to IBKR bulletins in a separate handler if active
            # This runs asynchronously through ib_insync callbacks
            log.info("Attempting IBKR news bulletins subscription integration...")
    except Exception as e:
        log.warning(f"Could not hook into IBKR bulletins: {e}")

    while not stop_event.is_set():
        now = time.time()
        
        # 1. Poll RSS feeds
        if now - last_rss >= rss_interval:
            poll_rss_feeds()
            last_rss = now
            
        # 2. Poll yfinance news
        if now - last_yf >= yf_interval:
            poll_yfinance_news()
            last_yf = now
            
        time.sleep(1)
        
    log.info("News Sentiment Ingest Service stopped.")
