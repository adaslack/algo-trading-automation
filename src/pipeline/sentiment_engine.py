"""
Sentiment Engine — Real-Time Sentiment & Consensus Analyzer
=========================================================
Listens to Topics.NEWS_ARRIVED, scores articles using NLTK VADER,
matches them to active watchlist symbols, and publishes sentiment alerts.
"""
import re
import datetime
import sqlite3
import os
import nltk
from event_bus import get_bus, Event, Topics
from logger import get_logger

log = get_logger("SentimentEngine")

from paths import DB_PATH

# Ensure NLTK VADER data is downloaded on startup
try:
    nltk.download('vader_lexicon', quiet=True)
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    _sia = SentimentIntensityAnalyzer()
    
    # Enrich VADER with specialized finance terminology (positive/negative overrides)
    _sia.lexicon.update({
        'trillion': 2.0,
        'surge': 1.5,
        'breakout': 1.5,
        'acquisition': 1.0,
        'upgrade': 1.0,
        'beat': 1.0,
        'downgrade': -1.5,
        'plummet': -2.0,
        'miss': -1.0,
        'bankruptcy': -3.0,
        'investigation': -1.5
    })
except Exception as e:
    log.error(f"Failed to initialize VADER: {e}")
    _sia = None

# Dynamic company aliases are built dynamically from daily_watchlist at runtime

# Stores recent sentiment alerts to evaluate consensus
# Format: {ticker: [(timestamp, sentiment_score, source)]}
_recent_alerts = {}

def get_watchlist() -> list[tuple[str, str]]:
    """Helper to fetch active watchlist with company names from daily_watchlist table."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        c = conn.cursor()
        c.execute('SELECT ticker, company_name FROM daily_watchlist')
        watchlist = [(r[0], r[1] or r[0]) for r in c.fetchall()]
        conn.close()
        return watchlist
    except Exception as e:
        log.warning(f"Could not load watchlist: {e}")
        return []

def build_aliases(ticker: str, company_name: str) -> list[str]:
    """Generates a list of matching aliases for a given ticker and company name."""
    aliases = [ticker.lower()]
    if company_name and company_name != ticker:
        name = company_name.lower()
        # Clean common business suffixes
        name = re.sub(r'\b(inc|corp|co|corporation|incorporated|ltd|limited|holdings|plc|etf)\b\.?', '', name)
        # Clean punctuation
        name = re.sub(r'[\,\.\-\&]', ' ', name).strip()
        name_clean = " ".join(name.split())
        if name_clean:
            aliases.append(name_clean)
            # Match the first word if it's long enough
            words = name_clean.split()
            if len(words) > 1 and len(words[0]) > 3:
                aliases.append(words[0])
                
    # Retain specialized anchor mappings
    custom_overrides = {
        'AAPL': ['apple', 'aapl', 'iphone'],
        'TSLA': ['tesla', 'tsla', 'musk', 'cybercab'],
        'NVDA': ['nvidia', 'nvda', 'jensen', 'blackwell'],
        'MSFT': ['microsoft', 'msft', 'copilot', 'azure'],
        'AMZN': ['amazon', 'amzn', 'aws'],
        'GOOGL': ['google', 'alphabet', 'googl', 'goog'],
        'META': ['meta', 'facebook', 'zuckerberg']
    }
    if ticker in custom_overrides:
        aliases.extend(custom_overrides[ticker])
        
    return list(set(aliases))

def match_tickers(title: str, summary: str) -> list[str]:
    """
    Scans the text for aliases of watchlist companies.
    Returns a list of matching ticker symbols.
    """
    text = (title + " " + summary).lower()
    matched = []
    
    watchlist = get_watchlist()
    if not watchlist:
        return []
        
    for ticker, company_name in watchlist:
        aliases = build_aliases(ticker, company_name)
        for alias in aliases:
            # Check for word boundaries to avoid false positives (e.g. 'mrvl' inside 'marvelous')
            pattern = rf'\b{re.escape(alias)}\b'
            if re.search(pattern, text):
                matched.append(ticker)
                break
                
    return list(set(matched))

def compute_consensus(ticker: str, new_score: float, source: str) -> float:
    """
    Combines new score with recent scores for the same ticker within 10 minutes.
    If multiple independent feeds report positive news, it boosts the confidence.
    """
    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(minutes=10)
    
    # Initialize history list
    if ticker not in _recent_alerts:
        _recent_alerts[ticker] = []
        
    # Filter out alerts older than 10 minutes
    _recent_alerts[ticker] = [
        (t, s, src) for t, s, src in _recent_alerts[ticker] if t > cutoff
    ]
    
    # Append the new alert
    _recent_alerts[ticker].append((now, new_score, source))
    
    # Count unique sources in the window
    unique_sources = set(src for _, _, src in _recent_alerts[ticker])
    
    # Calculate simple average score
    scores = [s for _, s, _ in _recent_alerts[ticker]]
    avg_score = sum(scores) / len(scores)
    
    # Apply a multi-source consensus multiplier
    # 2 sources = 10% boost, 3+ sources = 20% boost (capped at absolute 1.0)
    multiplier = 1.0
    if len(unique_sources) == 2:
        multiplier = 1.10
    elif len(unique_sources) >= 3:
        multiplier = 1.20
        
    final_score = avg_score * multiplier
    # Clamp to [-1.0, 1.0]
    return max(-1.0, min(1.0, final_score))

def on_news_arrived(event: Event):
    """Event handler triggered when Topics.NEWS_ARRIVED is published."""
    if _sia is None:
        log.warning("VADER is not initialized. News skipped.")
        return
        
    data = event.data
    title = data.get("title", "")
    summary = data.get("summary", "") or ""
    url = data.get("url", "")
    source = event.source
    
    # Allow manual override for associated ticker if specified in ingestion payload
    associated_ticker = data.get("associated_ticker", None)
    if associated_ticker:
        tickers = [associated_ticker]
    else:
        tickers = match_tickers(title, summary)
        
    if not tickers:
        return
        
    # Analyze sentiment
    text_to_score = f"{title}. {summary}"
    scores = _sia.polarity_scores(text_to_score)
    compound_score = scores['compound']
    
    # We only care about high-sentiment breaking news
    # Neutral news (between -0.3 and 0.3) is ignored to filter noise
    if abs(compound_score) < 0.3:
        return
        
    bus = get_bus()
    for ticker in tickers:
        # Compute consensus score
        consensus_score = compute_consensus(ticker, compound_score, source)
        
        log.info(f"SENTIMENT ALERT: {ticker} | Score: {compound_score:+.2f} (Consensus: {consensus_score:+.2f}) | Headline: '{title[:60]}...'")
        
        bus.publish(Topics.NEWS_SENTIMENT_ALERT, Event(
            source="sentiment_engine",
            data={
                "ticker": ticker,
                "sentiment_score": consensus_score,
                "raw_score": compound_score,
                "headline": title,
                "url": url,
                "source": source
            }
        ))

def run_sentiment_engine():
    """Starts the sentiment listener on the event bus."""
    log.info("Starting Sentiment Consensus Engine...")
    bus = get_bus()
    bus.subscribe(Topics.NEWS_ARRIVED, on_news_arrived)
