"""
Daily Market & Capital Markets Digest → Telegram
=================================================
Fetches: Global markets news, geopolitical news, India political/markets news,
         Listed company news (BSE/NSE), YouTube/podcast content from fund managers.
Sends formatted digest to your Telegram channel.
"""

import os
import json
import time
import logging
import requests
import feedparser
from datetime import datetime, timedelta
from typing import Optional
import pytz

# ─── CONFIG ────────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "@your_channel_id")  # e.g. @mychannel or -100xxxxxxxxxx
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "YOUR_YOUTUBE_API_KEY")
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")  # Optional - newsapi.org free tier

IST = pytz.timezone("Asia/Kolkata")
MAX_ITEMS_PER_SECTION = 5
MAX_MESSAGE_LENGTH = 4096  # Telegram limit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── RSS FEEDS ─────────────────────────────────────────────────────────────────
RSS_FEEDS = {
    "🌍 Global Markets & Macro": [
        "https://feeds.bloomberg.com/markets/news.rss",
        "https://www.ft.com/rss/home/uk",
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.marketwatch.com/marketwatch/marketpulse/",
        "https://www.wsj.com/xml/rss/3_7085.xml",  # WSJ Markets
    ],
    "🌐 Geopolitical": [
        "https://feeds.reuters.com/Reuters/worldNews",
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://foreignpolicy.com/feed/",
    ],
    "🇮🇳 India Political & Economy": [
        "https://feeds.feedburner.com/ndtvnews-india-news",
        "https://www.thehindu.com/business/Economy/feeder/default.rss",
        "https://economictimes.indiatimes.com/rssfeedsdefault.cms",
        "https://www.livemint.com/rss/economy",
        "https://www.business-standard.com/rss/home_page_top_stories.rss",
    ],
    "📈 India Capital Markets": [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.livemint.com/rss/markets",
        "https://www.business-standard.com/rss/markets-106.rss",
        "https://feeds.feedburner.com/moneycontrol-latest",
        "https://www.nseindia.com/content/RSS/latest_announcements.xml",
    ],
    "🏢 Listed Companies (BSE/NSE)": [
        "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",  # ET Stocks
        "https://www.livemint.com/rss/companies",
        "https://www.business-standard.com/rss/companies-101.rss",
        "https://www.moneycontrol.com/rss/MCtopnews.xml",
        "https://www.bseindia.com/xml-data/corpfiling/AttachLive/ATTHS.xml",  # BSE corporate filings
        "https://smallcases.com/blog/feed/",
    ],
}

# ─── NEWSAPI TOPICS (fallback / supplement when RSS is blocked) ────────────────
# Free tier: 100 req/day at newsapi.org
NEWSAPI_QUERIES = [
    {"q": "India stock market NSE BSE", "section": "🏢 Listed Companies (NewsAPI)"},
    {"q": "India economy RBI inflation", "section": "🇮🇳 India Economy (NewsAPI)"},
    {"q": "global markets Fed interest rates", "section": "🌍 Global Markets (NewsAPI)"},
]

# ─── YOUTUBE CHANNELS (Fund managers, Capital Markets) ─────────────────────────
YOUTUBE_CHANNELS = [
    # Indian Fund Managers & Market Experts
    {"id": "UCVvVZlKFmHwxZXhCnfm8JNw", "name": "Marcellus Investment"},
    {"id": "UCl4T8VYJKVkMFBOhWWI4f3Q", "name": "Capitalmind"},
    {"id": "UCkT_O1OhXFoF31sPkN0QKIA", "name": "Basant Maheshwari"},
    {"id": "UCWlUhZea7DRb1dZkv-0g-6Q", "name": "Saurabh Mukherjea"},
    {"id": "UC2KB_tFdJHVwcJfXxFWLyHg", "name": "Prashant Jain / HDFC MF Insights"},
    # Global
    {"id": "UCMtJYS0PrtiUwlk6zjGDEMA", "name": "Bloomberg Markets"},
    {"id": "UCCExUQXB12wJH4w1sUAVyQQ", "name": "CNBC Television"},
    {"id": "UCvJJ_dzjViJCoLf5uKUTwoA", "name": "CNBC-TV18"},
    {"id": "UCHqCBFoAtRB8NRQKG5fO3eA", "name": "ET Now"},
    {"id": "UCGdVL21GEF3WqS1_5QxIq4Q", "name": "Zerodha / Varsity"},
    {"id": "UCWX3yGbODI3FLCZpFHrJYJA", "name": "Nikhil Kamath"},
]

# ─── PODCAST RSS ────────────────────────────────────────────────────────────────
PODCAST_FEEDS = [
    {"url": "https://feeds.simplecast.com/0lxBYiKU", "name": "We Study Billionaires"},
    {"url": "https://rss.art19.com/the-knowledge-project", "name": "The Knowledge Project"},
    {"url": "https://feeds.buzzsprout.com/1817535.rss", "name": "Paisa Vaisa"},
    {"url": "https://www.ivmpodcasts.com/itunes-rss/paisa-vaisa", "name": "Paisa Vaisa (IVM)"},
    {"url": "https://feeds.transistor.fm/capitalmind-premium-podcast", "name": "Capitalmind Podcast"},
    {"url": "https://marcellusinvestment.com/feed/podcast/", "name": "Marcellus Podcast"},
    {"url": "https://rss.art19.com/acquired", "name": "Acquired Podcast"},
    {"url": "https://rss.art19.com/invest-like-the-best", "name": "Invest Like The Best"},
]

# ─── HELPERS ────────────────────────────────────────────────────────────────────

def fetch_rss(url: str, hours_back: int = 24) -> list[dict]:
    """Fetch RSS feed, return items from last N hours."""
    try:
        feed = feedparser.parse(url)
        cutoff = datetime.now(IST) - timedelta(hours=hours_back)
        items = []
        for entry in feed.entries[:15]:
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            # Try to parse published date
            published = None
            for field in ["published_parsed", "updated_parsed"]:
                if hasattr(entry, field) and getattr(entry, field):
                    import calendar
                    ts = calendar.timegm(getattr(entry, field))
                    published = datetime.fromtimestamp(ts, tz=IST)
                    break
            if published and published < cutoff:
                continue
            if title and link:
                items.append({"title": title, "url": link, "published": published})
        return items[:MAX_ITEMS_PER_SECTION]
    except Exception as e:
        log.warning(f"RSS fetch failed for {url}: {e}")
        return []


def fetch_youtube_recent(channel_id: str, channel_name: str, hours_back: int = 48) -> list[dict]:
    """Fetch recent YouTube videos from a channel using YouTube Data API."""
    if not YOUTUBE_API_KEY or YOUTUBE_API_KEY == "YOUR_YOUTUBE_API_KEY":
        return []
    try:
        published_after = (datetime.utcnow() - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        url = (
            f"https://www.googleapis.com/youtube/v3/search"
            f"?part=snippet&channelId={channel_id}&type=video"
            f"&publishedAfter={published_after}&maxResults=3&order=date"
            f"&key={YOUTUBE_API_KEY}"
        )
        resp = requests.get(url, timeout=10)
        data = resp.json()
        videos = []
        for item in data.get("items", []):
            vid_id = item["id"]["videoId"]
            title = item["snippet"]["title"]
            videos.append({
                "title": f"{title} [{channel_name}]",
                "url": f"https://youtube.com/watch?v={vid_id}",
            })
        return videos
    except Exception as e:
        log.warning(f"YouTube fetch failed for {channel_name}: {e}")
        return []


def fetch_podcasts(hours_back: int = 72) -> list[dict]:
    """Fetch recent podcast episodes."""
    results = []
    for feed_info in PODCAST_FEEDS:
        try:
            feed = feedparser.parse(feed_info["url"])
            if feed.entries:
                ep = feed.entries[0]
                results.append({
                    "title": f"{ep.get('title', 'Episode')} [{feed_info['name']}]",
                    "url": ep.get("link", ""),
                })
        except Exception as e:
            log.warning(f"Podcast fetch failed for {feed_info['name']}: {e}")
    return results[:5]


def fetch_newsapi(query: str, hours_back: int = 24) -> list[dict]:
    """Fetch from NewsAPI.org (free tier supplement)."""
    if not NEWS_API_KEY:
        return []
    try:
        from_time = (datetime.utcnow() - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%S")
        url = (
            f"https://newsapi.org/v2/everything?q={requests.utils.quote(query)}"
            f"&from={from_time}&sortBy=publishedAt&pageSize=5&language=en"
            f"&apiKey={NEWS_API_KEY}"
        )
        resp = requests.get(url, timeout=10)
        data = resp.json()
        items = []
        for a in data.get("articles", [])[:5]:
            if a.get("title") and a.get("url"):
                items.append({"title": a["title"], "url": a["url"]})
        return items
    except Exception as e:
        log.warning(f"NewsAPI failed for query '{query}': {e}")
        return []



def send_telegram(text: str, parse_mode: str = "HTML") -> bool:
    """Send a message to Telegram channel."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if not resp.ok:
            log.error(f"Telegram error: {resp.text}")
            return False
        return True
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False


def chunk_message(text: str) -> list[str]:
    """Split long messages at newlines to stay under Telegram's 4096 char limit."""
    if len(text) <= MAX_MESSAGE_LENGTH:
        return [text]
    chunks = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > MAX_MESSAGE_LENGTH:
            chunks.append(current.strip())
            current = line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        chunks.append(current.strip())
    return chunks


# ─── BUILD DIGEST ───────────────────────────────────────────────────────────────

def build_digest() -> list[str]:
    """Assemble the full digest and return as list of Telegram-ready message chunks."""
    now = datetime.now(IST)
    messages = []

    # ── Header ──
    header = (
        f"<b>📊 Daily Market Digest</b>\n"
        f"<i>{now.strftime('%A, %d %B %Y • %I:%M %p IST')}</i>\n"
        f"{'─' * 35}"
    )
    messages.append(header)
    time.sleep(0.5)

    # ── News Sections ──
    for section_name, feeds in RSS_FEEDS.items():
        items = []
        for feed_url in feeds:
            items.extend(fetch_rss(feed_url))
            if len(items) >= MAX_ITEMS_PER_SECTION:
                break
        items = items[:MAX_ITEMS_PER_SECTION]

        if not items:
            continue

        block = f"\n<b>{section_name}</b>\n"
        for i, item in enumerate(items, 1):
            title = item["title"][:120]  # truncate long titles
            block += f"{i}. <a href='{item['url']}'>{title}</a>\n"

        # Listed company news gets extra emphasis
        if "Listed" in section_name:
            block = f"\n{'⚡' * 5} PRIORITY {'⚡' * 5}\n" + block

        for chunk in chunk_message(block):
            messages.append(chunk)
        time.sleep(0.3)

    # ── NewsAPI Supplement (if key provided) ──
    if NEWS_API_KEY:
        for nq in NEWSAPI_QUERIES:
            items = fetch_newsapi(nq["q"])
            if items:
                block = f"\n<b>{nq['section']}</b>\n"
                for i, item in enumerate(items, 1):
                    block += f"{i}. <a href='{item['url']}'>{item['title'][:120]}</a>\n"
                messages.append(block)

    # ── YouTube ──
    yt_block = "\n<b>▶️ YouTube — Fund Managers & Markets</b>\n"
    yt_items = []
    for ch in YOUTUBE_CHANNELS:
        vids = fetch_youtube_recent(ch["id"], ch["name"])
        yt_items.extend(vids)
        if len(yt_items) >= 8:
            break

    if yt_items:
        for i, v in enumerate(yt_items[:8], 1):
            yt_block += f"{i}. <a href='{v['url']}'>{v['title'][:100]}</a>\n"
        messages.append(yt_block)

    # ── Podcasts ──
    pod_items = fetch_podcasts()
    if pod_items:
        pod_block = "\n<b>🎙️ Latest Podcast Episodes</b>\n"
        for i, p in enumerate(pod_items, 1):
            pod_block += f"{i}. <a href='{p['url']}'>{p['title'][:100]}</a>\n"
        messages.append(pod_block)

    # ── Footer ──
    messages.append(
        f"\n<i>Digest generated at {now.strftime('%I:%M %p IST')} • Next update tomorrow morning</i>"
    )

    return messages


# ─── MAIN ───────────────────────────────────────────────────────────────────────

def main():
    log.info("Starting daily digest...")
    messages = build_digest()
    log.info(f"Built {len(messages)} message blocks. Sending to Telegram...")

    for i, msg in enumerate(messages):
        if msg.strip():
            success = send_telegram(msg)
            if not success:
                log.error(f"Failed to send block {i+1}")
            time.sleep(1.2)  # Respect Telegram rate limits (30 msg/sec max)

    log.info("Digest sent successfully.")


if __name__ == "__main__":
    main()
