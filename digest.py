"""
Daily Market Digest — Professional Hedge Fund Style Morning Brief
=================================================================
High signal. Zero noise.
Covers: Global Macro, Geopolitics (market-relevant), India Policy,
        Capital Markets + Listed Companies (merged), Commodities,
        Investor Podcasts & Interviews.
"""

import os
import re
import time
import logging
import requests
import feedparser
from datetime import datetime, timedelta
from collections import defaultdict
import pytz

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")

IST = pytz.timezone("Asia/Kolkata")
MAX_MSG_LEN = 4096
HOURS_BACK = 24  # fetch news from last 24 hours

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── NOISE FILTERS ─────────────────────────────────────────────────────────────
# Words that indicate low-signal noise — any headline containing these is dropped
EXCLUDE_KEYWORDS = [
    # Crime / violence
    "shooting", "murder", "killed", "stabbed", "rape", "robbery", "kidnap",
    "arrested", "crime", "criminal", "massacre", "blast", "bomb", "terror attack",
    "riot", "mob", "lynching", "suicide bomber",
    # Celebrity / gossip
    "celebrity", "bollywood", "actor", "actress", "film star", "ipl score",
    "cricket score", "match highlights", "entertainment", "gossip",
    # Local noise
    "heatwave", "heat wave", "school holiday", "weather alert", "local body",
    "panchayat", "municipality", "village council", "ward councillor",
    "cow dung", "minor politician", "mla threatens", "bjp worker",
    # Trading noise
    "share price live", "stock price live", "nifty live", "sensex live",
    "technical analysis", "support level", "resistance level", "buy call",
    "sell call", "target price today", "trading call", "intraday",
    "multibagger tip", "stock tip",
    # Generic business fluff
    "lowe's bets", "gen z shopping", "how to make money", "get rich",
    "personal finance tips", "credit card tips",
]

# Words that signal HIGH value geopolitical content
GEO_INCLUDE_KEYWORDS = [
    "oil", "crude", "opec", "gas", "lng", "sanctions", "trade war", "tariff",
    "strait of hormuz", "supply chain", "inflation", "recession", "gdp",
    "federal reserve", "fed rate", "ecb", "central bank", "imf", "world bank",
    "nato", "ukraine", "russia", "china trade", "taiwan strait", "middle east",
    "israel", "iran", "commodity", "shipping", "freight", "port", "blockade",
    "dies", "death", "passed away", "central banker", "finance minister",
    "treasury secretary", "billionaire", "investor", "warren buffett",
    "ray dalio", "george soros", "charlie munger", "jerome powell",
    "election result", "coup", "regime change", "economic policy",
    "trade agreement", "wto", "g7", "g20", "brics", "economic sanctions",
]

# Words that signal HIGH value India policy content
INDIA_POLICY_INCLUDE = [
    "rbi", "sebi", "lok sabha", "rajya sabha", "parliament", "budget",
    "ministry", "minister", "pib", "press information bureau",
    "gst", "income tax", "customs duty", "fiscal", "monetary policy",
    "repo rate", "inflation data", "cpi", "wpi", "iip", "gdp data",
    "core sector", "pli scheme", "production linked", "infrastructure",
    "nirmala sitharaman", "rbi governor", "shaktikanta das",
    "sanjay malhotra", "finance ministry", "economic survey",
    "fdi", "foreign investment", "disinvestment", "privatisation",
    "capex", "public sector", "government scheme", "policy announcement",
    "regulation", "ordinance", "bill passed", "amendment",
    "trade deficit", "current account", "forex reserve", "rupee",
    "election commission", "election result", "exit poll",
]

# Words that signal HIGH value capital markets content
CAPITAL_MARKETS_INCLUDE = [
    "acquisition", "merger", "qip", "rights issue", "ipo", "fpo",
    "promoter", "stake sale", "stake purchase", "open offer",
    "board approves", "board meeting", "agm", "egm",
    "capex", "fundraise", "ncd", "debenture", "bond issue",
    "management change", "ceo", "md", "chairman", "appointed",
    "regulatory approval", "sebi approval", "nclt", "cci",
    "quarterly result", "q1", "q2", "q3", "q4", "earnings",
    "revenue", "profit", "ebitda", "margin", "guidance",
    "dividend", "buyback", "bonus share", "split",
    "new index", "index inclusion", "index exclusion",
    "52-week high", "52-week low", "circuit breaker",
    "fii", "dii", "institutional buying", "block deal", "bulk deal",
    "plant", "factory", "expansion", "new order", "contract won",
    "joint venture", "partnership", "collaboration",
    "product launch", "distribution", "export order",
]

# ── RSS FEEDS ─────────────────────────────────────────────────────────────────
RSS_FEEDS = {
    "🌍 Global Markets & Macro": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://www.wsj.com/xml/rss/3_7085.xml",
        "https://feeds.marketwatch.com/marketwatch/marketpulse/",
        "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        "https://www.ft.com/rss/home",
    ],
    "🌐 Geopolitical (Market-Relevant)": [
        "https://feeds.reuters.com/Reuters/worldNews",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://foreignpolicy.com/feed/",
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    ],
    "🇮🇳 India Policy & Economy": [
        "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3",  # PIB RSS
        "https://www.rbi.org.in/scripts/rss.aspx",                 # RBI
        "https://feeds.feedburner.com/ndtvnews-india-news",
        "https://economictimes.indiatimes.com/news/economy/policy/rssfeeds/1373380680.cms",
        "https://www.business-standard.com/rss/economy-policy-101.rss",
        "https://www.livemint.com/rss/economy",
        "https://www.thehindu.com/business/Economy/feeder/default.rss",
    ],
    "📈 Capital Markets & Listed Companies": [
        "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.business-standard.com/rss/markets-106.rss",
        "https://www.business-standard.com/rss/companies-101.rss",
        "https://www.livemint.com/rss/markets",
        "https://www.livemint.com/rss/companies",
        "https://www.moneycontrol.com/rss/MCtopnews.xml",
        "https://www.bseindia.com/xml-data/corpfiling/AttachLive/ATTHS.xml",
        "https://www.sebi.gov.in/sebi_data/rss/sebirss.xml",      # SEBI
    ],
    "🛢️ Commodities & Supply Chain": [
        "https://feeds.reuters.com/reuters/commoditiesNews",
        "https://oilprice.com/rss/main",
        "https://www.agrimoney.com/rss.xml",
        "https://feeds.bloomberg.com/energy-and-oil/news.rss",
    ],
}

# ── YOUTUBE CHANNELS ──────────────────────────────────────────────────────────
YOUTUBE_CHANNELS = [
    {"id": "UCWX3yGbODI3FLCZpFHrJYJA", "name": "Nikhil Kamath"},
    {"id": "UCHqCBFoAtRB8NRQKG5fO3eA", "name": "ET Now"},
    {"id": "UCvJJ_dzjViJCoLf5uKUTwoA", "name": "CNBC-TV18"},
    {"id": "UCMtJYS0PrtiUwlk6zjGDEMA", "name": "Bloomberg Markets"},
    {"id": "UCVvVZlKFmHwxZXhCnfm8JNw", "name": "Marcellus Investment"},
    {"id": "UCl4T8VYJKVkMFBOhWWI4f3Q", "name": "Capitalmind"},
    {"id": "UCCExUQXB12wJH4w1sUAVyQQ", "name": "CNBC Television"},
    {"id": "UCGdVL21GEF3WqS1_5QxIq4Q", "name": "Zerodha Varsity"},
]

# ── PODCAST FEEDS ─────────────────────────────────────────────────────────────
PODCAST_FEEDS = [
    {"url": "https://rss.art19.com/invest-like-the-best", "name": "Invest Like The Best"},
    {"url": "https://rss.art19.com/acquired", "name": "Acquired"},
    {"url": "https://feeds.simplecast.com/0lxBYiKU", "name": "We Study Billionaires"},
    {"url": "https://marcellusinvestment.com/feed/podcast/", "name": "Marcellus Podcast"},
    {"url": "https://feeds.transistor.fm/capitalmind-premium-podcast", "name": "Capitalmind Podcast"},
]

# Podcast / YouTube noise exclusions
MEDIA_EXCLUDE = [
    "kevin warsh", "lowe's", "gen z", "how to make money",
    "top 10", "viral", "shorts", "reels", "compilation",
]


# ── FILTER FUNCTIONS ──────────────────────────────────────────────────────────

def is_noise(title: str) -> bool:
    """Return True if the headline should be excluded as noise."""
    t = title.lower()
    return any(kw in t for kw in EXCLUDE_KEYWORDS)


def is_relevant_geo(title: str) -> bool:
    """Return True if geopolitical headline is market-relevant."""
    t = title.lower()
    return any(kw in t for kw in GEO_INCLUDE_KEYWORDS)


def is_relevant_india_policy(title: str) -> bool:
    """Return True if India headline is policy/economy relevant."""
    t = title.lower()
    return any(kw in t for kw in INDIA_POLICY_INCLUDE)


def is_relevant_markets(title: str) -> bool:
    """Return True if markets headline is substantive (not trading noise)."""
    t = title.lower()
    return any(kw in t for kw in CAPITAL_MARKETS_INCLUDE)


def is_relevant_media(title: str) -> bool:
    """Return True if YouTube/podcast title is investor-grade."""
    t = title.lower()
    return not any(kw in t for kw in MEDIA_EXCLUDE)


def deduplicate(items: list) -> list:
    """Remove items with very similar titles (fuzzy dedup by first 60 chars)."""
    seen = set()
    result = []
    for item in items:
        key = re.sub(r'\s+', ' ', item["title"][:65].lower().strip())
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


# ── FETCH FUNCTIONS ───────────────────────────────────────────────────────────

def fetch_rss(url: str, hours_back: int = HOURS_BACK) -> list:
    """Fetch an RSS feed and return items from the last N hours."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; MarketDigestBot/2.0)"}
        raw = requests.get(url, timeout=12, headers=headers)
        feed = feedparser.parse(raw.content)
        cutoff = datetime.now(IST) - timedelta(hours=hours_back)
        items = []
        for entry in feed.entries:
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            if not title or not link:
                continue
            # Parse date
            published = None
            for field in ["published_parsed", "updated_parsed"]:
                val = getattr(entry, field, None)
                if val:
                    import calendar
                    published = datetime.fromtimestamp(calendar.timegm(val), tz=IST)
                    break
            if published and published < cutoff:
                continue
            items.append({"title": title, "url": link, "published": published})
        return items
    except Exception as e:
        log.warning(f"RSS failed [{url}]: {e}")
        return []


def fetch_section(feeds: list, filter_fn=None, hours_back: int = HOURS_BACK) -> list:
    """Fetch all feeds for a section, apply filter, deduplicate."""
    all_items = []
    seen_urls = set()
    for url in feeds:
        items = fetch_rss(url, hours_back)
        for item in items:
            if item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                all_items.append(item)
    # Apply relevance filter
    if filter_fn:
        all_items = [i for i in all_items if not is_noise(i["title"]) and filter_fn(i["title"])]
    else:
        all_items = [i for i in all_items if not is_noise(i["title"])]
    return deduplicate(all_items)


def fetch_youtube(channel_id: str, channel_name: str, hours_back: int = 48) -> list:
    """Fetch recent videos from a YouTube channel."""
    if not YOUTUBE_API_KEY:
        return []
    try:
        since = (datetime.utcnow() - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        url = (
            f"https://www.googleapis.com/youtube/v3/search"
            f"?part=snippet&channelId={channel_id}&type=video"
            f"&publishedAfter={since}&maxResults=3&order=date"
            f"&key={YOUTUBE_API_KEY}"
        )
        resp = requests.get(url, timeout=10)
        data = resp.json()
        videos = []
        for item in data.get("items", []):
            vid_id = item["id"].get("videoId")
            title = item["snippet"].get("title", "")
            if vid_id and title and is_relevant_media(title):
                videos.append({
                    "title": f"{title} [{channel_name}]",
                    "url": f"https://youtube.com/watch?v={vid_id}",
                })
        return videos
    except Exception as e:
        log.warning(f"YouTube failed [{channel_name}]: {e}")
        return []


def fetch_podcasts(hours_back: int = 72) -> list:
    """Fetch latest podcast episodes."""
    results = []
    for pod in PODCAST_FEEDS:
        try:
            feed = feedparser.parse(pod["url"])
            if feed.entries:
                ep = feed.entries[0]
                title = ep.get("title", "")
                if title and is_relevant_media(title):
                    results.append({
                        "title": f"{title} [{pod['name']}]",
                        "url": ep.get("link", ""),
                    })
        except Exception as e:
            log.warning(f"Podcast failed [{pod['name']}]: {e}")
    return results


# ── TELEGRAM FUNCTIONS ────────────────────────────────────────────────────────

def send_telegram(text: str) -> bool:
    """Send a message to the Telegram channel."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if not resp.ok:
            log.error(f"Telegram error {resp.status_code}: {resp.text}")
            return False
        return True
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False


def split_and_send(text: str):
    """Split a message if it exceeds Telegram's limit and send all chunks."""
    if len(text) <= MAX_MSG_LEN:
        send_telegram(text)
        time.sleep(1.2)
        return
    lines = text.split("\n")
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > MAX_MSG_LEN:
            send_telegram(chunk.strip())
            time.sleep(1.2)
            chunk = line + "\n"
        else:
            chunk += line + "\n"
    if chunk.strip():
        send_telegram(chunk.strip())
        time.sleep(1.2)


# ── GLOBAL DEDUP ACROSS ALL SECTIONS ─────────────────────────────────────────

_global_seen_titles = set()

def global_dedup(items: list) -> list:
    """Remove items already seen in a previous section."""
    global _global_seen_titles
    result = []
    for item in items:
        key = re.sub(r'\s+', ' ', item["title"][:65].lower().strip())
        if key not in _global_seen_titles:
            _global_seen_titles.add(key)
            result.append(item)
    return result


# ── BUILD AND SEND DIGEST ─────────────────────────────────────────────────────

def build_and_send_digest():
    """Fetch all content, filter, deduplicate, and send to Telegram."""
    global _global_seen_titles
    _global_seen_titles = set()  # reset for each run

    now = datetime.now(IST)
    log.info("Building digest...")

    # ── Header ──
    send_telegram(
        f"<b>📊 Daily Market Digest</b>\n"
        f"<i>{now.strftime('%A, %d %B %Y  •  %I:%M %p IST')}</i>\n"
        f"{'─' * 32}\n"
        f"<i>High signal. Zero noise.</i>"
    )
    time.sleep(1.2)

    # ── Section 1: Global Markets & Macro (no filter — all macro is relevant) ──
    items = fetch_section(RSS_FEEDS["🌍 Global Markets & Macro"])
    items = global_dedup(items)
    if items:
        block = "<b>🌍 Global Markets &amp; Macro</b>\n"
        for i, item in enumerate(items, 1):
            block += f"{i}. <a href='{item['url']}'>{item['title'][:130]}</a>\n"
        split_and_send(block)

    # ── Section 2: Geopolitical (market-relevant only) ──
    items = fetch_section(RSS_FEEDS["🌐 Geopolitical (Market-Relevant)"], filter_fn=is_relevant_geo)
    items = global_dedup(items)
    if items:
        block = "<b>🌐 Geopolitical (Market-Relevant)</b>\n"
        for i, item in enumerate(items, 1):
            block += f"{i}. <a href='{item['url']}'>{item['title'][:130]}</a>\n"
        split_and_send(block)

    # ── Section 3: India Policy & Economy ──
    items = fetch_section(RSS_FEEDS["🇮🇳 India Policy & Economy"], filter_fn=is_relevant_india_policy)
    items = global_dedup(items)
    if items:
        block = "<b>🇮🇳 India Policy &amp; Economy</b>\n"
        for i, item in enumerate(items, 1):
            block += f"{i}. <a href='{item['url']}'>{item['title'][:130]}</a>\n"
        split_and_send(block)

    # ── Section 4: Capital Markets & Listed Companies (merged, priority) ──
    items = fetch_section(RSS_FEEDS["📈 Capital Markets & Listed Companies"], filter_fn=is_relevant_markets)
    items = global_dedup(items)
    if items:
        block = "⚡⚡⚡ <b>PRIORITY</b> ⚡⚡⚡\n<b>📈 Capital Markets &amp; Listed Companies</b>\n"
        for i, item in enumerate(items, 1):
            block += f"{i}. <a href='{item['url']}'>{item['title'][:130]}</a>\n"
        split_and_send(block)

    # ── Section 5: Commodities & Supply Chain ──
    items = fetch_section(RSS_FEEDS["🛢️ Commodities & Supply Chain"])
    items = global_dedup(items)
    if items:
        block = "<b>🛢️ Commodities &amp; Supply Chain</b>\n"
        for i, item in enumerate(items, 1):
            block += f"{i}. <a href='{item['url']}'>{item['title'][:130]}</a>\n"
        split_and_send(block)

    # ── Section 6: YouTube ──
    yt_items = []
    for ch in YOUTUBE_CHANNELS:
        yt_items.extend(fetch_youtube(ch["id"], ch["name"]))
    yt_items = deduplicate(yt_items)
    if yt_items:
        block = "<b>▶️ Fund Manager &amp; Investor Videos</b>\n"
        for i, v in enumerate(yt_items, 1):
            block += f"{i}. <a href='{v['url']}'>{v['title'][:120]}</a>\n"
        split_and_send(block)

    # ── Section 7: Podcasts ──
    pod_items = fetch_podcasts()
    if pod_items:
        block = "<b>🎙️ Investor Podcasts</b>\n"
        for i, p in enumerate(pod_items, 1):
            block += f"{i}. <a href='{p['url']}'>{p['title'][:120]}</a>\n"
        split_and_send(block)

    # ── Footer ──
    send_telegram(
        f"<i>Digest complete • {now.strftime('%I:%M %p IST')}\n"
        f"Send /news anytime for an on-demand update.</i>"
    )
    log.info("Digest sent successfully.")


# ── ON-DEMAND TELEGRAM BOT LISTENER ──────────────────────────────────────────

def run_bot_listener():
    """
    Poll Telegram for /news or 'news' commands and respond with a fresh digest.
    Runs continuously — used when deployed as a long-running process.
    For GitHub Actions (scheduled), this is NOT used — main() is used instead.
    """
    log.info("Starting on-demand bot listener...")
    offset = None
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {"timeout": 30, "allowed_updates": ["message"]}
            if offset:
                params["offset"] = offset
            resp = requests.get(url, params=params, timeout=40)
            data = resp.json()
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                text = msg.get("text", "").strip().lower()
                chat_id = msg.get("chat", {}).get("id")
                if text in ["/news", "news", "/digest", "digest"]:
                    log.info(f"On-demand request from chat {chat_id}")
                    send_telegram("⏳ Fetching latest market digest, please wait...")
                    time.sleep(1)
                    build_and_send_digest()
        except Exception as e:
            log.error(f"Bot listener error: {e}")
            time.sleep(10)


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

def main():
    """
    Default mode: build and send digest once.
    Used by GitHub Actions scheduled workflow.
    """
    build_and_send_digest()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--listen":
        # Run as persistent bot: python digest.py --listen
        run_bot_listener()
    else:
        # Run once (GitHub Actions / cron)
        main()
