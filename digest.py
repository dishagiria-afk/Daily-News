import os
import time
import logging
import requests
import feedparser
from datetime import datetime, timedelta
import pytz

# ── YOUR SETTINGS (loaded from GitHub Secrets) ──────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")

IST = pytz.timezone("Asia/Kolkata")
MAX_ITEMS = 5
MAX_MSG_LEN = 4096

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── RSS NEWS FEEDS ───────────────────────────────────────────────────────────
RSS_FEEDS = {
    "🌍 Global Markets & Macro": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.marketwatch.com/marketwatch/marketpulse/",
        "https://www.wsj.com/xml/rss/3_7085.xml",
    ],
    "🌐 Geopolitical": [
        "https://feeds.reuters.com/Reuters/worldNews",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://foreignpolicy.com/feed/",
    ],
    "🇮🇳 India Political & Economy": [
        "https://feeds.feedburner.com/ndtvnews-india-news",
        "https://economictimes.indiatimes.com/rssfeedsdefault.cms",
        "https://www.business-standard.com/rss/home_page_top_stories.rss",
    ],
    "📈 India Capital Markets": [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.business-standard.com/rss/markets-106.rss",
        "https://www.moneycontrol.com/rss/MCtopnews.xml",
    ],
    "🏢 Listed Companies BSE/NSE ⚡": [
        "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
        "https://www.livemint.com/rss/companies",
        "https://www.business-standard.com/rss/companies-101.rss",
        "https://www.bseindia.com/xml-data/corpfiling/AttachLive/ATTHS.xml",
    ],
}

# ── YOUTUBE CHANNELS ─────────────────────────────────────────────────────────
YOUTUBE_CHANNELS = [
    {"id": "UCVvVZlKFmHwxZXhCnfm8JNw", "name": "Marcellus Investment"},
    {"id": "UCl4T8VYJKVkMFBOhWWI4f3Q", "name": "Capitalmind"},
    {"id": "UCHqCBFoAtRB8NRQKG5fO3eA", "name": "ET Now"},
    {"id": "UCvJJ_dzjViJCoLf5uKUTwoA", "name": "CNBC-TV18"},
    {"id": "UCMtJYS0PrtiUwlk6zjGDEMA", "name": "Bloomberg Markets"},
    {"id": "UCCExUQXB12wJH4w1sUAVyQQ", "name": "CNBC Television"},
    {"id": "UCGdVL21GEF3WqS1_5QxIq4Q", "name": "Zerodha Varsity"},
    {"id": "UCWX3yGbODI3FLCZpFHrJYJA", "name": "Nikhil Kamath"},
]

# ── PODCAST FEEDS ─────────────────────────────────────────────────────────────
PODCAST_FEEDS = [
    {"url": "https://feeds.simplecast.com/0lxBYiKU", "name": "We Study Billionaires"},
    {"url": "https://rss.art19.com/invest-like-the-best", "name": "Invest Like The Best"},
    {"url": "https://rss.art19.com/acquired", "name": "Acquired"},
    {"url": "https://marcellusinvestment.com/feed/podcast/", "name": "Marcellus Podcast"},
    {"url": "https://feeds.transistor.fm/capitalmind-premium-podcast", "name": "Capitalmind Podcast"},
]


# ── FUNCTION: Fetch RSS news ──────────────────────────────────────────────────
def fetch_rss(url):
    try:
        feed = feedparser.parse(url)
        items = []
        cutoff = datetime.now(IST) - timedelta(hours=24)
        for entry in feed.entries[:15]:
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            published = None
            for field in ["published_parsed", "updated_parsed"]:
                val = getattr(entry, field, None)
                if val:
                    import calendar
                    published = datetime.fromtimestamp(calendar.timegm(val), tz=IST)
                    break
            if published and published < cutoff:
                continue
            if title and link:
                items.append({"title": title, "url": link})
        return items[:MAX_ITEMS]
    except Exception as e:
        log.warning(f"RSS failed {url}: {e}")
        return []


# ── FUNCTION: Fetch YouTube videos ───────────────────────────────────────────
def fetch_youtube(channel_id, channel_name):
    if not YOUTUBE_API_KEY:
        return []
    try:
        since = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%SZ")
        url = (
            f"https://www.googleapis.com/youtube/v3/search"
            f"?part=snippet&channelId={channel_id}&type=video"
            f"&publishedAfter={since}&maxResults=2&order=date"
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
        log.warning(f"YouTube failed {channel_name}: {e}")
        return []


# ── FUNCTION: Fetch podcasts ──────────────────────────────────────────────────
def fetch_podcasts():
    results = []
    for pod in PODCAST_FEEDS:
        try:
            feed = feedparser.parse(pod["url"])
            if feed.entries:
                ep = feed.entries[0]
                results.append({
                    "title": f"{ep.get('title', 'Episode')} [{pod['name']}]",
                    "url": ep.get("link", ""),
                })
        except Exception as e:
            log.warning(f"Podcast failed {pod['name']}: {e}")
    return results[:5]


# ── FUNCTION: Send message to Telegram ───────────────────────────────────────
def send_telegram(text):
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
            log.error(f"Telegram error: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False


# ── FUNCTION: Split long messages ────────────────────────────────────────────
def split_message(text):
    if len(text) <= MAX_MSG_LEN:
        return [text]
    chunks = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > MAX_MSG_LEN:
            chunks.append(current.strip())
            current = line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        chunks.append(current.strip())
    return chunks


# ── MAIN: Build and send digest ───────────────────────────────────────────────
def main():
    log.info("Starting daily digest...")
    now = datetime.now(IST)
    all_messages = []

    # Header
    all_messages.append(
        f"<b>📊 Daily Market Digest</b>\n"
        f"<i>{now.strftime('%A, %d %B %Y  •  %I:%M %p IST')}</i>\n"
        f"{'─' * 32}"
    )

    # News sections
    for section, feeds in RSS_FEEDS.items():
        items = []
        for feed_url in feeds:
            items.extend(fetch_rss(feed_url))
            if len(items) >= MAX_ITEMS:
                break
        items = items[:MAX_ITEMS]
        if not items:
            continue
        block = f"\n<b>{section}</b>\n"
        for i, item in enumerate(items, 1):
            title = item["title"][:120]
            block += f"{i}. <a href='{item['url']}'>{title}</a>\n"
        for chunk in split_message(block):
            all_messages.append(chunk)

    # YouTube
    yt_items = []
    for ch in YOUTUBE_CHANNELS:
        yt_items.extend(fetch_youtube(ch["id"], ch["name"]))
        if len(yt_items) >= 8:
            break
    if yt_items:
        block = "\n<b>▶️ YouTube — Fund Managers &amp; Markets</b>\n"
        for i, v in enumerate(yt_items[:8], 1):
            block += f"{i}. <a href='{v['url']}'>{v['title'][:100]}</a>\n"
        all_messages.append(block)

    # Podcasts
    pod_items = fetch_podcasts()
    if pod_items:
        block = "\n<b>🎙️ Latest Podcast Episodes</b>\n"
        for i, p in enumerate(pod_items, 1):
            block += f"{i}. <a href='{p['url']}'>{p['title'][:100]}</a>\n"
        all_messages.append(block)

    # Footer
    all_messages.append(
        f"\n<i>Digest sent at {now.strftime('%I:%M %p IST')} • See you tomorrow 🙏</i>"
    )

    # Send all messages
    log.info(f"Built {len(all_messages)} blocks. Sending to Telegram...")
    for i, msg in enumerate(all_messages):
        if msg.strip():
            ok = send_telegram(msg)
            if not ok:
                log.error(f"Failed on block {i+1}")
            time.sleep(1.2)

    log.info("Done.")


if __name__ == "__main__":
    main()
