"""
Daily Market Digest — Indian Fund Manager Morning Brief
=======================================================
Order: India Policy → Capital Markets → Geopolitical → Global Macro → Commodities → Media
Philosophy: Free sources only. Deep dedup. Zero noise. High signal.
"""

import os
import re
import time
import logging
import requests
import feedparser
from datetime import datetime, timedelta
import pytz

# ── CONFIGURATION ──────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")
YOUTUBE_API_KEY     = os.environ.get("YOUTUBE_API_KEY", "")

IST         = pytz.timezone("Asia/Kolkata")
MAX_MSG_LEN = 4096
HOURS_BACK  = 24

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  RSS SOURCES
#  Rule: Only free-to-read sources. No NYT, no FT, no Bloomberg (paywalled).
#  Priority Indian sources: ET, Business Standard, Livemint, Moneycontrol,
#                           PIB, RBI, SEBI, Hindu BusinessLine
#  Priority global free sources: Reuters, BBC, Al Jazeera English
# ══════════════════════════════════════════════════════════════════════════════

RSS_SOURCES = {

    # ── India Policy & Economy ────────────────────────────────────────────────
    # Sources: PIB (official govt), RBI (official), SEBI (official),
    #          ET Economy, Business Standard Economy, Livemint Economy,
    #          Hindu BusinessLine Economy
    "india_policy": [
        "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3",
        "https://www.rbi.org.in/scripts/rss.aspx",
        "https://economictimes.indiatimes.com/news/economy/policy/rssfeeds/1373380680.cms",
        "https://economictimes.indiatimes.com/news/economy/indicators/rssfeeds/1377301124.cms",
        "https://www.business-standard.com/rss/economy-policy-101.rss",
        "https://www.livemint.com/rss/economy",
        "https://www.thehindubusinessline.com/economy/feeder/default.rss",
    ],

    # ── Capital Markets & Listed Companies ───────────────────────────────────
    # Sources: ET Markets, ET Stocks, BS Markets, BS Companies,
    #          Livemint Markets, Livemint Companies, Moneycontrol,
    #          BSE Filings, SEBI
    "capital_markets": [
        "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://economictimes.indiatimes.com/markets/ipo/rssfeeds/3911288.cms",
        "https://www.business-standard.com/rss/markets-106.rss",
        "https://www.business-standard.com/rss/companies-101.rss",
        "https://www.livemint.com/rss/markets",
        "https://www.livemint.com/rss/companies",
        "https://www.moneycontrol.com/rss/MCtopnews.xml",
        "https://www.bseindia.com/xml-data/corpfiling/AttachLive/ATTHS.xml",
        "https://www.sebi.gov.in/sebi_data/rss/sebirss.xml",
    ],

    # ── Geopolitical (market-relevant only) ──────────────────────────────────
    # Sources: Reuters World, BBC World, Al Jazeera, Hindu International
    # Note: NO NYT, NO FT — paywalled and India-irrelevant stories dominate
    "geopolitical": [
        "https://feeds.reuters.com/Reuters/worldNews",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://www.aljazeera.com/xml/rss/all.xml",
        "https://www.thehindu.com/news/international/feeder/default.rss",
    ],

    # ── Global Markets & Macro ────────────────────────────────────────────────
    # Sources: Reuters Business, Reuters Finance, MarketWatch, AP Business
    # Note: NO NYT, NO FT, NO Bloomberg — all paywalled
    "global_macro": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.reuters.com/reuters/UKdomesticNews",
        "https://feeds.marketwatch.com/marketwatch/marketpulse/",
        "https://feeds.reuters.com/reuters/companyNews",
        "https://apnews.com/rss/business",
    ],

    # ── Commodities & Supply Chain ────────────────────────────────────────────
    # Sources: Reuters Commodities, OilPrice.com, Agrimoney, Hindu BusinessLine Commodities
    "commodities": [
        "https://feeds.reuters.com/reuters/commoditiesNews",
        "https://oilprice.com/rss/main",
        "https://www.agrimoney.com/rss.xml",
        "https://www.thehindubusinessline.com/markets/commodities/feeder/default.rss",
        "https://economictimes.indiatimes.com/industry/indl-goods/svs/engineering/rssfeeds/14424.cms",
    ],
}


# ══════════════════════════════════════════════════════════════════════════════
#  NOISE EXCLUSION — Drop a headline if ANY of these phrases appear in it
# ══════════════════════════════════════════════════════════════════════════════

NOISE_PHRASES = [
    # Crime / violence (non-market)
    "shooting", "gunman", "stabbed", "stabbing", "rape", "sexual assault",
    "robbery", "robbed", "kidnap", "kidnapped", "arrested", "murder",
    "massacre", "riot", "mob violence", "lynching", "suicide bomber",
    "serial killer", "mass shooting", "hostage",

    # Purely social / cultural stories with no market angle
    "vandalism of jesus", "sledgehammered a statue", "beauty queen",
    "apartheid beauty", "miss world", "hiv success",
    "white house correspondents dinner", "correspondents' dinner",
    "the onion", "infowars",
    "peace rally gets derailed",
    "skies over el paso",
    "ceos are the heads of companies. should",
    "social media addiction go the way of cigarettes",
    "mississippi liquor stores",
    "paycheck feels smaller",
    "living abroad for lower costs",
    "moving back seems unaffordable",
    "zambia", "uganda", "zimbabwe",  # African stories with no India market link

    # Celebrity / entertainment
    "celebrity", "bollywood", "actor", "actress", "film star",
    "ipl score", "cricket score", "match result", "match highlights",
    "entertainment news", "gossip",

    # Local Indian noise
    "heatwave", "heat wave", "school holiday", "weather alert",
    "panchayat", "municipality", "village", "ward councillor",
    "cow dung", "mla threatens",

    # Pure trading noise (no business event behind it)
    "share price live", "stock price live", "nifty live", "sensex live",
    "intraday call", "multibagger tip", "stock tip", "trading call",
    "buy or sell", "should you buy, sell or hold",
    "technical analysis", "support level", "resistance level",

    # Generic lifestyle / US domestic
    "gas prices go up fast",
    "why your paycheck",
    "how to save money",
    "personal finance tips",
    "credit card tips",
    "get rich",
]


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION-SPECIFIC INCLUSION FILTERS
#  A headline must contain at least ONE of these to pass into that section.
# ══════════════════════════════════════════════════════════════════════════════

INDIA_POLICY_SIGNALS = [
    "rbi", "sebi", "lok sabha", "rajya sabha", "parliament", "budget",
    "ministry", "minister", "pib", "nirmala sitharaman", "rbi governor",
    "sanjay malhotra", "finance ministry", "economic survey",
    "gst", "income tax", "customs duty", "fiscal", "monetary policy",
    "repo rate", "reverse repo", "crr", "slr",
    "inflation", "cpi", "wpi", "iip", "gdp", "core sector", "pmi",
    "trade deficit", "current account", "forex reserve", "rupee",
    "fdi", "foreign investment", "disinvestment", "privatisation",
    "pli scheme", "production linked", "infrastructure",
    "capex", "public sector", "government scheme", "policy",
    "regulation", "ordinance", "bill passed", "amendment",
    "election commission", "election result",
    "rbi draft", "rbi circular", "sebi circular", "sebi order",
    "nbfc", "bank regulation", "financial stability",
    "imf", "world bank india", "asian development bank",
]

GEO_SIGNALS = [
    # Energy & commodities with direct market impact
    "oil", "crude", "opec", "opec+", "gas", "lng", "petroleum",
    "strait of hormuz", "hormuz", "red sea", "shipping lane",
    "supply chain", "port blockade", "freight", "tanker",
    # Conflicts with supply chain / energy impact
    "iran", "ukraine", "russia", "middle east", "taiwan", "south china sea",
    "israel", "gaza", "hamas", "hezbollah", "pakistan china",
    # Economic policy & institutions
    "federal reserve", "fed rate", "interest rate", "ecb", "central bank",
    "imf", "world bank", "wto", "g7", "g20", "brics",
    "trade war", "tariff", "sanctions", "trade agreement",
    "inflation", "recession", "gdp", "economic growth",
    # Deaths / changes of globally significant people
    "dies", "death of", "passed away", "assassination",
    "warren buffett", "ray dalio", "jerome powell", "janet yellen",
    "xi jinping", "trump", "modi", "fed chair", "treasury secretary",
    # Geopolitical outcomes with market impact
    "ceasefire", "peace deal", "coup", "regime change",
    "nuclear", "missile", "blockade", "force majeure",
    "election result", "government formation",
]

CAPITAL_MARKETS_SIGNALS = [
    # Corporate actions
    "acquisition", "acquires", "merger", "takeover", "open offer",
    "qip", "rights issue", "ipo", "fpo", "listing",
    "promoter stake", "stake sale", "stake purchase", "stake increase",
    "board approves", "board meeting", "agm", "egm",
    "fundraise", "fund raise", "ncd", "debenture", "bond issue",
    "ncd issue", "commercial paper",
    # People changes
    "ceo appointed", "md appointed", "chairman appointed",
    "ceo resigns", "md resigns", "management change",
    # Regulatory
    "sebi approval", "sebi order", "nclt", "cci approval",
    "regulatory approval", "rbi approval",
    # Results & financials
    "quarterly result", "q1 result", "q2 result", "q3 result", "q4 result",
    "annual result", "earnings", "profit", "revenue", "ebitda",
    "net profit", "net loss", "margin expansion", "margin contraction",
    "guidance", "outlook",
    # Corporate events
    "dividend", "interim dividend", "final dividend",
    "buyback", "bonus share", "stock split", "share split",
    "record date", "demerger", "restructuring",
    # Market structure
    "new index", "index inclusion", "index exclusion", "index launch",
    "fii buying", "dii buying", "block deal", "bulk deal",
    "institutional investor",
    # Business developments
    "capex", "plant expansion", "new factory", "new order",
    "contract won", "joint venture", "partnership",
    "product launch", "distribution expansion", "export order",
    "52-week high", "52-week low",
]

COMMODITIES_SIGNALS = [
    "crude", "oil price", "brent", "wti", "opec",
    "palm oil", "soybean", "wheat", "corn", "rice", "cotton", "sugar",
    "coffee", "cocoa", "rubber",
    "gold", "silver", "copper", "aluminum", "steel", "iron ore",
    "fertilizer", "urea", "potash",
    "coal", "lng", "natural gas",
    "shipping", "freight rate", "container", "dry bulk",
    "supply chain", "port", "harvest", "crop", "drought", "flood",
    "indonesia", "malaysia", "palm", "ukraine grain", "black sea",
]

# YouTube / Podcast — what to EXCLUDE
MEDIA_NOISE = [
    "kevin warsh made his fortune", "lowe's bets", "gen z",
    "how to make money fast", "get rich", "top 10",
    "viral", "shorts #", "compilation", "reaction", "tiktok",
    "celebrity net worth", "who is", "net worth of",
]


# ══════════════════════════════════════════════════════════════════════════════
#  YOUTUBE & PODCAST SOURCES
# ══════════════════════════════════════════════════════════════════════════════

YOUTUBE_CHANNELS = [
    # Indian fund managers & capital markets — highest priority
    {"id": "UCVvVZlKFmHwxZXhCnfm8JNw", "name": "Marcellus Investment"},
    {"id": "UCl4T8VYJKVkMFBOhWWI4f3Q", "name": "Capitalmind"},
    {"id": "UCWX3yGbODI3FLCZpFHrJYJA", "name": "Nikhil Kamath"},
    {"id": "UCHqCBFoAtRB8NRQKG5fO3eA", "name": "ET Now"},
    {"id": "UCvJJ_dzjViJCoLf5uKUTwoA", "name": "CNBC-TV18"},
    # Global macro & markets
    {"id": "UCMtJYS0PrtiUwlk6zjGDEMA", "name": "Bloomberg Markets"},
    {"id": "UCCExUQXB12wJH4w1sUAVyQQ", "name": "CNBC"},
    {"id": "UCGdVL21GEF3WqS1_5QxIq4Q", "name": "Zerodha Varsity"},
]

PODCAST_FEEDS = [
    {"url": "https://rss.art19.com/invest-like-the-best",           "name": "Invest Like The Best"},
    {"url": "https://rss.art19.com/acquired",                       "name": "Acquired"},
    {"url": "https://feeds.simplecast.com/0lxBYiKU",                "name": "We Study Billionaires"},
    {"url": "https://marcellusinvestment.com/feed/podcast/",        "name": "Marcellus Podcast"},
    {"url": "https://feeds.transistor.fm/capitalmind-premium-podcast", "name": "Capitalmind Podcast"},
]


# ══════════════════════════════════════════════════════════════════════════════
#  SMART DEDUPLICATION
#  Problem: "Core sector output fell 0.4%" and "India core sector contracts 0.4%"
#  are the same story with different headlines.
#  Solution: Extract key named entities + numbers from titles and use them
#  as a fingerprint. If two headlines share 3+ significant words AND
#  any numbers/percentages, they are the same story.
# ══════════════════════════════════════════════════════════════════════════════

# Common words that carry no dedup signal — ignored in fingerprinting
STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "has", "have", "had", "will", "would", "could", "should", "may", "might",
    "its", "it", "this", "that", "these", "those", "as", "up", "down",
    "after", "before", "over", "under", "above", "below", "between",
    "why", "how", "what", "when", "who", "which", "where",
    "says", "said", "says", "report", "reports", "according",
    "amid", "after", "before", "despite", "following", "hit", "hits",
    "data", "news", "latest", "update", "today", "now", "new",
    "india's", "india", "indian",
}

def title_fingerprint(title: str) -> frozenset:
    """
    Extract meaningful tokens from a title for semantic deduplication.
    Returns a frozenset of significant words and all numbers/percentages.
    """
    t = title.lower()
    # extract percentages and numbers as-is (these are very discriminating)
    numbers = set(re.findall(r'\d+\.?\d*%?', t))
    # extract words, remove stop words and short words
    words = set(w.strip(".,;:!?\"'()[]") for w in t.split())
    words = {w for w in words if len(w) > 3 and w not in STOP_WORDS}
    return frozenset(words | numbers)

def is_duplicate(new_title: str, seen_fingerprints: list) -> bool:
    """
    Return True if new_title is semantically similar to any previously seen title.
    Two titles are duplicates if they share >= 3 significant tokens OR
    share any number/percentage AND >= 2 significant words.
    """
    new_fp = title_fingerprint(new_title)
    new_nums = {t for t in new_fp if re.search(r'\d', t)}
    new_words = new_fp - new_nums

    for seen_fp in seen_fingerprints:
        seen_nums = {t for t in seen_fp if re.search(r'\d', t)}
        seen_words = seen_fp - seen_nums

        shared_words = new_words & seen_words
        shared_nums  = new_nums  & seen_nums

        # Same story if: 3+ shared significant words, OR shared number + 2 words
        if len(shared_words) >= 3:
            return True
        if shared_nums and len(shared_words) >= 2:
            return True

    return False


def smart_dedup(items: list, seen_fingerprints: list) -> list:
    """
    Deduplicate a list of items against a running global fingerprint list.
    Modifies seen_fingerprints in place (global cross-section dedup).
    """
    result = []
    for item in items:
        if not is_duplicate(item["title"], seen_fingerprints):
            seen_fingerprints.append(title_fingerprint(item["title"]))
            result.append(item)
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  FILTERING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def passes_noise_filter(title: str) -> bool:
    t = title.lower()
    return not any(phrase in t for phrase in NOISE_PHRASES)

def passes_section_filter(title: str, signals: list) -> bool:
    t = title.lower()
    return any(sig in t for sig in signals)

def passes_media_filter(title: str) -> bool:
    t = title.lower()
    return not any(phrase in t for phrase in MEDIA_NOISE)


# ══════════════════════════════════════════════════════════════════════════════
#  FETCH FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MarketDigestBot/4.0; +https://github.com)"}

def fetch_rss(url: str) -> list:
    """Fetch a single RSS feed. Returns list of {title, url, published}."""
    try:
        raw = requests.get(url, timeout=12, headers=HEADERS)
        feed = feedparser.parse(raw.content)
        cutoff = datetime.now(IST) - timedelta(hours=HOURS_BACK)
        items = []
        for entry in feed.entries:
            title = entry.get("title", "").strip()
            link  = entry.get("link",  "").strip()
            if not title or not link:
                continue
            published = None
            for field in ["published_parsed", "updated_parsed"]:
                val = getattr(entry, field, None)
                if val:
                    import calendar
                    published = datetime.fromtimestamp(calendar.timegm(val), tz=IST)
                    break
            if published and published < cutoff:
                continue
            items.append({"title": title, "url": link})
        return items
    except Exception as e:
        log.warning(f"RSS failed [{url[:60]}]: {e}")
        return []


def fetch_section_items(feed_urls: list, section_signals: list = None) -> list:
    """
    Fetch all feeds for a section.
    Apply noise filter. Apply section-specific signal filter if provided.
    Return deduplicated list (URL-level dedup within section).
    """
    all_items = []
    seen_urls = set()
    for url in feed_urls:
        for item in fetch_rss(url):
            if item["url"] in seen_urls:
                continue
            seen_urls.add(item["url"])
            if not passes_noise_filter(item["title"]):
                continue
            if section_signals and not passes_section_filter(item["title"], section_signals):
                continue
            all_items.append(item)
    return all_items


def fetch_youtube_videos(hours_back: int = 48) -> list:
    if not YOUTUBE_API_KEY:
        return []
    results = []
    since = (datetime.utcnow() - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for ch in YOUTUBE_CHANNELS:
        try:
            url = (
                f"https://www.googleapis.com/youtube/v3/search"
                f"?part=snippet&channelId={ch['id']}&type=video"
                f"&publishedAfter={since}&maxResults=3&order=date"
                f"&key={YOUTUBE_API_KEY}"
            )
            resp = requests.get(url, timeout=10)
            for item in resp.json().get("items", []):
                vid_id = item["id"].get("videoId")
                title  = item["snippet"].get("title", "")
                if vid_id and title and passes_media_filter(title):
                    results.append({
                        "title": f"{title} [{ch['name']}]",
                        "url":   f"https://youtube.com/watch?v={vid_id}",
                    })
        except Exception as e:
            log.warning(f"YouTube failed [{ch['name']}]: {e}")
    return results


def fetch_podcast_episodes() -> list:
    results = []
    for pod in PODCAST_FEEDS:
        try:
            feed = feedparser.parse(pod["url"])
            if feed.entries:
                ep    = feed.entries[0]
                title = ep.get("title", "")
                link  = ep.get("link", "")
                if title and link and passes_media_filter(title):
                    results.append({
                        "title": f"{title} [{pod['name']}]",
                        "url":   link,
                    })
        except Exception as e:
            log.warning(f"Podcast failed [{pod['name']}]: {e}")
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

def send_telegram(text: str) -> bool:
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


def send_section(header: str, items: list):
    """Format and send a section. Handles Telegram 4096 char limit automatically."""
    if not items:
        return
    block = f"{header}\n"
    for i, item in enumerate(items, 1):
        line = f"{i}. <a href='{item['url']}'>{item['title'][:140]}</a>\n"
        if len(block) + len(line) > MAX_MSG_LEN:
            send_telegram(block.strip())
            time.sleep(1.2)
            block = f"{header} (contd.)\n" + line
        else:
            block += line
    send_telegram(block.strip())
    time.sleep(1.2)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN DIGEST BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_and_send_digest():
    """
    Fetch, filter, deduplicate across all sections, and send to Telegram.

    Section order (Indian fund manager priority):
    1. India Policy & Economy
    2. Capital Markets & Listed Companies  ← PRIORITY
    3. Geopolitical (market-relevant)
    4. Global Markets & Macro
    5. Commodities & Supply Chain
    6. Fund Manager Videos & Podcasts
    """
    now = datetime.now(IST)
    log.info("Building digest...")

    # Global fingerprint list — shared across all sections for cross-section dedup
    seen_fps = []

    # ── Header ────────────────────────────────────────────────────────────────
    send_telegram(
        f"<b>📊 Morning Market Brief</b>\n"
        f"<i>{now.strftime('%A, %d %B %Y  •  %I:%M %p IST')}</i>\n"
        f"{'─' * 30}"
    )
    time.sleep(1.2)

    # ── 1. India Policy & Economy ─────────────────────────────────────────────
    items = fetch_section_items(RSS_SOURCES["india_policy"], INDIA_POLICY_SIGNALS)
    items = smart_dedup(items, seen_fps)
    send_section("<b>🇮🇳 India Policy &amp; Economy</b>", items)

    # ── 2. Capital Markets & Listed Companies (PRIORITY) ─────────────────────
    items = fetch_section_items(RSS_SOURCES["capital_markets"], CAPITAL_MARKETS_SIGNALS)
    items = smart_dedup(items, seen_fps)
    send_section("⚡ <b>PRIORITY — Capital Markets &amp; Listed Companies</b> ⚡", items)

    # ── 3. Geopolitical (market-relevant) ─────────────────────────────────────
    items = fetch_section_items(RSS_SOURCES["geopolitical"], GEO_SIGNALS)
    items = smart_dedup(items, seen_fps)
    send_section("<b>🌐 Geopolitical (Market-Relevant)</b>", items)

    # ── 4. Global Markets & Macro ─────────────────────────────────────────────
    # No section_signals filter here — Reuters/AP business is already curated
    items = fetch_section_items(RSS_SOURCES["global_macro"])
    items = smart_dedup(items, seen_fps)
    send_section("<b>🌍 Global Markets &amp; Macro</b>", items)

    # ── 5. Commodities & Supply Chain ─────────────────────────────────────────
    items = fetch_section_items(RSS_SOURCES["commodities"], COMMODITIES_SIGNALS)
    items = smart_dedup(items, seen_fps)
    send_section("<b>🛢️ Commodities &amp; Supply Chain</b>", items)

    # ── 6. Fund Manager Videos & Podcasts ─────────────────────────────────────
    yt_items  = fetch_youtube_videos()
    pod_items = fetch_podcast_episodes()
    media_items = yt_items + pod_items

    if media_items:
        send_section("<b>▶️ Fund Manager Videos &amp; Podcasts</b>", media_items)

    # ── Footer ────────────────────────────────────────────────────────────────
    send_telegram(
        f"<i>Brief complete — {now.strftime('%I:%M %p IST')}\n"
        f"Send /news anytime for an on-demand update.</i>"
    )
    log.info("Digest sent.")


# ══════════════════════════════════════════════════════════════════════════════
#  ON-DEMAND: /news COMMAND LISTENER
#  Note: This requires the bot to be run as a persistent process (not GitHub
#  Actions). For on-demand via GitHub Actions, trigger manually from the
#  Actions tab or use the on-demand workflow (see on_demand.yml).
# ══════════════════════════════════════════════════════════════════════════════

def run_bot_listener():
    """Poll Telegram for /news commands and send digest on demand."""
    log.info("On-demand listener started. Waiting for /news commands...")
    offset = None
    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message"]}
            if offset:
                params["offset"] = offset
            resp = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params=params, timeout=40
            )
            for update in resp.json().get("result", []):
                offset = update["update_id"] + 1
                msg  = update.get("message", {})
                text = msg.get("text", "").strip().lower()
                if text in ["/news", "news", "/digest", "digest", "/brief", "brief"]:
                    log.info("On-demand request received.")
                    send_telegram("⏳ Fetching latest brief, please wait 30–60 seconds...")
                    time.sleep(1)
                    build_and_send_digest()
        except Exception as e:
            log.error(f"Listener error: {e}")
            time.sleep(15)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    build_and_send_digest()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--listen":
        run_bot_listener()
    else:
        main()
