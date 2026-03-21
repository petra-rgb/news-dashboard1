# =========================
# MULTI-SOURCE RSS-FIRST DAILY NEWS PIPELINE
# lightweight, feed-first, no HTML scraping, auto-tests candidate feeds
# =========================

import re
import warnings
from pathlib import Path
from datetime import datetime, timedelta, timezone

import feedparser
import pandas as pd
from dateutil import parser as dateparser
from dateutil.parser import UnknownTimezoneWarning

# =========================
# SETTINGS
# =========================
DAYS_BACK = 2
MAX_ITEMS_PER_FEED = 40
MAX_TOPICS_PER_ARTICLE = 2
MIN_ENTRIES_TO_ACCEPT_FEED = 3   # if a candidate feed has fewer than this, skip it unless it's still recent/useful
PRINT_FEED_DEBUG = True

OUTPUT_DIR = Path("daily_output")
OUTPUT_DIR.mkdir(exist_ok=True)

RAW_OUTPUT_FILE = OUTPUT_DIR / "articles_raw.csv"
ARTICLES_OUTPUT_FILE = OUTPUT_DIR / "articles_tagged.csv"
MARKDOWN_OUTPUT_FILE = OUTPUT_DIR / "daily_report.md"
WORKING_FEEDS_FILE = OUTPUT_DIR / "working_feeds.csv"

cutoff_date = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

warnings.filterwarnings("ignore", category=UnknownTimezoneWarning)

TZINFOS = {
    "EDT": -4 * 3600,
    "EST": -5 * 3600,
    "CDT": -5 * 3600,
    "CST": -6 * 3600,
    "MDT": -6 * 3600,
    "MST": -7 * 3600,
    "PDT": -7 * 3600,
    "PST": -8 * 3600,
    "BST": 1 * 3600,
    "CET": 1 * 3600,
    "CEST": 2 * 3600,
    "GMT": 0,
    "UTC": 0,
}

# =========================
# SOURCE CONFIG
# candidate_feeds = URLs to try in order
# source_tags = topic bias
#
# Notes:
# - Some URLs below are confirmed/common feed endpoints
# - Some are "candidate" endpoints; the script tests them and uses only those that parse
# =========================
SOURCES = [
    {
        "name": "Phys.org",
        "candidate_feeds": [
            "https://phys.org/rss-feed/",
        ],
        "source_tags": ["Science", "Biotech", "MedTech", "Space"],
    },
    {
        "name": "Science",
        "candidate_feeds": [
            "https://www.science.org/rss/news_current.xml",
        ],
        "source_tags": ["Science", "Biotech", "MedTech"],
    },
    {
        "name": "Defense News",
        "candidate_feeds": [
            "https://www.defensenews.com/arc/outboundfeeds/rss/",
        ],
        "source_tags": ["Defense"],
    },
    {
        "name": "TechCrunch",
        "candidate_feeds": [
            "https://techcrunch.com/feed/",
            "https://techcrunch.com/category/startups/feed/",
            "https://techcrunch.com/category/venture/feed/",
            "https://techcrunch.com/category/ai/feed/",
        ],
        "source_tags": ["AI", "Startups", "Funding", "Space"],
    },
    {
        "name": "Nature",
        "candidate_feeds": [
            "https://www.nature.com/nature.rss",
            "https://www.nature.com/subjects/news.rss",
            "https://www.nature.com/subjects/biotechnology.rss",
            "https://www.nature.com/subjects/medical-research.rss",
        ],
        "source_tags": ["Science", "Biotech", "MedTech"],
    },
    {
        "name": "Labiotech",
        "candidate_feeds": [
            "https://www.labiotech.eu/feed/",
            "https://www.labiotech.eu/trends-news/feed/",
            "https://www.labiotech.eu/in-depth/feed/",
        ],
        "source_tags": ["Biotech", "MedTech", "Funding", "Startups"],
    },
    {
        "name": "EU-Startups",
        "candidate_feeds": [
            "https://www.eu-startups.com/feed/",
            "https://www.eu-startups.com/category/funding/feed/",
            "https://www.eu-startups.com/category/startups/feed/",
        ],
        "source_tags": ["Startups", "Funding", "AI"],
    },
    {
        "name": "MIT Technology Review",
        "candidate_feeds": [
            "https://www.technologyreview.com/feed/",
            "https://www.technologyreview.com/topic/artificial-intelligence/feed/",
            "https://www.technologyreview.com/topic/biomedicine/feed/",
            "https://www.technologyreview.com/topic/space/feed/",
        ],
        "source_tags": ["AI", "Science", "Biotech", "MedTech", "Space"],
    },
    {
        "name": "SpaceNews",
        "candidate_feeds": [
            "https://spacenews.com/feed/",
            "https://spacenews.com/category/launch/feed/",
            "https://spacenews.com/category/commercial/feed/",
        ],
        "source_tags": ["Space"],
    },
    {
        "name": "Payload Space",
        "candidate_feeds": [
            "https://payloadspace.com/feed/",
            "https://payloadspace.com/category/news/feed/",
        ],
        "source_tags": ["Space", "Funding", "Startups"],
    },
    {
        "name": "Satellite Today",
        "candidate_feeds": [
            "https://www.satellitetoday.com/feed/",
            "https://www.satellitetoday.com/category/business/feed/",
            "https://www.satellitetoday.com/category/launch/feed/",
        ],
        "source_tags": ["Space"],
    },
    {
        "name": "Breaking Defense",
        "candidate_feeds": [
            "https://breakingdefense.com/feed/",
            "https://breakingdefense.com/category/air/feed/",
            "https://breakingdefense.com/category/space/feed/",
        ],
        "source_tags": ["Defense", "Space"],
    },
    {
        "name": "C4ISRNET",
        "candidate_feeds": [
            "https://www.c4isrnet.com/arc/outboundfeeds/rss/",
        ],
        "source_tags": ["Defense", "AI", "Space"],
    },
    {
        "name": "War on the Rocks",
        "candidate_feeds": [
            "https://warontherocks.com/feed/",
        ],
        "source_tags": ["Defense"],
    },
    {
        "name": "National Defense Magazine",
        "candidate_feeds": [
            "https://www.nationaldefensemagazine.org/rss",
            "https://www.nationaldefensemagazine.org/feed",
            "https://www.nationaldefensemagazine.org/articles/feed",
        ],
        "source_tags": ["Defense"],
    },
    {
        "name": "NWO",
        "candidate_feeds": [
            "https://www.nwo.nl/en/news/rss.xml",
            "https://www.nwo.nl/rss.xml",
        ],
        "source_tags": ["Science"],
    },
    # The Information and Dealroom are intentionally not included by default:
    # they are more likely to be paywalled / not feed-friendly for this workflow.
]

# =========================
# TOPIC RULES
# =========================
TOPIC_KEYWORDS = {
    "AI": [
        "artificial intelligence", "machine learning", "deep learning",
        "generative ai", "large language model", "large language models",
        "foundation model", "foundation models", "computer vision",
        "anthropic", "openai", "mistral ai", "llm", "llms",
        "copilot", "ai model", "ai system", "ai systems"
    ],
    "Defense": [
        "military", "defense", "defence", "army", "navy", "air force",
        "missile", "munition", "munitions", "battlefield", "uav", "drone",
        "counter-drone", "radar", "artillery", "nato", "national security",
        "warfare", "pentagon", "c4isr", "hypersonic"
    ],
    "MedTech": [
        "medical device", "medical devices", "diagnostic", "diagnostics",
        "digital health", "healthtech", "health tech", "wearable",
        "implant", "imaging", "patient monitoring", "hospital technology",
        "remote monitoring", "medtech"
    ],
    "Biotech": [
        "biotech", "biotechnology", "clinical trial", "clinical trials",
        "gene therapy", "cell therapy", "crispr", "genomics",
        "biopharma", "therapeutic", "therapeutics", "antibody",
        "vaccine", "protein engineering", "drug discovery",
        "precision medicine"
    ],
    "Startups": [
        "startup", "start-up", "startups", "founder", "founders",
        "venture", "scaleup", "scale-up", "spinout", "spin-out",
        "accelerator", "incubator", "entrepreneur", "early-stage",
        "seed-stage"
    ],
    "Funding": [
        "raised", "raises", "raise", "funding", "investment",
        "investor", "investors", "venture capital", "seed round",
        "pre-seed", "series a", "series b", "series c",
        "financing", "backed by", "valuation", "grant"
    ],
    "Space": [
        "spacecraft", "satellite", "satellites", "launch vehicle",
        "rocket", "payload", "orbital", "orbit", "space mission",
        "earth observation", "constellation", "launcher", "spacetech",
        "space station", "lunar", "mars mission"
    ],
    "Science": [
        "research", "study", "scientists", "scientist", "paper",
        "journal", "experiment", "discovery", "discoveries",
        "laboratory", "breakthrough", "published", "researchers"
    ],
}

TOPIC_ORDER = ["AI", "Defense", "MedTech", "Biotech", "Startups", "Funding", "Space", "Science"]

SOURCE_PRIORITY = {
    src["name"]: src["source_tags"] for src in SOURCES
}

MIN_HITS_BY_TOPIC = {
    "AI": 1,
    "Defense": 1,
    "MedTech": 1,
    "Biotech": 1,
    "Startups": 1,
    "Funding": 1,
    "Space": 2,
    "Science": 1,
}

# =========================
# HELPERS
# =========================
def normalize_datetime(dt):
    if dt is None:
        return None

    if isinstance(dt, str):
        try:
            dt = dateparser.parse(dt, tzinfos=TZINFOS)
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def clean_text(text):
    text = str(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_headline_for_dedupe(text):
    text = clean_text(text).lower()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def count_keyword_hits(text, keywords):
    text = text.lower()
    hits = 0
    for kw in keywords:
        pattern = r"\b" + re.escape(kw.lower()) + r"\b"
        if re.search(pattern, text):
            hits += 1
    return hits


def classify_topics(text, source_name, max_topics=2):
    text = clean_text(text)
    lowered = text.lower()
    scores = {}

    for topic, keywords in TOPIC_KEYWORDS.items():
        hits = count_keyword_hits(text, keywords)
        min_hits = MIN_HITS_BY_TOPIC.get(topic, 1)
        scores[topic] = hits if hits >= min_hits else 0

    preferred = SOURCE_PRIORITY.get(source_name, [])
    for topic in preferred:
        if scores.get(topic, 0) > 0:
            scores[topic] += 1

    if scores.get("Defense", 0) > 0:
        weak_defense_only = (
            "radar" in lowered and not any(
                x in lowered for x in [
                    "military", "army", "navy", "air force", "missile",
                    "drone", "uav", "pentagon", "defense", "defence",
                    "battlefield", "national security", "nato", "warfare"
                ]
            )
        )
        if weak_defense_only:
            scores["Defense"] = 0

    if scores.get("Space", 0) > 0:
        strong_space_terms = [
            "spacecraft", "rocket", "payload", "orbital", "orbit",
            "launch vehicle", "constellation", "launcher", "spacetech",
            "space station", "lunar", "mars mission", "earth observation"
        ]
        if not any(term in lowered for term in strong_space_terms):
            if count_keyword_hits(lowered, TOPIC_KEYWORDS["Space"]) < 2:
                scores["Space"] = 0

    ranked = sorted(scores.items(), key=lambda x: (-x[1], TOPIC_ORDER.index(x[0])))
    ranked = [topic for topic, score in ranked if score > 0]
    return ranked[:max_topics] if ranked else ["Other"]


def feed_entry_date(entry):
    for field in ["published", "updated", "created"]:
        if field in entry:
            dt = normalize_datetime(entry.get(field))
            if dt:
                return dt
    return None


def feed_entry_summary(entry):
    return clean_text(entry.get("summary", "") or entry.get("description", ""))


def parse_feed(url):
    try:
        feed = feedparser.parse(url)
        entries = getattr(feed, "entries", [])
        return feed, entries
    except Exception:
        return None, []


def choose_best_feed(source):
    best = None
    diagnostics = []

    for url in source["candidate_feeds"]:
        feed, entries = parse_feed(url)
        entry_count = len(entries)

        recent_count = 0
        for entry in entries[:MAX_ITEMS_PER_FEED]:
            dt = feed_entry_date(entry)
            if dt is not None and dt >= cutoff_date:
                recent_count += 1

        bozo = getattr(feed, "bozo", 1) if feed is not None else 1
        score = recent_count * 100 + entry_count - (50 if bozo else 0)

        diagnostics.append({
            "source": source["name"],
            "feed_url": url,
            "entry_count": entry_count,
            "recent_count": recent_count,
            "bozo": bozo,
            "score": score,
        })

        if best is None or score > best["score"]:
            best = {
                "source": source["name"],
                "feed_url": url,
                "feed": feed,
                "entries": entries,
                "entry_count": entry_count,
                "recent_count": recent_count,
                "bozo": bozo,
                "score": score,
            }

    return best, diagnostics


# =========================
# COLLECTION
# =========================
def scan_source(source):
    best, diagnostics = choose_best_feed(source)

    if PRINT_FEED_DEBUG:
        print(f"\n{source['name']}")
        for row in diagnostics:
            print(
                f"  tried: {row['feed_url']} | entries={row['entry_count']} "
                f"| recent={row['recent_count']} | bozo={row['bozo']} | score={row['score']}"
            )

    if best is None or best["entry_count"] == 0:
        print("  -> no working feed")
        return [], diagnostics

    if best["recent_count"] == 0 and best["entry_count"] < MIN_ENTRIES_TO_ACCEPT_FEED:
        print("  -> skipped: weak feed candidate")
        return [], diagnostics

    print(f"  -> using: {best['feed_url']}")

    results = []
    for entry in best["entries"][:MAX_ITEMS_PER_FEED]:
        published_date = feed_entry_date(entry)
        if published_date is None or published_date < cutoff_date:
            continue

        headline = clean_text(entry.get("title", ""))
        summary = feed_entry_summary(entry)
        url = clean_text(entry.get("link", ""))

        if not headline or not url:
            continue

        full_text = f"{headline}. {summary}".strip()
        topics = classify_topics(full_text, source["name"], max_topics=MAX_TOPICS_PER_ARTICLE)

        results.append({
            "source": source["name"],
            "feed_url": best["feed_url"],
            "headline": headline,
            "summary": summary,
            "url": url,
            "published_date": published_date,
            "topics": ", ".join(topics),
            "full_text": full_text,
            "headline_norm": normalize_headline_for_dedupe(headline),
        })

    return results, diagnostics


def collect_articles(sources):
    all_results = []
    all_diagnostics = []

    for source in sources:
        try:
            rows, diagnostics = scan_source(source)
            all_results.extend(rows)
            all_diagnostics.extend(diagnostics)
        except Exception as e:
            print(f"Error while scanning {source['name']}: {e}")

    diag_df = pd.DataFrame(all_diagnostics)

    df = pd.DataFrame(all_results)
    if df.empty:
        return df, diag_df

    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce", utc=True)

    df = df.drop_duplicates(subset=["url"]).copy()
    df = df.sort_values("published_date", ascending=False).reset_index(drop=True)
    df = df.drop_duplicates(subset=["headline_norm"], keep="first").copy()
    df = df.sort_values("published_date", ascending=False).reset_index(drop=True)

    return df, diag_df


# =========================
# EXPORT
# =========================
def export_markdown_report(df, output_path):
    lines = []
    now_str = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M UTC")

    lines.append("# Daily Intelligence Report")
    lines.append("")
    lines.append(f"Generated: {now_str}")
    lines.append("")

    for topic in TOPIC_ORDER + ["Other"]:
        if topic == "Other":
            topic_df = df[df["topics"] == "Other"].copy()
        else:
            topic_df = df[df["topics"].str.contains(rf"\b{re.escape(topic)}\b", na=False)].copy()

        if topic_df.empty:
            continue

        lines.append(f"## {topic}")
        lines.append("")

        for _, row in topic_df.iterrows():
            date_str = "Unknown date"
            if pd.notnull(row["published_date"]):
                date_str = pd.Timestamp(row["published_date"]).strftime("%Y-%m-%d %H:%M")

            lines.append(f"### {row['headline']}")
            lines.append(f"- Source: {row['source']}")
            lines.append(f"- Published: {date_str}")
            lines.append(f"- Topics: {row['topics']}")
            lines.append(f"- Link: {row['url']}")
            if row["summary"]:
                lines.append(f"- Summary: {row['summary'][:500]}")
            lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


# =========================
# RUN
# =========================
df, diag_df = collect_articles(SOURCES)

if diag_df is not None and not diag_df.empty:
    diag_df.to_csv(WORKING_FEEDS_FILE, index=False)

if df.empty:
    raise ValueError(
        "No recent articles collected. Check working_feeds.csv, increase DAYS_BACK, or remove weak sources."
    )

df_display = df.drop(columns=["full_text", "headline_norm"], errors="ignore").copy()

df_display.to_csv(RAW_OUTPUT_FILE, index=False)
if ARTICLES_OUTPUT_FILE.exists():
    old_df = pd.read_csv(ARTICLES_OUTPUT_FILE)
    combined = pd.concat([old_df, df_display], ignore_index=True)
else:
    combined = df_display.copy()

# remove duplicates again (important!)
combined = combined.drop_duplicates(subset=["url"])
combined["published_date"] = pd.to_datetime(combined["published_date"], errors="coerce")

# sort
combined = combined.sort_values("published_date", ascending=False)

combined.to_csv(ARTICLES_OUTPUT_FILE, index=False)
export_markdown_report(df_display, MARKDOWN_OUTPUT_FILE)

print(f"\nSaved raw articles to: {RAW_OUTPUT_FILE}")
print(f"Saved tagged articles to: {ARTICLES_OUTPUT_FILE}")
print(f"Saved markdown report to: {MARKDOWN_OUTPUT_FILE}")
print(f"Saved feed diagnostics to: {WORKING_FEEDS_FILE}")

print("\nWorking feeds summary:")
best_feeds = (
    diag_df.sort_values(["source", "score"], ascending=[True, False])
    .drop_duplicates(subset=["source"], keep="first")
    [["source", "feed_url", "entry_count", "recent_count", "bozo", "score"]]
)
print(best_feeds.sort_values("recent_count", ascending=False).to_string(index=False))

print("\nArticles collected by source:")
articles_by_source = (
    df_display.groupby("source")
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
)
print(articles_by_source.to_string(index=False))

print("\nArticles collected by topic:")
topic_counts = (
    df_display.assign(topic_split=df_display["topics"].str.split(", "))
    .explode("topic_split")
    .groupby("topic_split")
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
)
print(topic_counts.to_string(index=False))

print("\nPreview:")
print(df_display.head(30).to_string(index=False))

