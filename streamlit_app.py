import re
from collections import Counter

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Daily Intelligence Dashboard", layout="wide")

# -------------------------
# SETTINGS
# -------------------------
# Use the path that matches your repo.
# If your file is in the repo root, use "articles_tagged.csv"
# If your pipeline writes into daily_output/, use "daily_output/articles_tagged.csv"
CSV_PATH = "daily_output/articles_tagged.csv"

# -------------------------
# LOAD DATA
# -------------------------
@st.cache_data
def load_data():
    df = pd.read_csv(CSV_PATH)
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce", utc=True)
    df["topics"] = df["topics"].fillna("")
    df["summary"] = df["summary"].fillna("") if "summary" in df.columns else ""
    df["source"] = df["source"].fillna("")
    df["headline"] = df["headline"].fillna("")
    df["url"] = df["url"].fillna("")

    df["topics_list"] = df["topics"].apply(
        lambda x: [t.strip() for t in str(x).split(",") if t.strip()]
    )

    df["primary_topic"] = df["topics_list"].apply(lambda x: x[0] if x else "Other")
    df["cluster_text"] = (df["headline"].fillna("") + ". " + df["summary"].fillna("")).str.strip()

    return df


STOPWORDS = {
    "the","a","an","and","or","but","for","to","of","in","on","at","by",
    "with","from","as","is","are","was","were","be","been","being",
    "this","that","these","those","it","its","their","his","her",
    "they","them","will","would","could","should","about","after",
    "before","during","into","over","under","new","says","said",
    "report","reports","study","research","news","today","latest",
    "analysis","update","among","associated","fully","building",
    "young","photos","can"
}

GENERIC_WORDS = {
    "science","startup","startups","funding","space","defense","defence",
    "biotech","medtech","medical","technology","tech","ai",
    "company","companies","researchers","scientists"
}

BAD_LABEL_WORDS = STOPWORDS | GENERIC_WORDS


def tokenize(text):
    text = str(text).lower()
    text = re.sub(r"[^a-z0-9\s-]", " ", text)
    tokens = []
    for t in text.split():
        if len(t) < 4:
            continue
        if t in STOPWORDS:
            continue
        tokens.append(t)
    return tokens


def token_set(text):
    return set(tokenize(text))


def jaccard_similarity(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def make_cluster_label(cluster_df):
    tokens = []
    for text in cluster_df["cluster_text"]:
        tokens += tokenize(text)

    counts = Counter(t for t in tokens if t not in BAD_LABEL_WORDS)

    words = [w for w, c in counts.items() if c >= 2]
    words = sorted(words, key=lambda w: (-counts[w], w))

    if len(words) >= 2:
        return " / ".join(w.title() for w in words[:3])

    return cluster_df.iloc[0]["headline"]


def cluster_articles(df, similarity_threshold=0.28, min_shared_tokens=3):
    if df.empty:
        return pd.DataFrame(), {}, pd.DataFrame()

    work = df.copy().reset_index(drop=False).rename(columns={"index": "original_index"})
    work["token_set"] = work["cluster_text"].apply(token_set)

    clusters = []
    article_to_cluster = {}

    for _, row in work.sort_values("published_date", ascending=False).iterrows():
        placed = False

        for cluster in clusters:
            same_topic = row["primary_topic"] == cluster["primary_topic"]
            sim = jaccard_similarity(row["token_set"], cluster["token_union"])
            shared = len(row["token_set"] & cluster["token_union"])

            if same_topic and sim >= similarity_threshold and shared >= min_shared_tokens:
                cluster["rows"].append(row)
                cluster["token_union"] |= row["token_set"]
                article_to_cluster[row["original_index"]] = cluster["cluster_id"]
                placed = True
                break

        if not placed:
            cid = f"cluster_{len(clusters)+1}"
            clusters.append({
                "cluster_id": cid,
                "primary_topic": row["primary_topic"],
                "rows": [row],
                "token_union": set(row["token_set"]),
            })
            article_to_cluster[row["original_index"]] = cid

    now = pd.Timestamp.utcnow()
    cluster_rows = []
    singles = []

    for cluster in clusters:
        cdf = pd.DataFrame(cluster["rows"])
        count = len(cdf)
        sources = cdf["source"].nunique()
        latest = cdf["published_date"].max()

        hours = (now - latest).total_seconds() / 3600 if pd.notnull(latest) else 999
        recency = max(0, 24 - min(hours, 24)) / 24

        score = count * 4 + sources * 3 + recency

        row = {
            "cluster_id": cluster["cluster_id"],
            "label": make_cluster_label(cdf),
            "primary_topic": cluster["primary_topic"],
            "article_count": count,
            "source_count": sources,
            "latest_date": latest,
            "score": score,
            "representative_headline": cdf.iloc[0]["headline"],
        }

        if count >= 2:
            cluster_rows.append(row)
        else:
            singles.append(row)

    trending = pd.DataFrame(cluster_rows).sort_values("score", ascending=False)
    singles_df = pd.DataFrame(singles).sort_values("latest_date", ascending=False)

    return trending, article_to_cluster, singles_df


def render_article_card(row):
    st.markdown(f"### {row['headline']}")
    st.write(f"**Source:** {row['source']}")
    st.write(f"**Topics:** {row['topics']}")
    if str(row.get("summary", "")).strip():
        st.write(row["summary"])
    st.markdown(f"[Read article]({row['url']})")
    st.markdown("---")


# -------------------------
# DATA
# -------------------------
df = load_data()

st.title("🧠 Daily Intelligence Dashboard")
st.caption("News discovery + topic selection workspace")

# -------------------------
# SIDEBAR
# -------------------------
st.sidebar.header("Filters")
st.sidebar.subheader("📅 Date Filter")

min_date = df["published_date"].min().date()
max_date = df["published_date"].max().date()

date_range = st.sidebar.date_input(
    "Select date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

all_topics = sorted(set(t for topics in df["topics_list"] for t in topics))
all_sources = sorted(df["source"].dropna().unique().tolist())

selected_topics = st.sidebar.multiselect("Topics", all_topics)
selected_sources = st.sidebar.multiselect("Sources", all_sources)
search = st.sidebar.text_input("Search")

filtered = df.copy()

if selected_topics:
    filtered = filtered[
        filtered["topics_list"].apply(lambda x: any(t in x for t in selected_topics))
    ]

if selected_sources:
    filtered = filtered[filtered["source"].isin(selected_sources)]

if search:
    filtered = filtered[
        filtered["headline"].str.contains(search, case=False, na=False) |
        filtered["summary"].str.contains(search, case=False, na=False)
    ]
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered = filtered[
        (filtered["published_date"].dt.date >= start_date) &
        (filtered["published_date"].dt.date <= end_date)
    ]

filtered = filtered.sort_values("published_date", ascending=False)

# -------------------------
# TRENDING TOPICS
# -------------------------
st.subheader("🔥 Trending Topics")

trending_df, article_to_cluster, singles_df = cluster_articles(filtered)

if trending_df.empty:
    st.info("No strong trends found.")
else:
    st.caption("Only multi-article trends (2+ articles) shown.")

    cols = st.columns(2)

    for i, (_, cluster) in enumerate(trending_df.head(6).iterrows()):
        with cols[i % 2]:
            st.markdown(f"### {cluster['label']}")
            st.write(f"**Topic:** {cluster['primary_topic']}")
            st.write(f"**Articles:** {cluster['article_count']} | Sources: {cluster['source_count']}")

            if st.button("Open topic", key=cluster["cluster_id"]):
                st.session_state["selected_cluster_id"] = cluster["cluster_id"]

selected = st.session_state.get("selected_cluster_id")

if selected:
    meta = trending_df[trending_df["cluster_id"] == selected].iloc[0]
    related_idx = [i for i, cid in article_to_cluster.items() if cid == selected]
    related = filtered.loc[related_idx].sort_values("published_date", ascending=False)

    st.divider()
    st.subheader(f"🧩 {meta['label']}")

    for _, row in related.iterrows():
        render_article_card(row)

st.divider()

st.subheader("📝 Single Articles (Not Trending)")

for _, row in singles_df.head(10).iterrows():
    st.markdown(f"- {row['representative_headline']}")

st.divider()

# -------------------------
# METRICS
# -------------------------
col1, col2, col3 = st.columns(3)
col1.metric("Articles", len(filtered))
col2.metric("Sources", filtered["source"].nunique())
col3.metric("Topics", filtered["topics_list"].explode().nunique())

st.divider()

# -------------------------
# CHARTS
# -------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Topics")
    if not filtered.empty:
        st.bar_chart(filtered["topics_list"].explode().value_counts())

with col2:
    st.subheader("Sources")
    if not filtered.empty:
        st.bar_chart(filtered["source"].value_counts())

st.divider()

# -------------------------
# TABLE
# -------------------------
st.subheader("Articles")

st.dataframe(
    filtered[["published_date", "source", "headline", "topics"]],
    use_container_width=True
)

st.divider()

# -------------------------
# LATEST
# -------------------------
st.subheader("Latest")

for _, row in filtered.head(15).iterrows():
    render_article_card(row)
