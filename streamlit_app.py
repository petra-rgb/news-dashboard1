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

STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "for", "to", "of", "in", "on", "at", "by",
    "with", "from", "as", "is", "are", "was", "were", "be", "been", "being", "this",
    "that", "these", "those", "it", "its", "their", "his", "her", "they", "them",
    "will", "would", "could", "should", "about", "after", "before", "during", "into",
    "over", "under", "new", "says", "say", "said", "report", "reports", "study",
    "research", "news", "today", "latest", "analysis", "update"
}

GENERIC_TOPIC_WORDS = {
    "science", "startup", "startups", "funding", "space", "defense", "defence",
    "biotech", "medtech", "medical", "technology", "tech", "ai"
}

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


def tokenize(text: str):
    text = str(text).lower()
    text = re.sub(r"[^a-z0-9\s-]", " ", text)
    raw_tokens = text.split()

    tokens = []
    for token in raw_tokens:
        token = token.strip("-")
        if len(token) < 3:
            continue
        if token in STOPWORDS:
            continue
        if token.isdigit():
            continue
        tokens.append(token)

    return tokens


def token_set(text: str):
    return set(tokenize(text))


def jaccard_similarity(a: set, b: set):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def cluster_articles(df: pd.DataFrame, similarity_threshold=0.20, min_shared_tokens=2):
    if df.empty:
        return pd.DataFrame(), {}

    work = df.copy().reset_index(drop=False).rename(columns={"index": "original_index"})
    work["token_set"] = work["cluster_text"].apply(token_set)

    clusters = []
    article_to_cluster = {}

    for _, row in work.sort_values("published_date", ascending=False).iterrows():
        placed = False

        for cluster in clusters:
            same_topic = row["primary_topic"] == cluster["primary_topic"]
            similarity = jaccard_similarity(row["token_set"], cluster["token_union"])
            shared = len(row["token_set"] & cluster["token_union"])

            if same_topic and similarity >= similarity_threshold and shared >= min_shared_tokens:
                cluster["rows"].append(row)
                cluster["token_union"] = cluster["token_union"] | row["token_set"]
                article_to_cluster[row["original_index"]] = cluster["cluster_id"]
                placed = True
                break

        if not placed:
            new_cluster = {
                "cluster_id": f"cluster_{len(clusters) + 1}",
                "primary_topic": row["primary_topic"],
                "rows": [row],
                "token_union": set(row["token_set"]),
            }
            clusters.append(new_cluster)
            article_to_cluster[row["original_index"]] = new_cluster["cluster_id"]

    cluster_rows = []

    now_utc = pd.Timestamp.utcnow()

    for cluster in clusters:
        rows = cluster["rows"]
        cluster_df = pd.DataFrame(rows)

        article_count = len(cluster_df)
        source_count = cluster_df["source"].nunique()
        latest_date = cluster_df["published_date"].max()

        all_tokens = []
        for tokens in cluster_df["cluster_text"].apply(tokenize):
            all_tokens.extend(tokens)

        token_counts = Counter(
            t for t in all_tokens if t not in GENERIC_TOPIC_WORDS
        )

        top_terms = [word for word, _ in token_counts.most_common(4)]
        if top_terms:
            label = " / ".join(w.title() for w in top_terms[:3])
        else:
            label = cluster_df.iloc[0]["headline"][:80]

        representative_headline = cluster_df.iloc[0]["headline"]

        hours_since_latest = 999
        if pd.notnull(latest_date):
            hours_since_latest = max(
                1,
                (now_utc - latest_date).total_seconds() / 3600
            )

        recency_bonus = max(0, 24 - min(hours_since_latest, 24)) / 24
        score = article_count * 3 + source_count * 2 + recency_bonus

        cluster_rows.append({
            "cluster_id": cluster["cluster_id"],
            "label": label,
            "primary_topic": cluster["primary_topic"],
            "article_count": article_count,
            "source_count": source_count,
            "latest_date": latest_date,
            "score": score,
            "representative_headline": representative_headline,
        })

    trending_df = pd.DataFrame(cluster_rows).sort_values(
        ["score", "article_count", "source_count", "latest_date"],
        ascending=[False, False, False, False]
    )

    return trending_df, article_to_cluster


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

filtered = filtered.sort_values("published_date", ascending=False)

# -------------------------
# TRENDING TOPICS
# -------------------------
st.subheader("🔥 Trending Topics")

trending_df, article_to_cluster = cluster_articles(filtered)

if trending_df.empty:
    st.info("No trending topics found for the current filters.")
else:
    top_clusters = trending_df.head(8).copy()

    selected_cluster_id = st.session_state.get("selected_cluster_id")

    cluster_cols = st.columns(2)
    for i, (_, cluster) in enumerate(top_clusters.iterrows()):
        with cluster_cols[i % 2]:
            st.markdown(
                f"""
                **{cluster['label']}**  
                Topic: {cluster['primary_topic']}  
                Articles: {cluster['article_count']} | Sources: {cluster['source_count']}
                """
            )
            if st.button(
                f"Open topic: {cluster['label']}",
                key=f"open_{cluster['cluster_id']}",
                use_container_width=True
            ):
                st.session_state["selected_cluster_id"] = cluster["cluster_id"]

    selected_cluster_id = st.session_state.get("selected_cluster_id")

    if selected_cluster_id:
        selected_cluster_meta = trending_df[
            trending_df["cluster_id"] == selected_cluster_id
        ]

        if not selected_cluster_meta.empty:
            meta = selected_cluster_meta.iloc[0]

            related_indices = [
                idx for idx, cid in article_to_cluster.items() if cid == selected_cluster_id
            ]
            related_articles = filtered.loc[related_indices].sort_values(
                "published_date", ascending=False
            )

            st.divider()
            st.subheader(f"🧩 Topic Cluster: {meta['label']}")
            st.write(
                f"**Primary topic:** {meta['primary_topic']}  \n"
                f"**Articles:** {meta['article_count']}  \n"
                f"**Sources:** {meta['source_count']}"
            )

            st.markdown("**Related articles**")
            for _, row in related_articles.iterrows():
                render_article_card(row)

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
