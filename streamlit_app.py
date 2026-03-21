import streamlit as st
import pandas as pd

st.set_page_config(page_title="Daily Intelligence Dashboard", layout="wide")

# -------------------------
# LOAD DATA
# -------------------------
@st.cache_data
def load_data():
    df = pd.read_csv("articles_tagged.csv")
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")
    df["topics"] = df["topics"].fillna("")
    df["summary"] = df.get("summary", "").fillna("")
    df["source"] = df["source"].fillna("")
    df["headline"] = df["headline"].fillna("")
    df["url"] = df["url"].fillna("")
    df["topics_list"] = df["topics"].apply(
        lambda x: [t.strip() for t in str(x).split(",") if t.strip()]
    )
    return df

df = load_data()

st.title("🧠 Daily Intelligence Dashboard")

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
        filtered["headline"].str.contains(search, case=False, na=False)
    ]

filtered = filtered.sort_values("published_date", ascending=False)

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
# CARDS
# -------------------------
st.subheader("Latest")

for _, row in filtered.head(15).iterrows():
    st.markdown(f"### {row['headline']}")
    st.write(f"**Source:** {row['source']}")
    st.write(f"**Topics:** {row['topics']}")
    st.markdown(f"[Read article]({row['url']})")
    st.markdown("---")
