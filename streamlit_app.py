import streamlit as st
import pandas as pd

st.set_page_config(page_title="Daily Intelligence Dashboard", layout="wide")

@st.cache_data
def load_data():
    df = pd.read_csv("daily_output/articles_tagged.csv")
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
st.caption("Overview of collected news across sources and topics")

st.sidebar.header("Filters")

all_topics = sorted(set(t for topics in df["topics_list"] for t in topics))
all_sources = sorted(df["source"].dropna().unique().tolist())

selected_topics = st.sidebar.multiselect("Topics", all_topics)
selected_sources = st.sidebar.multiselect("Sources", all_sources)
search_text = st.sidebar.text_input("Search headline")

filtered = df.copy()
st.sidebar.subheader("📅 Date Filter")

min_date = df["published_date"].min()
max_date = df["published_date"].max()

start_date, end_date = st.sidebar.date_input(
    "Select date range",
    [min_date.date(), max_date.date()]
)
filtered = df[
    (df["published_date"].dt.date >= start_date) &
    (df["published_date"].dt.date <= end_date)
]

if selected_topics:
    filtered = filtered[
        filtered["topics_list"].apply(lambda topics: any(t in topics for t in selected_topics))
    ]

if selected_sources:
    filtered = filtered[filtered["source"].isin(selected_sources)]

if search_text.strip():
    filtered = filtered[
        filtered["headline"].str.contains(search_text, case=False, na=False)
    ]

filtered = filtered.sort_values("published_date", ascending=False)

col1, col2, col3 = st.columns(3)
col1.metric("Articles", len(filtered))
col2.metric("Sources", filtered["source"].nunique())
col3.metric(
    "Topics",
    filtered["topics_list"].explode().nunique() if not filtered.empty else 0
)

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Topic distribution")
    if not filtered.empty:
        topic_counts = filtered["topics_list"].explode().value_counts()
        st.bar_chart(topic_counts)
    else:
        st.info("No data for selected filters.")

with right:
    st.subheader("Source distribution")
    if not filtered.empty:
        source_counts = filtered["source"].value_counts()
        st.bar_chart(source_counts)
    else:
        st.info("No data for selected filters.")

st.divider()

st.subheader("Articles table")

if not filtered.empty:
    table_df = filtered[["published_date", "source", "headline", "topics", "summary", "url"]].copy()
    st.dataframe(table_df, use_container_width=True)
else:
    st.info("No articles match the current filters.")

st.divider()

st.subheader("Latest article previews")

if not filtered.empty:
    for _, row in filtered.head(20).iterrows():
        st.markdown(f"### {row['headline']}")
        st.write(f"**Source:** {row['source']}")
        st.write(f"**Published:** {row['published_date']}")
        st.write(f"**Topics:** {row['topics']}")
        if str(row['summary']).strip():
            st.write(f"**Summary:** {row['summary']}")
        else:
            st.write("**Summary:** No summary available")
        if str(row["url"]).strip():
            st.markdown(f"[Open article]({row['url']})")
        st.markdown("---")
else:
    st.info("Nothing to show.")
