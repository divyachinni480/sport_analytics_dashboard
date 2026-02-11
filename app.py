import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# ---------------- STYLE (COLORS & BACKGROUND) ----------------
st.markdown("""
<style>
.stApp {
    background: linear-gradient(to right, #f4f7fb, #e8f0fe);
}

/* Title */
h1 {
    color: #1f4e79;
    text-align: center;
    font-weight: bold;
}
h2, h3 {
    color: #2c7be5;
}

/* Metric cards */
div[data-testid="metric-container"] {
    background-color: #ffffff;
    border-radius: 14px;
    padding: 15px;
    box-shadow: 3px 3px 12px rgba(0,0,0,0.1);
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(to bottom, #dbeafe, #bfdbfe);
}

/* Dataframe header */
thead tr th {
    background-color: #2c7be5 !important;
    color: white !important;
}

/* Buttons */
button {
    background-color: #2563eb !important;
    color: white !important;
    border-radius: 10px;
}

/* Tabs */
button[data-baseweb="tab"] {
    background-color: #dbeafe !important;
    border-radius: 8px;
    margin: 2px;
}
</style>
""", unsafe_allow_html=True)

# ---------------- DB CONNECTION ----------------
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="Game_Analytics_sportsradar",
        user="postgres",
        password="divya@12"
    )

def fetch_data(query):
    try:
        conn = get_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

# ---------------- PAGE CONFIG ----------------
st.set_page_config(page_title="Sports Analytics Dashboard", layout="wide")
st.title("🏟️ Sports Analytics Dashboard")

# ================== HOMEPAGE DASHBOARD ==================
st.subheader("📊 Overview")

col1, col2, col3, col4 = st.columns(4)

total_competitors = fetch_data("SELECT COUNT(*) AS total FROM competitors")["total"][0]
total_countries = fetch_data("SELECT COUNT(DISTINCT country) AS total FROM competitors")["total"][0]
total_venues = fetch_data("SELECT COUNT(*) AS total FROM venues")["total"][0]
total_complexes = fetch_data("SELECT COUNT(*) AS total FROM complexes")["total"][0]

col1.metric("Total Competitors", total_competitors)
col2.metric("Countries", total_countries)
col3.metric("Venues", total_venues)
col4.metric("Complexes", total_complexes)

st.divider()

# ================== SEARCH ==================
st.subheader("🔍 Search Competitor")
search_name = st.text_input("Enter competitor name")

if search_name:
    df_search = fetch_data(f"""
        SELECT competitor_id, name, country, country_code, abbreviation
        FROM competitors
        WHERE name ILIKE '%{search_name}%';
    """)
    st.dataframe(df_search, use_container_width=True)

st.divider()

# ================== FILTERS ==================
st.subheader("🎛️ Filters")

f1, f2 = st.columns(2)

countries = fetch_data("SELECT DISTINCT country FROM competitors")["country"].tolist()
selected_country = f1.selectbox("Filter by Country", ["All"] + countries)

complexes = fetch_data("SELECT complex_name FROM complexes")["complex_name"].tolist()
selected_complex = f2.selectbox("Filter Venues by Complex", ["All"] + complexes)

# ================== TABS ==================
tab1, tab2 = st.tabs(["🏅 Competitors", "🌍 Venues & Complexes"])

# =====================================================
#                   COMPETITORS
# =====================================================
with tab1:
    st.header("Competitors Analysis")

    if selected_country == "All":
        query = "SELECT competitor_id, name, country, country_code, abbreviation FROM competitors;"
    else:
        query = f"""
            SELECT competitor_id, name, country, country_code, abbreviation
            FROM competitors
            WHERE country = '{selected_country}';
        """

    df_comp = fetch_data(query)
    st.dataframe(df_comp, use_container_width=True)

    df_count = fetch_data("""
        SELECT country, COUNT(*) AS competitor_count
        FROM competitors
        GROUP BY country
        ORDER BY competitor_count DESC;
    """)

    fig1 = px.bar(df_count, x="country", y="competitor_count",
                  title="Competitors per Country")
    st.plotly_chart(fig1, use_container_width=True)

# =====================================================
#                VENUES & COMPLEXES
# =====================================================
with tab2:
    st.header("Venues & Complexes Analysis")

    if selected_complex == "All":
        df_venues = fetch_data("""
            SELECT v.venue_name, v.city_name, v.country_name, v.timezone, c.complex_name
            FROM venues v
            JOIN complexes c ON v.complex_id = c.complex_id;
        """)
    else:
        df_venues = fetch_data(f"""
            SELECT v.venue_name, v.city_name, v.country_name, v.timezone, c.complex_name
            FROM venues v
            JOIN complexes c ON v.complex_id = c.complex_id
            WHERE c.complex_name = '{selected_complex}';
        """)

    st.dataframe(df_venues, use_container_width=True)

    df_venue_count = fetch_data("""
        SELECT c.complex_name, COUNT(v.venue_id) AS venue_count
        FROM complexes c
        JOIN venues v ON c.complex_id = v.complex_id
        GROUP BY c.complex_name
        ORDER BY venue_count DESC;
    """)

    fig2 = px.bar(df_venue_count, x="complex_name", y="venue_count",
                  title="Venues per Complex")
    st.plotly_chart(fig2, use_container_width=True)

# ================== FOOTER ==================
st.success("✅ Dashboard Loaded Successfully")
