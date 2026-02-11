import os
import json
import psycopg2

# --------------------
# CONFIG
# --------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "..", "data", "raw")

# RAW JSON FILES
competitions_path = os.path.join(RAW_DIR, "competitions.json")
rankings_path = os.path.join(RAW_DIR, "rankings.json")
complexes_path = os.path.join(RAW_DIR, "complexes.json")

# --------------------
# CONNECT TO POSTGRES
# --------------------
conn = psycopg2.connect(
    host="localhost",
    database="Game_Analytics_sportsradar",
    user="postgres",
    password="divya@12"
)
cur = conn.cursor()

# ===============================
# 1️⃣ INSERT CATEGORIES + COMPETITIONS
# ===============================
with open(competitions_path, "r", encoding="utf-8") as f:
    comp_data = json.load(f)

for comp in comp_data.get("competitions", []):
    category = comp.get("category", {})
    category_id = category.get("id")
    category_name = category.get("name")

    if category_id:
        cur.execute("""
            INSERT INTO Categories (category_id, category_name)
            VALUES (%s, %s)
            ON CONFLICT (category_id) DO NOTHING;
        """, (category_id, category_name))

    cur.execute("""
        INSERT INTO Competitions
        (competition_id, competition_name, parent_id, type, gender, category_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (competition_id) DO NOTHING;
    """, (
        comp.get("id"),
        comp.get("name"),
        comp.get("parent_id"),
        comp.get("type"),
        comp.get("gender"),
        category_id
    ))

# ===============================
# 2️⃣ INSERT COMPETITORS + RANKINGS
# ===============================
with open(rankings_path, "r", encoding="utf-8") as f:
    rankings_data = json.load(f)

for ranking_list in rankings_data.get("rankings", []):
    competitor_rankings = ranking_list.get("competitor_rankings", [])
    for r in competitor_rankings:
        competitor = r.get("competitor", {})
        competitor_id = competitor.get("id")
        name = competitor.get("name")
        country = competitor.get("country")

        if competitor_id:
            cur.execute("""
                INSERT INTO Competitors (competitor_id, name, country)
                VALUES (%s, %s, %s)
                ON CONFLICT (competitor_id) DO NOTHING;
            """, (competitor_id, name, country))

            cur.execute("""
                INSERT INTO Competitor_Rankings
                (rank, movement, points, competitions_played, competitor_id)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                r.get("rank"),
                r.get("movement"),
                r.get("points"),
                r.get("competitions_played"),
                competitor_id
            ))

# ===============================
# 3️⃣ INSERT COMPLEXES + VENUES
# ===============================
with open(complexes_path, "r", encoding="utf-8") as f:
    complexes_data = json.load(f)

for c in complexes_data.get("complexes", []):
    complex_id = c.get("id")
    complex_name = c.get("name")

    if complex_id:
        cur.execute("""
            INSERT INTO Complexes (complex_id, complex_name)
            VALUES (%s, %s)
            ON CONFLICT (complex_id) DO NOTHING;
        """, (complex_id, complex_name))

    # Venues under each complex
    for v in c.get("venues", []):
        venue_id = v.get("id")
        venue_name = v.get("name")
        city_name = v.get("city_name")
        country_name = v.get("country_name")
        country_code = v.get("country_code")
        timezone = v.get("timezone")

        if venue_id:
            cur.execute("""
                INSERT INTO Venues
                (venue_id, venue_name, city_name, country_name, country_code, timezone, complex_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (venue_id) DO NOTHING;
            """, (
                venue_id, venue_name, city_name, country_name,
                country_code, timezone, complex_id
            ))

# --------------------
# COMMIT & CLOSE
# --------------------
conn.commit()
cur.close()
conn.close()

print("✅ ETL complete: All tables loaded successfully")
