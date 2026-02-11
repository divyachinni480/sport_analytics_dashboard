import requests
import os
import json
import psycopg2

# --------------------
# CONFIG
# --------------------
API_KEY = "VllzKU2gyXidO5zYhsgiwcVIG9Cen2SIFagI6O1b"  # Replace with your API key
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

# API ENDPOINTS
COMPETITIONS_URL = f"https://api.sportradar.com/tennis/trial/v3/en/competitions.json?api_key={API_KEY}"
RANKINGS_URL = f"https://api.sportradar.com/tennis/trial/v3/en/rankings.json?api_key={API_KEY}"
COMPLEXES_URL = f"https://api.sportradar.com/tennis/trial/v3/en/complexes.json?api_key={API_KEY}"


# --------------------
# FETCH AND SAVE RAW JSON
# --------------------
def fetch_and_save(url, filename):
    try:
        print(f"Fetching {filename} ...")
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        path = os.path.join(RAW_DIR, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print(f"Saved {filename} with {len(data.get('competitions', data.get('rankings', data.get('complexes', []))))} items")
        return data
    except requests.exceptions.HTTPError as e:
        print(f"Failed to fetch {filename}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error for {filename}: {e}")
        return None


# --------------------
# ETL INTO DATABASE
# --------------------
def load_to_db():
    conn = psycopg2.connect(
        host="localhost",
        database="Game_Analytics_sportsradar",
        user="postgres",
        password="divya@12"
    )
    cur = conn.cursor()

    # --- Categories + Competitions ---
    competitions_data = fetch_and_save(COMPETITIONS_URL, "competitions.json")
    if competitions_data:
        for comp in competitions_data.get("competitions", []):
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

    # --- Competitors + Rankings ---
    rankings_data = fetch_and_save(RANKINGS_URL, "rankings.json")
    if rankings_data:
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

    # --- Complexes + Venues ---
    complexes_data = fetch_and_save(COMPLEXES_URL, "complexes.json")
    if complexes_data:
        for c in complexes_data.get("complexes", []):
            complex_id = c.get("id")
            complex_name = c.get("name")

            if complex_id:
                cur.execute("""
                    INSERT INTO Complexes (complex_id, complex_name)
                    VALUES (%s, %s)
                    ON CONFLICT (complex_id) DO NOTHING;
                """, (complex_id, complex_name))

            # Venues under this complex
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

    # --- Commit & Close ---
    conn.commit()
    cur.close()
    conn.close()
    print("✅ FULL ETL complete: All tables loaded")


# --------------------
# MAIN
# --------------------
if __name__ == "__main__":
    load_to_db()
