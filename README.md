# CWA Weather: fetch, store, view

This small Python project downloads JSON weather data from the Central Weather Administration open data endpoint, parses location-level records, stores them into a local SQLite database (`data.db`), and provides a Streamlit app to view the stored data.

Files
- `fetch_and_store.py`: Download JSON, parse records, insert into `data.db`.
- `app.py`: Streamlit app to read `data.db` and display rows.
- `schema.sql`: SQL `CREATE TABLE` statement used for the database structure.
- `requirements.txt`: Python dependencies.

Usage

1. (Recommended) Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/Scripts/activate      # on Windows (bash)
# or
# source .venv/bin/activate        # Linux / macOS
```

2. Install dependencies

```bash
pip install -r requirements.txt
```

3. Fetch and store data

```bash
python fetch_and_store.py
```

This will create `data.db` in the same folder and populate the `weather` table.

4. Run the Streamlit app

```bash
streamlit run app.py
```

Open the displayed URL (default `http://localhost:8501`) to view the table. Use the sidebar to filter by location.

Database schema

The `weather` table structure (also in `schema.sql`):

```sql
CREATE TABLE IF NOT EXISTS weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location TEXT,
    date TEXT,
    min_temp REAL,
    max_temp REAL,
    description TEXT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Notes
- The parser in `fetch_and_store.py` uses heuristics to support common CWA JSON shapes. Depending on upstream API changes, some fields may be None. The script logs parsing/insertion info.
- If you want to re-create the DB from scratch, remove `data.db` and run `python fetch_and_store.py` again.

Questions or improvements
- I can add more robust parsing for additional fields, deduplication logic, or a small web UI for filtering/sorting — tell me which you'd like.
