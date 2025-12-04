-- SQLite schema for weather data
CREATE TABLE IF NOT EXISTS weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location TEXT,
    date TEXT,
    min_temp REAL,
    max_temp REAL,
    description TEXT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- precipitation table: stores rainfall measurements per period (e.g. Past1hr, Past24hr)
CREATE TABLE IF NOT EXISTS precipitation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location TEXT,
    date TEXT,
    period TEXT,
    precipitation REAL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
