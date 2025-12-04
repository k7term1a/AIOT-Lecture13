#!/usr/bin/env python3
"""Fetch JSON from CWA URL, parse weather data, and store into SQLite.

Usage:
    python fetch_and_store.py

This will create (or open) `data.db` in the same folder and insert parsed rows into `weather` table.
"""
import requests
import sqlite3
import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

URL = (
    "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/O-A0002-001?"
    "Authorization=CWA-6A637F34-99A3-4F45-8497-EAB9D7FA1CA7&downloadType=WEB&format=JSON"
)
DB_PATH = "data.db"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def fetch_json(url: str) -> Any:
    try:
        logging.info(f"Requesting: {url}")
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Failed to fetch JSON: {e}")
        raise


def _get_first_available(obj: Dict, keys: List[str]) -> Optional[Any]:
    for k in keys:
        if k in obj and obj[k] not in (None, ""):
            return obj[k]
    return None


def parse_locations(data: Any) -> List[Dict[str, Optional[str]]]:
    """Try to parse common CWA / open-data JSON structures into rows.

    Returns list of dict with keys: location, date, min_temp, max_temp, description
    """
    rows: List[Dict[str, Optional[str]]] = []

    # helper: detect cwaopendata.dataset.Station
    def _get_station_list(d: Any) -> Optional[List[Dict]]:
        try:
            if isinstance(d, dict) and "cwaopendata" in d and isinstance(d["cwaopendata"], dict):
                ds = d["cwaopendata"].get("dataset")
                if isinstance(ds, dict):
                    st = ds.get("Station")
                    if isinstance(st, list):
                        return st
        except Exception:
            return None
        return None

    # Find candidate location list
    locations = None
    # check cwaopendata dataset Station first
    stations = _get_station_list(data)
    if stations:
        locations = stations
    else:
        if isinstance(data, dict):
            if "records" in data:
                rec = data["records"]
                if isinstance(rec, dict) and "location" in rec:
                    locations = rec["location"]
                elif isinstance(rec, list):
                    locations = rec
            if locations is None:
                # typical CWA datasets sometimes expose a top-level list under 'location' or 'locations'
                if "location" in data and isinstance(data["location"], list):
                    locations = data["location"]
                elif "locations" in data and isinstance(data["locations"], list):
                    locations = data["locations"]
                elif "features" in data and isinstance(data["features"], list):
                    # GeoJSON-like
                    locations = [f.get("properties", {}) for f in data["features"]]
        elif isinstance(data, list):
            locations = data

    if not locations:
        logging.warning("No location list found in JSON; attempting to interpret top-level dict as one record")
        if isinstance(data, dict):
            locations = [data]
        else:
            return rows

    for loc in locations:
        try:
            if not isinstance(loc, dict):
                continue

            # If Station format, prefer StationName and ObsTime.DateTime
            location_name = None
            if "StationName" in loc:
                location_name = loc.get("StationName")
            if not location_name:
                location_name = _get_first_available(loc, ["locationName", "location", "name", "area", "county", "city"]) or "Unknown"

            # Date heuristics
            date_val = None
            # 1) common: 'time' -> list -> startTime
            if "time" in loc and isinstance(loc["time"], list) and loc["time"]:
                first_time = loc["time"][0]
                date_val = _get_first_available(first_time, ["startTime", "dataTime", "time"]) or date_val

            # If Station format, check ObsTime.DateTime
            if not date_val and "ObsTime" in loc and isinstance(loc["ObsTime"], dict):
                dt = loc["ObsTime"].get("DateTime")
                if dt:
                    date_val = dt

            # 2) weatherElement entries may have time lists
            if not date_val and "weatherElement" in loc and isinstance(loc["weatherElement"], list):
                for e in loc["weatherElement"]:
                    if isinstance(e, dict) and "time" in e and isinstance(e["time"], list) and e["time"]:
                        t0 = e["time"][0]
                        date_val = _get_first_available(t0, ["startTime", "dataTime"]) or date_val
                        if date_val:
                            break

            # 3) fallback: direct keys
            date_val = date_val or _get_first_available(loc, ["date", "forecastDate", "dataTime"]) or datetime.utcnow().isoformat()

            # Temperatures and description heuristics
            min_temp = None
            max_temp = None
            description = None

            if "weatherElement" in loc and isinstance(loc["weatherElement"], list):
                for element in loc["weatherElement"]:
                    if not isinstance(element, dict):
                        continue
                    name = element.get("elementName") or element.get("element") or element.get("parameterName") or element.get("name")

                    # parameter value could be under element['time'][0]['parameter']['parameterName']
                    val = None
                    if "time" in element and isinstance(element["time"], list) and element["time"]:
                        t0 = element["time"][0]
                        if isinstance(t0, dict):
                            # many variants
                            val = _get_first_available(t0, ["startTime", "dataTime"])  # not the value
                            # parameter nested
                            param = None
                            if "parameter" in t0 and isinstance(t0["parameter"], dict):
                                param = t0["parameter"].get("parameterName") or t0["parameter"].get("parameterValue")
                            elif "elementValue" in t0 and isinstance(t0["elementValue"], dict):
                                param = t0["elementValue"].get("value")
                            if param is not None:
                                val = param
                    # direct 'parameter' on element
                    if val is None:
                        val = _get_first_available(element, ["parameter", "value", "elementValue", "forecast", "parameterName"]) or None

                    if val is None and isinstance(element.get("time"), list) and element["time"]:
                        # sometimes parameter nested deeper
                        try:
                            val = element["time"][0].get("parameter", {}).get("parameterName")
                        except Exception:
                            val = None

                    lname = (name or "").lower() if name else ""
                    if any(k in lname for k in ["mint", "min", "tmin"]):
                        min_temp = val
                    elif any(k in lname for k in ["maxt", "max", "tmax"]):
                        max_temp = val
                    elif any(k in lname for k in ["wx", "weather", "description", "wxvalue", "wx_desc"]):
                        description = val
                    elif any(k in lname for k in ["temp"]):
                        # some datasets have a temp field - skip
                        pass

            # If description not found, try other keys
            if not description:
                description = _get_first_available(loc, ["description", "weather", "wx", "parameterName"]) or description

            rows.append({
                "location": str(location_name),
                "date": str(date_val),
                "min_temp": (None if min_temp in (None, "") else str(min_temp)),
                "max_temp": (None if max_temp in (None, "") else str(max_temp)),
                "description": (None if description in (None, "") else str(description)),
            })
        except Exception as e:
            logging.debug(f"Skipping invalid location record: {e}")
            continue

    return rows


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            date TEXT,
            min_temp REAL,
            max_temp REAL,
            description TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()

    # precipitation table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS precipitation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            date TEXT,
            period TEXT,
            precipitation REAL,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def insert_rows(conn: sqlite3.Connection, rows: List[Dict[str, Optional[str]]]) -> int:
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        try:
            # Try to coerce min/max to float when possible
            minv = float(r["min_temp"]) if r.get("min_temp") not in (None, "") else None
            maxv = float(r["max_temp"]) if r.get("max_temp") not in (None, "") else None
            cur.execute(
                "INSERT INTO weather (location, date, min_temp, max_temp, description) VALUES (?, ?, ?, ?, ?)",
                (r.get("location"), r.get("date"), minv, maxv, r.get("description")),
            )
            inserted += 1
        except Exception as e:
            logging.debug(f"Failed to insert row {r}: {e}")
            continue
    conn.commit()
    return inserted


def parse_precipitation(data: Any) -> List[Dict[str, Optional[str]]]:
    """Parse RainfallElement from JSON into list of records:
       {location, date, period, precipitation}
    """
    out: List[Dict[str, Optional[str]]] = []

    # helper: detect cwaopendata.dataset.Station
    def _get_station_list(d: Any) -> Optional[List[Dict]]:
        try:
            if isinstance(d, dict) and "cwaopendata" in d and isinstance(d["cwaopendata"], dict):
                ds = d["cwaopendata"].get("dataset")
                if isinstance(ds, dict):
                    st = ds.get("Station")
                    if isinstance(st, list):
                        return st
        except Exception:
            return None
        return None

    # locate records list similar to parse_locations
    locations = None
    stations = _get_station_list(data)
    if stations:
        locations = stations
    if isinstance(data, dict):
        if "records" in data:
            rec = data["records"]
            if isinstance(rec, dict) and "location" in rec:
                locations = rec["location"]
            elif isinstance(rec, list):
                locations = rec
        if locations is None:
            if "location" in data and isinstance(data["location"], list):
                locations = data["location"]
            elif "locations" in data and isinstance(data["locations"], list):
                locations = data["locations"]
            elif "features" in data and isinstance(data["features"], list):
                locations = [f.get("properties", {}) for f in data["features"]]
    elif isinstance(data, list):
        locations = data

    if not locations:
        return out

    for loc in locations:
        try:
            if not isinstance(loc, dict):
                continue
            # prefer StationName if present
            location_name = None
            if "StationName" in loc:
                location_name = loc.get("StationName")
            if not location_name:
                location_name = _get_first_available(loc, ["locationName", "location", "name", "area", "county", "city"]) or "Unknown"

            # extract date: prefer ObsTime.DateTime for Station format
            date_val = None
            if "ObsTime" in loc and isinstance(loc["ObsTime"], dict):
                date_val = loc["ObsTime"].get("DateTime") or date_val
            if not date_val and "time" in loc and isinstance(loc["time"], list) and loc["time"]:
                first_time = loc["time"][0]
                date_val = _get_first_available(first_time, ["startTime", "dataTime", "time"]) or date_val
            if not date_val and "weatherElement" in loc and isinstance(loc["weatherElement"], list):
                for e in loc["weatherElement"]:
                    if isinstance(e, dict) and "time" in e and isinstance(e["time"], list) and e["time"]:
                        t0 = e["time"][0]
                        date_val = _get_first_available(t0, ["startTime", "dataTime"]) or date_val
                        if date_val:
                            break
            date_val = date_val or _get_first_available(loc, ["date", "forecastDate", "dataTime"]) or datetime.utcnow().isoformat()

            rf = loc.get("RainfallElement") or loc.get("Rainfall") or loc.get("rainfall")
            if isinstance(rf, dict):
                for period, sub in rf.items():
                    # sub may be dict like {"Precipitation":"0.0"} or nested
                    val = None
                    if isinstance(sub, dict):
                        val = _get_first_available(sub, ["Precipitation", "Precip", "Value", "precipitation", "value"]) or None
                    else:
                        val = sub
                    try:
                        precip = float(val) if val not in (None, "") else None
                    except Exception:
                        precip = None
                    out.append({
                        "location": str(location_name),
                        "date": str(date_val),
                        "period": str(period),
                        "precipitation": (None if precip is None else precip),
                    })
        except Exception:
            continue

    return out


def insert_precip_rows(conn: sqlite3.Connection, rows: List[Dict[str, Optional[str]]]) -> int:
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        try:
            p = float(r.get("precipitation")) if r.get("precipitation") not in (None, "") else None
            cur.execute(
                "INSERT INTO precipitation (location, date, period, precipitation) VALUES (?, ?, ?, ?)",
                (r.get("location"), r.get("date"), r.get("period"), p),
            )
            inserted += 1
        except Exception:
            continue
    conn.commit()
    return inserted


def main():
    try:
        data = fetch_json(URL)
    except Exception:
        logging.error("Exiting due to fetch error.")
        sys.exit(1)

    # Save raw JSON for inspection
    try:
        with open("raw.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.info("Saved raw JSON to raw.json")
    except Exception as e:
        logging.warning(f"Failed to save raw JSON: {e}")

    rows = parse_locations(data)
    logging.info(f"Parsed {len(rows)} rows from JSON")

    if not rows:
        logging.error("No rows to insert. Exiting.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        n = insert_rows(conn, rows)
        logging.info(f"Inserted {n} rows into {DB_PATH} (weather)")

        # parse and insert precipitation rows if present
        precip_rows = parse_precipitation(data)
        if precip_rows:
            m = insert_precip_rows(conn, precip_rows)
            logging.info(f"Inserted {m} rows into {DB_PATH} (precipitation)")
        else:
            logging.info("No precipitation records found in JSON")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
