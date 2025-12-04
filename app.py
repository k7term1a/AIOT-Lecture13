"""Streamlit app to display weather rows from `data.db`.

Run with:
    streamlit run app.py
"""
import streamlit as st
import pandas as pd
import sqlite3
import altair as alt

DB_PATH = "data.db"


@st.cache_data
def load_data(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT id, location, date, min_temp, max_temp, description, inserted_at FROM weather ORDER BY date DESC, id DESC",
            conn,
        )
        return df
    finally:
        conn.close()


@st.cache_data
def load_precip(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT id, location, date, period, precipitation, inserted_at FROM precipitation ORDER BY date DESC, id DESC",
            conn,
        )
        return df
    finally:
        conn.close()


def main():
    st.title("Weather Data Viewer")
    st.write("Displays parsed weather data stored in `data.db`.")

    try:
        df = load_data(DB_PATH)
    except Exception as e:
        st.error(f"Failed to load data from {DB_PATH}: {e}")
        return

    st.sidebar.write(f"Rows: {len(df)}")

    if df.empty:
        st.info("No data found. Run `python fetch_and_store.py` to fetch and store data.")
        return

    locations = ["All"] + sorted(df["location"].dropna().unique().tolist())
    sel = st.sidebar.selectbox("Filter by location", locations)

    if sel != "All":
        df = df[df["location"] == sel]

    st.dataframe(df)

    # precipitation plot
    try:
        pdf = load_precip(DB_PATH)
    except Exception as e:
        st.warning(f"Cannot load precipitation data: {e}")
        return

    if pdf.empty:
        st.info("No precipitation data available.")
        return

    # filter by location for precip (default to same selection as weather)
    locs_precip = ["All"] + sorted(pdf["location"].dropna().unique().tolist())
    sel_precip_loc = st.sidebar.selectbox("Precipitation: select location", locs_precip, index=0)
    if sel_precip_loc == "All":
        # if user selected a specific location in the weather table, prefer that
        if sel != "All":
            sel_precip_loc = sel

    if sel_precip_loc != "All":
        plot_df = pdf[pdf["location"] == sel_precip_loc].copy()
    else:
        plot_df = pdf.copy()

    if plot_df.empty:
        st.info("No precipitation records for selection.")
        return

    st.subheader("Precipitation Viewer")
    st.write("Choose a record time to view precipitation across periods, or view a time-series of a given period.")

    # offer two modes: period-as-x (user requested) or time-series
    mode = st.sidebar.radio("Plot mode", ["Period as X-axis", "Time series (date as X)"])

    PERIOD_ORDER = [
        "Now",
        "Past10Min",
        "Past1hr",
        "Past3hr",
        "Past6hr",
        "Past12hr",
        "Past24hr",
        "Past2days",
        "Past3days",
    ]

    if mode == "Period as X-axis":
        # select a specific observation time (date)
        dates = sorted(plot_df["date"].dropna().unique().tolist(), reverse=True)
        if not dates:
            st.info("No valid observation dates available for plotting.")
        else:
            date_opts = ["Latest"] + dates
            sel_date = st.sidebar.selectbox("Select observation time", date_opts, index=0)
            chosen_date = dates[0] if sel_date == "Latest" else sel_date

            recs = plot_df[plot_df["date"] == chosen_date]
            if recs.empty:
                st.info("No precipitation records for that date.")
            else:
                # map periods to values
                val_map = {r["period"]: r["precipitation"] for _, r in recs.iterrows()}
                vals = [val_map.get(p, None) for p in PERIOD_ORDER]
                periods_df = pd.DataFrame({"period": PERIOD_ORDER, "precipitation": vals})
                st.write(f"Precipitation for {sel_precip_loc} at {chosen_date}")
                chart = alt.Chart(periods_df).mark_line(point=True).encode(
                    x=alt.X("period:N", sort=PERIOD_ORDER, title="Period"),
                    y=alt.Y("precipitation:Q", title="Precipitation (mm)"),
                ).properties(width=700)
                st.altair_chart(chart, use_container_width=True)
                st.dataframe(periods_df)

    else:
        # Time series mode: user picks a period (or All)
        periods = ["All"] + sorted(plot_df["period"].dropna().unique().tolist(), key=lambda x: PERIOD_ORDER.index(x) if x in PERIOD_ORDER else 999)
        sel_period = st.sidebar.selectbox("Filter by period (e.g. Past24hr)", periods)
        if sel_period != "All":
            ts = plot_df[plot_df["period"] == sel_period].copy()
            ts["date_parsed"] = pd.to_datetime(ts["date"], errors="coerce")
            ts = ts.sort_values("date_parsed")
            if ts.empty or ts["date_parsed"].isna().all():
                st.info("No valid time series data for that period.")
            else:
                st.line_chart(ts.set_index("date_parsed")["precipitation"])
        else:
            # pivot so multiple periods can be shown
            plot_df["date_parsed"] = pd.to_datetime(plot_df["date"], errors="coerce")
            plot_df = plot_df.sort_values("date_parsed")
            pivot = plot_df.pivot_table(index="date_parsed", columns="period", values="precipitation", aggfunc="mean")
            if pivot.empty:
                st.info("No valid time series data to plot.")
            else:
                st.line_chart(pivot)


if __name__ == "__main__":
    main()
