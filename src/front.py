import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import streamlit as st
import polars as pl
from datetime import datetime
import subprocess

from src.consts import INDEX_PATH


@st.cache_data(ttl=1800)
def load_data():
    """
    Load data from index.parquet or run parser if missing.
    """
    if not INDEX_PATH.exists():
        try:
            subprocess.run(["python", "-m", "src.run"], check=True)
        except subprocess.CalledProcessError as e:
            st.error(f"Parser failed with error code {e.returncode}")
            return pl.DataFrame()

    if not INDEX_PATH.exists():
        st.error("Failed to create data file. Check logs.")
        return pl.DataFrame()

    return pl.read_parquet(INDEX_PATH)


def main():
    """
    Main UI for YC S25 startup tracker.
    """
    st.set_page_config(page_title="YC S25 Tracker", layout="wide")
    st.title("Y Combinator S25 Startup Tracker")

    df = load_data()
    if df.is_empty():
        st.warning("No data loaded.")
        return

    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    query = st.text_input("🔍 Search company name or description")
    if query:
        df = df.filter(
            pl.col("name").str.contains(query, case=False) |
            pl.col("long_description").str.contains(query, case=False)
        )

    df = df.select(pl.exclude("slug", "s25_tag"))
    st.dataframe(df.to_pandas(), use_container_width=True)

    if st.button("🔁 Refresh data now"):
        """
        Run backend parser and show logs.
        """
        with st.spinner("Running Parser..."):
            logs = []
            proc = subprocess.Popen(
                ["python", "-m", "src.run"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )

            for line in proc.stdout:
                if "[INFO]" in line:
                    logs.append(line.strip())

            proc.wait()
            st.session_state["refresh_logs"] = logs
            st.session_state["refresh_done"] = True

            st.cache_data.clear()
            st.rerun()
    
    if st.session_state.get("refresh_done"):
        st.success("✅ Just updated!")
        
        for log in st.session_state.get("refresh_logs", []):
            st.write(log)

        del st.session_state["refresh_done"]
        del st.session_state["refresh_logs"] 

if __name__ == "__main__":
    main()
