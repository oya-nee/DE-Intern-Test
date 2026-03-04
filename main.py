import requests
import polars as pl
import sqlite3
import logging
import sys
from datetime import datetime
from typing import List, Dict, Any

# --- CONFIGURATION ---
API_URL = "http://universities.hipolabs.com/search?country=Thailand"
DB_NAME = "intern_test.db"
LOG_FILE = "pipeline.log"
CSV_EXPORT = "universities_result.csv"

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)


# ============================================================
# E - EXTRACT
# ============================================================
def extract(url: str) -> List[Dict[str, Any]]:
    """ดึงข้อมูลจาก API พร้อม Error Handling"""
    try:
        logging.info(f"Extracting data from: {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Successfully retrieved {len(data)} records.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to extract data: {e}")
        sys.exit(1)


# ============================================================
# T - TRANSFORM
# ============================================================
def transform(data: List[Dict[str, Any]]) -> pl.DataFrame:
    """แปลงข้อมูลและทำ Data Cleaning"""
    try:
        logging.info("Transforming data with Polars...")
        df = pl.DataFrame(data)

        df_clean = df.select([
            pl.col("name").alias("university_name"),
            pl.col("web_pages").list.get(0).alias("website")
        ]).with_columns([
            pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("load_timestamp")
        ])

        # Data Quality: ลบ duplicate
        before = df_clean.height
        df_clean = df_clean.unique(subset=["university_name"])
        after = df_clean.height
        if before != after:
            logging.warning(f"Removed {before - after} duplicate records.")

        if df_clean.height == 0:
            raise ValueError("Data is empty after transformation.")

        logging.info(f"Transformation complete. {after} clean records ready.")
        return df_clean

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        sys.exit(1)


# ============================================================
# L - LOAD
# ============================================================
def load(df: pl.DataFrame, db_path: str):
    """โหลดข้อมูลลง SQLite (Idempotent Load)"""
    try:
        logging.info(f"Loading data into '{db_path}'...")
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS universities")
            cursor.execute("""
                CREATE TABLE universities (
                    university_name TEXT,
                    website         TEXT,
                    load_timestamp  TEXT
                )
            """)
            cursor.executemany(
                "INSERT INTO universities VALUES (?, ?, ?)",
                df.rows()
            )
            conn.commit()
        logging.info(f"Loaded {df.height} rows successfully.")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        sys.exit(1)


# ============================================================
# CHECK RESULTS  (มาจาก โค้ด 2)
# ============================================================
def check_results(db_path: str, csv_path: str):
    """ตรวจสอบผลลัพธ์และ Export CSV"""
    try:
        logging.info("Running result check...")
        with sqlite3.connect(db_path) as conn:
            df_all = pl.read_database("SELECT * FROM universities", conn)

        # Export CSV
        df_all.write_csv(csv_path)
        logging.info(f"Data exported to '{csv_path}'")

        # Preview ใน Terminal
        print("\n--- Preview Data (First 10 rows) ---")
        print(df_all.head(10))
        print(f"\nTotal records: {df_all.height}")

    except Exception as e:
        logging.error(f"Could not read database for check: {e}")


# ============================================================
# MAIN
# ============================================================
def main():
    start_time = datetime.now()
    logging.info("=" * 50)
    logging.info("  ETL Pipeline Started")
    logging.info("=" * 50)

    # E → T → L
    raw_data = extract(API_URL)
    clean_df = transform(raw_data)
    load(clean_df, DB_NAME)

    # Check & Export
    check_results(DB_NAME, CSV_EXPORT)

    duration = datetime.now() - start_time
    logging.info(f"Pipeline finished successfully in {duration.total_seconds():.2f}s")
    logging.info("=" * 50)


if __name__ == "__main__":
    main()