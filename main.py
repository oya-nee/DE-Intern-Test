import requests
import polars as pl
import sqlite3
import logging
from datetime import datetime

# 1. ตั้งค่า Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

def run_pipeline():
    start_time = datetime.now()
    logging.info("--- Pipeline Started ---")

    try:
        # --- Step A: Extract ---
        url = "http://universities.hipolabs.com/search?country=Thailand"
        logging.info(f"Extracting data from: {url}")
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Successfully retrieved {len(data)} records.")

        # --- Step B: Transform ---
        logging.info("Transforming data...")
        df = pl.DataFrame(data)
        
        df_clean = df.select([
            pl.col("name").alias("university_name"),
            pl.col("web_pages").list.get(0).alias("website")
        ]).with_columns([
            pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("load_timestamp")
        ])

        if df_clean.height == 0:
            raise ValueError("No data found after transformation")

        # --- Step C: Load ---
        logging.info("Loading data into SQLite...")
        conn = sqlite3.connect("intern_test.db")
        
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS universities")
        cursor.execute("""
            CREATE TABLE universities (
                university_name TEXT,
                website TEXT,
                load_timestamp TEXT
            )
        """)
        
        cursor.executemany(
            "INSERT INTO universities VALUES (?, ?, ?)",
            df_clean.rows()
        )
        
        conn.commit()
        conn.close()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Pipeline finished successfully in {duration.total_seconds():.2f} seconds.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error during Extraction: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        logging.info("--- Pipeline Process Ended ---")

# --- ส่วนของการตรวจสอบผลลัพธ์ (Check Results) ---
def check_results():
    try:
        conn = sqlite3.connect("intern_test.db")
        
        # ดึงข้อมูลทั้งหมด (ไม่จำกัดแค่ 10) มาเพื่อทำไฟล์ CSV
        query_all = "SELECT * FROM universities"
        df_all = pl.read_database(query_all, conn)
        
        # --- เพิ่มบรรทัดนี้ลงไปเพื่อสร้างไฟล์ CSV ---
        df_all.write_csv("universities_result.csv")
        print("\n[Success] Data exported to 'universities_result.csv'")
        
        # ส่วนโชว์ Preview ใน Terminal เดิม
        print("\n--- Preview Data in SQLite Table (First 10 rows) ---")
        print(df_all.head(10)) 
        
        conn.close()
    except Exception as e:
        print(f"Could not read database: {e}")

if __name__ == "__main__":
    run_pipeline()  # รันระบบ ETL
    check_results() # โชว์ผลลัพธ์ที่ได้