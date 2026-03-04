import requests
import polars as pl  # เปลี่ยนจาก pandas เป็น polars
import sqlite3
from datetime import datetime
import logging

# ตั้งค่าการแจ้งเตือน
logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_pipeline():
    try:
        # --- a) Extract ---
        logging.info("Extracting data from API...")
        url = "http://universities.hipolabs.com/search?country=Thailand"
        response = requests.get(url)
        data = response.json()

        # --- b) Transform (ใช้ Polars) ---
        logging.info("Transforming data with Polars...")
        
        # สร้าง DataFrame จาก List ของ Dictionary
        df = pl.DataFrame(data)
        
        # เลือกคอลัมน์ และจัดการข้อมูลเว็บ (เอาตัวแรกจาก list)
        # ใน Polars เราใช้ .list.get(0) ได้เลย
        df_clean = df.select([
            pl.col("name").alias("university_name"),
            pl.col("web_pages").list.get(0).alias("website")
        ])
        
        # เพิ่มเวลาที่ประมวลผล
        df_clean = df_clean.with_columns(
            pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("processed_at")
        )

        # --- c) Load ---
        logging.info("Loading data into SQLite...")
        conn = sqlite3.connect("intern_test.db")
        
        # Polars สามารถแปลงเป็น Pandas ชั่วคราวเพื่อใช้ .to_sql หรือเขียนลง SQL โดยตรง
        # เนื่องจากเราต้องการความง่าย เราจะใช้คำสั่ง SQL พื้นฐานเขียนลงไปครับ
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS universities")
        cursor.execute("""
            CREATE TABLE universities (
                university_name TEXT,
                website TEXT,
                processed_at TEXT
            )
        """)
        
        # ใส่ข้อมูลลงในตาราง
        cursor.executemany(
            "INSERT INTO universities VALUES (?, ?, ?)",
            df_clean.rows()
        )
        
        conn.commit()
        conn.close()
        logging.info("Successfully loaded into 'intern_test.db'!")

    except Exception as e:
        logging.error(f"Error occurred: {e}")

if __name__ == "__main__":
    run_pipeline()