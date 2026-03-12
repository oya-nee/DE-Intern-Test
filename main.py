import requests  # library สำหรับส่ง HTTP request ไปหา API ผ่าน internet เช่น requests.get(url) เพื่อดึงข้อมูลจากระบบภายนอก ในโค้ดนี้ใช้ดึงข้อมูลหนังสือเด็กจาก OpenLibrary API
import polars as pl  # Polars เป็น library สำหรับจัดการข้อมูลแบบ DataFrame (ตารางข้อมูลเหมือน Excel) ใช้สำหรับ data processing เช่น select column, filter, clean data เร็วกว่า pandas เพราะเขียนด้วยภาษา Rust
import sqlite3  # module สำหรับเชื่อมต่อและจัดการฐานข้อมูล SQLite ซึ่งเป็น relational database ที่เก็บข้อมูลเป็นไฟล์ .db ไม่ต้องติดตั้ง server เหมาะกับงาน pipeline ขนาดเล็กหรือ local storage
import logging  # module สำหรับบันทึก log การทำงานของโปรแกรม เช่น INFO, WARNING, ERROR ช่วยให้ตรวจสอบการทำงานย้อนหลังได้ สำคัญมากใน data pipeline เพราะช่วย debug ปัญหาได้
import sys  # module สำหรับควบคุมการทำงานของ Python runtime เช่น การหยุดโปรแกรม ในโค้ดนี้ใช้ sys.exit(1) เพื่อหยุด pipeline เมื่อเกิด error ร้ายแรง
from datetime import datetime  # ใช้สำหรับจัดการข้อมูลวันและเวลา เช่น บันทึกเวลาที่ pipeline ดึงข้อมูลจาก API
from pathlib import Path  # ใช้สำหรับจัดการ path ของไฟล์ เช่น path ของ database ข้อดีคือทำงานได้ทุก OS (Windows / Linux / Mac) และปลอดภัยกว่าการใช้ string path ปกติ
from typing import List, Dict, Any  # ใช้สำหรับ type hint เพื่อบอกชนิดข้อมูลของตัวแปร เช่น List = list , Dict = dictionary ช่วยให้โค้ดอ่านง่ายและช่วย IDE ตรวจ error ได้

API_URL  = "https://openlibrary.org/subjects/children.json?limit=1000"  # URL ของ OpenLibrary API สำหรับดึงข้อมูลหนังสือหมวด children โดย limit=1000 หมายถึงดึงข้อมูลสูงสุด 1000 รายการ
DB_PATH  = Path("kids_library.db")  # กำหนดตำแหน่งไฟล์ SQLite database ที่จะใช้เก็บข้อมูล เมื่อรัน pipeline ครั้งแรกจะสร้างไฟล์ kids_library.db ขึ้นมา
LOG_FILE = "pipeline.log"  # กำหนดชื่อไฟล์สำหรับเก็บ log ของ pipeline เพื่อใช้ตรวจสอบการทำงานย้อนหลัง

logging.basicConfig(  # ตั้งค่าระบบ logging เพื่อกำหนดว่าระบบจะบันทึก log อย่างไรo
    level=logging.INFO,  # กำหนดระดับ log เป็น INFO หมายถึงจะแสดง log ตั้งแต่ระดับ INFO ขึ้นไป เช่น INFO, WARNING, ERROR
    format="%(asctime)s - %(levelname)s - %(message)s",  # กำหนดรูปแบบข้อความ log เช่น เวลา - ระดับ log - ข้อความ
    handlers=[
        logging.FileHandler(LOG_FILE),  # บันทึก log ลงไฟล์ pipeline.log เพื่อเก็บ history การทำงานของ pipeline
        logging.StreamHandler(sys.stdout),  # แสดง log บนหน้าจอ console ทำให้ผู้ใช้เห็นการทำงานของ pipeline แบบ real-time
    ],
)


def extract(url: str) -> List[Dict[str, Any]]:  # ฟังก์ชัน extract เป็นขั้นตอนแรกของ ETL pipeline มีหน้าที่ดึงข้อมูลจาก API และคืนค่าเป็น list ของ dictionary
    logging.info(f"Pulling data from: {url}")  # บันทึก log เพื่อแจ้งว่ากำลังเริ่มดึงข้อมูลจาก API endpoint ที่กำหนด
    try:
        resp = requests.get(url, timeout=30)  # ส่ง HTTP GET request ไปยัง API เพื่อดึงข้อมูล โดยกำหนด timeout 30 วินาทีเพื่อป้องกัน request ค้าง
        resp.raise_for_status()  # ตรวจสอบ HTTP status code ถ้าไม่ใช่ 200 (success) จะ throw exception เพื่อแจ้งว่า request ล้มเหลว
    except requests.exceptions.RequestException as e:  # ดักจับ error ที่เกิดจาก request เช่น network error, timeout หรือ connection error
        logging.error(f"Request failed: {e}")  # บันทึก log ระดับ ERROR เพื่อแจ้งว่าการดึงข้อมูลจาก API ล้มเหลว
        sys.exit(1)  # หยุดการทำงานของ pipeline ทันที เพราะไม่สามารถดึงข้อมูลจากแหล่งต้นทางได้

    works = resp.json().get("works", [])  # แปลง response จาก API ให้อยู่ในรูปแบบ JSON แล้วดึงเฉพาะ field "works" ซึ่งเป็น list ของข้อมูลหนังสือ
    if not isinstance(works, list):  # ตรวจสอบว่าข้อมูลที่ได้เป็น list หรือไม่ เพราะ pipeline คาดหวังว่าจะต้องเป็น list ของหนังสือ
        logging.error(f"Unexpected response format: {type(works)}")  # ถ้า format ไม่ถูกต้องให้บันทึก error เพื่อแจ้งปัญหา
        sys.exit(1)  # หยุด pipeline เพราะข้อมูลที่ได้ไม่อยู่ในรูปแบบที่คาดหวัง

    logging.info(f"Got {len(works)} records.")  # บันทึก log จำนวน record หนังสือที่ดึงมาจาก API
    return works  # ส่งข้อมูลดิบ (raw data) ไปยังขั้นตอน transform


def transform(data: List[Dict[str, Any]]) -> pl.DataFrame:  # ฟังก์ชัน transform เป็นขั้นตอนที่สองของ ETL ใช้สำหรับทำความสะอาดและจัดรูปแบบข้อมูล
    logging.info("Transforming...")  # บันทึก log ว่ากำลังเริ่มกระบวนการ transform data

    df = pl.DataFrame(data)  # แปลงข้อมูล raw data (list of dictionary) ให้เป็น DataFrame เพื่อให้จัดการข้อมูลได้ง่าย

    df_clean = (  # เริ่มกระบวนการ data transformation และ data cleaning
        df.select(["title", "first_publish_year"])  # เลือกเฉพาะ column ที่ต้องการจาก API เพื่อลดข้อมูลที่ไม่จำเป็น
        .with_columns(  # สร้างหรือแก้ไข column
            pl.col("title").str.strip_chars().alias("book_title"),  # ลบ whitespace ที่ต้นและท้ายของชื่อหนังสือ และเปลี่ยนชื่อ column เป็น book_title
            pl.col("first_publish_year").cast(pl.Int64, strict=False),  # แปลงข้อมูลปีที่พิมพ์เป็น integer ถ้าแปลงไม่ได้จะให้ค่า null
            pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("extracted_at"),  # เพิ่ม column extracted_at เพื่อบันทึกเวลาที่ pipeline ดึงข้อมูล
        )
        .select(["book_title", "first_publish_year", "extracted_at"])  # เลือกเฉพาะ column ที่ต้องการเก็บใน database
        .filter(pl.col("book_title").is_not_null() & (pl.col("book_title") != ""))  # กรองข้อมูลที่ชื่อหนังสือเป็น null หรือ string ว่างออก
        .unique(subset=["book_title"], keep="first")  # ลบข้อมูลหนังสือที่ซ้ำกัน โดยเก็บ record แรกไว้
        .sort("first_publish_year", descending=True, nulls_last=True)  # เรียงข้อมูลตามปีที่พิมพ์จากใหม่ไปเก่า และให้ค่า null อยู่ท้ายสุด
    )

    logging.info(f"{df_clean.height} books ready.")  # บันทึก log จำนวนหนังสือหลังจากผ่านกระบวนการ cleaning แล้ว
    return df_clean  # ส่ง DataFrame ที่สะอาดและพร้อมใช้งานไปยังขั้นตอน load


def load(df: pl.DataFrame, db_path: Path):  # ฟังก์ชัน load เป็นขั้นตอนสุดท้ายของ ETL มีหน้าที่นำข้อมูลไปเก็บใน database
    logging.info(f"Writing to SQLite: {db_path}")  # บันทึก log ว่ากำลังเริ่มเขียนข้อมูลลง SQLite database
    try:
        with sqlite3.connect(db_path) as conn:  # เชื่อมต่อกับ SQLite database โดยใช้ path ที่กำหนด
            cur = conn.cursor()  # สร้าง cursor object เพื่อใช้ execute SQL commands
            cur.execute("DROP TABLE IF EXISTS kids_books")  # ลบ table kids_books ถ้ามีอยู่แล้ว เพื่อป้องกันข้อมูลซ้ำจาก pipeline run ก่อนหน้า
            cur.execute("""  # สร้าง table ใหม่ชื่อ kids_books สำหรับเก็บข้อมูลหนังสือ
                CREATE TABLE kids_books (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,  # id เป็น primary key และเพิ่มค่าอัตโนมัติ
                    book_title   TEXT    NOT NULL,  # ชื่อหนังสือ กำหนดเป็น text และห้ามเป็น null
                    publish_year INTEGER,  # ปีที่พิมพ์ของหนังสือ
                    extracted_at TEXT    NOT NULL  # เวลาที่ pipeline ดึงข้อมูล
                )
            """)
            cur.executemany(  # ใช้สำหรับ insert ข้อมูลหลาย record พร้อมกัน
                "INSERT INTO kids_books (book_title, publish_year, extracted_at) VALUES (?, ?, ?)",  # SQL statement สำหรับ insert ข้อมูล
                df.rows(),  # ดึงข้อมูลจาก DataFrame ในรูปแบบ list ของ rows เพื่อนำไป insert
            )
            conn.commit()  # commit transaction เพื่อบันทึกข้อมูลลง database จริง
    except sqlite3.Error as e:  # ดักจับ error ที่อาจเกิดขึ้นจาก SQLite เช่น connection error หรือ SQL syntax error
        logging.error(f"SQLite error: {e}")  # บันทึก log error เพื่อแจ้งปัญหา
        sys.exit(1)  # หยุด pipeline ถ้าไม่สามารถบันทึกข้อมูลลง database ได้

    logging.info(f"Loaded {df.height} rows -> {db_path}")  # บันทึก log จำนวนข้อมูลที่ถูกโหลดเข้า database


if __name__ == "__main__":  # ตรวจสอบว่าไฟล์นี้ถูก run โดยตรง ไม่ได้ถูก import เป็น module
    start = datetime.now()  # บันทึกเวลาเริ่มต้นของ pipeline เพื่อใช้คำนวณ runtime

    raw      = extract(API_URL)  # Step 1: Extract ดึงข้อมูลดิบจาก OpenLibrary API
    clean_df = transform(raw)  # Step 2: Transform ทำ data cleaning และ data transformation
    load(clean_df, DB_PATH)  # Step 3: Load บันทึกข้อมูลที่สะอาดแล้วลง SQLite database

    elapsed = (datetime.now() - start).total_seconds()  # คำนวณเวลาที่ pipeline ใช้ในการทำงานทั้งหมด
    print(f"\nDone! {clean_df.height} books loaded in {elapsed:.1f}s")  # แสดงจำนวนหนังสือที่โหลดเข้า database และเวลาที่ใช้
    print(f"  SQLite -> {DB_PATH.resolve()}")  # แสดงตำแหน่งไฟล์ database ที่ถูกสร้าง
