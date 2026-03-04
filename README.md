1.Understand what data engineering is and what it is for.
Data Engineering คือการสร้าง Data Pipelines เพื่อเปลี่ยนข้อมูลดิบให้กลายเป็นข้อมูลที่สะอาด  และพร้อมสำหรับการนำไปใช้งานต่อ เพื่อให้ Data Scientists หรือ Analysts สามารถเข้าถึงข้อมูลที่มีคุณภาพได้ทันทีโดยไม่ต้องเสียเวลาคลีนข้อมูลเอง และช่วยให้องค์กรตัดสินใจด้วยข้อมูลได้อย่างแม่นยำและรวดเร็ว

2.Basic knowledge that data engineer needs to know
🐍 Python: ไม่ใช่แค่เขียนโปรแกรมได้ แต่ต้องใช้ Python เพื่อเชื่อมต่อระบบต่างๆ เข้าด้วยกัน
            การใช้ pandas หรือ polars (ที่เราใช้ในโปรเจกต์นี้) เพื่อจัดการ Dataframe
            เขียน Airflow DAGs เพื่อควบคุมลำดับการทำงานของท่อส่งข้อมูล (Pipeline) ให้ทำงานโดยอัตโนมัติ

🗄️ SQL: ต้องแม่นยำเรื่อง Window Functions, Complex Joins และ Query Optimization
        ออกแบบ Schema ใน Data Warehouse เพื่อให้ DA ดึงข้อมูลได้รวดเร็วที่สุด

⚙️ Distributed Computing System : เข้าใจเบื้องหลังของ Big Data Tech stack ว่าทำงานแบบ Master-Worker (Cluster) 
                                   เข้าใจความต่างระหว่างการประมวลผลแบบ Batch (ทำเป็นรอบ) และ Streaming (ทำทันทีที่ข้อมูลมา)

🚀 Data Pipeline Orchestration : เปลี่ยนจากการรัน Script มือ (Manual) มาใช้เครื่องมือจัดการ Workflow เช่น Apache Airflow
                                 เช่น ต้องการตั้งเวลาให้ Pipeline รันทุกวันเวลา 08:00 น. อัตโนมัติ

Thailand University ETL Pipeline
 Data Pipeline สำหรับการดึงข้อมูล (Extract), แปลงข้อมูล (Transform) และจัดเก็บข้อมูล (Load) รายชื่อมหาวิทยาลัยในประเทศไทย เพื่อสร้างฐานข้อมูลที่สะอาดและพร้อมสำหรับการวิเคราะห์ต่อ 

📌 Overview
ระบบนี้ถูกออกแบบมาเพื่อแก้ปัญหาความยุ่งยากในการดึงข้อมูลจาก External API โดยเปลี่ยนข้อมูล JSON ที่ซับซ้อนให้กลายเป็น Relational Database (SQLite) ที่คัดกรองเฉพาะข้อมูลสำคัญ พร้อมระบุช่วงเวลาที่นำเข้าข้อมูลเพื่อการตรวจสอบย้อนหลัง (Observability)

🛠️ Tech Stack
Engine: Python 3.x
Data Processing: Polars (ประสิทธิภาพสูงกว่า Pandas ในการจัดการหน่วยความจำ)
Database: SQLite (RDBMS ที่เบาและรวดเร็ว)
API Client: Requests (พร้อมระบบ Error Handling)

🏗️ ETL Architecture
ท่อส่งข้อมูล (Pipeline) นี้ประกอบด้วย 3 ส่วนหลัก:
Extract: เรียกข้อมูลจาก Hipo University API ผ่าน HTTP GET โดยมีการตั้งค่า timeout และ raise_for_status() เพื่อป้องกันระบบค้างหาก API ล่ม

Transform:
แปลง JSON เป็น Polars DataFrame
ทำ Data Cleaning: เลือกคอลัมน์ university_name, website
เพิ่ม Technical Metadata: คอลัมน์ load_timestamp เพื่อทำ Data Lineage
Load: นำข้อมูลลงตาราง SQL ในโหมด DROP & CREATE เพื่อให้มั่นใจว่าข้อมูลในฐานข้อมูลเป็นเวอร์ชันล่าสุดเสมอ (Idempotent Load)

🚀 Getting Started
1. Installation
Bash
pip install requests polars
2. Run Pipeline
Bash
python main.py
3. Project Structure
main.py - โค้ดหลักของระบบ ETL
intern_test.db - ฐานข้อมูล SQLite ปลายทาง
pipeline.log - บันทึกเหตุการณ์ (Logs) สำหรับ Debugging
universities_result.csv - ไฟล์สำรองข้อมูลสำหรับ Data Analyst

🧪 Data Validation (SQL Audit)
เราใช้ SQL เพื่อตรวจสอบคุณภาพข้อมูล (Data Quality) ดังนี้:
Duplicate Check: ตรวจสอบชื่อมหาวิทยาลัยที่ซ้ำซ้อน
Null Audit: เช็คค่าว่างในคอลัมน์สำคัญ
Freshness Check: ตรวจสอบว่าข้อมูลถูกดึงมาล่าสุดเมื่อไหร่ผ่าน load_timestamp

🙏 Credits
Data provided by Hipo Labs: university-domains-list