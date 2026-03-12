# ตรวจสอบจำนวนหนังสือทั้งหมด
SELECT COUNT(*) AS total_books
FROM kids_books;

# ตรวจสอบ Missing Data
SELECT
    SUM(CASE WHEN book_title IS NULL THEN 1 ELSE 0 END) AS missing_title,
    SUM(CASE WHEN publish_year IS NULL THEN 1 ELSE 0 END) AS missing_year
FROM kids_books;

# หา Duplicate Book Title
SELECT book_title, COUNT(*) AS total
FROM kids_books
GROUP BY book_title
HAVING COUNT(*) > 1;

# จำนวนหนังสือต่อปี
SELECT publish_year, COUNT(*) AS total_books
FROM kids_books
GROUP BY publish_year
ORDER BY publish_year;

# ปีที่มีหนังสือมากที่สุด
SELECT publish_year, COUNT(*) AS total_books
FROM kids_books
GROUP BY publish_year
ORDER BY total_books DESC
LIMIT 1;

# หา Top 3 ปีที่มีหนังสือมากที่สุด
SELECT publish_year, COUNT(*) AS total_books
FROM kids_books
GROUP BY publish_year
ORDER BY total_books DESC
LIMIT 3;

# window function หา rank ของจำนวนหนังสือในแต่ละปี
SELECT publish_year, count(*) AS total_books FROM kids_books GROUP BY publish_year ORDER BY total_books DESC LIMIT 3;SELECT
    publish_year,
    COUNT(*) AS total_books,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking
FROM kids_books
GROUP BY publish_year;

# หา Book ที่ถูก extract ล่าสุด
SELECT *
FROM kids_books
ORDER BY extracted_at DESC
LIMIT 5;

# Deduplicate ด้วย Window Function (ระดับ Data Engineer จริง) สมมติว่ามีหนังสือชื่อซ้ำ ต้องการเก็บแค่ record แรก
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY book_title ORDER BY id) AS rn
    FROM kids_books
) t
WHERE rn = 1;

