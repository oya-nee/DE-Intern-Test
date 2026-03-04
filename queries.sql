SELECT * FROM universities 
WHERE university_name LIKE '%Chulalongkorn%';

SELECT university_name, COUNT(*) as count
FROM universities
GROUP BY university_name
HAVING count > 1;

SELECT 
    SUM(CASE WHEN university_name IS NULL THEN 1 ELSE 0 END) as missing_names,
    SUM(CASE WHEN website IS NULL THEN 1 ELSE 0 END) as missing_websites
FROM universities;