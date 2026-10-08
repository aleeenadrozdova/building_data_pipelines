-- 0) full scan по справочнику/таблице (Parquet - озеро / своя таблица)
SELECT COUNT(*) FROM sales;

-- 1) фильтр + агрегация (hot)
SELECT category_id, SUM(amount) AS rev
FROM sales
WHERE dt = DATE '2026-09-10'
GROUP BY category_id
ORDER BY rev DESC;

-- 2) join большой-с-малой (broadcast/custom)
SELECT c.category_name, SUM(s.amount) AS rev
FROM sales s
JOIN category c ON s.category_id = c.category_id
GROUP BY c.category_name
ORDER BY rev DESC;

-- 3) топ-N в группе (оконная)
SELECT * FROM (
    SELECT seller_id, category_id, SUM(amount) AS rev,
           ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY SUM(amount) DESC) AS rn
    FROM sales GROUP BY seller_id, category_id
) t WHERE rn <= 5;

-- 4) много-агрегат по дням
SELECT dt, COUNT(*), COUNT(DISTINCT user_id), SUM(amount), AVG(qty)
FROM sales GROUP BY dt ORDER BY dt;
