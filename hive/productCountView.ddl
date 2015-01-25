CREATE VIEW IF NOT EXISTS productCountView (category1,count) AS
SELECT category1, count(*)
FROM products
group by category1
;

