-- create a view, building best selling products per month

CREATE VIEW IF NOT EXISTS productPerDayView (month,product_id,name,qty) AS
SELECT date, product_id, name , sum(qty)
FROM sales
group by date, product_id, name
;

