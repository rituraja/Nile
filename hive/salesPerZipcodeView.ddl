-- create a view, building best selling products per zipcode

CREATE VIEW IF NOT EXISTS productPerZipcodeView (zipcode,product_id,name,qty) AS
SELECT zipcode, product_id, name , sum(qty)
FROM sales
group by zipcode, product_id, name
;

