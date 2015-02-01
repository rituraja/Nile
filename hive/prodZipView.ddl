-- create a view, building best selling products per zipcode

CREATE VIEW IF NOT EXISTS prodZipView (zipcode,product_id,name,qty) AS
SELECT zipcode, products.product_id, products.name , sum(qty)
FROM sales
join products
on (sales.product_id = products.product_id)
group by zipcode, products.product_id, products.name
;

