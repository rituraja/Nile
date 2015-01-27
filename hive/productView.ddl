-- creating a view over product,

CREATE VIEW IF NOT EXISTS productView (product_id,name,category1,category2,category3) AS
SELECT product_id,name,category1,category2,category3
FROM products;

