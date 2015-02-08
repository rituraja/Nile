-- create a view, building best selling products per zipcode

CREATE VIEW IF NOT EXISTS prodZipView (zipcode,product_id,name,qty) AS
SELECT zipcode, products.product_id, products.name , sum(qty)
FROM sales
join products
on (sales.product_id = products.product_id)
group by zipcode, products.product_id, products.name
;

-- create a table in hbase to host the hive sale per zipcode View

CREATE TABLE IF NOT EXISTS salesPerZipcode_hbase (zipcode INT, product_id INT, name STRING, qty INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1,f:c2,f:c3')
TBLPROPERTIES ('hbase.table.name' = 'salesPerZipcode');

-- populate our hbase table named salesPerZipcode from the hive view

FROM prodZipView 
INSERT INTO TABLE salesPerZipcode_hbase 
select * ;
