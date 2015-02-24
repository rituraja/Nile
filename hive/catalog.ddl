CREATE EXTERNAL TABLE IF NOT EXISTS catalog
(
product_id BIGINT,
name STRING,
category STRING
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/ubuntu/NileData/catalog';

-- Number of products per category
CREATE VIEW IF NOT EXISTS cat_product_view (category,count) AS
SELECT category, count(*)
FROM catalog
GROUP BY category;

CREATE TABLE IF NOT EXISTS cat_product_hbase (category STRING, count INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1')
TBLPROPERTIES ('hbase.table.name' = 'cat_product');

FROM cat_product_view 
INSERT INTO TABLE cat_product_hbase 
SELECT * ;
