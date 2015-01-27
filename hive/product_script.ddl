-- define an external table over raw product data ingested in hdfs through kafka

CREATE TABLE IF NOT EXISTS products
(
product_id INT,
name STRING,
category1 STRING,
category2 STRING,
category3 STRING)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/ubuntu/NileData/products';
