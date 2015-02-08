-- Raw product data ingested in HDFS through batch
-- Using an external table instead of hive internal to allow same data for MR and Hive 

CREATE EXTERNAL TABLE IF NOT EXISTS products
(
product_id BIGINT,
name STRING,
category STRING
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/ubuntu/NileData/products';

-- Raw sales data ingested in HDFS through Kafka

CREATE EXTERNAL TABLE IF NOT EXISTS sales
(
time BIGINT,
customer_id INT,
product_id INT,
name STRING,
qty INT,
cost FLOAT,
zipcode INT)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/ubuntu/NileData/salesorder';


