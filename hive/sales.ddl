-- define an external table over raw sales data ingested in hdfs through kafka

CREATE TABLE IF NOT EXISTS sales
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
