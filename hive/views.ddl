-- creating views & hbase tables to host corresponding views


-- Number of products per category
CREATE VIEW IF NOT EXISTS cat_pcount_view (category,count) AS
SELECT category, count(*)
FROM products
GROUP BY category;

CREATE TABLE IF NOT EXISTS cat_pcount_hbase (category STRING, count INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1')
TBLPROPERTIES ('hbase.table.name' = 'cat_pcount');


-- Sales distribution over categories in a day 
CREATE VIEW IF NOT EXISTS day_cat_vol_view (day, category, total) AS
SELECT to_date(FROM_UNIXTIME(BIGINT(time / 1000))), category, sum(qty) 
FROM products JOIN sales                                             
ON ( products.product_id  = sales.product_id )                          
GROUP BY to_date(FROM_UNIXTIME(BIGINT(time / 1000))) , category ;

CREATE TABLE IF NOT EXISTS cat_day_vol_hbase (cat_day STRING, total INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1')
TBLPROPERTIES ('hbase.table.name' = 'cat_day_vol');


-- Sales distribution over categories in a month
CREATE VIEW IF NOT EXISTS mth_cat_vol_view (mth, category, total) AS
SELECT concat_ws('-', cast(year(FROM_UNIXTIME(BIGINT(time / 1000))) AS string ),cast(month(FROM_UNIXTIME(BIGINT(time / 1000))) as string)),
		category, sum(qty)
FROM products JOIN sales
ON (products.product_id = sales.product_id)
GROUP BY
concat_ws('-', cast(year(FROM_UNIXTIME(BIGINT(time / 1000))) AS string ), cast(month(FROM_UNIXTIME(BIGINT(time / 1000))) AS string)) , category ;

CREATE TABLE IF NOT EXISTS cat_mth_vol_hbase (cat_mth STRING, total INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1')
TBLPROPERTIES ('hbase.table.name' = 'cat_mth_vol');


-- Total sales revenue, volume for each day
CREATE VIEW IF NOT EXISTS day_vol_revenue_view (sale_date, total, revenue) AS
SELECT to_date(FROM_UNIXTIME(BIGINT(time / 1000))), sum(qty), sum(qty*cost)
FROM sales
GROUP BY to_date(FROM_UNIXTIME(BIGINT(time / 1000))) ;


CREATE TABLE IF NOT EXISTS day_vol_revenue_hbase (sale_date STRING, total INT, revenue DOUBLE)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1,f:c2')
TBLPROPERTIES ('hbase.table.name' = 'day_vol_revenue');



