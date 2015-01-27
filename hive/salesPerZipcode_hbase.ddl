-- create a table in hbase to host the hive sale per zipcode View

CREATE TABLE IF NOT EXISTS salesPerZipcode_hbase (zipcode INT, product_id INT, name STRING, qty INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1,f:c2,f:c3')
TBLPROPERTIES ('hbase.table.name' = 'salesPerZipcode');
