-- create a table in hbase to host the hive category wise sale per day View

CREATE TABLE IF NOT EXISTS salesCatPerDay_hbase (date_category STRING, total INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1')
TBLPROPERTIES ('hbase.table.name' = 'salesCatPerDay');
