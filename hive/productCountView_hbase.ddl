CREATE TABLE IF NOT EXISTS productCount_hbase (category STRING, count INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1')
TBLPROPERTIES ('hbase.table.name' = 'productCounts');
