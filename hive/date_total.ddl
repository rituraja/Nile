-- create a view, building total sales for every day

CREATE VIEW IF NOT EXISTS date_total_revenue_View (sale_date, total, revenue) AS
select to_date(FROM_UNIXTIME(BIGINT(time / 1000))), sum(qty), sum(qty*cost)
from products join sales
on (products.product_id = sales.product_id )
group by to_date(FROM_UNIXTIME(BIGINT(time / 1000))) 
;

-- create a table in hbase to host the hive category wise sale per day View

CREATE TABLE IF NOT EXISTS date_total_revenue__hbase (sale_date STRING, total INT, revenue DOUBLE)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f:c1,f:c2')
TBLPROPERTIES ('hbase.table.name' = 'date_total_revenue_hbase');
