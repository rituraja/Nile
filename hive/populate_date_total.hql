-- populate our hbase table named saleCatPerDay from the hive view
-- todo: remove old data before populating it

from date_total_revenue_view
insert into table date_total_revenue__hbase 
select * ;
