-- populate our hbase table named saleCatPerDay from the hive view
-- todo: remove old data before populating it

from catDayView 
insert into table salescatperday_hbase 
select concat_ws('_',sale_date,cast(category as STRING)), total ;
