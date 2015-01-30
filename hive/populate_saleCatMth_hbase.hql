-- populate our hbase table named saleCatPerMonth from the hive view
-- todo: remove old data before populating it

from salesCat_MthView insert into table salescatperMth_hbase select concat_ws('_',sale_mth,cast(category as STRING)), total ;
