-- populate our hbase table named salesPerZipcode from the hive view

FROM prodZipView 
INSERT INTO TABLE salesPerZipcode_hbase 
select * ;
