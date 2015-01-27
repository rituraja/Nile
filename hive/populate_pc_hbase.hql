-- populate our hbase table named productCount from the hive view

FROM productCountView INSERT INTO TABLE productCount_hbase select productCountView.* ;
