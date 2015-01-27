-- populate our hbase table named salesPerZipcode from the hive view

FROM productPerZipcodeView INSERT INTO TABLE salesPerZipcode_hbase select productPerZipcodeView.* ;
