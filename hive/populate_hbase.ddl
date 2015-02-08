-- populate our hbase tables FROM the hive views

FROM cat_pcount_view 
INSERT INTO TABLE cat_pcount_hbase 
SELECT * ;


FROM day_cat_vol_view 
INSERT INTO table cat_day_vol_hbase 
SELECT concat_ws('-' , day , cast(category as STRING)) , total ;


FROM mth_cat_vol_view 
INSERT INTO table cat_mth_vol_hbase 
SELECT concat_ws('-' , mth , cast(category as STRING)) , total ;


FROM day_vol_revenue__view
INSERT INTO table day_vol_revenue__hbase 
SELECT * ;
