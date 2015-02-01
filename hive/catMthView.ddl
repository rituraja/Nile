-- create a view, building sales distribution over categories in a month

CREATE VIEW IF NOT EXISTS catmthview (sale_mth, category, total) AS
select concat_ws('_', cast(year(FROM_UNIXTIME(BIGINT(time / 1000))) as string ), cast(month(FROM_UNIXTIME(BIGINT(time / 1000))) as string)),
category1, sum(qty)
from sales join products
on (sales.product_id = products.product_id)
group by
concat_ws('_', cast(year(FROM_UNIXTIME(BIGINT(time / 1000))) as string ), cast(month(FROM_UNIXTIME(BIGINT(time / 1000))) as string)) , category1
;

