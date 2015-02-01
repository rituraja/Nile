-- create a view, building sales distribution over categories in a day 

CREATE VIEW IF NOT EXISTS catDayView (sale_date, category, total) AS
select to_date(FROM_UNIXTIME(BIGINT(time / 1000))), category1, sum(qty) 
from sales join products                                             
on (sales.product_id = products.product_id)                          
group by to_date(FROM_UNIXTIME(BIGINT(time / 1000))) , category1 
;


