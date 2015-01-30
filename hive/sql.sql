select category1, count(*)             
from sales join product                  
on (sales.product_id = product.product_id)
group by category1
