select cust, prod, count(quant) as count_0_quant, max(quant) as max_0_quant, avg(quant) as avg_0_quant
from sales
group by cust, prod
having avg(quant) > 100