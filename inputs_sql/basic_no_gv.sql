select cust, prod, count(quant), max(quant), avg(quant)
from sales
group by cust, prod
having avg(quant) > 100