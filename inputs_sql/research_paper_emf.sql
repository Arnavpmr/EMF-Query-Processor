select s.prod, s.month, 
(select avg(quant) from sales s1 where s1.prod = s.prod and s1.month < s.month and s1.year = 2020) as avg_1_quant,
(select avg(quant) from sales s2 where s2.prod = s.prod and s2.month > s.month and s2.year = 2020) as avg_2_quant
from sales s
where year = 2020
group by s.prod, s.month