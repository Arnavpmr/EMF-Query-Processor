with NYTable as (
    select cust, prod, count(quant) as count_1_quant
    from sales
    where state='NY'
    group by cust, prod
),
NJTable as (
    select cust, prod, avg(quant) as avg_2_quant
    from sales
    where state='NJ'
    group by cust, prod
),
CTTable as (
    select cust, prod, max(quant) as max_3_quant
    from sales
    where state='CT'
    group by cust, prod
)
select NYTable.cust, NYTable.prod, count_1_quant, avg_2_quant, max_3_quant
from NYTable
left join NJTable on NYTable.cust = NJTable.cust and NYTable.prod = NJTable.prod
left join CTTable on NYTable.cust = CTTable.cust and NYTable.prod = CTTable.prod