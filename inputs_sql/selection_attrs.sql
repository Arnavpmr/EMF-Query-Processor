with nyt as (
    select cust, prod, max(quant) as max_1_quant
    from sales
    where state = 'NY'
    group by cust, prod
),
NYMaxDate as (
    select nyt.cust, nyt.prod, nyt.max_1_quant, s.date as date_2
    from nyt
    left join sales s on nyt.cust = s.cust and nyt.prod = s.prod and s.quant = nyt.max_1_quant
),
njt as (
    select cust, prod, max(quant) as max_3_quant
    from sales
    where state = 'NJ'
    group by cust, prod
),
NJMaxDate as (
    select njt.cust, njt.prod, njt.max_3_quant, s.date as date_4
    from njt
    left join sales s on njt.cust = s.cust and njt.prod = s.prod and s.quant = njt.max_3_quant
),
ctt as (
    select cust, prod, max(quant) as max_5_quant
    from sales
    where state = 'CT'
    group by cust, prod
),
CTMaxDate as (
    select ctt.cust, ctt.prod, ctt.max_5_quant, s.date as date_6
    from ctt
    left join sales s on ctt.cust = s.cust and ctt.prod = s.prod and s.quant = ctt.max_5_quant
)
select NYMaxDate.cust, NYMaxDate.prod, NYMaxDate.max_1_quant, NYMaxDate.date_2, NJMaxDate.max_3_quant, NJMaxDate.date_4, CTMaxDate.max_5_quant, CTMaxDate.date_6
from NYMaxDate
left join NJMaxDate on NYMaxDate.cust = NJMaxDate.cust and NYMaxDate.prod = NJMaxDate.prod
left join CTMaxDate on NYMaxDate.cust = CTMaxDate.cust and NYMaxDate.prod = CTMaxDate.prod