with base as (
    select prod, month
    from sales
    where year=2020
    group by prod, month
),
prevMonth as (
    select prod, month, count(quant) as count_1_quant
    from sales
    where year=2020
    group by prod, month
),
nextMonth as (
    select prod, month, count(quant) as count_2_quant
    from sales
    where year=2020
    group by prod, month
)
select base.prod, base.month, prevMonth.count_1_quant, nextMonth.count_2_quant
from base
left join prevMonth on base.prod = prevMonth.prod and base.month-1 = prevMonth.month
left join nextMonth on base.prod = nextMonth.prod and base.month+1 = nextMonth.month
where base.month > 1 and base.month < 12