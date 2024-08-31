with dates as (
    with day_seq as (select (sequence(cast(date_trunc('month',now()) - interval '12' month as date), current_date, interval '1' day)) as day)
select days.day
from day_seq
cross join unnest(day) as days(day)
)

, volumes as (
select u.call_block_time as time,  "output_0" as steth, "_wstETHAmount" as wsteth 
from  lido_ethereum.WstETH_call_unwrap u 
where "call_success" = TRUE 
union all
select u."call_block_time", "_stETHAmount" as steth, "output_0" as wsteth 
from  lido_ethereum.WstETH_call_wrap u
where "call_success" = TRUE 
)


, wsteth_rate as (
select
  day, rate as rate0, value_partition, first_value(rate) over (partition by value_partition order by day) as rate,
  lead(day,1,date_trunc('day', now() + interval '1' day)) over(order by day) as next_day

from (
select day, rate,
sum(case when rate is null then 0 else 1 end) over (order by day) as value_partition
from (
select  date_trunc('day', d.day) as day, 
       sum(cast(steth as double))/sum(cast(wsteth as double))  AS rate
from dates  d
left join volumes v on date_trunc('day', v.time)  = date_trunc('day', d.day) 
group by 1
))

)


, lending_pools as (
select d.day as time, sum(amount) as steth_collateral
from dates d
left join dune.lido.result_wsteth_in_lending_pools q on date_trunc('day', d.day) = date_trunc('day', q.time) 
group by 1
)


, steth_locked_in_defi as (
select  cast(time as timestamp) as time, 
        sum(liquidity_pools_balance) as "Liquidity pools",
        sum(lending_pools_balance) as "Lendings",
        sum(other_protocols_balance) as "Other protocols", 
        sum(liquidity_pools_balance) +  sum(lending_pools_balance) + sum(other_protocols_balance) as total_in_defi
from (

select time,  0 as liquidity_pools_balance, steth_collateral as lending_pools_balance, 0 as other_protocols_balance
from lending_pools

)
group by 1
)


select  l.*, 
        total.lido_amount, 
        100*total_in_defi/total.lido_amount as "stETH locked in Lendings, share"
from steth_locked_in_defi l
left join query_2111543 total on l.time = total.day
order by time desc
