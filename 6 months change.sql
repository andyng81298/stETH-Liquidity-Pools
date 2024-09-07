with current_total_collateral as (

select  sum(amount) as amount
from dune.lido.result_wsteth_in_lending_pools
where date_trunc('day', time) = (select max(time) from dune.lido.result_wsteth_in_lending_pools)

)

, ago_3m_total_collateral as (

select  sum(amount) as amount
from dune.lido.result_wsteth_in_lending_pools
where date_trunc('day', time) = (select max(time) from dune.lido.result_wsteth_in_lending_pools) - interval '3' month


)

, ago_6m_total_collateral as (

select  sum(amount) as amount
from dune.lido.result_wsteth_in_lending_pools
where date_trunc('day', time) = (select max(time) from dune.lido.result_wsteth_in_lending_pools) - interval '6' month


)


select 100*((select amount from current_total_collateral) - (select amount from ago_3m_total_collateral))/(select amount from ago_3m_total_collateral) as change_3m,
(select amount from current_total_collateral) as current, (select amount from ago_3m_total_collateral) as ago_3m,
(select amount from current_total_collateral) - (select amount from ago_3m_total_collateral) as diff_3m,
100*((select amount from current_total_collateral) - (select amount from ago_6m_total_collateral))/(select amount from ago_6m_total_collateral) as change_6m,
(select amount from ago_6m_total_collateral) as ago_6m,
(select amount from current_total_collateral) - (select amount from ago_6m_total_collateral) as diff_6m
