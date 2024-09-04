with collateral_by_blockchain as (
select  cast(time as timestamp) as time, 	
        blockchain, 
        sum(amount) as amount
        
from dune.lido.result_wsteth_in_lending_pools
--dune.lido.result_2688773
where date_trunc('month', time) >= date_trunc('month',now()) - interval '3' month
group by 1,2
)



select *
from collateral_by_blockchain
order by time desc, amount desc
