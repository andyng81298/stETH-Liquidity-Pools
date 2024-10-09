with lendings_list as (
 select
      project
    from
      dune.lido.result_wsteth_in_lending_pools
    where
      time = (
        select
          max(time)
        from
          dune.lido.result_wsteth_in_lending_pools
      )
      and amount >= 100
      and blockchain = 'optimism'

) 
, collateral_by_project as (
select  cast(time as timestamp) as time, 	
        project, 
        sum(amount) as amount
        
from dune.lido.result_wsteth_in_lending_pools

where  date_trunc('month', time) >= date_trunc('month',now()) - interval '3' month
  and blockchain = 'optimism'
  and project in (select project from lendings_list)
group by 1,2
)



select collateral_by_project.*
from collateral_by_project
order by collateral_by_project.time desc, amount desc
