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
      and amount >= 500 -- select markets with >= 500 stETH on todays' date

)  


, collateral_by_project as (
select  cast(time as timestamp) as time, 	
        project, 
        sum(amount) as amount
        
from dune.lido.result_wsteth_in_lending_pools
--dune.lido.result_2688773
where  date_trunc('month', time) >= date_trunc('month',now()) - interval '12' month
  and project  in (select * from lendings_list)
group by 1,2
)

, total_collateral as (

select  cast(time as timestamp) as time, 'total collateral' as project,	
        sum(amount) as amount
        
from dune.lido.result_wsteth_in_lending_pools
--dune.lido.result_2688773
where date_trunc('month', time) >= date_trunc('month',now()) - interval '12' month
  and project  in (select * from lendings_list)
group by 1

)


select collateral_by_project.*,  total_collateral.amount as total_amount
from collateral_by_project
left join total_collateral on collateral_by_project.time = total_collateral.time
order by collateral_by_project.time desc, amount desc 
