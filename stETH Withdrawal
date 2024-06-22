select *, 
sum(withdrawal) over (order by day) as cumulative_steth,
sum(request_count) over (order by day) as cumulative_request
from (
select date_trunc('day', evt_block_time) as day, 
sum(cast(value as double)/1e18) as withdrawal, count(*) as request_count
from erc20_ethereum.evt_Transfer
where to = 0x889edc2edab5f40e902b864ad4d7ade8e412f9b1
and contract_address = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
and date(evt_block_time)>=date('2023-05-15')
group by 1)a
order by 4 desc

