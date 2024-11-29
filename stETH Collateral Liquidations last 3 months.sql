with reserves(address, symbol) AS (
    select * from (values 
    (0xae7ab96520de3a18e5e111b5eaab095312d7fe84, 'stETH'),
    (0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0, 'wstETH'),
    (0x5979d7b546e38e414f7e9822514be443a4800529, 'wstETH'), --arbitrum
    (0x1f32b1c2345538c0c6f582fcb022739c4a194ebb, 'wstETH'), --opti
    (0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD, 'wstETH') --poly
  ))
  
  
, markets_data as (  
select  'aave v2' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM("liquidatedCollateralAmount") / CAST(1e18 AS DOUBLE) AS collateral_amount,
        r.symbol as token,
        COUNT(*) AS "# liq"
from aave_v2_ethereum.LendingPool_evt_LiquidationCall l
join reserves r on l."collateralAsset" = r.address 
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'aave v3' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM("liquidatedCollateralAmount") / CAST(1e18 AS DOUBLE) AS collateral_amount,
        r.symbol as token,
        COUNT(*) AS "# liq"
from aave_v3_ethereum.Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'aave v3' as protocol,
        'arbitrum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM("liquidatedCollateralAmount") / CAST(1e18 AS DOUBLE) AS collateral_amount,
        r.symbol as token,
        COUNT(*) AS "# liq"
from aave_v3_arbitrum.L2Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'aave v3' as protocol,
        'optimism' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM("liquidatedCollateralAmount") / CAST(1e18 AS DOUBLE) AS collateral_amount,
        r.symbol as token,
        COUNT(*) AS "# liq"
from aave_v3_optimism.Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'aave v3' as protocol,
        'polygon' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM("liquidatedCollateralAmount") / CAST(1e18 AS DOUBLE) AS collateral_amount,
        r.symbol as token,
        COUNT(*) AS "# liq"
from aave_v3_polygon.Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'compound' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(collateralAbsorbed ) / CAST(1e18 AS DOUBLE) AS collateral_amount,
        r.symbol as token,
        COUNT(*) AS "# liq"

from compound_v3_ethereum.cWETHv3_evt_AbsorbCollateral l
join reserves r on l.contract_address= r.address 
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'maker(wsteth-a)' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        sum("ink")/1e18 as collateral_amount,
        'wstETH' as token,
        COUNT(*) AS "# liq"
from maker_ethereum.dog_evt_Bark dog
where dog."ilk" = 0x5753544554482d41000000000000000000000000000000000000000000000000
  and  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'maker(wsteth-b)' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        sum("ink")/1e18 as collateral_amount,
        'wstETH' as token,
        COUNT(*) AS "# liq"
from maker_ethereum.dog_evt_Bark dog
where dog."ilk" = 0x5753544554482d42000000000000000000000000000000000000000000000000
  and  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'maker(stecrv)' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        sum("ink")/1e18 as collateral_amount,
        'steCRV' as token,
        COUNT(*) AS "# liq"
from maker_ethereum.dog_evt_Bark dog
where dog."ilk" = 0x5753544554482d42000000000000000000000000000000000000000000000000
  and  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all


select  'raft' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(cast(collateralLiquidated as double)) / CAST(1e18 AS DOUBLE) AS collateral_amount,
        r.symbol as token,
        COUNT(*) AS "# liq"
from raft_deposit_ethereum.PositionManager_evt_Liquidation l
join reserves r on l.collateralToken = r.address 
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all


select  'spark' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM("liquidatedCollateralAmount") / CAST(1e18 AS DOUBLE) AS collateral_amount,
        r.symbol as token,
        COUNT(*) AS "# liq"
from spark_protocol_ethereum.Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'radiant' as protocol,
        'arbitrum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM("liquidatedCollateralAmount") / CAST(1e18 AS DOUBLE) AS collateral_amount,
        r.symbol as token,
        COUNT(*) AS "# liq"
from radiant_capital_arbitrum.LendingPool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'curve(crvUSD)' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(cast(collateral_received as double)) / CAST(1e18 AS DOUBLE) AS collateral_amount,
        'wstETH' as token,
        COUNT(*) AS "# liq"
from curvefi_ethereum.crvusd_controller_wsteth_evt_Liquidate
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5
)

, dates as (
    with day_seq as (select (sequence(cast('2021-10-29' as timestamp), cast(now() as timestamp), interval '1' day)) as day)
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
SELECT
  day, rate as rate0, value_partition, first_value(rate) over (partition by value_partition order by day) as rate,
  lead(day,1,date_trunc('day', now() + interval '1' day)) over(order by day) as next_day
  
FROM (
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



select time, sum("# liq") as "number of liquidations",
        sum(case when upper(token) = 'WSTETH' then collateral_amount*rate
            when upper(token) = 'STETH' then collateral_amount end ) as "liquidated stETH collateral"
from markets_data
left join wsteth_rate on markets_data.time >=wsteth_rate.day and markets_data.time < wsteth_rate.next_day
where collateral_amount > 0.01
group by 1
order by time desc

