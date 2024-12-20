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
        SUM(p.price *debtToCover) / pow(10, decimals)  AS debt_to_cover_usd,
        p.symbol  as token
from aave_v2_ethereum.LendingPool_evt_LiquidationCall l
join reserves r on l."collateralAsset" = r.address 
 left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and l.debtAsset = p.contract_address and p.blockchain = 'ethereum'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5, decimals

union all

select  'aave v3' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(p.price *debtToCover) / pow(10, decimals)  AS debt_to_cover_usd,
        p.symbol  as token
from aave_v3_ethereum.Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and l.debtAsset = p.contract_address and p.blockchain = 'ethereum'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5, decimals

union all

select  'aave v3' as protocol,
        'arbitrum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(p.price *debtToCover) / pow(10, decimals)  AS debt_to_cover_usd,
        p.symbol  as token
from aave_v3_arbitrum.L2Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and l.debtAsset = p.contract_address and p.blockchain = 'arbitrum'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5, decimals

union all

select  'aave v3' as protocol,
        'optimism' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(p.price *debtToCover) / pow(10, decimals)  AS debt_to_cover_usd,
        p.symbol  as token
from aave_v3_optimism.Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and l.debtAsset = p.contract_address and p.blockchain = 'optimism'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5, decimals

union all

select  'aave v3' as protocol,
        'polygon' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(p.price *debtToCover) / pow(10, decimals)  AS debt_to_cover_usd,
        p.symbol  as token
from aave_v3_polygon.Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and l.debtAsset = p.contract_address and p.blockchain = 'polygon'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5, decimals

union all

select  'compound' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(usdValue) / CAST(1e18 AS DOUBLE),
        p.symbol
        
from compound_v3_ethereum.cWETHv3_evt_AbsorbCollateral l
join reserves r on l.contract_address= r.address 
left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and l.asset = p.contract_address and p.blockchain = 'ethereum'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'maker(wsteth-a)' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        sum("art")/1e18,
        'DAI'
        
from maker_ethereum.dog_evt_Bark dog
where dog."ilk" = 0x5753544554482d41000000000000000000000000000000000000000000000000
  and  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'maker(wsteth-b)' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        sum("art")/1e18,
        'DAI'
        
from maker_ethereum.dog_evt_Bark dog
where dog."ilk" = 0x5753544554482d42000000000000000000000000000000000000000000000000
  and  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all

select  'maker(stecrv)' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        sum("art")/1e18,
        'DAI'
from maker_ethereum.dog_evt_Bark dog
where dog."ilk" = 0x5753544554482d42000000000000000000000000000000000000000000000000
  and  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

union all


select 'raft' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(p.price *cast(debtLiquidated as double)) / pow(10, decimals) ,
        p.symbol 
from raft_deposit_ethereum.PositionManager_evt_Liquidation l
join reserves r on l.collateralToken = r.address 
left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and p.contract_address = 0x183015a9ba6ff60230fdeadc3f43b3d788b13e21 and p.blockchain = 'ethereum'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5,decimals

union all


select  'spark' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(p.price *"debtToCover") / pow(10, decimals) ,
        p.symbol as token
from spark_protocol_ethereum.Pool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and l.debtAsset = p.contract_address and p.blockchain = 'ethereum'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5, decimals

union all

select  'radiant' as protocol,
        'arbitrum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
        SUM(p.price * "debtToCover") / pow(10, decimals) ,
        p.symbol as token
from radiant_capital_arbitrum.LendingPool_evt_LiquidationCall l
join reserves r on l.collateralAsset = r.address 
left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and l.debtAsset = p.contract_address and p.blockchain = 'arbitrum'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5, decimals

union all

select  'curve(crvUSD)' as protocol,
        'ethereum' as blockchain,
        DATE_TRUNC('day', "evt_block_time") AS time,
         SUM(p.price * cast(collateral_received as double)) / CAST(1e18 AS DOUBLE),
        'crvUSD'
from curvefi_ethereum.crvusd_controller_wsteth_evt_Liquidate l
left join prices.usd p on date_trunc('minute', l.evt_block_time) = p.minute and p.contract_address = 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0 and p.blockchain = 'ethereum'
where  date_trunc('month', "evt_block_time") >= date_trunc('month',now()) - interval '3' month
group by 1,2,3,5

)


select  --time, 
        --protocol, --blockchain, 
        token,  
        sum(debt_to_cover_usd) as "$ Covered Debt"
        
from markets_data
group by 1--,2,3,4
having sum(debt_to_cover_usd) > 0
--order by time desc
