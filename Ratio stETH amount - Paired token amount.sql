/*
This query calculates the liquidity and trading data for the WETH:wstETH pool
across various blockchains and projects (protocols) over the past three months
*/
-- This CTE generates a data sequence for the past three mounths with 1d interval
with
  calendar AS (
    with
      day_seq AS (
        SELECT
          (
            sequence(
              cast(
                date_trunc('month', now()) - interval '3' month AS date
              ),
              cast(now() AS date),
              interval '1' day
            )
          ) day
      )
    SELECT
      days.day
    FROM
      day_seq
      CROSS JOIN unnest (day) AS days (day)
  )
 

 -- to retrieve daily amounts of (w)stETH involved in unwrapping and wrapping transactions
  -- for the further calculation of wstETH:stETH
 , volumes AS (
    SELECT
      u.call_block_time AS time,
      "output_0" AS steth,
      "_wstETHAmount" AS wsteth
    FROM
      lido_ethereum.WstETH_call_unwrap u
    WHERE
      "call_success" = TRUE
    UNION all
    SELECT
      u."call_block_time",
      "_stETHAmount" AS steth,
      "output_0" AS wsteth
    FROM
      lido_ethereum.WstETH_call_wrap u
    WHERE
      "call_success" = TRUE
  )
  
  -- to calculate the daily rate of wstETH:stETH from CTE 'volumes' combined with CTE 'dates'
  , wsteth_rate AS (
            SELECT
              date_trunc('day', d.day) AS day,
              SUM(cast(steth AS DOUBLE)) / SUM(cast(wsteth AS DOUBLE)) AS rate
            FROM
              calendar d
              LEFT JOIN volumes v ON date_trunc('day', v.time) = date_trunc('day', d.day)
            GROUP BY
              1
  )

  -- to gather stablecoins addresses from query 'Stablecoins'
  , stables_address (address) AS (
    SELECT
      *
    FROM
      query_2330017
  )
  
 -- to gather WETH addresses on different blockchains from the query 'ETH addresses across blockchains'
, weth_addresses AS (
    SELECT 
      * 
    FROM query_3237443
  )
 
 , liquidity as (
    SELECT  l.project, l.blockchain, l.pool, l.pool_name, main_token, paired_token,
          cast(l.time AS TIMESTAMP) AS time,
          SUM(
            COALESCE(main_token_usd_reserve, 0) + COALESCE(paired_token_usd_reserve, 0) 
          ) AS tvl,
          SUM(
            paired_token_usd_reserve 
          ) AS liquidity_usd,
          SUM(paired_token_reserve) as paired_token_amount,
          SUM(case when lower(main_token_symbol) = 'wsteth' then main_token_reserve * wsteth_rate.rate else main_token_reserve end) AS steth_amount,
          CASE
            WHEN SUM(COALESCE(trading_volume, 0)) < 1 THEN 0
            ELSE SUM(COALESCE(trading_volume, 0))
          END AS trading_volume
          
    FROM lido.liquidity l
    LEFT JOIN wsteth_rate ON l.time = wsteth_rate.day    
    WHERE date_trunc('month', time) >= date_trunc('month', now()) - interval '3' month
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    
    UNION ALL
    
    SELECT  l.project, 'ethereum' as blockchain, l.pool, l.pool_name, main_token, paired_token,
          cast(l.time AS TIMESTAMP) AS time,
          SUM(
            COALESCE(main_token_usd_reserve, 0) + COALESCE(paired_token_usd_reserve, 0) + COALESCE(paired1_token_usd_reserve, 0)
          ) AS tvl,
          SUM(
            paired_token_usd_reserve + COALESCE(paired1_token_usd_reserve, 0)
          ) AS liquidity_usd,
          
          SUM(paired_token_reserve) AS 	paired_token_amount,
          SUM(main_token_reserve * wsteth_rate.rate) AS steth_amount,
          CASE
            WHEN SUM(COALESCE(trading_volume, 0)) < 1 THEN 0
            ELSE SUM(COALESCE(trading_volume, 0))
          END AS trading_volume
          
    FROM query_3050644 l
    LEFT JOIN wsteth_rate ON l.time = wsteth_rate.day    
    WHERE date_trunc('month', time) >= date_trunc('month', now()) - interval '3' month
    GROUP BY 1, 2, 3, 4, 5, 6, 7
 
 )
  -- This CTE collects distinct pool lido liquidity spell filtred by main token reserve
,  pools_list AS (
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
          liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND paired_token in (SELECT address FROM weth_addresses) AND '{{Pool type}}' = 'ETH'
      AND project = '{{Venue}}' AND '{{Venue}}' != 'all' AND blockchain = '{{Blockchain}}' AND '{{Blockchain}}' != 'all'
    
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
          liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND paired_token in (SELECT address FROM weth_addresses) AND '{{Pool type}}' = 'ETH'
      AND project = '{{Venue}}' AND '{{Venue}}' != 'all' AND '{{Blockchain}}' = 'all'
      
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
         liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND paired_token in (SELECT address FROM weth_addresses)  AND '{{Pool type}}' = 'ETH'
      AND '{{Venue}}' = 'all' AND blockchain = '{{Blockchain}}' AND '{{Blockchain}}' != 'all'
       
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
         liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND paired_token in (SELECT address FROM weth_addresses)  AND '{{Pool type}}' = 'ETH'
      AND '{{Venue}}' = 'all' AND '{{Blockchain}}' = 'all'
      
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
         liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND paired_token in (SELECT address FROM stables_address) AND '{{Pool type}}' = 'Stables'
      AND project = '{{Venue}}' AND '{{Venue}}' != 'all' AND blockchain = '{{Blockchain}}' AND '{{Blockchain}}' != 'all'
    
      
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
         liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND paired_token in (SELECT address FROM stables_address) AND '{{Pool type}}' = 'Stables'
      AND project = '{{Venue}}' AND '{{Venue}}' != 'all' AND '{{Blockchain}}' = 'all'
      
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
          liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND paired_token in (SELECT address FROM stables_address)  AND '{{Pool type}}' = 'Stables'
      AND '{{Venue}}' = 'all'  AND blockchain = '{{Blockchain}}' AND '{{Blockchain}}' != 'all'
      
      
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
          liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND paired_token in (SELECT address FROM stables_address)  AND '{{Pool type}}' = 'Stables'
      AND '{{Venue}}' = 'all'  AND '{{Blockchain}}' =  'all'
      
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
          liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND '{{Pool type}}' = 'all'
      AND project = '{{Venue}}' AND '{{Venue}}' != 'all' AND blockchain = '{{Blockchain}}' AND '{{Blockchain}}' != 'all'
    
      
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
          liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND '{{Pool type}}' = 'all'
      AND project = '{{Venue}}' AND '{{Venue}}' != 'all' AND '{{Blockchain}}' =  'all'
      
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
          liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND '{{Pool type}}' = 'all' AND '{{Venue}}' = 'all' AND blockchain = '{{Blockchain}}' AND '{{Blockchain}}' != 'all'
      
    UNION ALL
    
    SELECT distinct
      pool
    FROM
      liquidity
    WHERE
      time = (
        SELECT
          MAX(time)
        FROM
          liquidity
      )
      AND steth_amount >= {{stETH amount From}}
      AND steth_amount <= {{stETH amount To}}
      AND '{{Pool type}}' = 'all' AND '{{Venue}}' = 'all' AND '{{Blockchain}}' = 'all'
    
  )
  
  -- This CTE retrieves liquidity and trading data, and calculates liquidity utilization and coefficient liquidity per 1 wsteth.
 , liquidity_by_pool AS (
    SELECT
      l.pool_name,
      l.pool,
      cast(l.time AS TIMESTAMP) AS time,
      lead(l.time, 1, now()) over (partition BY l.pool_name   ORDER BY time) AS next_time,
      tvl,
      liquidity_usd,
      steth_amount,
      paired_token_amount,
      trading_volume,
      CASE
        WHEN tvl < 1 THEN 0
        ELSE COALESCE(trading_volume, 0) / tvl END AS liquidity_utilization
      
    FROM
      liquidity l
      
  )
  
-- final query aggregates data from previous CTE and calculates moving averages for liquidity utilization and trading volume  
SELECT
    cast(c.day AS TIMESTAMP) AS day,
    l.pool_name,
    l.pool,
    time,
    tvl,
    liquidity_usd,
    steth_amount,
    paired_token_amount,
    case when paired_token_amount = 0 then 0 else steth_amount / paired_token_amount end as rate,
    trading_volume,
    liquidity_utilization,
    AVG(liquidity_utilization) over (
    partition BY
      pool_name
    ORDER BY
      time rows between 29 preceding
      AND current row
    ) AS "Liquidity utilization (ma_30)",
    AVG(trading_volume) over (
    partition BY
      pool_name
    ORDER BY
      time rows between 29 preceding
      AND current row
    ) AS "Trading volume (ma_30)"
FROM
  calendar c
  left join liquidity_by_pool l on c.day = l.time
WHERE l.pool in (SELECT  * FROM  pools_list)
ORDER BY day desc, tvl desc
