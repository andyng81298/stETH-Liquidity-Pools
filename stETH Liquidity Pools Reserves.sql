/*
This query created to track the amount of (w)stETH in various liquidity pools over time.
*/
-- to retrieve the daily price of wstETH combining the successful unwrapping and wrapping transactions 
with
  wsteth_steth_price AS (
    SELECT
      time,
      SUM(steth) / SUM(wsteth) AS price
    FROM
      (-- daily amounts of stETH and wstETH involved in successful unwrapping transactions 
        SELECT
          date_trunc('day', call_block_time) AS time,
          COALESCE(CAST("output_0" AS DOUBLE), 0) AS steth,
          COALESCE(CAST("_wstETHAmount" AS DOUBLE), 0) AS wsteth
        FROM
          lido_ethereum.WstETH_call_unwrap
        WHERE
          call_success = TRUE
        UNION all
        -- daily amounts of stETH and wstETH involved in successful wrapping transactions 
        SELECT
          date_trunc('day', call_block_time) AS time,
          COALESCE(CAST("_stETHAmount" AS DOUBLE), 0) AS steth,
          COALESCE(CAST("output_0" AS DOUBLE), 0) AS wsteth
        FROM
          lido_ethereum.WstETH_call_wrap
        WHERE
          "call_success" = TRUE
      )
    GROUP BY
      1
  ) ,
  -- to calculate the daily amount of stETH in each project, 
  -- summing the main token reserves and converting wstETH to stETH using price calculated earlier
  reserves AS (
    SELECT
      cast(balances.time AS TIMESTAMP) AS time,
      project,
      SUM(
        CASE
          WHEN upper(balances.main_token_symbol) = 'WSTETH' THEN balances.main_token_reserve * COALESCE(price.price, 1)
          ELSE balances.main_token_reserve
        END
      ) AS amount_steth,
      SUM(paired_token_usd_reserve) AS paired_token_usd_reserve
    FROM
      lido.liquidity balances
      LEFT JOIN wsteth_steth_price price ON price.time = balances.time
    WHERE
      date_trunc('month', balances.time) >= date_trunc('month', NOW()) - interval '3' month
    
    GROUP BY 1,2
    
    UNION all
    
    SELECT
      cast(l.time AS TIMESTAMP) AS time,
      project,
      SUM(
        CASE
          WHEN upper(l.main_token_symbol) = 'WSTETH' THEN l.main_token_reserve * COALESCE(price.price, 1)
          ELSE l.main_token_reserve
        END ) AS amount_steth,
      SUM(paired_token_usd_reserve) + SUM(paired1_token_usd_reserve) AS paired_token_usd_reserve
    FROM
      query_3050644 l --Curve TricryptoLLAMA https://curve.fi/#/ethereum/pools/factory-tricrypto-2/deposit
      LEFT JOIN wsteth_steth_price price ON price.time = l.time
    WHERE
      date_trunc('month', l.time) >= date_trunc('month', NOW()) - interval '3' month
    GROUP BY
      1,
      2
  )
  
  -- to aggregate the daily stETH amounts and paired token reserves in USD by project from previous CTE
, steth_by_project as (
    SELECT
      time,
      project,
      SUM(amount_steth) AS amount_steth,
      SUM(paired_token_usd_reserve) AS paired_token_usd_reserve
    FROM
      reserves
    GROUP BY
      1,
      2
  )
  

-- to retrieve daily stETH amounts, paired token reserves in USD for each project 
SELECT
 time,
 case when amount_steth >= 100 then project else 'others' end as project,
 sum(amount_steth) as amount_steth,
 sum(paired_token_usd_reserve) as paired_token_usd_reserve
FROM
  steth_by_project
GROUP BY 1, 2  
ORDER BY 1 desc, 4 desc
