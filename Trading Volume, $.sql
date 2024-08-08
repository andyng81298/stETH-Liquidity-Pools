/*
This query calculates the liquidity of (w)stETH pools in different blockchains over the past three mounths
*/
--  to create a calendar for the past full three months with a daily interval
with
  dates AS (
    with
      day_seq AS (
        SELECT
          (
            sequence(
              cast(
                date_trunc('month', NOW()) - interval '3' month AS date
              ),
              current_date,
              interval '1' day
            )
          ) AS day
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
              dates d
              LEFT JOIN volumes v ON date_trunc('day', v.time) = date_trunc('day', d.day)
            GROUP BY
              1
    
    
  )

-- This CTE retrieves liquidity and trading data from the subquery and calculates liquidity utilization by blockchain
,  liquidity AS (
    SELECT
      blockchain,
      time,
      SUM(tvl) AS tvl,
      SUM(trading_volume) AS trading_volume,
      SUM(COALESCE(liquidity_usd, 0)) AS liquidity_usd,
      case when SUM(COALESCE(tvl, 0)) > 0 then SUM(COALESCE(trading_volume, 0)) / SUM(COALESCE(tvl, 0)) else 0 end AS liquidity_utilization,
      SUM(steth_amount) as steth_amount
    FROM
      (--This subquery retrieves the trading data in USD, calculates the TVL in USD by blockchain,
      -- combining data from Lido liquidity spell and from 'query_3050644'(Curve TricryptoLLAMA spell) for the last three mounths
        SELECT
          blockchain,
          cast(time AS TIMESTAMP) AS time,
          SUM(main_token_usd_reserve + paired_token_usd_reserve) AS tvl,
          SUM(COALESCE(trading_volume, 0)) AS trading_volume,
   
          SUM(COALESCE(paired_token_usd_reserve, 0)) AS liquidity_usd,
          SUM(
            CASE
              WHEN lower(main_token_symbol) = 'steth' THEN main_token_reserve
              ELSE wsteth_rate.rate * main_token_reserve
            END
            )  AS steth_amount
        FROM
          lido.liquidity l
        LEFT JOIN wsteth_rate ON l.time = wsteth_rate.day    
        WHERE
          date_trunc('month', time) >= date_trunc('month', now()) - interval '3' month
        GROUP BY
          1,
          2
        UNION all
        SELECT
          blockchain,
          cast(time AS TIMESTAMP) AS time,
          SUM(
            main_token_usd_reserve + paired_token_usd_reserve + paired1_token_usd_reserve
          ) AS tvl,
          SUM(COALESCE(trading_volume, 0)) AS trading_volume,
          
          SUM(
            COALESCE(
              paired_token_usd_reserve + paired1_token_usd_reserve,
              0
            )
          ) AS liquidity_usd,
          SUM(main_token_reserve * wsteth_rate.rate) AS steth_amount
        FROM
          query_3050644 l--Curve TricryptoLLAMA https://curve.fi/#/ethereum/pools/factory-tricrypto-2/deposit
         LEFT JOIN wsteth_rate ON l.time = wsteth_rate.day     
        WHERE
          date_trunc('month', time) >= date_trunc('month', now()) - interval '3' month
        GROUP BY
          1,
          2
      )
    GROUP BY
      1,
      2
  )
-- final query aggregates data from previous CTE and calculates moving averages for liquidity utilization and trading volume  
SELECT
  *,
  AVG(liquidity_utilization) over (
    partition BY
      blockchain
    ORDER BY
      time rows between 29 preceding
      AND current row
  ) AS "Liquidity utilization (ma_30)",
  AVG(trading_volume) over (
    partition BY
      blockchain
    ORDER BY
      time rows between 29 preceding
      AND current row
  ) AS "Trading volume (ma_30)"
FROM
  liquidity
  ORDER BY time desc, tvl desc, "Trading volume (ma_30)" desc
