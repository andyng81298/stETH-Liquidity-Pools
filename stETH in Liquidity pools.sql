/*
This query retrieves the present amount of stETH liquidity in USD and the TVL in USD for the current date
 */
 
 -- to retrieve daily amounts of (w)stETH involved in unwrapping and wrapping transactions
  -- for the further calculation of wstETH:stETH
 with volumes AS (
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
  
  -- to calculate the current rate of wstETH:stETH from CTE 'volumes'
  , wsteth_rate AS (
            SELECT  SUM(cast(steth AS DOUBLE)) / SUM(cast(wsteth AS DOUBLE)) AS rate
            FROM
              volumes v 
             WHERE date_trunc('day', v.time)  = date_trunc('day', now())
            
  )
 
SELECT
  SUM(liquidity_usd) AS liquidity_usd,
  SUM(tvl) AS tvl,
  SUM(steth_amount) as steth_amount
FROM
  ( --this subquery calculates the pared token reserve in USD and TVL in USD filtering for the current date
    SELECT
      SUM(paired_token_usd_reserve) AS liquidity_usd,
      SUM(main_token_usd_reserve) + SUM(paired_token_usd_reserve) AS tvl,
       SUM(
            CASE
              WHEN lower(main_token_symbol) = 'steth' THEN main_token_reserve
              ELSE main_token_reserve * (select rate from wsteth_rate)
            END
            )  AS steth_amount
    FROM
      lido.liquidity
    WHERE
      cast(time AS DATE) = cast(now() AS DATE)
    UNION all
    --this subquery calculates the two pared token reserves in USD and tvl in USD filtering for the current date
    SELECT
      SUM(paired_token_usd_reserve) + SUM(paired1_token_usd_reserve) AS liquidity_usd,
      SUM(main_token_usd_reserve) + SUM(paired_token_usd_reserve) + SUM(paired1_token_usd_reserve) AS tvl,
      SUM(main_token_reserve * (select rate from wsteth_rate)) as steth_amount
    FROM
      query_3050644 l --Curve TricryptoLLAMA https://curve.fi/#/ethereum/pools/factory-tricrypto-2/deposit
    WHERE
      cast(time AS DATE) = cast(now() AS DATE)
  )
