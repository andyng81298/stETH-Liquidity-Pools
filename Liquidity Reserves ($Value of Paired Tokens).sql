/*
This query retrieves the current liquidity reserves in USD braking down by pared token categories
 */
-- This CTE generates a sequency of dates from  three  mounts ago to current date
with
  calendar AS (
    with
      day_seq AS (
        SELECT
          (
            sequence(
              cast(
                date_trunc('month', now()) - interval '3' month AS DATE
              ),
              cast(now() AS DATE),
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
  -- This CTE collects stablecoins contracts addresses frpm 'query_2330017'(stablecoins)
,
  stables (address) AS (
    SELECT
      *
    FROM
      query_2330017
  )
  -- This CTE collects WETH addresses on different chains
,
  weth (address) AS (
    values
      (0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2), -- WETH Ethereum
      (0x0000000000000000000000000000000000000000), --ETH Ethereum dex.trades
      (0x4200000000000000000000000000000000000006), -- WETH Opti
      (0x82aF49447D8a07e3bd95BD0d56f35241523fBab1), --WETH Arbitrum
      (0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619), --Polygon WETH
      (0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), --ETH Opti, Arbi, Polygon
      (0x60D604890feaa0b5460B28A424407c24fe89374a), --bb-a-WETH Bal
      (0x43894DE14462B421372bCFe445fA51b1b4A0Ff3D), --bb-a-WETH Bal Poly
      (0xad28940024117b442a9efb6d0f25c8b59e1c950b), --bb-a-WETH Bal Arbi
      (0xdd89c7cd0613c1557b2daac6ae663282900204f1), --Beets-a-WETH
      (0xda1cd1711743e57dd57102e9e61b75f3587703da), --(bb-a-WETH) Arbi
      (0xbb6881874825e60e1160416d6c426eae65f2459e) --bb-a-WETH
  )
  -- This CTE retrieves the paired token reserves in USD and calculates TVL in USD
,
  liquidity AS (
    SELECT --paired_token_symbol,
      category,
      time,
      sum(tvl) AS tvl,
      sum(liquidity_usd) AS liquidity_usd
    FROM
      (
        SELECT
          CASE
            WHEN l.paired_token in (
              SELECT
                address
              FROM
                stables
            ) THEN 'Stablecoins'
            WHEN l.paired_token in (
              SELECT
                address
              FROM
                weth
            ) THEN 'ETH'
            ELSE 'Others'
          END AS category,
          cast(l.time AS TIMESTAMP) AS time,
          CASE
            WHEN sum(
              coalesce(main_token_usd_reserve, 0) + coalesce(paired_token_usd_reserve, 0)
            ) < 1 THEN 0
            ELSE sum(
              coalesce(main_token_usd_reserve, 0) + coalesce(paired_token_usd_reserve, 0)
            )
          END AS tvl,
          sum(paired_token_usd_reserve) AS liquidity_usd
        FROM
          lido.liquidity l
        WHERE
          l.time = (
            SELECT
              max(time)
            FROM
              lido.liquidity
          )
        GROUP BY
          1,
          2
        UNION all
        SELECT --paired_token_symbol,
          'Stablecoins',
          cast(l.time AS TIMESTAMP) AS time,
          CASE
            WHEN sum(
              coalesce(main_token_usd_reserve, 0) + coalesce(paired_token_usd_reserve, 0) + coalesce(paired1_token_usd_reserve, 0)
            ) < 1 THEN 0
            ELSE sum(
              coalesce(main_token_usd_reserve, 0) + coalesce(paired_token_usd_reserve, 0) + coalesce(paired1_token_usd_reserve, 0)
            )
          END AS tvl,
          sum(paired_token_usd_reserve) + sum(paired1_token_usd_reserve) AS liquidity_usd
        FROM
          query_3050644 l --Curve TricryptoLLAMA https://curve.fi/#/ethereum/pools/factory-tricrypto-2/deposit        
        WHERE
          l.time = (
            SELECT
              max(time)
            FROM
              lido.liquidity
          )
        GROUP BY
          1,
          2
      )
    GROUP BY
      1,
      2
  )
  -- final query selects the timestamp and all data from the `liquidity` subquery
SELECT
  cast(l.time AS TIMESTAMP) AS day,
  l.*
FROM
  liquidity l
 
