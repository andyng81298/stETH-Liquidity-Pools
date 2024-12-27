/* 
This query calculates the daily and cumulative balances of the Aave v2 lending pool in stETH, 
as well as the TVL in USD
 */
-- THIS CTE generates a sequence of days 
with
  dates AS (
    -- This inner CTE generates a sequence of days from 2022-02-27 with 1-hour intervals
    with
      day_seq AS (
        SELECT
          (
            sequence(
              cast('2022-02-27' AS TIMESTAMP),
              cast(now() AS TIMESTAMP),
              interval '1' day
            )
          ) AS day
      )
      -- selects the individual days from the day_seq CTE    
    SELECT
      days.day
    FROM
      day_seq
      CROSS JOIN unnest (day) AS days (day)
  ),
  -- This CTE retrives 
  volumes AS (
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
  ),
  -- This CTE calculates the stETH to wstETH conversion rate 
  wsteth_rate AS (
    SELECT
      day,
      rate AS rate0,
      value_partition,
      first_value(rate) over (
        partition by
          value_partition
        ORDER BY
          day
      ) AS rate,
      lead(
        day,
        1,
        date_trunc('day', now() + interval '1' day)
      ) over (
        ORDER BY
          day
      ) AS next_day
    FROM
      (
        SELECT
          day,
          rate,
          SUM(
            CASE
              WHEN rate is NULL THEN 0
              ELSE 1
            END
          ) over (
            ORDER BY
              day
          ) AS value_partition
        FROM
          (
            SELECT
              date_trunc('day', d.day) AS day,
              SUM(cast(steth AS DOUBLE)) / SUM(cast(wsteth AS DOUBLE)) AS rate
            FROM
              dates d
              LEFT JOIN volumes v ON date_trunc('day', v.time) = date_trunc('day', d.day)
            GROUP BY
              1
          )
      )
  ),
  -- This CTE calculate the daily amount of stETH transferred into the Aave v2 contract
  steth_in AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS time,
      SUM(cast("value" AS DOUBLE)) / 1e18 AS steth_in,
      SUM(cast("value" AS DOUBLE) / coalesce(r.rate, 1)) / 1e18 AS wsteth_in,
      r.rate
    FROM
      erc20_ethereum.evt_Transfer t
      LEFT JOIN wsteth_rate r on DATE_TRUNC('day', evt_block_time) >= r.day
      AND DATE_TRUNC('day', evt_block_time) < r.next_day
    WHERE
      "contract_address" = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
      AND "to" = 0x1982b2F5814301d4e9a8b0201555376e62F82428
    GROUP BY
      1,
      4
  ),
  -- This CTE calculate the daily amount of stETH transferred out of the Aave v2 contract
  steth_out AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS time,
      - SUM(cast("value" AS DOUBLE)) / 1e18 AS steth_in,
      - SUM(cast("value" AS DOUBLE) / coalesce(r.rate, 1)) / 1e18 AS wsteth_in,
      rate
    FROM
      erc20_ethereum.evt_Transfer t
      LEFT JOIN wsteth_rate r on DATE_TRUNC('day', evt_block_time) >= r.day
      AND DATE_TRUNC('day', evt_block_time) < r.next_day
    WHERE
      "contract_address" = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84
      AND "from" = 0x1982b2F5814301d4e9a8b0201555376e62F82428
    GROUP BY
      1,
      4
  ),
  -- This CTE calculates the daily balance of stETH and wstETH 
  daily_balances AS (
    SELECT
      time,
      SUM(steth_in) steth_balance,
      SUM(wsteth_in) AS wsteth_balance
    FROM
      (
        SELECT
          *
        FROM
          steth_in
        UNION all
        SELECT
          *
        FROM
          steth_out
      )
    GROUP BY
      1
  ),
  -- This CTE calculates the cumulative balance of (w)stETH and daily conversion from wstETH to stETH
  steth_balances AS (
    SELECT
      time,
      SUM(steth_balance) over (
        ORDER BY
          time
      ) AS steth_cumu,
      SUM(coalesce(wsteth_balance, steth_balance)) over (
        ORDER BY
          time
      ) AS wsteth_cumu,
      r.rate,
      (
        SUM(coalesce(wsteth_balance, steth_balance)) over (
          ORDER BY
            time
        )
      ) * coalesce(r.rate, 1) AS steth_from_wsteth,
      wsteth_balance * coalesce(r.rate, 1) AS steth_from_wsteth_daily,
      lead(
        time,
        1,
        date_trunc('day', now() + interval '1' day)
      ) over (
        ORDER BY
          time
      ) AS next_time
    FROM
      daily_balances b
      LEFT JOIN wsteth_rate r on b.time >= r.day
      AND b.time < r.next_day
    ORDER BY
      1
  ),
  -- This CTE retrieves the daily average price of stETH 
  steth_prices_daily AS (
    SELECT distinct
      DATE_TRUNC('day', minute) AS time,
      avg(price) AS price
    FROM
      prices.usd
    WHERE
      date_trunc('day', minute) >= cast('2022-02-27' AS TIMESTAMP)
      AND date_trunc('day', minute) < date_trunc('day', now())
      AND blockchain = 'ethereum'
      AND contract_address = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
    GROUP BY
      1
    UNION all
    SELECT distinct
      DATE_TRUNC('day', minute),
      last_value(price) over (
        partition by
          DATE_TRUNC('day', minute),
          contract_address
        ORDER BY
          minute range between unbounded preceding
          AND unbounded following
      ) AS price
    FROM
      prices.usd
    WHERE
      date_trunc('day', minute) = date_trunc('day', now())
      AND blockchain = 'ethereum'
      AND contract_address = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84
  )
  -- final query combines all the computed data, associates labels, and calculates the TVL   
SELECT
  d.day,
  steth_from_wsteth AS "cumulative balance, stETH",
  COALESCE(steth_balance, 0) AS "daily balance, stETH",
  steth_from_wsteth * stethp.price AS tvl
FROM
  dates d
  LEFT JOIN steth_balances b on d.day >= b.time
  AND d.day < b.next_time
  LEFT JOIN steth_prices_daily stethp on d.day = stethp.time
  LEFT JOIN daily_balances db on d.day = db.time
ORDER BY
  1
 
