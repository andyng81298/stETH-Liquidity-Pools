with dates AS ( 
    with
        day_seq AS (
            SELECT
          (
            sequence(
              cast('2023-01-27' AS DATE),
              cast(now() AS DATE),
              interval '1' day
            )
          ) day
      )
    SELECT
      days.day
    FROM
      day_seq
      CROSS JOIN unnest(day) AS days(day)
    )


, reserve AS (
SELECT 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0 --wstETH
)

, deposit_evt AS (
SELECT 
date_trunc('day',evt_block_time) AS time,
sum(amount)/1e18 AS amount
FROM (
    SELECT 
        evt_block_time
        , reserve
        , user
        , CAST(amount AS DOUBLE) AS amount
        , evt_tx_hash
    FROM aave_v3_ethereum.Pool_evt_Supply 
    WHERE reserve in (SELECT * FROM reserve)
    ORDER BY  evt_block_time DESC 
    )
GROUP BY 1
)

, withdraw_evt AS (
    SELECT 
        DATE_TRUNC('day', evt_block_time) AS time
        , sum(amount)/1e18 AS amount
    FROM (
        SELECT 
            evt_block_time
            , reserve
            , user
            , CAST(amount AS DOUBLE) AS amount
            , evt_tx_hash
        FROM aave_v3_ethereum.Pool_evt_Withdraw
        WHERE reserve in (SELECT * FROM reserve)
        ORDER BY  evt_block_time DESC 
    )
    GROUP BY 1
)

, borrow_evt AS (
    SELECT 
        DATE_TRUNC('day', evt_block_time) AS time
        , sum(amount)/1e18 AS amount
    FROM (
        SELECT 
            evt_block_time
            , reserve
            , user
            , CAST(amount AS DOUBLE) AS amount
            , evt_tx_hash
        FROM aave_v3_ethereum.Pool_evt_Borrow
        WHERE reserve in (SELECT * FROM reserve)
        ORDER BY  evt_block_time DESC 
    )
    GROUP BY 1
)

, repay_evt AS (
    SELECT 
        DATE_TRUNC('day',evt_block_time) AS time
        , sum(amount)/1e18 AS amount
    FROM (
        SELECT 
            evt_block_time
            , reserve
            , user
            , CAST(amount AS DOUBLE) AS amount
            , evt_tx_hash
        FROM aave_v3_ethereum.Pool_evt_Repay
        WHERE reserve in (SELECT * FROM reserve)
        AND useATokens = False
        ORDER BY  evt_block_time DESC 
    )
    GROUP BY 1
)

, repay_with_collateral AS (
    SELECT 
        DATE_TRUNC('day',evt_block_time) AS time
        , sum(amount)/1e18 AS amount
    FROM (
        SELECT 
            evt_block_time
            , reserve
            , user
            , CAST(amount AS DOUBLE) AS amount
            , evt_tx_hash
    FROM aave_v3_ethereum.Pool_evt_Repay
    WHERE reserve in (SELECT * FROM reserve)
    AND useATokens = True
    ORDER BY  evt_block_time DESC 
    )
    GROUP BY 1
)

, unique_depositors AS (
    SELECT
        *
        , lead(time, 1, DATE_TRUNC('day',now() + interval '24' hour)) over (ORDER BY  time) AS next_time
    FROM (
        SELECT
        distinct DATE_TRUNC('day', evt_block_time) AS time
        , (SELECT
                COUNT( distinct onBehalfOf) 
            FROM aave_v3_ethereum.Pool_evt_Supply  t
            WHERE reserve in (SELECT * FROM reserve)
            AND DATE_TRUNC('day', t.evt_block_time) <= DATE_TRUNC('day', tt.evt_block_time)
        ) 
    AS unique_depositors
    FROM aave_v3_ethereum.Pool_evt_Supply tt
    WHERE reserve in (SELECT * FROM reserve)
    ORDER BY  DATE_TRUNC('day', evt_block_time) DESC
    ) t
    ORDER BY  1 DESC
)

, unique_deposits as (
    SELECT
        *
        , lead(time, 1, DATE_TRUNC('day',now() + interval '24' hour)) over (ORDER BY  time) AS next_time
    FROM (
        SELECT
            DATE_TRUNC('day', evt_block_time) AS time
            , SUM(COUNT(amount)) over (ORDER BY  DATE_TRUNC('day', evt_block_time)) AS unique_deposits
        FROM aave_v3_ethereum.Pool_evt_Supply tt
        WHERE reserve in (SELECT * FROM reserve)
        GROUP BY 1
        ORDER BY  DATE_TRUNC('day', evt_block_time) DESC
    ) t
    ORDER BY  1 DESC
)


, unique_borrowers AS (
    SELECT
        *
        , lead(time, 1, DATE_TRUNC('day',now() + interval '24' hour)) over (ORDER BY  time) AS next_time
    FROM (
        SELECT
            distinct DATE_TRUNC('day', evt_block_time) AS time
            , (SELECT 
                COUNT( distinct onBehalfOf) 
            FROM aave_v3_ethereum.Pool_evt_Borrow  t
            WHERE reserve in (SELECT * FROM reserve)
            AND DATE_TRUNC('day', t.evt_block_time) <= DATE_TRUNC('day', tt.evt_block_time)
        ) 
        AS unique_borrowers
        FROM aave_v3_ethereum.Pool_evt_Borrow tt
        WHERE reserve in (SELECT * FROM reserve)
        ORDER BY  DATE_TRUNC('day', evt_block_time) DESC
    ) t
    ORDER BY  1 DESC
)

, unique_borrows AS (
    SELECT
        *
        , lead(time, 1, DATE_TRUNC('day',now() + interval '24' hour)) over (ORDER BY  time) AS next_time
    FROM (
        SELECT
            DATE_TRUNC('day', evt_block_time) AS time,
            SUM(COUNT(amount)) over (ORDER BY  DATE_TRUNC('day', evt_block_time)) AS unique_borrows
        FROM aave_v3_ethereum.Pool_evt_Borrow tt
        WHERE reserve in (SELECT * FROM reserve)
        GROUP BY 1
        ORDER BY  DATE_TRUNC('day', evt_block_time) DESC
    ) t
    ORDER BY  1 DESC
)

, wstETH_prices AS (
    SELECT distinct
        DATE_TRUNC('day', minute) AS time,
        last_value(price) over (partition by DATE_TRUNC('day', minute) ORDER BY  minute range between unbounded preceding AND unbounded following) AS price
    FROM prices.usd
    WHERE contract_address = 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0
)

, daily_balances AS (
    SELECT
        dates.day AS time
        , COALESCE(deposit.amount,0) AS daily_deposits
        , COALESCE(withdraw.amount,0) AS daily_withdraws
        , COALESCE(borrow.amount,0) AS daily_borrows
        , COALESCE(repay.amount,0) + COALESCE(repay_collateral.amount,0) AS daily_repays
        , COALESCE(deposit.amount,0) - COALESCE(withdraw.amount,0)  - COALESCE(repay_collateral.amount,0) AS daily_net_deposits_balance
        , COALESCE(borrow.amount,0) - COALESCE(repay.amount,0) - COALESCE(repay_collateral.amount,0) AS daily_net_borrows_balance
        , COALESCE(deposit.amount,0) - COALESCE(withdraw.amount,0) - COALESCE(borrow.amount,0) + COALESCE(repay.amount,0) AS daily_pool_balance

    FROM dates
    LEFT JOIN deposit_evt deposit ON dates.day = deposit.time
    LEFT JOIN withdraw_evt withdraw ON dates.day = withdraw.time
    LEFT JOIN borrow_evt borrow ON dates.day = borrow.time
    LEFT JOIN repay_evt repay ON dates.day = repay.time
    LEFT JOIN repay_with_collateral repay_collateral ON dates.day = repay_collateral.time
)


SELECT
    CAST(balances.time AS TIMESTAMP) AS time
    , daily_deposits as daily_inflows
    , daily_withdraws
    , daily_borrows
    , daily_repays
    , daily_net_deposits_balance as daily_net_supply_balance
    , daily_net_borrows_balance as daily_net_demand_balance
    , daily_pool_balance
    , SUM(daily_net_deposits_balance) over (ORDER BY  CAST(balances.time AS TIMESTAMP)) AS "cumulative balance, wstETH" --amount deposited to the pool by users
    , SUM(daily_net_borrows_balance) over (ORDER BY  CAST(balances.time AS TIMESTAMP)) AS cumulative_borrows --amount borrowed FROM the pool by users
    , SUM(daily_pool_balance) over (ORDER BY  CAST(balances.time AS TIMESTAMP)) AS pool_balance --amount currently in the pool
    --, SUM(daily_pool_balance) over (ORDER BY  CAST(balances.time AS TIMESTAMP)) * price AS TVL --amount currently in the pool * token price
    , SUM(daily_net_deposits_balance) over (ORDER BY  CAST(balances.time AS TIMESTAMP)) * price AS TVL
    , unique_deposits as unique_supplies
    , unique_depositors as unique_suppliers
    , unique_borrows
    , unique_borrowers
    , wsteth.price AS price

FROM daily_balances balances
LEFT JOIN wsteth_prices wsteth ON CAST(balances.time AS TIMESTAMP) = wsteth.time
LEFT JOIN unique_deposits ud ON CAST(balances.time AS TIMESTAMP) >= ud.time AND CAST(balances.time AS TIMESTAMP) < ud.next_time
LEFT JOIN unique_depositors uds ON CAST(balances.time AS TIMESTAMP) >= uds.time AND CAST(balances.time AS TIMESTAMP) < uds.next_time
LEFT JOIN unique_borrows ub ON CAST(balances.time AS TIMESTAMP) >= ub.time AND CAST(balances.time AS TIMESTAMP) < ub.next_time
LEFT JOIN unique_borrowers ubs ON CAST(balances.time AS TIMESTAMP) >= ubs.time AND CAST(balances.time AS TIMESTAMP) < ubs.next_time

GROUP BY 1,2,3,4,5,6,7,8,13,14,15,16,17
ORDER BY  1 DESC
