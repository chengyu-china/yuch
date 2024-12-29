WITH 
-- 1. Date series to generate all days between June 1, 2020 and September 30, 2020
date_series AS (
    SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'::interval) AS dt_report
),

-- 2. Filter enabled users and join with trades data
user_trades AS (
    SELECT 
        t.dt_report, 
        t.login_hash,
        t.server_hash,
        t.symbol,
        t.currency,
        t.volume,
        t.trade_count,
        u.enabled
    FROM trades t
    JOIN users u ON t.login_hash = u.login_hash
    WHERE u.enabled = true
),

-- 3. Calculate volume for the previous 7 days and total volume so far for each login/server/symbol
volume_calculations AS (
    SELECT 
        dt_report,
        login_hash,
        server_hash,
        symbol,
        currency,
        SUM(volume) OVER (PARTITION BY login_hash, server_hash, symbol, currency 
                          ORDER BY dt_report ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sum_volume_prev_7d,
        SUM(volume) OVER (PARTITION BY login_hash, server_hash, symbol, currency) AS sum_volume_prev_all
    FROM user_trades
),

-- 4. Rank based on volume in the last 7 days and trade count
volume_ranks AS (
    SELECT 
        dt_report,
        login_hash,
        server_hash,
        symbol,
        currency,
        RANK() OVER (PARTITION BY symbol ORDER BY sum_volume_prev_7d DESC) AS rank_volume_symbol_prev_7d,
        RANK() OVER (PARTITION BY login_hash ORDER BY trade_count DESC) AS rank_count_prev_7d
    FROM volume_calculations
),

-- 5. Calculate August 2020 volume only
august_volume AS (
    SELECT 
        login_hash,
        server_hash,
        symbol,
        currency,
        SUM(volume) AS sum_volume_2020_08
    FROM user_trades
    WHERE dt_report BETWEEN '2020-08-01' AND '2020-08-31'
    GROUP BY login_hash, server_hash, symbol, currency
),

-- 6. Calculate first trade date
first_trade AS (
    SELECT 
        login_hash,
        server_hash,
        symbol,
        currency,
        MIN(dt_report) AS date_first_trade
    FROM user_trades
    GROUP BY login_hash, server_hash, symbol, currency
),

-- 7. Final report combining all data
final_report AS (
    SELECT 
        ds.dt_report,
        ut.login_hash,
        ut.server_hash,
        ut.symbol,
        ut.currency,
        COALESCE(vc.sum_volume_prev_7d, 0) AS sum_volume_prev_7d,
        COALESCE(vc.sum_volume_prev_all, 0) AS sum_volume_prev_all,
        COALESCE(vr.rank_volume_symbol_prev_7d, 0) AS rank_volume_symbol_prev_7d,
        COALESCE(vr.rank_count_prev_7d, 0) AS rank_count_prev_7d,
        COALESCE(av.sum_volume_2020_08, 0) AS sum_volume_2020_08,
        ft.date_first_trade,
        ROW_NUMBER() OVER (PARTITION BY ds.dt_report, ut.login_hash, ut.server_hash, ut.symbol ORDER BY ds.dt_report DESC) AS row_number
    FROM date_series ds
    LEFT JOIN user_trades ut ON ds.dt_report = ut.dt_report
    LEFT JOIN volume_calculations vc ON ut.login_hash = vc.login_hash 
                                      AND ut.server_hash = vc.server_hash 
                                      AND ut.symbol = vc.symbol 
                                      AND ut.currency = vc.currency
    LEFT JOIN volume_ranks vr ON ut.login_hash = vr.login_hash 
                               AND ut.server_hash = vr.server_hash 
                               AND ut.symbol = vr.symbol 
                               AND ut.currency = vr.currency
    LEFT JOIN august_volume av ON ut.login_hash = av.login_hash 
                               AND ut.server_hash = av.server_hash 
                               AND ut.symbol = av.symbol 
                               AND ut.currency = av.currency
    LEFT JOIN first_trade ft ON ut.login_hash = ft.login_hash 
                              AND ut.server_hash = ft.server_hash 
                              AND ut.symbol = ft.symbol 
                              AND ut.currency = ft.currency
)

-- 8. Final result with sorting by row_number in descending order
SELECT 
    dt_report,
    login_hash,
    server_hash,
    symbol,
    currency,
    sum_volume_prev_7d,
    sum_volume_prev_all,
    rank_volume_symbol_prev_7d,
    rank_count_prev_7d,
    sum_volume_2020_08,
    date_first_trade,
    row_number
FROM final_report
ORDER BY row_number DESC;
