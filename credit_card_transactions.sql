-- credit_card_transactions.sql

-- 1. Top 5 cities with highest spends and their percentage contribution
WITH cte AS (
    SELECT city, SUM(amount) AS city_amount
    FROM credit_card_transcations
    GROUP BY city
)
SELECT TOP 5 city, city_amount,
       ROUND(city_amount * 100.0 / SUM(city_amount) OVER (), 2) AS percent_contribution
FROM cte
ORDER BY city_amount DESC;

-- 2. Highest spend month and amount for each card type
WITH cte AS (
    SELECT card_type, MONTH(transaction_date) AS mnth, YEAR(transaction_date) AS yr,
           SUM(amount) AS total_amount
    FROM credit_card_transcations
    GROUP BY card_type, MONTH(transaction_date), YEAR(transaction_date)
)
SELECT card_type, mnth, yr, total_amount
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY card_type ORDER BY total_amount DESC) AS rn
    FROM cte
) a
WHERE rn = 1;

-- 3. Transaction details when each card type reaches 1,000,000 cumulative spend
WITH cte AS (
    SELECT *,
           SUM(amount) OVER (PARTITION BY card_type ORDER BY transaction_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
    FROM credit_card_transcations
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY card_type ORDER BY running_sum) AS rn
    FROM cte
    WHERE running_sum > 1000000
) a
WHERE rn = 1;

-- 4. City with lowest percentage spend for Gold card type
WITH cte AS (
    SELECT city,
           SUM(CASE WHEN card_type = 'Gold' THEN amount ELSE 0 END) AS gold_card_spend,
           SUM(amount) AS total_spend
    FROM credit_card_transcations
    GROUP BY city
    HAVING SUM(CASE WHEN card_type = 'Gold' THEN amount ELSE 0 END) > 0
)
SELECT TOP 1 city,
       gold_card_spend * 100.0 / total_spend AS percentage_gold_spend
FROM cte
ORDER BY percentage_gold_spend ASC;

-- 5. City-wise highest and lowest expense types
WITH cte AS (
    SELECT city, exp_type, SUM(amount) AS total_amount
    FROM credit_card_transcations
    GROUP BY city, exp_type
),
cte1 AS (
    SELECT *,
           FIRST_VALUE(total_amount) OVER (PARTITION BY city ORDER BY total_amount DESC) AS highest_expense,
           FIRST_VALUE(total_amount) OVER (PARTITION BY city ORDER BY total_amount) AS lowest_expense
    FROM cte
)
SELECT city,
       MAX(CASE WHEN highest_expense = total_amount THEN exp_type ELSE NULL END) AS highest_expense_type,
       MAX(CASE WHEN lowest_expense = total_amount THEN exp_type ELSE NULL END) AS lowest_expense_type
FROM cte1
GROUP BY city;

-- 6. Female spend contribution by expense type
WITH cte AS (
    SELECT exp_type,
           SUM(CASE WHEN gender = 'F' THEN amount ELSE 0 END) AS female_spend,
           SUM(amount) AS total_amount
    FROM credit_card_transcations
    GROUP BY exp_type
)
SELECT exp_type,
       ROUND(female_spend * 100.0 / total_amount, 1) AS percentage_female_spend
FROM cte;

-- 7. Highest MoM growth in Jan-2014 by card and expense type
WITH monthly_data AS (
    SELECT card_type, exp_type, YEAR(transaction_date) AS yr, MONTH(transaction_date) AS mnth,
           SUM(amount) AS total_spend
    FROM credit_card_transcations
    GROUP BY card_type, exp_type, YEAR(transaction_date), MONTH(transaction_date)
),
mom_growth AS (
    SELECT a.card_type, a.exp_type, a.total_spend AS jan_spend, b.total_spend AS dec_spend,
           (a.total_spend - b.total_spend) * 1.0 / NULLIF(b.total_spend, 0) AS growth_rate
    FROM monthly_data a
    JOIN monthly_data b ON a.card_type = b.card_type AND a.exp_type = b.exp_type
                      AND a.yr = 2014 AND a.mnth = 1 AND b.yr = 2013 AND b.mnth = 12
)
SELECT TOP 1 card_type, exp_type, growth_rate
FROM mom_growth
ORDER BY growth_rate DESC;

-- 8. Weekend spend to transaction ratio per city
SELECT TOP 1 city,
       SUM(amount) * 1.0 / COUNT(transaction_id) AS transactions_ratio
FROM credit_card_transcations
WHERE DATEPART(WEEKDAY, transaction_date) IN (1, 7)
GROUP BY city
ORDER BY transactions_ratio DESC;

-- 9. City that reached 500 transactions fastest
WITH city_tx_dates AS (
    SELECT city, transaction_date,
           ROW_NUMBER() OVER (PARTITION BY city ORDER BY transaction_date) AS rn
    FROM credit_card_transcations
),
days_diff AS (
    SELECT city,
           MIN(CASE WHEN rn = 1 THEN transaction_date END) AS first_date,
           MIN(CASE WHEN rn = 500 THEN transaction_date END) AS fifth_date
    FROM city_tx_dates
    WHERE rn IN (1, 500)
    GROUP BY city
)
SELECT TOP 1 city,
       DATEDIFF(DAY, first_date, fifth_date) AS days_to_500th_txn
FROM days_diff
ORDER BY days_to_500th_txn;
