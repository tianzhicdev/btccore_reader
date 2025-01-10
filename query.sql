MAX_DATE = SELECT MAX(timestamp) FROM transactions;
if WORKING_DATE < MAX_DATE:
    # data is up to date

    SELECT COUNT(DISTINCT address) FROM (
    SELECT address
    FROM transactions WHERE timestamp < WORKING_DATE
    GROUP BY address
    HAVING SUM(amount) > 1 AND MIN(timestamp) < WORKING_DATE - INTERVAL '1 year' 
    ) AS HODLER_ADDRESS;


SELECT * from (SELECT 
    address as address, 
    SUM(amount) as balance,
    MIN(timestamp) created_at,
    ARRAY_AGG(block_number order by block_number) AS block_numbers,
    ARRAY_AGG(amount order by block_number) AS changes
FROM 
    transactions
GROUP BY 
    address HAVING SUM(amount) > 1) AS balances WHERE created_at < '2011-03-28';