SELECT *
FROM news 
WHERE symbol = 'PRGO'
    AND TO_DATE(created_at, 'yyyy-MM-dd') = '2025-01-06'