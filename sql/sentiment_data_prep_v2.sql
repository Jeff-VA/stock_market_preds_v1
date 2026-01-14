WITH cte_news AS (
    SELECT DISTINCT id AS news_article_id
        , TO_DATE(created_at, 'yyyy-MM-dd') AS news_date
        , created_at
        , symbols
        , explode(SPLIT(symbols, ',')) AS symbol
        , 'Headline: ' || headline 
            || ' Summary: ' || COALESCE(summary, '')
            -- || ' All Symbols Referenced in Article: ' || symbols 
            AS article_text
    FROM news
)

, cte_stock_prices AS (
    SELECT TO_DATE(timestamp, 'yyyy-MM-dd') AS stock_date
        , symbol
        , (close - open) / open AS percent_daily_price_change
    FROM stocks
)

SELECT n.news_article_id
    , n.symbol
    , n.news_date
    , n.article_text
    , s.percent_daily_price_change
FROM cte_news n 
    JOIN cte_stock_prices s 
        ON n.symbol = s.symbol
        --join next day's stock data to get percent change
        AND n.news_date = DATE_ADD(s.stock_date, -1)
;
