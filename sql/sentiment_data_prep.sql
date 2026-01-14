WITH cte_news AS (
    SELECT DISTINCT TO_DATE(created_at, 'yyyy-MM-dd') AS news_date
        , created_at
        , symbols
        , explode(SPLIT(symbols, ',')) AS symbol
        , 'Headline: ' || headline 
            || ' Summary: ' || COALESCE(summary, '')
            || ' All Symbols Referenced in Article: ' || symbols AS article_text
    FROM news
)

, cte_articles_ranked(
    SELECT news_date
        , symbol
        , 'Article Number ' 
            || ROW_NUMBER() OVER(PARTITION BY news_date, symbol ORDER BY created_at)
            || ': '
            || article_text AS ranked_article_text
    FROM cte_news
)

, cte_daily_news_agg AS (
    SELECT news_date
        , symbol
        , ARRAY_JOIN(ARRAY_AGG(ranked_article_text), '    /n/n/n/n/n     ') AS daily_news_text
    FROM cte_articles_ranked
    GROUP BY news_date
        , symbol
)

, cte_stock_prices AS (
    SELECT TO_DATE(timestamp, 'yyyy-MM-dd') AS stock_date
        , symbol
        , (close - open) / open AS percent_daily_price_change
    FROM stocks
)

SELECT n.symbol
    , n.news_date
    , n.daily_news_text
    , s.percent_daily_price_change
FROM cte_daily_news_agg n 
    JOIN cte_stock_prices s 
        ON n.symbol = s.symbol
        --join next day's stock data to get percent change
        AND n.news_date = DATE_ADD(s.stock_date, -1)
;
