-- XGBoost Feature Set: Join all data sources with point-in-time awareness
-- All features are lagged by 1 day to reflect what was known at prediction time

WITH 
-- Step 1: Base stock data with percent daily price change
base_stock AS (
    SELECT 
        sd.symbol,
        DATE(sd.timestamp) AS prediction_date,
        sd.open AS sd_open,
        sd.high AS sd_high,
        sd.low AS sd_low,
        sd.close AS sd_close,
        sd.volume AS sd_volume,
        sd.vwap AS sd_vwap,
        sd.trade_count AS sd_trade_count,
        -- Target variable: percent daily price change
        (sd.close - sd.open) / sd.open AS percent_daily_price_change,
        -- Lag date for joining features (what was known the day before)
        DATE_ADD(DATE(sd.timestamp), -1) AS feature_date
    FROM stock_data sd
),

-- Step 2a: Map earnings release dates to fiscal periods
earnings_release_mapping AS (
    SELECT 
        ed.symbol,
        DATE(ed.release_date) AS release_date,
        ed.`EPS Estimate` AS ed_eps_estimate,
        ed.`Reported EPS` AS ed_reported_eps,
        ed.`Surprise(%)` AS ed_surprise_pct
    FROM earnings_dates ed
    WHERE ed.release_date IS NOT NULL
),

-- Step 2b: Income statement with release dates
income_with_release AS (
    SELECT 
        i.symbol,
        DATE(i.fiscal_period_end) AS fiscal_period_end,
        -- Get first release date after fiscal period end
        (SELECT MIN(DATE(e.release_date)) 
         FROM earnings_dates e 
         WHERE e.symbol = i.symbol 
         AND DATE(e.release_date) > DATE(i.fiscal_period_end)) AS release_date,
        -- Income statement features (selected key metrics)
        i.`Total Revenue` AS is_total_revenue,
        i.`Gross Profit` AS is_gross_profit,
        i.`Operating Income` AS is_operating_income,
        i.`Net Income` AS is_net_income,
        i.`EBITDA` AS is_ebitda,
        i.`EBIT` AS is_ebit,
        i.`Diluted EPS` AS is_diluted_eps,
        i.`Basic EPS` AS is_basic_eps,
        i.`Cost Of Revenue` AS is_cost_of_revenue,
        i.`Operating Expense` AS is_operating_expense,
        i.`Interest Expense` AS is_interest_expense,
        i.`Tax Provision` AS is_tax_provision,
        i.`Research And Development` AS is_research_and_development,
        i.`Selling General And Administration` AS is_selling_general_admin,
        -- Calculated ratios
        CASE WHEN i.`Total Revenue` > 0 THEN i.`Gross Profit` / i.`Total Revenue` ELSE NULL END AS is_gross_margin,
        CASE WHEN i.`Total Revenue` > 0 THEN i.`Operating Income` / i.`Total Revenue` ELSE NULL END AS is_operating_margin,
        CASE WHEN i.`Total Revenue` > 0 THEN i.`Net Income` / i.`Total Revenue` ELSE NULL END AS is_net_margin
    FROM income_statement i
),

-- Step 2c: Balance sheet with release dates
balance_with_release AS (
    SELECT 
        b.symbol,
        DATE(b.fiscal_period_end) AS fiscal_period_end,
        (SELECT MIN(DATE(e.release_date)) 
         FROM earnings_dates e 
         WHERE e.symbol = b.symbol 
         AND DATE(e.release_date) > DATE(b.fiscal_period_end)) AS release_date,
        -- Balance sheet features (selected key metrics)
        b.`Total Assets` AS bs_total_assets,
        b.`Total Liabilities Net Minority Interest` AS bs_total_liabilities,
        b.`Stockholders Equity` AS bs_stockholders_equity,
        b.`Total Debt` AS bs_total_debt,
        b.`Net Debt` AS bs_net_debt,
        b.`Cash Cash Equivalents And Short Term Investments` AS bs_cash_and_equivalents,
        b.`Current Assets` AS bs_current_assets,
        b.`Current Liabilities` AS bs_current_liabilities,
        b.`Working Capital` AS bs_working_capital,
        b.`Goodwill And Other Intangible Assets` AS bs_goodwill_intangibles,
        b.`Net PPE` AS bs_net_ppe,
        b.`Inventory` AS bs_inventory,
        b.`Accounts Receivable` AS bs_accounts_receivable,
        b.`Accounts Payable` AS bs_accounts_payable,
        b.`Retained Earnings` AS bs_retained_earnings,
        -- Calculated ratios
        CASE WHEN b.`Current Liabilities` > 0 THEN b.`Current Assets` / b.`Current Liabilities` ELSE NULL END AS bs_current_ratio,
        CASE WHEN b.`Stockholders Equity` > 0 THEN b.`Total Debt` / b.`Stockholders Equity` ELSE NULL END AS bs_debt_to_equity
    FROM balance_sheet b
),

-- Step 2d: Cashflow with release dates
cashflow_with_release AS (
    SELECT 
        c.symbol,
        DATE(c.fiscal_period_end) AS fiscal_period_end,
        (SELECT MIN(DATE(e.release_date)) 
         FROM earnings_dates e 
         WHERE e.symbol = c.symbol 
         AND DATE(e.release_date) > DATE(c.fiscal_period_end)) AS release_date,
        -- Cashflow features (selected key metrics)
        c.`Operating Cash Flow` AS cf_operating_cash_flow,
        c.`Investing Cash Flow` AS cf_investing_cash_flow,
        c.`Financing Cash Flow` AS cf_financing_cash_flow,
        c.`Free Cash Flow` AS cf_free_cash_flow,
        c.`Capital Expenditure` AS cf_capital_expenditure,
        c.`Depreciation And Amortization` AS cf_depreciation_amortization,
        c.`Stock Based Compensation` AS cf_stock_based_compensation,
        c.`Change In Working Capital` AS cf_change_in_working_capital,
        c.`Cash Dividends Paid` AS cf_dividends_paid,
        c.`Repurchase Of Capital Stock` AS cf_stock_repurchase,
        c.`Net Issuance Payments Of Debt` AS cf_net_debt_issuance,
        c.`End Cash Position` AS cf_end_cash_position
    FROM cashflow c
),

-- Step 2e: Combine all financial statements with release dates
financials_combined AS (
    SELECT 
        i.symbol,
        i.fiscal_period_end,
        i.release_date,
        -- Earnings date features
        er.ed_eps_estimate,
        er.ed_reported_eps,
        er.ed_surprise_pct,
        -- Income statement
        i.is_total_revenue,
        i.is_gross_profit,
        i.is_operating_income,
        i.is_net_income,
        i.is_ebitda,
        i.is_ebit,
        i.is_diluted_eps,
        i.is_basic_eps,
        i.is_cost_of_revenue,
        i.is_operating_expense,
        i.is_interest_expense,
        i.is_tax_provision,
        i.is_research_and_development,
        i.is_selling_general_admin,
        i.is_gross_margin,
        i.is_operating_margin,
        i.is_net_margin,
        -- Balance sheet
        b.bs_total_assets,
        b.bs_total_liabilities,
        b.bs_stockholders_equity,
        b.bs_total_debt,
        b.bs_net_debt,
        b.bs_cash_and_equivalents,
        b.bs_current_assets,
        b.bs_current_liabilities,
        b.bs_working_capital,
        b.bs_goodwill_intangibles,
        b.bs_net_ppe,
        b.bs_inventory,
        b.bs_accounts_receivable,
        b.bs_accounts_payable,
        b.bs_retained_earnings,
        b.bs_current_ratio,
        b.bs_debt_to_equity,
        -- Cashflow
        c.cf_operating_cash_flow,
        c.cf_investing_cash_flow,
        c.cf_financing_cash_flow,
        c.cf_free_cash_flow,
        c.cf_capital_expenditure,
        c.cf_depreciation_amortization,
        c.cf_stock_based_compensation,
        c.cf_change_in_working_capital,
        c.cf_dividends_paid,
        c.cf_stock_repurchase,
        c.cf_net_debt_issuance,
        c.cf_end_cash_position
    FROM income_with_release i
    LEFT JOIN balance_with_release b 
        ON i.symbol = b.symbol AND i.fiscal_period_end = b.fiscal_period_end
    LEFT JOIN cashflow_with_release c 
        ON i.symbol = c.symbol AND i.fiscal_period_end = c.fiscal_period_end
    LEFT JOIN earnings_release_mapping er
        ON i.symbol = er.symbol AND i.release_date = er.release_date
    WHERE i.release_date IS NOT NULL
),

-- Step 2f: Get most recent financials for each symbol/date (released at most 1 day before)
-- Use window function instead of correlated subquery (Spark doesn't support correlated subqueries in JOINs)
-- First get unique symbol/feature_date pairs to reduce join explosion
unique_symbol_dates AS (
    SELECT DISTINCT symbol, feature_date
    FROM base_stock
),

financials_ranked AS (
    SELECT 
        usd.symbol,
        usd.feature_date,
        fc.fiscal_period_end,
        fc.release_date AS fin_release_date,
        DATEDIFF(usd.feature_date, fc.release_date) AS fin_days_since_release,
        fc.ed_eps_estimate,
        fc.ed_reported_eps,
        fc.ed_surprise_pct,
        fc.is_total_revenue,
        fc.is_gross_profit,
        fc.is_operating_income,
        fc.is_net_income,
        fc.is_ebitda,
        fc.is_ebit,
        fc.is_diluted_eps,
        fc.is_basic_eps,
        fc.is_cost_of_revenue,
        fc.is_operating_expense,
        fc.is_interest_expense,
        fc.is_tax_provision,
        fc.is_research_and_development,
        fc.is_selling_general_admin,
        fc.is_gross_margin,
        fc.is_operating_margin,
        fc.is_net_margin,
        fc.bs_total_assets,
        fc.bs_total_liabilities,
        fc.bs_stockholders_equity,
        fc.bs_total_debt,
        fc.bs_net_debt,
        fc.bs_cash_and_equivalents,
        fc.bs_current_assets,
        fc.bs_current_liabilities,
        fc.bs_working_capital,
        fc.bs_goodwill_intangibles,
        fc.bs_net_ppe,
        fc.bs_inventory,
        fc.bs_accounts_receivable,
        fc.bs_accounts_payable,
        fc.bs_retained_earnings,
        fc.bs_current_ratio,
        fc.bs_debt_to_equity,
        fc.cf_operating_cash_flow,
        fc.cf_investing_cash_flow,
        fc.cf_financing_cash_flow,
        fc.cf_free_cash_flow,
        fc.cf_capital_expenditure,
        fc.cf_depreciation_amortization,
        fc.cf_stock_based_compensation,
        fc.cf_change_in_working_capital,
        fc.cf_dividends_paid,
        fc.cf_stock_repurchase,
        fc.cf_net_debt_issuance,
        fc.cf_end_cash_position,
        ROW_NUMBER() OVER (
            PARTITION BY usd.symbol, usd.feature_date 
            ORDER BY fc.release_date DESC
        ) AS fin_rn
    FROM unique_symbol_dates usd
    INNER JOIN financials_combined fc 
        ON usd.symbol = fc.symbol
        AND fc.release_date <= usd.feature_date
),

-- Filter to most recent financials only
financials_latest AS (
    SELECT 
        symbol,
        feature_date,
        fiscal_period_end,
        fin_release_date,
        fin_days_since_release,
        ed_eps_estimate,
        ed_reported_eps,
        ed_surprise_pct,
        is_total_revenue,
        is_gross_profit,
        is_operating_income,
        is_net_income,
        is_ebitda,
        is_ebit,
        is_diluted_eps,
        is_basic_eps,
        is_cost_of_revenue,
        is_operating_expense,
        is_interest_expense,
        is_tax_provision,
        is_research_and_development,
        is_selling_general_admin,
        is_gross_margin,
        is_operating_margin,
        is_net_margin,
        bs_total_assets,
        bs_total_liabilities,
        bs_stockholders_equity,
        bs_total_debt,
        bs_net_debt,
        bs_cash_and_equivalents,
        bs_current_assets,
        bs_current_liabilities,
        bs_working_capital,
        bs_goodwill_intangibles,
        bs_net_ppe,
        bs_inventory,
        bs_accounts_receivable,
        bs_accounts_payable,
        bs_retained_earnings,
        bs_current_ratio,
        bs_debt_to_equity,
        cf_operating_cash_flow,
        cf_investing_cash_flow,
        cf_financing_cash_flow,
        cf_free_cash_flow,
        cf_capital_expenditure,
        cf_depreciation_amortization,
        cf_stock_based_compensation,
        cf_change_in_working_capital,
        cf_dividends_paid,
        cf_stock_repurchase,
        cf_net_debt_issuance,
        cf_end_cash_position
    FROM financials_ranked
    WHERE fin_rn = 1
),

-- Step 3: ALFRED economic data - pivot each metric to column
-- Get the latest value for each series as of feature_date (using realtime_start for point-in-time)
-- First get unique feature_dates to avoid cross-join explosion
unique_dates AS (
    SELECT DISTINCT feature_date
    FROM base_stock
),

-- Rank ALFRED data by recency for each series/date combination
alfred_ranked AS (
    SELECT 
        ud.feature_date,
        a.series_id,
        a.value,
        ROW_NUMBER() OVER (
            PARTITION BY ud.feature_date, a.series_id 
            ORDER BY a.realtime_start DESC
        ) AS econ_rn
    FROM unique_dates ud
    INNER JOIN alfred_economic_data a 
        ON DATE(a.realtime_start) <= ud.feature_date
),

-- Pivot economic data to columns (only most recent value per series)
alfred_pivoted AS (
    SELECT 
        feature_date,
        MAX(CASE WHEN series_id = 'FEDFUNDS' THEN value END) AS econ_fedfunds,
        MAX(CASE WHEN series_id = 'DFF' THEN value END) AS econ_dff,
        MAX(CASE WHEN series_id = 'DGS10' THEN value END) AS econ_dgs10,
        MAX(CASE WHEN series_id = 'DGS2' THEN value END) AS econ_dgs2,
        MAX(CASE WHEN series_id = 'T10Y2Y' THEN value END) AS econ_t10y2y,
        MAX(CASE WHEN series_id = 'T10Y3M' THEN value END) AS econ_t10y3m,
        MAX(CASE WHEN series_id = 'CPIAUCSL' THEN value END) AS econ_cpi,
        MAX(CASE WHEN series_id = 'CPILFESL' THEN value END) AS econ_core_cpi,
        MAX(CASE WHEN series_id = 'PCEPI' THEN value END) AS econ_pce,
        MAX(CASE WHEN series_id = 'PCEPILFE' THEN value END) AS econ_core_pce,
        MAX(CASE WHEN series_id = 'UNRATE' THEN value END) AS econ_unemployment,
        MAX(CASE WHEN series_id = 'PAYEMS' THEN value END) AS econ_nonfarm_payrolls,
        MAX(CASE WHEN series_id = 'ICSA' THEN value END) AS econ_initial_claims,
        MAX(CASE WHEN series_id = 'CCSA' THEN value END) AS econ_continued_claims,
        MAX(CASE WHEN series_id = 'GDP' THEN value END) AS econ_gdp,
        MAX(CASE WHEN series_id = 'GDPC1' THEN value END) AS econ_real_gdp,
        MAX(CASE WHEN series_id = 'INDPRO' THEN value END) AS econ_industrial_prod,
        MAX(CASE WHEN series_id = 'RSAFS' THEN value END) AS econ_retail_sales,
        MAX(CASE WHEN series_id = 'M2SL' THEN value END) AS econ_m2_money_supply,
        MAX(CASE WHEN series_id = 'BOGMBASE' THEN value END) AS econ_monetary_base,
        MAX(CASE WHEN series_id = 'HOUST' THEN value END) AS econ_housing_starts,
        MAX(CASE WHEN series_id = 'PERMIT' THEN value END) AS econ_building_permits,
        MAX(CASE WHEN series_id = 'CSUSHPINSA' THEN value END) AS econ_home_price_index,
        MAX(CASE WHEN series_id = 'UMCSENT' THEN value END) AS econ_consumer_sentiment,
        MAX(CASE WHEN series_id = 'MANEMP' THEN value END) AS econ_manufacturing_emp,
        MAX(CASE WHEN series_id = 'DGORDER' THEN value END) AS econ_durable_goods,
        MAX(CASE WHEN series_id = 'BOPGSTB' THEN value END) AS econ_trade_balance,
        MAX(CASE WHEN series_id = 'BAMLH0A0HYM2' THEN value END) AS econ_high_yield_spread,
        MAX(CASE WHEN series_id = 'VIXCLS' THEN value END) AS econ_vix
    FROM alfred_ranked
    WHERE econ_rn = 1
    GROUP BY feature_date
),

-- Step 4: FinGPT sentiment aggregation by symbol and date
fingpt_agg AS (
    SELECT 
        fg.symbol,
        DATE(fg.news_date) AS news_date,
        SUM(CASE WHEN fg.sentiment = 'positive' THEN 1 ELSE 0 END) AS fingpt_positive_count,
        SUM(CASE WHEN fg.sentiment = 'neutral' THEN 1 ELSE 0 END) AS fingpt_neutral_count,
        SUM(CASE WHEN fg.sentiment = 'negative' THEN 1 ELSE 0 END) AS fingpt_negative_count,
        COUNT(*) AS fingpt_total_articles,
        AVG(fg.sentiment_confidence) AS fingpt_avg_confidence
    FROM fingpt_sentiment_checkpoint fg
    GROUP BY fg.symbol, DATE(fg.news_date)
),

-- Step 5: FinBERT sentiment aggregation by symbol and date
finbert_agg AS (
    SELECT 
        fb.symbol,
        DATE(fb.news_date) AS news_date,
        SUM(CASE WHEN fb.sentiment = 'positive' THEN 1 ELSE 0 END) AS finbert_positive_count,
        SUM(CASE WHEN fb.sentiment = 'neutral' THEN 1 ELSE 0 END) AS finbert_neutral_count,
        SUM(CASE WHEN fb.sentiment = 'negative' THEN 1 ELSE 0 END) AS finbert_negative_count,
        COUNT(*) AS finbert_total_articles,
        AVG(fb.confidence) AS finbert_avg_confidence
    FROM finbert_news_classifications fb
    GROUP BY fb.symbol, DATE(fb.news_date)
),

-- Final assembly: Join all features to base stock data
final_features AS (
    SELECT 
        -- Identifiers
        bs.symbol,
        bs.prediction_date,
        
        -- Target variable
        bs.percent_daily_price_change,
        
        -- Stock data features (current day)
        bs.sd_open,
        bs.sd_high,
        bs.sd_low,
        bs.sd_close,
        bs.sd_volume,
        bs.sd_vwap,
        bs.sd_trade_count,
        
        -- Financials features (most recent release before feature_date)
        fl.fin_days_since_release,
        fl.ed_eps_estimate,
        fl.ed_reported_eps,
        fl.ed_surprise_pct,
        fl.is_total_revenue,
        fl.is_gross_profit,
        fl.is_operating_income,
        fl.is_net_income,
        fl.is_ebitda,
        fl.is_ebit,
        fl.is_diluted_eps,
        fl.is_basic_eps,
        fl.is_cost_of_revenue,
        fl.is_operating_expense,
        fl.is_interest_expense,
        fl.is_tax_provision,
        fl.is_research_and_development,
        fl.is_selling_general_admin,
        fl.is_gross_margin,
        fl.is_operating_margin,
        fl.is_net_margin,
        fl.bs_total_assets,
        fl.bs_total_liabilities,
        fl.bs_stockholders_equity,
        fl.bs_total_debt,
        fl.bs_net_debt,
        fl.bs_cash_and_equivalents,
        fl.bs_current_assets,
        fl.bs_current_liabilities,
        fl.bs_working_capital,
        fl.bs_goodwill_intangibles,
        fl.bs_net_ppe,
        fl.bs_inventory,
        fl.bs_accounts_receivable,
        fl.bs_accounts_payable,
        fl.bs_retained_earnings,
        fl.bs_current_ratio,
        fl.bs_debt_to_equity,
        fl.cf_operating_cash_flow,
        fl.cf_investing_cash_flow,
        fl.cf_financing_cash_flow,
        fl.cf_free_cash_flow,
        fl.cf_capital_expenditure,
        fl.cf_depreciation_amortization,
        fl.cf_stock_based_compensation,
        fl.cf_change_in_working_capital,
        fl.cf_dividends_paid,
        fl.cf_stock_repurchase,
        fl.cf_net_debt_issuance,
        fl.cf_end_cash_position,
        
        -- Economic indicators (as of feature_date)
        ap.econ_fedfunds,
        ap.econ_dff,
        ap.econ_dgs10,
        ap.econ_dgs2,
        ap.econ_t10y2y,
        ap.econ_t10y3m,
        ap.econ_cpi,
        ap.econ_core_cpi,
        ap.econ_pce,
        ap.econ_core_pce,
        ap.econ_unemployment,
        ap.econ_nonfarm_payrolls,
        ap.econ_initial_claims,
        ap.econ_continued_claims,
        ap.econ_gdp,
        ap.econ_real_gdp,
        ap.econ_industrial_prod,
        ap.econ_retail_sales,
        ap.econ_m2_money_supply,
        ap.econ_monetary_base,
        ap.econ_housing_starts,
        ap.econ_building_permits,
        ap.econ_home_price_index,
        ap.econ_consumer_sentiment,
        ap.econ_manufacturing_emp,
        ap.econ_durable_goods,
        ap.econ_trade_balance,
        ap.econ_high_yield_spread,
        ap.econ_vix,
        
        -- FinGPT sentiment (from day before)
        fg.fingpt_positive_count,
        fg.fingpt_neutral_count,
        fg.fingpt_negative_count,
        fg.fingpt_total_articles,
        fg.fingpt_avg_confidence,
        
        -- FinBERT sentiment (from day before)
        fb.finbert_positive_count,
        fb.finbert_neutral_count,
        fb.finbert_negative_count,
        fb.finbert_total_articles,
        fb.finbert_avg_confidence,
        
        -- Step 6: Train/test split indicator
        CASE 
            WHEN bs.prediction_date <= DATE('2025-10-24') THEN 'train'
            ELSE 'test'
        END AS split
        
    FROM base_stock bs
    
    -- Join financials (most recent release as of feature_date)
    LEFT JOIN financials_latest fl
        ON bs.symbol = fl.symbol 
        AND bs.feature_date = fl.feature_date
    
    -- Join ALFRED economic data (as of day before prediction)
    LEFT JOIN alfred_pivoted ap
        ON bs.feature_date = ap.feature_date
    
    -- Join FinGPT sentiment (from day before prediction)
    LEFT JOIN fingpt_agg fg
        ON bs.symbol = fg.symbol 
        AND bs.feature_date = fg.news_date
    
    -- Join FinBERT sentiment (from day before prediction)
    LEFT JOIN finbert_agg fb
        ON bs.symbol = fb.symbol 
        AND bs.feature_date = fb.news_date
)

-- Final output: Deduplicated by symbol and prediction_date
SELECT DISTINCT *
FROM final_features
ORDER BY symbol, prediction_date
