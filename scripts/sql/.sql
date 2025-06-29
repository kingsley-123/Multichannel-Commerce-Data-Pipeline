-- This script creates a view for daily KPIs in the fashion_gold schema.
-- It aggregates data from multiple CM2 tables to provide a unified view of key performance indicators.
CREATE OR REPLACE VIEW fashion_gold.exec_daily_kpi AS
SELECT 
    date_key,
    channel_id,
    country,
    region,
    toDate(parseDateTimeBestEffort(date_key)) as date,   
    -- Core KPIs only
    round(SUM(net_revenue), 2) as total_revenue,
    round(SUM(cm2_amount), 2) as net_margin,
    COUNT(DISTINCT order_no) as total_orders,
    round(SUM(net_revenue) / COUNT(DISTINCT order_no), 2) as avg_order_value,
    round((SUM(cm2_amount) / SUM(net_revenue)) * 100, 2) as margin_percentage
FROM (
    -- Union all CM2 tables for unified view with geography
    SELECT date_key, order_no, net_revenue, cm2_amount, channel_id, country, region FROM wholesale_cm2
    UNION ALL
    SELECT date_key, order_no, net_revenue, cm2_amount, channel_id, country, region FROM shopify_cm2  
    UNION ALL
    SELECT date_key, order_no, net_revenue, cm2_amount, channel_id, country, region FROM livestreaming_cm2
)
GROUP BY
    date_key, channel_id, country, region
ORDER BY date_key DESC;


