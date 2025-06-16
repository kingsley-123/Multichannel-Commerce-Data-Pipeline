-- ClickHouse Table Creation Script
-- Run this once to create all tables with proper engines

CREATE DATABASE IF NOT EXISTS fashion_gold;

USE fashion_gold;

-- Dimension Tables (Reference data)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key String,
    date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    day_of_week String,
    is_weekend UInt8
) ENGINE = MergeTree()
ORDER BY date_key;

CREATE TABLE IF NOT EXISTS dim_channels (
    channel_id String,
    channel_name String,
    channel_type String,
    description String
) ENGINE = MergeTree()
ORDER BY channel_id;

-- Fact Tables (Transaction data) - Partitioned by date for performance
CREATE TABLE IF NOT EXISTS wholesale_cm1 (
    date_key String,
    date Date,
    order_no String,
    style_no String,
    style_name String,
    unified_style_no String,
    unified_style_name String,
    payment_source String,
    season String,
    buyer_name String,
    payment_terms String,
    country String,
    currency String,
    qty Int32,
    gross_revenue Decimal(10,2),
    total_discount Decimal(10,2),
    net_revenue Decimal(10,2),
    item_gross_price Decimal(10,2),
    item_discount Decimal(10,2),
    item_net_price Decimal(10,2),
    avg_item_unit_cost Decimal(10,2),
    unit_cost Decimal(10,2),
    prod_com_percent Decimal(5,4),
    prod_com Decimal(10,2),
    margin Decimal(10,2),
    channel_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, order_no, style_no);

CREATE TABLE IF NOT EXISTS wholesale_cm2 (
    date_key String,
    order_no String,
    buyer_name String,
    currency String,
    payment_source String,
    qty Int32,
    net_revenue Decimal(10,2),
    production_cost Decimal(10,2),
    production_comm Decimal(10,2),
    freight_out_status String,
    freight_currency String,
    freight_in Decimal(10,2),
    freight_out Decimal(10,2),
    trx_currency String,
    trx_fees Decimal(10,2),
    comm_currency String,
    sales_comm Decimal(5,4),
    insurance_currency String,
    insurance Decimal(10,2),
    cm2_amount Decimal(10,2),
    channel_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(parseDateTimeBestEffort(date_key))
ORDER BY (date_key, order_no);

CREATE TABLE IF NOT EXISTS shopify_cm1 (
    date_key String,
    date Date,
    order_no String,
    style_no String,
    style_name String,
    unified_style_no String,
    unified_style_name String,
    buyer_name String,
    country String,
    currency String,
    qty Int32,
    gross_revenue Decimal(10,2),
    total_discount Decimal(10,2),
    net_revenue Decimal(10,2),
    item_gross_price Decimal(10,2),
    item_discount Decimal(10,2),
    item_net_price Decimal(10,2),
    total_returns Decimal(10,2),
    unit_cost_currency String,
    item_unit_cost Decimal(10,2),
    prod_com_percent Decimal(5,4),
    prod_com Decimal(10,2),
    margin Decimal(10,2),
    channel_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, order_no, style_no);

CREATE TABLE IF NOT EXISTS shopify_cm2 (
    date_key String,
    order_no String,
    buyer_name String,
    country String,
    currency String,
    qty Int32,
    net_revenue Decimal(10,2),
    total_returns Decimal(10,2),
    cost_currency String,
    total_unit_cost Decimal(10,2),
    cm1_amount Decimal(10,2),
    freight_out_status String,
    freight_in Decimal(10,2),
    freight_out Decimal(10,2),
    return_status String,
    freight_return Decimal(10,2),
    freight_income Decimal(10,2),
    shopify_fees Decimal(10,2),
    cm2_amount Decimal(10,2),
    channel_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(parseDateTimeBestEffort(date_key))
ORDER BY (date_key, order_no);

CREATE TABLE IF NOT EXISTS livestreaming_cm1 (
    date_key String,
    date Date,
    order_no String,
    style_no String,
    style_name String,
    unified_style_no String,
    unified_style_name String,
    buyer_name String,
    country String,
    currency String,
    qty Int32,
    gross_revenue Decimal(10,2),
    total_discount Decimal(10,2),
    net_revenue Decimal(10,2),
    item_gross_price Decimal(10,2),
    item_discount Decimal(10,2),
    item_net_price Decimal(10,2),
    total_returns Decimal(10,2),
    unit_cost_currency String,
    item_unit_cost Decimal(10,2),
    prod_com_percent Decimal(5,4),
    prod_com Decimal(10,2),
    margin Decimal(10,2),
    channel_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, order_no, style_no);

CREATE TABLE IF NOT EXISTS livestreaming_cm2 (
    date_key String,
    order_no String,
    buyer_name String,
    currency String,
    qty Int32,
    net_revenue Decimal(10,2),
    production_cost Decimal(10,2),
    production_comm Decimal(10,2),
    freight_currency String,
    freight_in Decimal(10,2),
    freight_out Decimal(10,2),
    trx_currency String,
    trx_fees Decimal(10,2),
    comm_currency String,
    sales_comm Decimal(5,4),
    cm2_amount Decimal(10,2),
    channel_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(parseDateTimeBestEffort(date_key))
ORDER BY (date_key, order_no);

CREATE TABLE IF NOT EXISTS fact_freight (
    date_key String,
    tracking_number String,
    provider String,
    cost Decimal(10,2),
    order_no String,
    created_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(parseDateTimeBestEffort(date_key))
ORDER BY (date_key, tracking_number);