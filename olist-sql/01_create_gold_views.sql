-- ==================================================================================
-- PROJECT: Olist E-commerce Analysis
-- LAYER: Gold Layer (Analytical Views)
-- TECHNOLOGY: Azure Synapse Serverless SQL
-- DESCRIPTION: Creating a logical data warehouse by mapping Parquet files from 
--              the Silver layer to SQL Views for easier business intelligence.
-- ==================================================================================

-- 1. Infrastructure Check: Verify Parquet file accessibility from the Data Lake
SELECT TOP 10 *
FROM OPENROWSET(
    BULK 'https://liufei2026storage.dfs.core.windows.net/data/02-silver/olist_ecommerce/orders/*.parquet',
    FORMAT = 'PARQUET'
) AS [result];

-- 2. Schema Management: Define the Analytical Environment
CREATE DATABASE olist_gold;

-- 3. Logical Modeling: Creating Views for Downstream Analytics (Power BI/Tableau)
-- Mapping orders data to a reusable SQL view
CREATE OR ALTER VIEW gold_orders AS
SELECT *
FROM OPENROWSET(
    BULK 'https://liufei2026storage.dfs.core.windows.net/data/02-silver/olist_ecommerce/orders/*.parquet',
    FORMAT = 'PARQUET'
) AS [orders];

-- Mapping customer information
CREATE OR ALTER VIEW gold_customers AS
SELECT *
FROM OPENROWSET(
    BULK 'https://liufei2026storage.dfs.core.windows.net/data/02-silver/olist_ecommerce/customers/*.parquet',
    FORMAT = 'PARQUET'
) AS [customers];

-- [Repeat pattern for remaining tables: geolocation, items, category, payments, products, reviews, sellers]
-- Note: Views allow analysts to query data using standard SQL without managing Parquet file paths.

