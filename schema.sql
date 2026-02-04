
-- 1. STAGING TABLES (Inputs)
-- (Already defined as External Tables: agency_ledger, po_details, external_payments)

-- 2. TRANSFORMATION VIEWS

-- Rate Source: Derived from 'DEPOSIT' entries in Agency Ledger to ensure 100% money link.
CREATE OR REPLACE VIEW `analytics.stg_exchange_rates` AS
SELECT
    date as rate_date,
    -- Calculate implied rate: JPY Sent / CNY Received. 
    -- Or use explicit 'real_rate' col if user insists on typing it.
    -- Better: JPY Sent / CNY Received = Actual Cost Rate.
    SAFE_CAST(sent_amount_jpy AS FLOAT64) / NULLIF(SAFE_CAST(received_amount_cny AS FLOAT64), 0) as rate
FROM `google_sheets.deposit_inflow` 
WHERE received_amount_cny IS NOT NULL;

-- Landed Cost Calculation
CREATE OR REPLACE VIEW `analytics.stg_po_costs` AS
WITH 
ledger_costs AS (
    SELECT 
        l.po_number,
        SUM(
            SAFE_CAST(l.expense_cny AS FLOAT64) * 
            (SELECT rate FROM `analytics.stg_exchange_rates` r WHERE r.rate_date <= l.date ORDER BY r.rate_date DESC LIMIT 1)
        ) as total_ledger_jpy
    FROM `google_sheets.agency_ledger` l
    WHERE l.po_number IS NOT NULL
    GROUP BY l.po_number
),
external_costs AS (
    SELECT
        l.po_number,
        SUM(SAFE_CAST(ep.amount_jpy AS FLOAT64)) as total_external_jpy
    FROM `google_sheets.external_payments` ep
    JOIN `google_sheets.agency_ledger` l ON ep.related_awb = l.awb_number
    GROUP BY l.po_number
),
po_qtys AS (
    SELECT po_number, SUM(quantity) as total_qty FROM `google_sheets.po_details` GROUP BY po_number
)
SELECT
    d.po_number,
    d.component_id as sku,
    d.quantity,
    -- Allocating Total PO Cost by Quantity (Simple & Robust)
    (COALESCE(lc.total_ledger_jpy, 0) + COALESCE(ec.total_external_jpy, 0)) / pq.total_qty as unit_cost_jpy
FROM `google_sheets.po_details` d
JOIN po_qtys pq ON d.po_number = pq.po_number
LEFT JOIN ledger_costs lc ON d.po_number = lc.po_number
LEFT JOIN external_costs ec ON d.po_number = ec.po_number;

-- 3. REPORTING VIEWS

-- 5-Stage P&L
-- This requires joining Sales Data (SP-API) with Unit Costs (stg_po_costs).
-- Simplification: We assume First-In-First-Out or Monthly Avg Cost for COGS.
-- For this DDL, we show the structure.
/*
CREATE OR REPLACE VIEW `analytics.rpt_pnl_5stage` AS
SELECT
    DATE(posted_date) as date,
    sku,
    SUM(amount) as sales,
    -- COGS would be dynamic based on inventory valuation
    SUM(quantity_sold * avg_unit_cost) as cogs,
    -- ... other levels
FROM ...
*/
