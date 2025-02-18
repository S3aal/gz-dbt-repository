WITH daily_data AS (
    SELECT 
        sa.date_date,  -- Correctly reference date_date from the stg_sales model (sa)
        COUNT(DISTINCT o.orders_id) AS nb_transactions,  -- Count the unique order IDs
        SUM(CAST(o.revenue AS NUMERIC)) AS revenue,                       -- Total revenue for the day (cast to NUMERIC)
        SUM(CAST(o.revenue AS NUMERIC)) / COUNT(DISTINCT o.orders_id) AS average_basket, -- Average basket (Revenue / Transactions)
        SUM(CAST(o.revenue AS NUMERIC) - CAST(o.purchase_cost AS NUMERIC)) AS margin,       -- Margin (Revenue - Purchase Cost)
        SUM(CAST(o.revenue AS NUMERIC) - CAST(o.purchase_cost AS NUMERIC)) + 
        SUM(CAST(s.shipping_fee AS NUMERIC)) - 
        SUM(CAST(s.logCost AS NUMERIC)) - 
        SUM(CAST(s.ship_cost AS NUMERIC)) AS operational_margin, -- Operational margin
        SUM(CAST(o.purchase_cost AS NUMERIC)) AS total_purchase_cost,     -- Total purchase cost
        SUM(CAST(s.shipping_fee AS NUMERIC)) AS total_shipping_fees,      -- Total shipping fees
        SUM(CAST(s.logCost AS NUMERIC)) AS total_log_costs,               -- Total logistics costs
        SUM(CAST(o.quantity AS NUMERIC)) AS total_quantity                -- Total quantity sold
    FROM 
        {{ ref('int_orders_margin') }} o                -- Reference the int_orders_margin model
    LEFT JOIN 
        {{ ref('stg_ship') }} s                         -- Join with the staging ship model for shipping data
    ON 
        o.orders_id = s.orders_id
    LEFT JOIN 
        {{ ref('stg_sales') }} sa                        -- Join with the stg_sales model to get the transaction dates
    ON 
        o.orders_id = sa.orders_id
    GROUP BY 
        sa.date_date  -- Group by date from the stg_sales model (not from stg_ship)
)

SELECT * 
FROM daily_data
