WITH daily_data AS (
    SELECT 
        sa.date_date,  
        COUNT(DISTINCT o.orders_id) AS nb_transactions,  
        ROUND(SUM(CAST(o.revenue AS NUMERIC)), 0) AS revenue,  
        ROUND(SUM(CAST(o.revenue AS NUMERIC)) / NULLIF(COUNT(DISTINCT o.orders_id), 0), 1) AS average_basket,  
        ROUND(SUM(CAST(o.revenue AS NUMERIC) - CAST(o.purchase_cost AS NUMERIC)), 0) AS margin,  
        ROUND(SUM(CAST(o.revenue AS NUMERIC) - CAST(o.purchase_cost AS NUMERIC)) + 
              SUM(CAST(s.shipping_fee AS NUMERIC)) - 
              SUM(CAST(s.logCost AS NUMERIC)) - 
              SUM(CAST(s.ship_cost AS NUMERIC)), 0) AS operational_margin,  
        ROUND(SUM(CAST(o.purchase_cost AS NUMERIC)), 0) AS purchase_cost,  
        ROUND(SUM(CAST(s.shipping_fee AS NUMERIC)), 0) AS shipping_fee,  
        ROUND(SUM(CAST(s.logCost AS NUMERIC)), 0) AS logcost,  
        ROUND(SUM(CAST(s.ship_cost AS NUMERIC)), 0) AS ship_cost,  -- Ensure ship_cost is included
        ROUND(SUM(CAST(o.quantity AS NUMERIC)), 0) AS quantity  
    FROM 
        {{ ref('int_orders_margin') }} o  
    LEFT JOIN 
        {{ ref('stg_ship') }} s  
    ON 
        o.orders_id = s.orders_id
    LEFT JOIN 
        {{ ref('stg_sales') }} sa  
    ON 
        o.orders_id = sa.orders_id
    GROUP BY 
        sa.date_date  
)

SELECT * FROM daily_data  -- Ensure `ship_cost` is in the final SELECT
ORDER BY date_date DESC
