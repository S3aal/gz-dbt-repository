-- models/intermediate/int_orders_operational.sql

WITH orders_margin AS (
    SELECT * 
    FROM {{ ref('int_orders_margin') }}  
),

shipping_data AS (
    SELECT 
        orders_id,
        shipping_fee,
        logCost,  
        CAST(ship_cost AS FLOAT64) AS ship_cost
    FROM {{ ref('stg_ship') }}  
)

SELECT 
    o.orders_id,
    o.date_date,
    (o.margin + s.shipping_fee - s.logCost - s.ship_cost) AS operational_margin
FROM orders_margin AS o
LEFT JOIN shipping_data AS s
    ON o.orders_id = s.orders_id
