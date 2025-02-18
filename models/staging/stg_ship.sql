WITH raw_ship AS (
    SELECT
        orders_id,
        shipping_fee,
        ship_cost,
        logCost  
    FROM {{ source('raw', 'ship') }} 
)

SELECT * 
FROM raw_ship
