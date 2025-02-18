WITH raw_sales AS (
    SELECT
        orders_id,
        pdt_id,
        date_date,
        revenue,
        quantity
    FROM {{ source('raw', 'sales') }}
)

SELECT * FROM raw_sales
