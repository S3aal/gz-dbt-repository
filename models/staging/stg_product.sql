WITH raw_product AS (
    SELECT
        products_id,
        CAST(purchSE_PRICE AS FLOAT64) AS purchase_price  -- ✅ Fix column name
    FROM {{ source('raw', 'product') }}  
)

SELECT * FROM raw_product
