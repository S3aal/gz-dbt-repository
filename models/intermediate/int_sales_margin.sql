WITH sales_data AS (
    SELECT
        s.orders_id,
        s.pdt_id,
        s.date_date,
        s.revenue,
        s.quantity,
        p.purchase_price
    FROM {{ ref('stg_sales') }} AS s
    LEFT JOIN {{ ref('stg_product') }} AS p
        ON s.pdt_id = p.products_id  -- ✅ Ensure column names match
),
margin_data AS (
    SELECT
        orders_id,
        pdt_id,
        date_date,
        revenue,
        quantity,
        purchase_price,
        (quantity * purchase_price) AS purchase_cost,  -- ✅ Purchase cost calculation
        (revenue - (quantity * purchase_price)) AS margin  -- ✅ Margin calculation
    FROM sales_data
)

SELECT * FROM margin_data
