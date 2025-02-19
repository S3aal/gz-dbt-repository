WITH sales_data AS (
    SELECT 
        s.orders_id,
        s.date_date,
        s.pdt_id AS products_id,  -- Alias pdt_id to products_id
        s.revenue,
        s.quantity,
        COUNT(*) OVER (PARTITION BY s.orders_id, s.date_date) AS duplicate_count  -- Track duplicates by orders_id and date
    FROM `rapid-domain-449108-b9.gz_raw_data.raw_gz_sales` s  -- Fully qualify raw_gz_sales
    LEFT JOIN `rapid-domain-449108-b9.gz_raw_data.raw_gz_product` p  -- Fully qualify raw_gz_product
        ON s.pdt_id = p.products_id  -- Join to get the products_id from raw_gz_product
)
SELECT 
    orders_id,
    date_date,
    products_id,
    revenue,
    quantity,
    CASE WHEN duplicate_count = 1 THEN TRUE ELSE FALSE END AS is_unique  -- Mark rows as unique or duplicate
FROM sales_data
WHERE duplicate_count = 1;  -- Ensure only unique rows are kept based on your logic
