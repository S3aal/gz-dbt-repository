{% macro test_custom_unique_test(model) %}
WITH duplicate_check AS (
    SELECT
        orders_id,
        date_date,
        COUNT(*) AS count
    FROM {{ ref('stg_sales') }}  -- Explicitly passing the model name as a string
    GROUP BY orders_id, date_date
    HAVING COUNT(*) > 1
)
SELECT
    COUNT(*) = 0 AS is_unique
FROM duplicate_check
{% endmacro %}
