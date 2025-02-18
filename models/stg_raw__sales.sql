with
    source as (select * from {{ source("raw", "sales") }}),

    renamed as (
        select
            date_date,
            orders_id,
            pdt_id as product_id,  -- Renaming the column
            revenue,
            quantity
        from source
    ),

    casted as (
        select
            date_date,
            orders_id,
            product_id,
            cast(revenue as float64) as revenue,  -- Casting revenue to FLOAT64
            cast(quantity as int64) as quantity  -- Casting quantity to INT64
        from renamed
    )

select *
from casted
