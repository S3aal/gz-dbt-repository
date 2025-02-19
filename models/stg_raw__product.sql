with
    source as (select * from `rapid-domain-449108-b9`.`gz_raw_data`.`raw_gz_product`),

    renamed as (
        select
            products_id as product_id,  -- Renaming products_id to product_id
            purchse_price as price  -- Renaming purchSE_PRICE to price
        from source
    ),

    casted as (
        select product_id, cast(price as float64) as price  -- Casting price to FLOAT64
        from renamed
    )

select *
from casted
