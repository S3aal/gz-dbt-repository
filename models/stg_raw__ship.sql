with
    source as (select * from `rapid-domain-449108-b9`.`gz_raw_data`.`raw_gz_ship`),

    renamed as (
        select
            orders_id as ship_id,  -- Renaming orders_id to ship_id
            shipping_fee,  -- Retain the shipping_fee column
            ship_cost
        from source
    ),

    casted as (
        select
            ship_id,
            shipping_fee,  -- Retain only the relevant shipping_fee column
            cast(ship_cost as float64) as ship_cost  -- Cast ship_cost to FLOAT64
        from renamed
    )

select *
from casted
