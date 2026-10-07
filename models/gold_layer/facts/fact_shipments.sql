{# select f.shipment_id, f.shipment_date,
        f.delivery_date, s.status_key, sr.status_reason_key, c.carrier_key, f.shipping_cost,
        f.created_at, f.updated_at
from {{ source('routeflow_logistics', 'silver_routeflow') }} as f
left join {{ ref('dim_carrier') }} as c
on c.carrier = f.carrier
left join {{ ref('dim_status') }} as s
on s.status = f.status
left join {{ ref('dim_status_reason') }} as sr
on f.status_reason = sr.status_reason #}

with validated as 
        (
            select shipment_id, cast(shipment_date as date) as shipment_date,
            cast(delivery_date as date) as delivery_date,
            carrier, status, status_reason, origin, destination, 
            shipping_cost, created_at, updated_at
            from {{ source('routeflow_logistics', 'silver_routeflow') }}
        ),

     cleaned as 
        (
            select f.shipment_id, f.shipment_date, f.delivery_date,
                    sd.date_key as shipment_date_key,
                    dld.date_key as delivery_date_key, s.status_key, 
                    sr.status_reason_key, l.location_key as origin_key, 
                    d.location_key as destination_key, c.carrier_key, f.shipping_cost,
                    f.created_at, f.updated_at
            from validated as f
            left join {{ ref('dim_carrier') }} as c
            on c.carrier = f.carrier
            left join {{ ref('dim_status') }} as s
            on s.status = f.status
            left join {{ ref('dim_location') }} as l
            on f.origin = concat(l.city, ', ', l.country)
            or f.origin = l.city
            left join {{ ref('dim_location') }} d
            on f.destination = concat(d.city, ', ', d.country)
            or f.destination = d.city
            left join {{ ref('dim_status_reason') }} as sr
            on f.status_reason = sr.status_reason
            left join {{ ref('dim_date') }} as sd
            on sd.full_date = f.shipment_date
            left join {{ ref('dim_date') }} as dld
            on dld.full_date = f.delivery_date
        )

        select * from cleaned