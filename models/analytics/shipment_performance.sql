
with aggregated as 
    (
    select month, month_name, carrier, count(*) as total_shipments,
        count(case when status = 'Failed' then 1 end) as failed_shipments,
        count(case when status = 'Delivered' then 1 end) as delivered_shipments,
        sum(case when status = 'Failed' then shipping_cost end) as failed_shipping_cost,
        sum(case when status = 'Delivered' then shipping_cost end) as successful_shipping_cost
    from {{ ref('dim_date') }} as dd
    join {{ ref('fact_shipments') }} as f
    on f.shipment_date_key = dd.date_key
    join {{ ref('dim_status') }} as s
    on s.status_key = f.status_key
    join {{ ref('dim_carrier') }} as c
    on c.carrier_key = f.carrier_key
    group by 1, 2, 3
    )

select * from aggregated