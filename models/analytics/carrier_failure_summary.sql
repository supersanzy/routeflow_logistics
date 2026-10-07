
with table1 as 
    (
        select carrier, count(*) as total_shipments_by_carrier, count(case when status = 'Failed' then 1 end) as failed_shipments,
                sum(case when status = 'Failed' then shipping_cost end) as failed_shipping_cost_by_carrier
        from {{ ref('fact_shipments') }} as f
        join {{ ref('dim_status') }} as s
        on s.status_key = f.status_key
        join {{ ref('dim_carrier') }} as c
        on c.carrier_key = f.carrier_key
        group by 1
    ),
    aggregated as 
        (
            select carrier, total_shipments_by_carrier, failed_shipments, failed_shipping_cost_by_carrier, 
                    round((failed_shipments / total_shipments_by_carrier) * 100, 2) as failure_rate_by_carrier,                    
            from table1                    
        )


        select * from aggregated