with distinct_carrier as 
    (
        select distinct carrier as carrier
        from {{ source('routeflow_logistics', 'silver_routeflow') }}
    ),

    validated as 
        (
            select {{ dbt_utils.generate_surrogate_key(['carrier']) }} as carrier_key,
                carrier
            from distinct_carrier
        )

        select * from validated

