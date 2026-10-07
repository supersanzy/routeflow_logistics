with distinct_status as
    (
        select distinct status as status
        from {{ source('routeflow_logistics', 'silver_routeflow') }}
    ),

    validated as 
        (
            select {{ dbt_utils.generate_surrogate_key(['status']) }} as status_key,
                status
            from distinct_status
        )

    select * from validated

