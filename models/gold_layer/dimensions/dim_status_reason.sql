with distinct_status_reason as 
    (
        select distinct status_reason as status_reason
        from {{ source('routeflow_logistics', 'silver_routeflow') }} 
    )

    select {{ dbt_utils.generate_surrogate_key(['status_reason']) }} as status_reason_key,
        status_reason
    from distinct_status_reason