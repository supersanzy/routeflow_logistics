with location_data as 
    (
        select origin as location from {{ source('routeflow_logistics', 'silver_routeflow') }}
        union
        select destination as location from {{ source ('routeflow_logistics', 'silver_routeflow')}}
    ),

    validated as 
        (
            select split_part(location, ',', 1) as city,
                    trim(split_part(location, ',', 2)) as country
            from location_data
        ),

    cleaned as
        (
            select {{ dbt_utils.generate_surrogate_key(['city', 'country']) }} as location_key,
                    city, case when country IS NULL OR country = '' THEN city 
                    ELSE country END AS country
            from validated
        )

        select * from cleaned