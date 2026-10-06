import duckdb

bronze_layer = duckdb.sql("""select * from 'routeflow_shipments_2026.csv' """)

quarantine = duckdb.sql(""" with quarantine_base as 
                              ( select trim(shipment_id) as shipment_id, shipment_date, delivery_date, upper(trim(status)) as status,
                                    trim(failure_reason) as failure_reason, TRIM(origin) as origin, trim(destination) as destination, 
                                    trim(carrier) as carrier, shipping_cost
                                    
                                
                                from bronze_layer
                              ),

                                validate as 
                                    (select case when try_cast(shipment_id as integer) is not null
                                          then UPPER('S' || shipment_id) else UPPER(shipment_id)
                                        end as shipment_id, shipment_date, delivery_date, UPPER(status) as status,
                                        failure_reason, origin, destination, carrier, shipping_cost 
                                     FROM quarantine_base    
                                    )
                                    
                                    select *, 
                                        ROW_NUMBER() OVER(partition by shipment_id order by shipment_date) as rn
                                         from validate               
                               """)

invalid_data = duckdb.sql(""" with quarantine_base_1 as 
                                ( select shipment_id, (LEFT(shipment_id, 1) = 'S'
                                           AND LENGTH(shipment_id) IN (8, 9)
                                           AND TRY_CAST(SUBSTRING(shipment_id, 2) AS INTEGER) IS NOT NULL
                                          ) AS is_valid_format,
                                        shipment_date, delivery_date, status, failure_reason, origin, destination,
                                        carrier, shipping_cost, rn
                                        from quarantine
                            ),

                                validate as 
                                    (SELECT *, CASE
                                          WHEN NOT is_valid_format THEN 'INVALID_FORMAT'
                                           ELSE 'VALID'
                                        END AS shipment_id_quality_flag
                                     from quarantine_base_1
                                    ),

                                invalid_data as 
                                (SELECT shipment_id, shipment_date, delivery_date, status,
                                        failure_reason, origin, destination, carrier, shipping_cost
                                        
                                 FROM validate 
                                 WHERE (status IN ('DELIVERED','FAILED','RETURNED')
                                AND (delivery_date IS NULL OR delivery_date IN ('UNKNOWN','NULL','')))
                                -- OR (UPPER(status) IN ('PENDING', 'IN_TRANSIT', 'CANCELLED'))
                                OR shipment_id_quality_flag = 'INVALID_FORMAT'
                                 OR rn > 1
                                )

                                select * from invalid_data
                               -- WHERE shipment_id = 'S13374680'
                                -- WHERE LEN(shipment_id) IN (8,9) AND UPPER(status) = 'PENDING'

                                
                        """)

# print(invalid_data)

invalid_data.to_parquet("routeflow_shipments_quarantine_data.parquet")

staging_layer = duckdb.sql("""with raw_data as 
                ( select trim(shipment_id) as shipment_id, shipment_date, delivery_date, 
                    case when UPPER(status) = 'FAILED' THEN 'Failed'
                        when UPPER(status) = 'DELIVERED' THEN 'Delivered'
                        when UPPER(status) = 'PENDING' THEN 'Pending'
                        when UPPER(status) = 'CANCELLED' THEN 'Cancelled'
                        when UPPER(status) = 'RETURNED' THEN 'Returned'
                        when UPPER(status) = 'IN_TRANSIT' THEN 'In Transit'
                        else status
                    end as status, failure_reason, origin, destination, 
                    carrier, shipping_cost
                    from bronze_layer),

                     validated as
                       (select case when
                            try_cast(shipment_id as integer) is not null
                                then UPPER('S' || shipment_id) else UPPER(shipment_id)
                                end as shipment_id, 
                            shipment_date,  replace(delivery_date, '/','-') as delivery_date, trim(status) as status,
                            trim(failure_reason) as status_reason, trim(origin) as origin, trim(destination) as destination, trim(carrier) as carrier,
                            shipping_cost from raw_data WHERE (status IN ('Pending', 'In Transit', 'Cancelled'))
                            OR (status IN ('Delivered', 'Failed', 'Returned') 
                            AND delivery_date IS NOT NULL 
                            AND delivery_date NOT IN ('UNKNOWN', 'NULL', ''))),

                        cleaned as 
                            (select *, 
                                ROW_NUMBER() OVER (PARTITION BY shipment_id order by shipment_date) as rn
                        from validated)

                        select shipment_id, shipment_date, delivery_date, origin, destination, status, status_reason, carrier, shipping_cost
                        from cleaned where rn = 1
                         
                 """)



# # print(staging_layer)

silver_layer = duckdb.sql("""  WITH validate as 
                        (SELECT
                            shipment_id, (
                                        LEFT(shipment_id, 1) = 'S'
                                        AND LENGTH(shipment_id) IN (8, 9)
                                        AND TRY_CAST(SUBSTRING(shipment_id, 2) AS INTEGER) IS NOT NULL
                                    ) AS is_valid_format, 
                            COUNT(*) OVER (PARTITION BY shipment_id) > 1 AS is_duplicate,
                            shipment_date,
                            CASE
                                WHEN delivery_date ~ '^\d{4}-\d{2}-\d{2}$'
                                    THEN delivery_date::DATE

                                WHEN delivery_date ~ '^\d{2}-\d{2}-\d{4}$'
                                    AND split_part(delivery_date, '-', 1)::INT > 12
                                    THEN strptime(delivery_date, '%d-%m-%Y')::DATE

                                WHEN delivery_date ~ '^\d{2}-\d{2}-\d{4}$'
                                    AND split_part(delivery_date, '-', 2)::INT > 12
                                    THEN strptime(delivery_date, '%m-%d-%Y')::DATE

                                -- both parts <= 12: genuinely ambiguous, default to MM-DD-YYYY
                                WHEN delivery_date ~ '^\d{2}-\d{2}-\d{4}$'
                                    THEN strptime(delivery_date, '%m-%d-%Y')::DATE

                                ELSE NULL
                            END AS delivery_date,
                            status, status_reason, origin, destination, carrier, shipping_cost
                        FROM staging_layer),
                        
                        cleaned as 
                        (select *, CASE
                                        WHEN NOT is_valid_format THEN 'INVALID_FORMAT'
                                      --  WHEN is_duplicate        THEN 'DUPLICATE_ID'
                                        ELSE 'VALID'
                                    END AS shipment_id_quality_flag
                        from validate)

                        select shipment_id, shipment_date, delivery_date,
                            status, status_reason, origin, destination, carrier, CAST(shipping_cost AS DECIMAL(7,2)) as shipping_cost,
                            case when shipment_date is null then null
                                else shipment_date end as created_at,
                            case when delivery_date is null then null
                                else delivery_date end as updated_at from cleaned
                         where shipment_id_quality_flag = 'VALID'
                        
                        """)

# print(silver_layer)


