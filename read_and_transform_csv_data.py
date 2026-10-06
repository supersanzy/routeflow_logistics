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

# staging_layer = duckdb.sql("""with raw_data as 
#                 ( select trim(shipment_id) as shipment_id, shipment_date, delivery_date, 
#                     case when UPPER(status) = 'FAILED' THEN 'Failed'
#                         when UPPER(status) = 'DELIVERED' THEN 'Delivered'
#                         when UPPER(status) = 'PENDING' THEN 'Pending'
#                         when UPPER(status) = 'CANCELLED' THEN 'Cancelled'
#                         when UPPER(status) = 'RETURNED' THEN 'Returned'
#                         when UPPER(status) = 'IN_TRANSIT' THEN 'In Transit'
#                         else status
#                     end as status, failure_reason, origin, destination, 
#                     carrier, shipping_cost
#                     from bronze_layer),

#                      validated as
#                        (select case when
#                             try_cast(shipment_id as integer) is not null
#                                 then UPPER('S' || shipment_id) else UPPER(shipment_id)
#                                 end as shipment_id, 
#                             shipment_date,  replace(delivery_date, '/','-') as delivery_date, trim(status) as status,
#                             trim(failure_reason) as status_reason, trim(origin) as origin, trim(destination) as destination, trim(carrier) as carrier,
#                             shipping_cost from raw_data WHERE (status IN ('Pending', 'In Transit', 'Cancelled'))
#                             OR (status IN ('Delivered', 'Failed', 'Returned') 
#                             AND delivery_date IS NOT NULL 
#                             AND delivery_date NOT IN ('UNKNOWN', 'NULL', ''))),

#                         cleaned as 
#                             (select *, 
#                                 ROW_NUMBER() OVER (PARTITION BY shipment_id order by shipment_date) as rn
#                         from validated)

#                         select shipment_id, shipment_date, delivery_date, origin, destination, status, status_reason, carrier, shipping_cost
#                         from cleaned where rn = 1
                         
#                  """)



# # print(staging_layer)

# silver_layer = duckdb.sql("""  WITH validate as 
#                         (SELECT
#                             shipment_id, (
#                                         LEFT(shipment_id, 1) = 'S'
#                                         AND LENGTH(shipment_id) IN (8, 9)
#                                         AND TRY_CAST(SUBSTRING(shipment_id, 2) AS INTEGER) IS NOT NULL
#                                     ) AS is_valid_format, 
#                             COUNT(*) OVER (PARTITION BY shipment_id) > 1 AS is_duplicate,
#                             shipment_date,
#                             CASE
#                                 WHEN delivery_date ~ '^\d{4}-\d{2}-\d{2}$'
#                                     THEN delivery_date::DATE

#                                 WHEN delivery_date ~ '^\d{2}-\d{2}-\d{4}$'
#                                     AND split_part(delivery_date, '-', 1)::INT > 12
#                                     THEN strptime(delivery_date, '%d-%m-%Y')::DATE

#                                 WHEN delivery_date ~ '^\d{2}-\d{2}-\d{4}$'
#                                     AND split_part(delivery_date, '-', 2)::INT > 12
#                                     THEN strptime(delivery_date, '%m-%d-%Y')::DATE

#                                 -- both parts <= 12: genuinely ambiguous, default to MM-DD-YYYY
#                                 WHEN delivery_date ~ '^\d{2}-\d{2}-\d{4}$'
#                                     THEN strptime(delivery_date, '%m-%d-%Y')::DATE

#                                 ELSE NULL
#                             END AS delivery_date,
#                             status, status_reason, origin, destination, carrier, shipping_cost
#                         FROM staging_layer),
                        
#                         cleaned as 
#                         (select *, CASE
#                                         WHEN NOT is_valid_format THEN 'INVALID_FORMAT'
#                                       --  WHEN is_duplicate        THEN 'DUPLICATE_ID'
#                                         ELSE 'VALID'
#                                     END AS shipment_id_quality_flag
#                         from validate)

#                         select shipment_id, shipment_date, delivery_date,
#                             status, status_reason, origin, destination, carrier, CAST(shipping_cost AS DECIMAL(7,2)) as shipping_cost,
#                             case when shipment_date is null then null
#                                 else shipment_date end as created_at,
#                             case when delivery_date is null then null
#                                 else delivery_date end as updated_at from cleaned
#                          where shipment_id_quality_flag = 'VALID'
                        
#                         """)

# print(silver_layer)
# cnt = duckdb.sql("select count(*) from silver_layer")
# print(cnt)


# silver_layer.to_parquet("routeflow_shipments_silver_layer.parquet")
# silver_layer.to_parquet("routeflow_shipments.parquet")

# gold_layer = duckdb.sql("""""")


































# chk_sil = duckdb.sql("select * from staging_layer where status = 'Delivered' and delivery_date is null or delivery_date in ('UNKNOWN', '', 'NULL')")
# print(chk_sil)


# chk_id = duckdb.sql(""" SELECT
#         shipment_id,
#         (
#             LEFT(shipment_id, 1) = 'S'
#             AND LENGTH(shipment_id) IN (8, 9)
#             AND TRY_CAST(SUBSTRING(shipment_id, 2) AS INTEGER) IS NOT NULL
#         ) AS is_valid_format ,
 
#         -- duplicate check: same cleaned id appears more than once
#         COUNT(*) OVER (PARTITION BY shipment_id) > 1 AS is_duplicate
#     FROM staging_layer

# """)

# tot = duckdb.sql("""select *,     CASE
#         WHEN NOT is_valid_format THEN 'INVALID_FORMAT'
#         WHEN is_duplicate        THEN 'DUPLICATE_ID'
#         ELSE 'VALID'
#     END AS shipment_id_quality_flag
#  from chk_id""")



# cnt_inv = duckdb.sql("select shipment_id, count(*) as cnt from tot where shipment_id_quality_flag = 'INVALID_FORMAT' group by 1")
# print(cnt_inv)
# total_cnt = duckdb.sql("select sum(cnt) from cnt_inv")
# print(total_cnt)

# cnt_val = duckdb.sql("select * from tot where shipment_id_quality_flag = 'VALID'")

# print(cnt_inv)


# cnt_star = duckdb.sql("select count(*) from silver_layer")
# print(cnt_star)


# chk_dt = duckdb.sql("""select delivery_date, count(*) as cnt from staging_layer where try_cast(delivery_date as date) is null 
#                             and status in ('Failed', 'Delivered', 'Returned')
#                             group by 1""")
# print(chk_dt)
# sum_cnt = duckdb.sql("select sum(cnt) from chk_dt")
# print(sum_cnt)

# chk_shp_id = duckdb.sql("""select shipment_id, count(*) as cnt from bronze_layer group by 1 having count(*) > 1""")
# print(chk_shp_id)

# chk_shp_id = duckdb.sql("""select shipment_id, ROW_NUMBER() OVER(partition by shipment_id order by shipment_date) as cnt from bronze_layer""")
# print(chk_shp_id)
# sum_cnt = duckdb.sql("select SUM(cnt) from chk_shp_id where cnt > 1")
# print(sum_cnt)


# sum_cnt = duckdb.sql("select sum(cnt) from chk_shp_id")
# print(sum_cnt)
# shpid = duckdb.sql("select * from bronze_layer where delivery_date IS NULL")
# print(shpid)

# still_null = duckdb.sql("select delivery_date, COUNT(*) as cnt from silver_layer where TRY_CAST(delivery_date as date) IS NULL GROUP BY 1 ")
# print(still_null)

# still_null = duckdb.sql("select shipment_id, COUNT(*) as cnt from staging_layer GROUP BY 1 having count(*) > 1")
# print(still_null)


# shpid = duckdb.sql("select sum(cnt) from still_null")
# print(shpid)
# date_chk = duckdb.sql("""SELECT delivery_date, COUNT(*)
# FROM staging_layer
# WHERE delivery_date IS NOT NULL
#   AND NOT (delivery_date ~ '^\d{4}-\d{2}-\d{2}$' OR delivery_date ~ '^\d{2}-\d{2}-\d{4}$')
# GROUP BY delivery_date """)

# print(date_chk)

# print(duckdb.sql("DESCRIBE silver_layer"))

# blank_val = duckdb.sql("""select DISTINCT status from 'routeflow_shipments_2026.csv'""")
# print(blank_val)
# duplicate = duckdb.sql("""SELECT delivery_date, COUNT(*) AS cnt
# FROM staging_layer
# WHERE try_cast(delivery_date as date) IS NULL
# GROUP BY delivery_date
# ORDER BY cnt DESC;
#  """)

# count = duckdb.sql("select delivery_date, count(*) as cnt from duplicate where delivery_date IS NOT NULL and delivery_date <> 'UNKNOWN' group by 1")
# # print(count)

# summ = duckdb.sql("select sum(cnt) from 'count'")
# print(summ)
# duplicate = duckdb.sql("""SELECT delivery_date, COUNT(*) AS cnt
# FROM staging_layer
# group by 
# WHERE delivery_date IS NULL;
#  """)

# print(duplicate)



# duplicate = duckdb.sql("""SELECT shipment_id, count(*) as cnt
# FROM 'staging_layer' 

# group by 1
# having count(*) > 1
#  """)

# print(duplicate)
# sum_tot = duckdb.sql("select sum(cnt) from 'duplicate'")
# print(sum_tot)


# blank_val = duckdb.sql("""select delivery_date from 'quarantine_data' WHERE delivery_date IS NULL OR DELIVERY_DATE = 'UNKNOWN'""")
# print(blank_val)

