-- backend: flink
-- config: easy_sql.etl_type=streaming
-- conf1ig: easy_sql.flink_tables_file_path=/opt/flink/usrlib/catalog.json

-- inputs: pg_cdc.orders
-- outputs: ods.orders


-- target=variables
select
    'append'           as __save_mode__

-- target=action
set 'pipeline.name' = 'ods_orders';
insert into ods_orders select *, DATE_FORMAT(purchase_timestamp, 'yyyy-MM-dd') as dd, cast( hour(purchase_timestamp) as int) as hh from orders_source;
