-- backend: flink
-- config: easy_sql.etl_type=streaming
-- config: easy_sql.flink_tables_file_path=/opt/flink/usrlib/catalog.json
-- config: flink.pipeline.jars=/opt/flink/lib/userlib/paimon-flink-1.16-0.5-20230515.002018-12.jar;/opt/flink/lib/userlib/flink-sql-connector-postgres-cdc-2.3.0.jar;/opt/flink/lib/userlib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
-- config: flink.execution.checkpointing.interval=10 s

-- inputs: pg_cdc.orders,pg_cdc.products,pg_cdc.customers,ods.orders,ods.products,ods.customers,dwd.order_enriched,dws.product_sales_by_hh

-- target=action.ingest_ods_orders
insert into ods.orders select *, DATE_FORMAT(purchase_timestamp, 'yyyy-MM-dd') as dd, cast( hour(purchase_timestamp) as int) as hh from pg_cdc.orders;

-- target=action.ingest_ods_produts
insert into ods.products select * from pg_cdc.products;

-- target=action.ingest_ods_customers
insert into ods.customers select * from pg_cdc.customers;


-- target=action.order_enriched_by_order_with_product_at_the_time
insert into dwd.order_enriched(order_id, product_id, customer_id, purchase_timestamp, product_name, sale_price)
  select 
  /*+ LOOKUP('table'='ods.products',  'async'='true', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s','max-attempts'='3') */
  o.order_id, o.product_id, o.customer_id, o.purchase_timestamp, p.product_name, p.sale_price 
  from ods.orders o 
  left join ods.products FOR SYSTEM_TIME AS OF o.pts as p
  on o.product_id = p.product_id;

-- target=action.order_enriched_by_customer
insert into dwd.order_enriched(order_id, customer_name)
 select o.order_id, c.customer_name 
 from ods.customers c
 join ods.orders o 
 on c.customer_id = o.customer_id;

-- target=action.generate_dws_product_sales_by_hh
insert into dws.product_sales_by_hh
  select oe.product_id, hour(oe.purchase_timestamp) as hh, sale_price from dwd.order_enriched oe;
