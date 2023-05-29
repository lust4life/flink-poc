-- backend: flink
-- config: easy_sql.etl_type=streaming
-- config: flink.execution.checkpointing.interval=10 s

-- target=action
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon'
);

-- target=action
USE CATALOG paimon;

-- target=action
CREATE TABLE if not exists ods_orders (
  order_id STRING,
  product_id STRING,
  customer_id STRING,
  purchase_timestamp TIMESTAMP_LTZ,
  dd STRING,
  hh INT,
  pts as PROCTIME(),
  ts as cast(purchase_timestamp as TIMESTAMP_LTZ(3)),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,
  PRIMARY KEY (order_id, dd, hh) NOT ENFORCED
) PARTITIONED BY (dd ,hh)
 with (
  'changelog-producer' = 'input'
);

-- target=action
-- register a PostgreSQL table 'orders' in Flink SQL
CREATE TEMPORARY TABLE if not exists orders_source (
  order_id STRING,
  product_id STRING,
  customer_id STRING,
  purchase_timestamp TIMESTAMP_LTZ,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'postgres',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'orders',
  'decoding.plugin.name' = 'pgoutput',
  'slot.name' = 'orders'
);

-- target=action
-- TODO: set config from env
-- set 'pipeline.name' = 'ods_orders';
insert into ods_orders select *, DATE_FORMAT(purchase_timestamp, 'yyyy-MM-dd') as dd, cast( hour(purchase_timestamp) as int) as hh from orders_source;


-- target=action
CREATE TABLE if not exists ods_products (
  product_id STRING,
  product_name STRING,
  sale_price INT,
  product_rating DOUBLE,
  ts TIMESTAMP_LTZ(3),
  -- WATERMARK FOR ts AS ts,
  pts as PROCTIME(),
  PRIMARY KEY (product_id) NOT ENFORCED
) with (
  'changelog-producer' = 'input'
);

-- target=action
CREATE TEMPORARY TABLE if not exists products_source (
  product_id STRING,
  product_name STRING,
  sale_price INT,
  product_rating DOUBLE,
  ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'postgres',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'products',
  'decoding.plugin.name' = 'pgoutput',
  'slot.name' = 'products'
);

-- target=action
-- set 'pipeline.name' = 'ods_products';
insert into ods_products select * from products_source;

-- target=action
-- ods customers
CREATE TABLE if not exists ods_customers (
  customer_id STRING,
  customer_name STRING,
  ts TIMESTAMP_LTZ(3),
  pts as PROCTIME(),
  PRIMARY KEY (customer_id) NOT ENFORCED
) with (
  'changelog-producer' = 'input'
);

-- target=action
CREATE TEMPORARY TABLE if not exists customers_source (
  customer_id STRING,
  customer_name STRING,
  ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'postgres',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'customers',
  'decoding.plugin.name' = 'pgoutput',
  'slot.name' = 'customers'
);

-- target=action
-- set 'pipeline.name' = 'ods_customers';
insert into ods_customers select * from customers_source;

-- target=action
-- order enriched with product info at the time and with customer info

CREATE TABLE if not exists dwd_order_enriched (
  order_id STRING,
  product_id STRING,
  customer_id STRING,
  purchase_timestamp TIMESTAMP,

  product_name STRING,
  sale_price INT,

  customer_name STRING,

  PRIMARY KEY (order_id) NOT ENFORCED
)
 with (
  'changelog-producer' = 'lookup',
  'merge-engine' = 'partial-update',
  'partial-update.ignore-delete' = 'true'
);

-- target=action
-- enriched by order_with_product_at_the_time
-- set 'pipeline.name' = 'enriched by order_with_product_at_the_time';
insert into dwd_order_enriched(order_id, product_id, customer_id, purchase_timestamp, product_name, sale_price)
  select 
  /*+ LOOKUP('table'='ods_products',  'async'='true', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s','max-attempts'='3') */
  o.order_id, o.product_id, o.customer_id, o.purchase_timestamp, p.product_name, p.sale_price 
  from ods_orders o 
  left join ods_products FOR SYSTEM_TIME AS OF o.pts as p
  on o.product_id = p.product_id;

-- target=action
-- enriched by customer
-- set 'pipeline.name' = 'enriched by customer';
insert into dwd_order_enriched(order_id, customer_name)
 select o.order_id, c.customer_name 
 from ods_customers c
 join ods_orders o 
 on c.customer_id = o.customer_id;

-- target=action
-- product_sales_by_hh
CREATE TABLE if not exists dws_product_sales_by_hh (
    product_id STRING,
    hh BIGINT,
    sales INT,
    PRIMARY KEY (product_id, hh) NOT ENFORCED
) WITH (
    'changelog-producer' = 'lookup',
    'merge-engine' = 'aggregation',
    'fields.sales.aggregate-function' = 'sum'
);

-- target=action
-- set 'pipeline.name' = 'dws_product_sales_by_hh';
insert into dws_product_sales_by_hh
  select oe.product_id, hour(oe.purchase_timestamp) as hh, sale_price from dwd_order_enriched oe;
