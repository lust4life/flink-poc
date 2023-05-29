-- wget -P flink-userlib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
-- wget -P flink-userlib/ https://repository.apache.org/content/groups/snapshots/org/apache/paimon/paimon-flink-1.16/0.5-SNAPSHOT/paimon-flink-1.16-0.5-20230515.002018-12.jar
-- wget -P flink-userlib/ https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.3.0/flink-sql-connector-postgres-cdc-2.3.0.jar


CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon'
);

USE CATALOG paimon;
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE ods_orders (
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

-- register a PostgreSQL table 'orders' in Flink SQL
CREATE TEMPORARY TABLE orders_source (
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


set 'pipeline.name' = 'ods_orders';
insert into ods_orders select *, DATE_FORMAT(purchase_timestamp, 'yyyy-MM-dd') as dd, cast( hour(purchase_timestamp) as int) as hh from orders_source;

CREATE TABLE ods_products (
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

CREATE TEMPORARY TABLE products_source (
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

set 'pipeline.name' = 'ods_products';
insert into ods_products select * from products_source;

-- ods customers
CREATE TABLE ods_customers (
  customer_id STRING,
  customer_name STRING,
  ts TIMESTAMP_LTZ(3),
  pts as PROCTIME(),
  PRIMARY KEY (customer_id) NOT ENFORCED
) with (
  'changelog-producer' = 'input'
);

CREATE TEMPORARY TABLE customers_source (
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

set 'pipeline.name' = 'ods_customers';
insert into ods_customers select * from customers_source;

-- order enriched with product info at the time and with customer info

CREATE TABLE dwd_order_enriched (
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


-- enriched by order_with_product_at_the_time
set 'pipeline.name' = 'enriched by order_with_product_at_the_time';
insert into dwd_order_enriched(order_id, product_id, customer_id, purchase_timestamp, product_name, sale_price)
  select 
  /*+ LOOKUP('table'='ods_products',  'async'='true', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s','max-attempts'='3') */
  o.order_id, o.product_id, o.customer_id, o.purchase_timestamp, p.product_name, p.sale_price 
  from ods_orders o 
  left join ods_products FOR SYSTEM_TIME AS OF o.pts as p
  on o.product_id = p.product_id;

-- enriched by customer
set 'pipeline.name' = 'enriched by customer';
insert into dwd_order_enriched(order_id, customer_name)
 select o.order_id, c.customer_name 
 from ods_customers c
 join ods_orders o 
 on c.customer_id = o.customer_id;

-- product_sales_by_hh
CREATE TABLE dws_product_sales_by_hh (
    product_id STRING,
    hh BIGINT,
    sales INT,
    PRIMARY KEY (product_id, hh) NOT ENFORCED
) WITH (
    'changelog-producer' = 'lookup',
    'merge-engine' = 'aggregation',
    'fields.sales.aggregate-function' = 'sum'
);

set 'pipeline.name' = 'dws_product_sales_by_hh';
insert into dws_product_sales_by_hh
  select oe.product_id, hour(oe.purchase_timestamp) as hh, sale_price from dwd_order_enriched oe;



-- test
-- in streaming mode
-- normal case, product (dim) has prepared first
  -- insert into products (product_id, product_name, sale_price, product_rating) values ('1','p1',100,0);
  -- wait until ods_products has been synced 
  -- insert into orders (order_id, product_id, customer_id, purchase_timestamp)  values ('1','1','1',now());
  -- should have join in dwd_order_enriched

-- look up failed case, insert order without product in 30s, should without product info
  -- insert into orders (order_id, product_id, customer_id, purchase_timestamp)  values ('2','2','1',now());
  -- insert into orders (order_id, product_id, customer_id, purchase_timestamp)  values ('3','2','1',now());
  -- after 30s, should see order 2,3 without product info in dwd_order_enriched
  
-- look up normal case
  -- insert into orders (order_id, product_id, customer_id, purchase_timestamp)  values ('4','3','1',now());
  -- wait for 15s
  -- insert into products (product_id, product_name, sale_price, product_rating) values ('3','p3',300,0);
  -- after 15s, should see order 4 with product info in dwd_order_enriched

-- dim updated case: update product price info, wait a moment to let it stream, then insert a new order, old order should have old price, new order should have new price
  -- update products set sale_price = 200  where product_id = '1';
  -- wait for 15s
  -- insert into orders (order_id, product_id, customer_id, purchase_timestamp)  values ('5','1','1',now());
  -- should see order (5 => 200) , (1 => 100) info in dwd_order_enriched

-- in batch mode, should fix item without product info, should unified price info within same product(use latest)
SET 'execution.runtime-mode' = 'batch';
select o.order_id, o.product_id, o.customer_id, o.purchase_timestamp, p.product_name, p.sale_price
from ods_orders o
left join ods_products
on o.product_id = p.product_id;
-- then we can refine dwd_order_enriched table by batched result periodically
INSERT OVERWRITE dwd_order_enriched 
select o.order_id, o.product_id, o.customer_id, o.purchase_timestamp, p.product_name, p.sale_price, cast(null as STRING)
from ods_orders o
left join ods_products p
on o.product_id = p.product_id;


-- 场景一， batch 修复 streaming 数据
  -- streaming 持续写入某小时的订单汇总， 但是可能因为 window 和 watermark 而丢数据 （构建丢数据的场景）
  -- batch 截止某小时的 按小时订单金额汇总 (模拟某天)， 确保正确性
  SET 'execution.runtime-mode' = 'batch';
  select o.product_id, hour(o.purchase_timestamp), sum(sale_price) 
  from ods_orders o /*+ OPTIONS('scan.timestamp-millis' = '1684423952084') */
  left join ods_products p on o.product_id = p.product_id
  group by o.product_id, hour(o.purchase_timestamp);

-- 场景二，根据 snapshots 重跑历史数据换维度进行汇总 （某用户历史所有订单汇总）
SET 'execution.runtime-mode' = 'batch';
select customer_id, sum(sale_price) 
from dwd_order_enriched  /*+ OPTIONS('scan.timestamp-millis' = '1678883047356') */
where customer_id = '1'
group by customer_id;

-- 场景三， streaming 和 batch 下不同的结果
-- product 更新价格，enriched 应该展示当时订单产品的价格， 不影响 dws_product_sales_by_hh
-- order 更新 product id, enriched 应该展现新的 product 名称，价格， dws_product_sales_by_hh 应该要调整
-- order 更新 customer id, enriched 应该展现新的 customer 名称， 不影响 dws_product_sales_by_hh
-- customer 更新名称，enriched 应该展现最新的名称， 不影响 dws_product_sales_by_hh
-- order 删除，不影响 enriched 结果， 不影响 dws_product_sales_by_hh
-- product 删除，不影响 enriched 结果， 不影响 dws_product_sales_by_hh
-- customer 删除，不影响 enriched 结果， 不影响 dws_product_sales_by_hh
