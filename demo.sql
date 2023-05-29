-- register a PostgreSQL table 'orders' in Flink SQL
CREATE TABLE orders (
  order_id STRING,
  product_id STRING,
  customer_id STRING,
  purchase_timestamp TIMESTAMP
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'postgres',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'orders',
  'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE products (
  product_id STRING,
  product_name STRING,
  sale_price INT,
  product_rating DOUBLE
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'postgres',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'public',
  'table-name' = 'products',
  'decoding.plugin.name' = 'pgoutput'
);

CREATE CATALOG ods WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon'
);

USE CATALOG ods;

CREATE TABLE orders (
  order_id STRING,
  product_id STRING,
  customer_id STRING,
  purchase_timestamp TIMESTAMP
);

insert into orders select * from default_catalog.default_database.orders;
