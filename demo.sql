-- CREATE CATALOG pg WITH(
--     'type' = 'jdbc',
--     'default-database' = 'postgres',
--     'username' = 'postgres',
--     'password' = 'postgres',
--     'base-url' = 'jdbc:postgresql://postgres:5432'
-- );

-- USE CATALOG pg;


CREATE CATALOG ods WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///opt/flink/paimon'
);

USE CATALOG ods;

CREATE TABLE orders (
  order_id STRING,
  product_id STRING,
  customer_id STRING,
  purchase_timestamp TIMESTAMP,
  PRIMARY KEY (order_id) NOT ENFORCED
);

-- register a PostgreSQL table 'orders' in Flink SQL
CREATE TABLE default_catalog.default_database.orders (
  order_id STRING,
  product_id STRING,
  customer_id STRING,
  purchase_timestamp TIMESTAMP,
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
  'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE default_catalog.default_database.products (
  product_id STRING,
  product_name STRING,
  sale_price INT,
  product_rating DOUBLE,
  PRIMARY KEY (product_id) NOT ENFORCED
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

insert into orders select * from default_catalog.default_database.orders;
