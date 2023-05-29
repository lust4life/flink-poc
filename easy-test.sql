-- backend: flink
-- config: easy_sql.etl_type=streaming

-- target=variables
select 2 as a

-- target=log.a
select 'a => ${a}' as b
