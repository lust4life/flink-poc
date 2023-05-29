from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, TableEnvironment


def run_multi_sink_in_one_job():
    config = Configuration()
    config.set_string("execution.buffer-timeout", "1 min")
    env_settings = (
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .with_configuration(config)
        .build()
    )
    tab_env = TableEnvironment.create(env_settings)

    tab_env.execute_sql(
        """
    CREATE TABLE first_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
"""
    )

    tab_env.execute_sql(
        """
    CREATE TABLE second_sink_table (
        id BIGINT, 
        data VARCHAR
    ) WITH (
        'connector' = 'print'
    )
"""
    )

    table1 = tab_env.from_elements([(1, "Hi"), (2, "Hello")], ["id", "data"])
    table2 = tab_env.from_elements([(3, "Hi"), (4, "Hello")], ["id", "data"])

    statement_set = tab_env.create_statement_set()

    # emit the "table" object to the "first_sink_table"
    statement_set.add_insert("first_sink_table", table1)
    statement_set.add_insert("first_sink_table", table2)

    # execute the statement set
    statement_set.execute()


def run_multi_job():
    t1 = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .with_configuration(Configuration())
        .build()
    )
    t2 = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .with_configuration(Configuration())
        .build()
    )

    with t1.execute_sql("select 3 as c limit 1").collect() as rs:
        for r in rs:
            c = str(r)

    with t2.execute_sql("select 4 as d limit 1").collect() as rs1:
        for r in rs1:
            d = str(r)

    print(f"c => {c}, d => {d}")


if __name__ == "__main__":
    run_multi_job()
