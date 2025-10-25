# PyFlink SQL API example with 1-minute tumbling window
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # Use a temporary source table with values connector for demo
    t_env.execute_sql(        """CREATE TEMPORARY TABLE payments (
              user STRING,
              amount DOUBLE,
              ts TIMESTAMP_LTZ(3),
              WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            ) WITH (
              'connector' = 'values',
              'data-id' = '1',
              'bounded' = 'true'
            )"""
    )

    # Feed demo rows
    t_env.execute_sql("""INSERT INTO payments VALUES
        ('alice', 10.0, TO_TIMESTAMP_LTZ(1700000000000, 3)),
        ('alice', 20.0, TO_TIMESTAMP_LTZ(1700000030000, 3)),
        ('bob',   5.0,  TO_TIMESTAMP_LTZ(1700000020000, 3))""")

    # Windowed aggregation
    t_env.execute_sql(        """CREATE TEMPORARY VIEW per_min AS
           SELECT
             window_start, window_end, user, SUM(amount) AS total
           FROM TABLE(
             TUMBLE(TABLE payments, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
           )
           GROUP BY window_start, window_end, user"""
    )

    # Print results using values sink
    t_env.execute_sql(        """CREATE TEMPORARY TABLE out_print WITH ('connector'='print')
           LIKE per_min (EXCLUDING ALL)""")

    t_env.execute_sql("INSERT INTO out_print SELECT * FROM per_min").wait()

if __name__ == "__main__":
    main()
