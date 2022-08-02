import requests
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.types import DataTypes
from pyflink.table.udf import ScalarFunction, udf
import time

input_path = "/tmp/flink_files/practice0_input.json"
output_path = "/tmp/flink_files/practice0_output.json"
url = "http://api.open-notify.org/iss-now.json"


env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)
src_ddl = f"""
    create table src (
        message STRING,
        iss_position ROW<longitude FLOAT, latitude FLOAT>,
        `timestamp` BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'json',
        'path' = '{input_path}'
    )
"""

sink_ddl = f"""
    create table sink (
        message STRING,
        latitude FLOAT,
        longitude FLOAT,
        `timestamp` BIGINT,
        ts TIMESTAMP(3)
    ) with (
        'connector' = 'filesystem',
        'format' = 'json',
        'path' = '{output_path}'
    )
"""

t_env.execute_sql(src_ddl)
t_env.execute_sql(sink_ddl)

for i in range(5):
    r = requests.get(url)
    with open(input_path, "w") as f:
        f.write(r.text)

    append_to_sink = t_env.execute_sql(
        """
        INSERT INTO sink
        SELECT message
        , iss_position.latitude
        , iss_position.longitude
        , `timestamp`
        , TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)) AS ts
        FROM source
        """
    )

    time.sleep(2)
