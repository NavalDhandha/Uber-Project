from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Storing schema
df = spark.sql('select * from uber.bronze.bulk_rides')
rides_schema = df.schema

# Empty streaming table
dp.create_streaming_table('stg_rides')


# Bulk stream
@dp.append_flow(
  target = "stg_rides"
)

def rides_bulk():
    df = spark.readStream.table('bulk_rides')
    df = df.withColumn('booking_timestamp', col('booking_timestamp').cast('timestamp'))
    df = df.withColumn('pickup_timestamp', col('pickup_timestamp').cast('timestamp'))
    df = df.withColumn('dropoff_timestamp', col('dropoff_timestamp').cast('timestamp'))
    return df

# Streaming Load
@dp.append_flow(
  target = "stg_rides"
)

def rides_stream():
    df = spark.readStream.table('rides_raw')
    df_parsed = df.withColumn('parsed_rides', from_json(col('rides'), rides_schema))\
    .select('parsed_rides.*')
    df_parsed = df_parsed.withColumn('booking_timestamp', col('booking_timestamp').cast('timestamp'))
    df_parsed = df_parsed.withColumn('pickup_timestamp', col('pickup_timestamp').cast('timestamp'))
    df_parsed = df_parsed.withColumn('dropoff_timestamp', col('dropoff_timestamp').cast('timestamp'))
    return df_parsed
