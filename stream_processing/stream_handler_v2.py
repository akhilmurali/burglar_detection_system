from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, isnull, mean
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql.functions import regexp_extract
from functools import partial
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("BDSStreamHandler") \
    .getOrCreate()

microwaveSensorSchema = StructType().add("time", "string").add("device_id", "string").add("region_id","string").add("value", "string")
fields = partial(
    regexp_extract, str="value", pattern="^([0-9]*),(\w*),(\w*),([-+]?[0-9]*\.?[0-9]+),(\w*),(\w*),([-+]?[0-9]*\.?[0-9]+),(\w*),(\w*),([0-9]*)$"
)
# reference: https://stackoverflow.com/questions/41378447/spark-structured-streaming-using-sockets-set-schema-display-dataframe-in-conso
sensor_feed_DF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
print(type(sensor_feed_DF))

sensor_feed_DF = sensor_feed_DF.select(
    fields(idx=1).alias("time"),
    fields(idx=2).alias("mw_sensor_id"), 
    fields(idx=3).alias("mw_region_id"),
    fields(idx=4).alias("mw_value"),
    fields(idx=5).alias("pressure_sensor_id"),
    fields(idx=6).alias("pressure_sensor_region_id"),
    fields(idx=7).alias("pressure_value"),
    fields(idx=8).alias("ir_fence_sensor_id"),
    fields(idx=9).alias("ir_fence_region_id"),
    fields(idx=10).alias("ir_fence_value"),
)

# Basic bound checks and filtering & cleaning
sensor_feed_DF.na.drop()
sensor_feed_DF.printSchema()
sensor_feed_DF.filter(sensor_feed_DF.mw_value < 10)
sensor_feed_DF.filter(sensor_feed_DF.mw_value > 300)
sensor_feed_DF.filter(sensor_feed_DF.pressure_value > 120)
sensor_feed_DF.filter(sensor_feed_DF.pressure_value < 40)
sensor_feed_DF.filter(sensor_feed_DF.ir_fence_value > 255)
sensor_feed_DF.filter(sensor_feed_DF.ir_fence_value < 0)

query = sensor_feed_DF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
query.awaitTermination()