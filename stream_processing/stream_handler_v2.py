from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, isnull, mean
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql.functions import regexp_extract
from functools import partial
from pyspark.sql.functions import col

# Load the data into a dataframe and into a csv before spark starts.
# Use that dataframe to train an ML model
# Use the model in the below lines to make the prediction
# branch out that maybe into a separate result set
spark = SparkSession \
    .builder \
    .appName("BDSStreamHandler") \
    .getOrCreate()

microwaveSensorSchema = StructType().add("time", "string").add("device_id", "string").add("region_id","string").add("value", "string")
fields = partial(
    regexp_extract, str="value", pattern="^([0-9]*)\s*,\s*(\w*)\s*,\s*(\w*)\s*,\s*([+-]?([0-9]*[.])?[0-9]*)\s*$"
)
# reference: https://stackoverflow.com/questions/41378447/spark-structured-streaming-using-sockets-set-schema-display-dataframe-in-conso

mw_sensor_feed_DF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
print(type(mw_sensor_feed_DF))

mw_sensor_feed_DF = mw_sensor_feed_DF.select(
    fields(idx=1).alias("time"),
    fields(idx=2).alias("device_id"), 
    fields(idx=3).alias("region_id"),
    fields(idx=4).alias("mw_value")
)
mw_sensor_feed_DF.na.drop()
print(type(mw_sensor_feed_DF))
mw_sensor_feed_DF.printSchema()
# Basic bound checks and filtering
mw_sensor_feed_DF.filter(mw_sensor_feed_DF.mw_value < 0)
mw_sensor_feed_DF.filter(mw_sensor_feed_DF.mw_value > 200)
# variance_of_dataframe = mw_sensor_feed_DF.select("VAR(value)")
# mean_of_dataframe = mw_sensor_feed_DF.select("MEAN(value)")
# variance_of_dataframe.pprint()
# mean_of_dataframe.pprint()
# Split the lines into words
query = mw_sensor_feed_DF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
query.awaitTermination()