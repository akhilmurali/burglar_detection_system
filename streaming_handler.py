from datetime import time
from typing import Collection
from pyspark import SparkContext
from pyspark.streaming import StreamingContext, dstream
import statistics
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

DOPPLER_MEAN_FREQUENCY = 120
DOPPLER_VARIANCE = 40
# This is assumed noise in doppler frequency reading
DOPLLER_NOISE_FREQUENCY = 1 
DOPLLER_NOISE_FREQUNECY_VARIANCE = 0
confidence_interval = 0.01
WINDOW_DURATION = 5
MIN_READINGS = 3

def perform_bayseian_cleaning(sensor_data_rdd):
    # calculate the mean of past 5 values in the time window
    # Now look at the mean and variance of the noise. 
    # On that basis see if the value can be in the valid range.
    # If yes then return the value as is
    # else assign the mean value to that value
    list_of_values = []
    for item in sensor_data_rdd:
        list_of_values.append(item)
    return statistics.mean(list_of_values)


def check_float(data):
    try:
        float(data)
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    spark_context = SparkContext(appName="BDSStream")
    ssc = StreamingContext(spark_context, WINDOW_DURATION) 
    # # As we are staggering the output by 1 we will be able to consolidate the result only every 30 seconds.
    ds_microwave_sensor = ssc.socketTextStream('localhost', 12000)
    ds_microwave_sensor.pprint()
    ds_pressure_sensor = ssc.socketTextStream('localhost', 12001)
    ds_microwave_sensor = ds_microwave_sensor.map(lambda l: tuple(l.split(",")))
    ds_microwave_sensor.pprint()
    ds_microwave_sensor = ds_microwave_sensor.filter(lambda l: check_float(l[3])) # filter out the values that are null or empty
    ds_pressure_sensor = ds_pressure_sensor.map(lambda l: tuple(l.split(",")))
    ds_pressure_sensor.pprint()
    ds_pressure_sensor = ds_pressure_sensor.filter(lambda l: check_float(l[3])) # filter out the values that are null or empty
    combined_stream = ds_pressure_sensor.union(ds_microwave_sensor)
    combined_stream.pprint()
    #ds_microwave_sensor.map(lambda data: data.value).flatMap(lambda rdd: perform_bayseian_cleaning(rdd))
    #ds_microwave_sensor = ds_microwave_sensor.map(lambda sensorData: perform_bayseian_cleaning(sensorData, sensor_array_value))
    #pressure_data = ds_pressure_sensor.map(lambda l: l.split(",")[1])
    #pressure_data.pprint()
    #combined_stream = microwave_data.join(pressure_data)

    # #combined_stream.pprint()
    # # The data object that is returned is a new RDD which is an array maybe?
    ssc.start() # Starts the streaming serivce.
    ssc.awaitTermination() # API that awaits manual termination.
    ###############################################################

# What set of data is present in an RDD? How can we group data in a time window together?
# How can we batch elements into a single RDD?
# What is the correct way to run a spark program?
# How can we debug a spark program? Can we do that with a local cluster?
# Does spark require me to run a local cluster with workers to be able to do the job?
# The idea here would be to join the three RDDs, extract the consolidated values from those and then just input
# the data into the model to get a prediction.
# How to access individual elements in an RDD?

# Well on second thought we need not join multiple streams to get to our result.

# Why isn't the data printing out?

# RDD is a subset of DStream.

# So basically an RDD can be operated on in parallel. That is the reason why we have it as a batch of object?

# So, RDD basically is like a set of objects that can be processed in parallel. 
# We can do data cleaning in spark itself.

# The spark context object tells spark how to access a cluster.
# The app name is the name to show on the UI.

# RDDs cannot hold complex data objects

# What is it that I want to achive?
# Convert the input data stream into a tuple set.
# Then apply bayseian filtering on the data that is obtained.
# do this thing for all the three streams of data
# Finally use the data in a single frame of data to make a prediction i,e make a prediction about weather a burglar has 
# come in or not. This could become the descriptive analytics?
# Then omit one sensor data and predict the possibility of a breakin. This could be the predictive analytics.