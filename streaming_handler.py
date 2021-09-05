from datetime import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext, dstream

DOPPLER_MEAN_FREQUENCY = 120
DOPPLER_VARIANCE = 40
# This is assumed noise in doppler frequency reading
DOPLLER_NOISE_FREQUENCY = 1 
DOPLLER_NOISE_FREQUNECY_VARIANCE = 0
confidence_interval = 0.01
WINDOW_DURATION = 5
MIN_READINGS = 3

class MicrowaveSensor(object):
    def __init__(self, time, device_id, region_id, value) -> None:
        self.time = time
        self.device_id = device_id
        self.region_id = region_id
        self.value = value

class PressureSensor(object):
    def __init__(self, time, device_id, region_id, value) -> None:
        self.time = time
        self.device_id = device_id
        self.region_id = region_id
        self.value = value

def perform_bayseian_cleaning(value, microwave_data_array):
    print(value)
    print(microwave_data_array)
    return value


def transform_To_MS(line):
    arr = line.split(",")
    sensor = MicrowaveSensor(arr[0], arr[1], arr[2], arr[3])
    return sensor.value

if __name__ == "__main__":
    spark_context = SparkContext(appName="BDSStream")
    ssc = StreamingContext(spark_context, WINDOW_DURATION) 
    # As we are staggering the output by 1 we will be able to consolidate the result only every 30 seconds.
    ds_microwave_sensor = ssc.socketTextStream('localhost', 12000)
    #ds_pressure_sensor = ssc.socketTextStream('localhost', 12001)
    ds_microwave_sensor = ds_microwave_sensor.map(lambda l: transform_To_MS(l))
    ds_microwave_sensor.pprint()
    #pressure_data = ds_pressure_sensor.map(lambda l: l.split(",")[1])
    #pressure_data.pprint()
    #combined_stream = microwave_data.join(pressure_data)

    #combined_stream.pprint()
    # The data object that is returned is a new RDD which is an array maybe?
    ssc.start() # Starts the streaming serivce.
    ssc.awaitTermination() # API that awaits manual termination.

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