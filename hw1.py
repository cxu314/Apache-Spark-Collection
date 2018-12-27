from pyspark import SparkContext, SparkConf
from user_definition import *
# Do not add any additional libraries.

#Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName(app_name)
sc = SparkContext(conf = conf)

status = sc.textFile(input_file1).map(lambda x:x.split(',')).map(lambda x:(x[0],[x[3],x[1],x[2]]))
stations = sc.textFile(input_file2).map(lambda x:(x.split(',')[1],x.split(',')[0]))
broadcastVar = sc.broadcast(stations.lookup(station_name)[0])

for x in sorted(status.lookup(broadcastVar.value), key = lambda x:x[0]):
    print ','.join(x)

sc.stop()