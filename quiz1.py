from pyspark import SparkContext, SparkConf
from user_definition import *

#Do not add additional libraries.

#Create SparkContext. - You can change this, if you want.
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf = conf)
##########
text = sc.textFile(file_name, 8).map(lambda x:(x, 1)).reduceByKey(lambda x,y:x+y).\
    sortBy(lambda (x,y):-y).coalesce(2, shuffle = False)

for (x,y) in text.take(5):
    print x
print
print text.toDebugString()

##########
sc.stop()
