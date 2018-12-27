from pyspark import SparkContext, SparkConf
from user_definition import *
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
#Do not add additional libraries.

#Create SparkContext. - You can change this, if you want.
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

##########
schema = StructType([
    StructField("station_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("total_num_dock", IntegerType(), True),
    StructField("city", StringType(), True)
    ])

row = sc.textFile(file_name).map(lambda x:x.split(","))
rowRDD = row.map(lambda r: Row(int(r[0]),r[1],float(r[2]),float(r[3])
                               ,int(r[4]),r[5]))
df = sqlContext.createDataFrame(rowRDD, schema)

#print
df.printSchema()
df.filter(df['station_id'] == station_id).select("station_id", "latitude", "longitude").show()

#########
sc.stop()