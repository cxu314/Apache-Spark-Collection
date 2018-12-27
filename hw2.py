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
schema1 = StructType([
    StructField("zip", IntegerType(), False),
    StructField("business_name", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
    ])

row1 = sc.textFile(file_name_1).map(lambda x:x.split(",")).filter(lambda x:len(x[0])==5)
rowRDD1 = row1.map(lambda r: Row(int(r[0]),r[1],r[2],r[3],r[4]))
df1 = sqlContext.createDataFrame(rowRDD1, schema1).distinct()

schema2 = StructType([
    StructField("zip", IntegerType(), False),
    StructField("supervisor", StringType(), True)
    ])

row2 = sc.textFile(file_name_2).map(lambda x:x.split(",")).filter(lambda x:len(x[0])==5)
rowRDD2 = row2.map(lambda r: Row(int(r[0]),r[1]))
df2 = sqlContext.createDataFrame(rowRDD2, schema2).distinct()

DF = df1.join(df2, 'zip')
DF_filter = DF.filter(DF['supervisor']==supervisor_id).select('zip', 'business_name', 'city').\
    sort('zip', 'business_name', 'city', ascending=True)

DF.printSchema()
print DF_filter.count()
DF_filter.show(10)

#########
sc.stop()
