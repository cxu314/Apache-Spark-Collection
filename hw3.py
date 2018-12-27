from pyspark import SparkContext, SparkConf
from user_definition import *
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf

#Create SparkContext. - You can change this, if you want.
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

##########
partitionSize = 4
train = sc.textFile(training_data, partitionSize).map(lambda x:  x.split(","))
test = sc.textFile(test_data, partitionSize).map(lambda x:  x.split(","))

def transform_label(x):
    if x == 'g':
        return float(1)
    if x == 'b':
        return float(0)

schema = StructType([
   StructField("features", ArrayType(elementType=DoubleType(),containsNull=False),True),
   StructField("label", DoubleType(),True)
])

df_train = sqlContext.createDataFrame(train.map(lambda x : Row([float(m) for m in x[0:-1]],transform_label(x[-1]))), schema)
df_test = sqlContext.createDataFrame(test.map(lambda x : Row([float(m) for m in x[0:-1]],transform_label(x[-1]))), schema)

list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
df_train = df_train.select(list_to_vector_udf(df_train["features"]).alias("features"),'label').cache()
df_test = df_test.select(list_to_vector_udf(df_test["features"]).alias("features"),'label').cache()

rf = RandomForestClassifier(maxDepth=30)
rfmodel = rf.fit(df_train)

rfpredicts = rfmodel.transform(df_test)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

print("F1 = %0.4f" % evaluator.evaluate(rfpredicts))
#########
sc.stop()
