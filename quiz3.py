from pyspark import SparkContext, SparkConf
from user_definition import *
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
#Feel free to add if you want.


#Create SparkContext. - You can change this, if you want.
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

##########
partitionSize = 4
train = sc.textFile(training_data, partitionSize).map(lambda x:x.split(","))
test = sc.textFile(test_data, partitionSize).map(lambda x:x.split(","))

def transform_label(x):
    if x == 'Iris-setosa':
        return int(0)
    if x == 'Iris-versicolor':
        return int(1)
    if x == 'Iris-virginica':
        return int(2)

schema = StructType([
  StructField("sepal_length", FloatType(), False),
  StructField("sepal_width", FloatType(), False),
  StructField("petal_length", FloatType(), False),
  StructField("petal_width", FloatType(), False),
  StructField("class", IntegerType(), False)
  ])

df_train = sqlContext.createDataFrame(train.map(lambda x:Row(float(x[0]),float(x[1]),float(x[2]),float(x[3]),transform_label(x[4]))), schema)
df_test = sqlContext.createDataFrame(test.map(lambda x:Row(float(x[0]),float(x[1]),float(x[2]),float(x[3]),transform_label(x[4]))), schema)

df_train.printSchema()

va = VectorAssembler(outputCol="features", inputCols=["sepal_length","sepal_width","petal_length","petal_width"])
df_train = va.transform(df_train).select("features", "class").withColumnRenamed("class", "label").cache()
df_test = va.transform(df_test).select("features", "class").withColumnRenamed("class", "label").cache()

dt = DecisionTreeClassifier(maxDepth=20, maxBins=10, minInstancesPerNode=1, minInfoGain=10)
dtmodel = dt.fit(df_train)
dtpredicts = dtmodel.transform(df_test)

multiEvaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
print "F1 = %0.4f" % multiEvaluator.evaluate(dtpredicts)
#########
sc.stop()

