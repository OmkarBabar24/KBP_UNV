from pyspark.sql import functions
from pyspark.sql import SparkSession

spark=SparkSession.Builder.appName("ETL").getOrCreate

data1=spark.read.csv("E:\Project\FRS\data\model_config.csv",header=True,inferschema=True)
data1.show(6)