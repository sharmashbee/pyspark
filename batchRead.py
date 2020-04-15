
from pyspark.streaming import *
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()

df=spark.read.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","test").load()
df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df2.show()
