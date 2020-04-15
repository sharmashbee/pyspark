
from pyspark.streaming import *
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()


data =  [("iphone", "2007"),
         ("iphone 3G","2008"),
         ("iphone 3GS","2009"),
         ("iphone 4","2010"),
         ("iphone 4S","2011"),
         ("iphone 5","2012"),
         ("iphone 8","2014"),
         ("iphone 10","2017")]

df = spark.createDataFrame(data,['key', 'value'])
df.write.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("topic","test3").save()

     
