from pyspark.sql import SparkSession
from pyspark import SparkContext
import requests
import json
import csv
from itertools import islice
sc = SparkContext('local','myapp')

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
rdd = sc.textFile("/home/sharmashbee/Documents/geocode.csv")
location = rdd.map(lambda x: x.split(",")[1]) 
rows = location.mapPartitionsWithIndex(
    lambda idx, itr: islice(itr, 1, None) if idx == 0 else itr 
)
def geo(location):
  
  url = 'http://www.mapquestapi.com/geocoding/v1/address?key=vyE1IHafMGeoxMFPBrfC1rUacsGPjosj &location={}'.format(location)
  results = requests.get(url) 
  results = results.json()     
  if len(results['results']) == 0:
      output = {
                "Latitude":None,
                "Longitude":None
               }
         
  else: 
       answer = results['results'][0]
       loc = answer['providedLocation']
       ans1 = answer['locations'][0]
       ans2 = ans1['displayLatLng']
       csv_columns = ['Latitude','Longitude','address']
       dictionary = [{
                "address" : loc['location'],
                "Latitude":ans2['lat'],
                "Longitude":ans2['lng']
            
               }]
      
       output = json.dumps(dictionary)
       
      
  return output
res1 = rows.map(geo)
def f(x):
   print(x)
res1.foreach(f)

