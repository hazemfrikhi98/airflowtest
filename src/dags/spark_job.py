from pyspark.sql import SparkSession
import os
import requests


print(os.environ)

spark = SparkSession.builder.appName("APIConsumer").getOrCreate()

url = "https://jsonplaceholder.typicode.com/posts"

response = requests.get(url)
data = response.json()

df = spark.createDataFrame(data)

# Perform some basic treatments on the data
df = df.filter(df["userId"] == 1).select("id", "title")

df.show()
