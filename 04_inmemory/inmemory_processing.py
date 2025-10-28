from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("InMemoryExample").getOrCreate()

df = spark.range(0, 1_000_000).withColumnRenamed("id", "reading")

start = time.time()
df.selectExpr("sum(reading)").show()
print("Without cache:", time.time() - start)

df.cache()
start = time.time()
df.selectExpr("sum(reading)").show()
print("With cache:", time.time() - start)
