from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IncrementalProcessing").getOrCreate()

base = spark.createDataFrame([
    (1, "sensor_A", 25.0),
    (2, "sensor_B", 30.0)
], ["device_id", "name", "temperature"])

updates = spark.createDataFrame([
    (2, "sensor_B", 32.5),
    (3, "sensor_C", 27.0)
], ["device_id", "name", "temperature"])

merged = base.union(updates).dropDuplicates(["device_id"]).orderBy("device_id")
merged.show()
