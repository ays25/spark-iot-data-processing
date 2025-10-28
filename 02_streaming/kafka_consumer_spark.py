from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

spark = SparkSession.builder.appName("KafkaSparkConsumer").getOrCreate()

schema = StructType([
    StructField("device_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType())
])

df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "sensor_data")
      .load())

json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

aggregated = json_df.groupBy("device_id").agg(avg("temperature").alias("avg_temp"),
                                              avg("humidity").alias("avg_humidity"))

query = (aggregated.writeStream.outputMode("complete")
         .format("console")
         .option("truncate", "false")
         .start())

query.awaitTermination()
