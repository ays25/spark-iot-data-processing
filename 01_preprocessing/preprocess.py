from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler

spark = SparkSession.builder.appName("IoTPreprocessing").getOrCreate()

df = spark.read.csv("/app/01_preprocessing/iot_data.csv", header=True, inferSchema=True)
df.show(5)

df = df.na.fill({"temperature": 0, "humidity": 0})
df = df.dropDuplicates()
df = df.withColumn("temp_humidity_ratio", col("temperature") / (col("humidity") + 1))

assembler = VectorAssembler(inputCols=["temperature", "humidity", "temp_humidity_ratio"], outputCol="features")
assembled = assembler.transform(df)
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaled = scaler.fit(assembled).transform(assembled)

scaled.show(5)
scaled.write.mode("overwrite").parquet("/app/01_preprocessing/cleaned_iot_data.parquet")
