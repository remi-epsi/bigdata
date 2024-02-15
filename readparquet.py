from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when

spark = SparkSession.builder \
    .appName("LectureParquet") \
    .getOrCreate()

df = spark.read.parquet("elem")
#df = df.withColumn("devise", when(col("devise") == "EUR", "USD").otherwise(col("devise")))
df.show(100000,False)

spark.stop()
