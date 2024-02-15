from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExample") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9002") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.options(inferSchema="true", delimiter=",", header="true") \
    .parquet("elem")

df.show()
df.printSchema()


bucket_name = "warehouse"
df.write.mode("overwrite") \
    .format("parquet") \
    .option("path", f"s3a://{bucket_name}/elem") \
    .save()