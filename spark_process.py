import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# WINDOWS HATA ÇÖZÜMÜ: Hadoop ve winutils hatasını engellemek için geçici klasör atıyoruz
os.environ['hadoop.home.dir'] = "C:\\"

spark = SparkSession.builder \
    .appName("SteamReviewStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
    .master("local[*]") \
    .getOrCreate()

# Kafka Şeması
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("app_id", StringType()),
    StructField("review_id", IntegerType()),
    StructField("review_text", StringType()),
    StructField("recommended", BooleanType())
])

# Kafka'dan Oku (Localhost üzerinden Docker'daki Kafka'ya bağlanacak)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "steam_reviews") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Delta Lake'e Yaz
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .start("./delta_lake/bronze_reviews")

print("🚀 Yerel Spark başlatıldı! Kafka'dan veriler okunuyor ve Delta Lake'e yazılıyor...")
query.awaitTermination()