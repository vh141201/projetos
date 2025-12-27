from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta import *
import os

# --- CONFIGURAÇÃO OBRIGATÓRIA PARA WINDOWS (NÃO REMOVA É TERRIVEL) ---
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["hadoop.home.dir"] = "C:/hadoop"
# Força o Java a reconhecer o caminho correto
os.environ["PYSPARK_SUBMIT_ARGS"] = '--conf "spark.driver.extraJavaOptions=-Dhadoop.home.dir=C:/hadoop" pyspark-shell'

builder = SparkSession.builder.appName("ClickstreamProcessor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")


spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"]).getOrCreate()

schema = StructType([
    StructField("user_id", StringType()),
    StructField("page", StringType()),
    StructField("refereer", StringType()),
    StructField("device", StringType()),
    StructField("cart", StringType()),
    StructField("location", StringType()),
    StructField("load_time_ms", IntegerType()),
    StructField("timestamp", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web_clicks") \
    .option("startingOffsets", "latest") \
    .load()

# esse df ta desserializado
processed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# salvamos no delta lake
query = processed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoint") \
    .start("./data/clickstream_table")

query.awaitTermination()