from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from delta import *

# Configuração para usar Delta Lake e Kafka
builder = SparkSession.builder.appName("ClickstreamProcessor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Inicializa Spark com pacotes necessários
spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"]).getOrCreate()

# Schema do JSON que vem do Kafka
schema = StructType([
    StructField("user_id", StringType()),
    StructField("page", StringType()),
    StructField("refereer", StringType()),
    StructField("device", StringType()),
    StructField("cart", StringType()),
    StructField("location", StringType()),
    StructField("timestamp", StringType())
])

# 1. Lendo do Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web_clicks") \
    .load()

# 2. Transformando (Deserializando o JSON)
processed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 3. Escrevendo em Delta Lake (Sink)
query = processed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoint") \
    .start("./data/clickstream_table")

query.awaitTermination()