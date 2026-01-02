from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, hour, minute, second, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta import configure_spark_with_delta_pip
import os
import shutil
# --- CONFIGURAÇÃO OBRIGATÓRIA PARA WINDOWS (NÃO REMOVA É TERRIVEL) ---
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["hadoop.home.dir"] = "C:/hadoop"
# Força o Java a reconhecer o caminho correto
os.environ["PYSPARK_SUBMIT_ARGS"] = '--conf "spark.driver.extraJavaOptions=-Dhadoop.home.dir=C:/hadoop" pyspark-shell'

#
# Garante pasta temp para o Snappy
#if not os.path.exists("C:/tmp"):
#    os.makedirs("C:/tmp")
base_dir = os.path.dirname(os.path.abspath(__file__))
checkpoint_path = os.path.join(base_dir, "checkpoint_delta").replace("\\", "/")
output_path = os.path.join(base_dir, "data", "clickstream_gold").replace("\\", "/")

builder = SparkSession.builder.appName("ClickstreamEnriched") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "2")  # Otimização para rodar local (padrão é 200)

print(f"--- Output Delta Lake: {output_path} ---")
spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"]).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

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


# ==============================================================================
# 4. LEITURA (BRONZE LAYER)
# ==============================================================================

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web_clicks") \
    .option("startingOffsets", "latest") \
    .load()

enriched_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp")) \
    .withWatermark("event_time", "10 minutes") \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("hour_extract", hour(col("event_time"))) \
    .withColumn("minute_extract", minute(col("event_time"))) \
    .withColumn("second_extract", second(col("event_time"))) \
    .withColumn("is_mobile", when(col("device").isin("Smartphone", "Tablet"), 1).otherwise(0)) \
    .withColumn("is_conversion", when(col("page") == "/checkout", 1).otherwise(0)) \
    .withColumn("status_performance", 
        when(col("load_time_ms") < 500, "Excellent")
        .when((col("load_time_ms") >= 500) & (col("load_time_ms") < 1000), "Good")
        .when((col("load_time_ms") >= 1000) & (col("load_time_ms") < 2000), "Slow")
        .otherwise("Critical")
    )

query = enriched_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='5 seconds') \
    .start(output_path)

print("Streaming Iniciado. Processando dados.")
query.awaitTermination()