import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

print(" INITIALIZING SPARK STREAMING SESSION....")

spark = SparkSession.builder \
    .appName("Telemetry-Streaming-Consumer") \
    .config("spark.jars.ivy", "/tmp/.ivy") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Creating Iceberg table for telemetry data....")
spark.sql(""" 
CREATE TABLE IF NOT EXISTS nessie.autonomous_telemetry(
          vehicle_id STRING,
          timestamp TIMESTAMP,
          speed_kmh DOUBLE,
          battery_kwh DOUBLE,
          lidar_anomaly_flag BOOLEAN,
          cooperative_platoon_id STRING
          )
          USING iceberg
          PARTITIONED BY(cooperative_platoon_id)
          """)

schema = StructType([
    StructField("vehicle_id", StringType(),True),
    StructField("timestamp",TimestampType(),True),
    StructField("speed_kmh",DoubleType(),True),
    StructField("battery_kwh",DoubleType(),True),
    StructField("lidar_anomaly_flag",BooleanType(), True),
    StructField("cooperative_platoon_id", StringType(),True)
])

LANDING_ZONE = "/home/spark/landing_zone"
CHECKPOINT_DIR = "/home/spark/checkpoints/telemetry"

print("\n STARTING STRUCTURED STREAM")
print("Watching landing zone for new vehicle telemetry...")

streaming_df = spark.readStream\
    .schema(schema)\
    .json(LANDING_ZONE)

query= streaming_df.writeStream\
    .format("iceberg")\
    .outputMode("append")\
    .trigger(processingTime="10 seconds")\
    .option("checkpointLocation",CHECKPOINT_DIR)\
    .toTable("nessie.autonomous_telemetry")

query.awaitTermination()