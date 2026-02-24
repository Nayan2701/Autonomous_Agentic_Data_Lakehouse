import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

print("--- STARTING INFRASTRUCTURE CHECK ---")

# 1. PRE-FLIGHT CHECK: Ensure MinIO Bucket Exists
def ensure_bucket_exists(bucket_name):
    s3 = boto3.client('s3',
                      endpoint_url='http://minio:9000',
                      aws_access_key_id='admin',
                      aws_secret_access_key='password')
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists.")
    except:
        print(f"⚠️ Bucket '{bucket_name}' missing. Creating it now...")
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' created successfully.")

ensure_bucket_exists("warehouse")

print("--- STARTING SPARK SESSION ---")

# 2. Initialize Spark (GLOBAL VARIABLE)
spark = SparkSession.builder \
    .appName("Lakehouse-Test") \
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

print("Spark Session Created Successfully!")

# 3. Define a simple schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("data", StringType(), False)
])

# 4. Create a DataFrame
data = [(1, "Hello Nessie"), (2, "Iceberg is cool")]
df = spark.createDataFrame(data, schema)

# 5. Write data to the Lakehouse
print("Writing data to Nessie/Iceberg...")
df.writeTo("nessie.demo_table").createOrReplace()

# 6. Read it back to verify
print("Reading data back...")
spark.sql("SELECT * FROM nessie.demo_table").show()

print("Test Passed: Lakehouse is operational.")