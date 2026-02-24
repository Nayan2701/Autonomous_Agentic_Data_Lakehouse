import boto3
from pyspark.sql import SparkSession
import time

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

# Initialize Spark
spark = SparkSession.builder \
    .appName("Nessie-Branching-Demo") \
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

def print_header(msg):
    print("\n" + "="*50)
    print(f" {msg}")
    print("="*50)

# ---------------------------------------------------------
# SCENARIO: PRODUCTION HAS VALUABLE DATA
# ---------------------------------------------------------
print_header("1. CHECKING PRODUCTION (Main)")
spark.sql("CREATE TABLE IF NOT EXISTS nessie.salaries (name STRING, salary INT) USING iceberg")
spark.sql("INSERT INTO nessie.salaries VALUES ('Alice', 100000), ('Bob', 120000)")

print("Current Production Data:")
spark.sql("SELECT * FROM nessie.salaries").show()

# ---------------------------------------------------------
# SCENARIO: DEV WANTS TO EXPERIMENT
# ---------------------------------------------------------
print_header("2. CREATING BRANCH 'experiment'")
spark.sql("CREATE BRANCH IF NOT EXISTS experiment IN nessie FROM main")

# Switch our context to the experiment branch
spark.sql("USE REFERENCE experiment IN nessie")
print("Switched context to branch: experiment")

# ---------------------------------------------------------
# SCENARIO: DEV MAKES A MISTAKE
# ---------------------------------------------------------
print_header("3. SIMULATING MISTAKE IN 'experiment'")
spark.sql("DELETE FROM nessie.salaries WHERE name = 'Alice'")
spark.sql("INSERT INTO nessie.salaries VALUES ('Charlie', 500)")

print("Data in 'experiment' branch (Alice is gone!):")
spark.sql("SELECT * FROM nessie.salaries").show()

# ---------------------------------------------------------
# SCENARIO: BOSS CHECKS PRODUCTION
# ---------------------------------------------------------
print_header("4. CHECKING PRODUCTION (Main) AGAIN")
spark.sql("USE REFERENCE main IN nessie")

print("Data in 'main' branch (Alice should still be here):")
spark.sql("SELECT * FROM nessie.salaries").show()

# ---------------------------------------------------------
# SCENARIO: CLEANUP
# ---------------------------------------------------------
print_header("5. CLEANUP")
print("The experiment was a failure. Dropping the branch safely.")
spark.sql("DROP BRANCH experiment IN nessie")
print("Branch dropped. Production was never touched.")