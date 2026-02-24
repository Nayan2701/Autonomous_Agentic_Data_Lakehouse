from pyspark.sql import SparkSession
import boto3

print("TRAFFIC DATA INGESTION...")

spark = SparkSession.builder \
    .appName("Traffic-Data-Ingestion") \
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

print("GENERATING 10,000 TRAFFIC VIOLATION RECORDS...")
df = spark.range(0,10000).selectExpr(
    "id as ticket_id",
    "cast(from_unixtime(unix_timestamp('2025-01-01 00:00:00') + cast(rand() * 31536000 as int)) as timestamp) as violation_date",
    "element_at(array('Speeding', 'Red Light', 'Illegal Parking', 'Stop Sign', 'Expired Registration'), cast(rand() * 5 + 1 as int)) as violation_type",
    "element_at(array('Main St', 'Broadway', 'Elm St', 'Maple Ave', 'Washington Blvd'), cast(rand() * 5 + 1 as int)) as location",
    "cast(rand() * 250 + 50 as int) as fine_amount",
    "rand() > 0.7 as radar_captured"
)

print("WRITTING DATA TO ICEBERG...")

spark.sql("DROP TABLE IF EXISTS nessie.traffic_violations")

df.writeTo("nessie.traffic_violations").create()

print("\n SAMPLE TRAFFIC DATA")
spark.sql("SELECT * FROM nessie.traffic_violations ORDER BY ticket_id LIMIT 5").show()
print("Total fines by violation type:")
spark.sql("""
    SELECT violation_type, COUNT(*) as ticket_amount, SUM(fine_amount) as total_revenue 
    FROM nessie.traffic_violations 
    GROUP BY violation_type 
    ORDER BY total_revenue DESC
""").show()
print("Ingestion Complete, The Lakehouse is primed.")
