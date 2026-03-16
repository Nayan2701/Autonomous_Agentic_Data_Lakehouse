import os
import json
import urllib.request
from pyspark.sql import SparkSession

def ask_llm(prompt):
    
    url = "http://host.docker.internal:11434/api/generate"
    payload = {
        "model": "llama3.1",
        "prompt": prompt,
        "stream": False
    }
    
    req = urllib.request.Request(
        url, 
        data=json.dumps(payload).encode("utf-8"), 
        headers={"Content-Type": "application/json"}
    )
    
    try:
        response = urllib.request.urlopen(req)
        result = json.loads(response.read().decode("utf-8"))
        return result["response"].strip()
    except Exception as e:
        return f"Error communicating with Local LLM: {e}"

print("--- INITIALIZING SPARK ENGINE ---")
print("Please wait while the Lakehouse boots up...")

spark = SparkSession.builder \
    .appName("Agentic-Lakehouse-Local") \
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

print("\n" + "="*50)
print(" 🚀 LIMITLESS LOCAL LAKEHOUSE TERMINAL ONLINE")
print(" Type 'exit' to close the session.")
print("="*50 + "\n")

while True:
    question = input("\nUSER 👤: ")
    
    if question.lower() in ['quit', 'exit', 'q']:
        print("Shutting down the agent. Goodbye!")
        break
        
    if not question.strip():
        continue

    print("🤖 AGENT THOUGHT: Translating to SQL and planning actions...")
    
    sql_prompt = f"""
    You are an autonomous Data Engineering AI. 
    You manage an Apache Iceberg table named `nessie.autonomous_telemetry`.
    Columns: vehicle_id (STRING), timestamp (TIMESTAMP), speed_kmh (DOUBLE), battery_kwh (DOUBLE), lidar_anomaly_flag (BOOLEAN), cooperative_platoon_id (STRING).

    CRITICAL SYNTAX RULES:
    1. BRANCHING commands MUST use ONLY 'nessie' as the catalog.
       Correct: CREATE BRANCH IF NOT EXISTS experiment IN nessie FROM main
       Correct: USE REFERENCE experiment IN nessie
    
    2. DATA commands (SELECT, DELETE, UPDATE) MUST use the full table name 'nessie.autonomous_telemetry'.
       Correct: SELECT COUNT(*) FROM nessie.autonomous_telemetry
       
    3. COMPACTION / OPTIMIZATION commands MUST use the Iceberg system procedure exactly like this:
       Correct: CALL nessie.system.rewrite_data_files('nessie.autonomous_telemetry')

    The user command is: "{question}"
    Write the Spark SQL queries to execute this. Separate multiple commands with a semicolon (;).
    IMPORTANT: Return ONLY the raw SQL. Do not include markdown formatting or explanations.
    """
    
    sql_query = ask_llm(sql_prompt)
    
    sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
    sql_query = sql_query.replace('\n', ' ')
    
    sql_query = sql_query.replace("IN nessie.autonomous_telemetry", "IN nessie FROM main")
    sql_query = sql_query.replace("ALTER TABLE nessie.autonomous_telemetry", "")
    
    sql_query = sql_query.replace("SHOW PARTITIONS 'nessie.autonomous_telemetry'", "SHOW PARTITIONS nessie.autonomous_telemetry")
    sql_query = sql_query.replace('SHOW PARTITIONS "nessie.autonomous_telemetry"', "SHOW PARTITIONS nessie.autonomous_telemetry")
    
    clean_queries = []
    for q in sql_query.split(';'):
        q = q.strip()
        if not q: 
            continue
            
        if "nessie.system." in q.lower() and "rewrite_data_files" not in q.lower():
            print(f"🛡️ INTERCEPTOR BLOCKED HALLUCINATION: {q}")
            continue
            
        clean_queries.append(q)
        
    if not clean_queries:
        print("❌ AGENT FAILED: No valid queries to execute.")
        continue

    queries = clean_queries
    
    try:
        results_str = ""
        for q in queries:
            print(f"🤖 AGENT ACTION: Executing -> {q}")
            df = spark.sql(q)
            
            if df is not None and len(df.columns) > 0:
                results_str += df._jdf.showString(10, 20, False) + "\n"
            else:
                results_str += f"[Command executed successfully]\n"
        
        print("\nDATABASE RESULTS:")
        print(results_str)
        
        print("🤖 AGENT THOUGHT: Summarizing outcome...")
        
        summary_prompt = f"""
        The user asked: "{question}"
        I executed these SQL commands: {clean_queries}
        The database returned: {results_str}
        Provide a brief, natural language summary of what was accomplished.
        """
        
        final_answer = ask_llm(summary_prompt)
        
        print("\n" + "="*50)
        print(" FINAL ANSWER")
        print("="*50)
        print(final_answer)

    except Exception as e:
        print(f"\n⚠️ SPARK ERROR: Failed to execute the SQL pipeline.")
        print(f"Details: {str(e)}")