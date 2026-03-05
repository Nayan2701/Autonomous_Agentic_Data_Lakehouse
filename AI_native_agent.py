import os
import json
import urllib.request
import urllib.error
from pyspark.sql import SparkSession

# NO MORE API KEYS. WE ARE RUNNING LOCAL!

def ask_llm(prompt):
    # host.docker.internal allows the Docker container to talk to Ollama running on your Mac
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
        return f"Error communicating with Local LLM: {e}\n(Did you leave the Ollama app running on your Mac?)"

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
    You manage an Apache Iceberg table named `nessie.traffic_violations`.

    CRITICAL SYNTAX RULES:
    1. BRANCHING commands MUST use ONLY 'nessie' as the catalog.
       Correct: CREATE BRANCH IF NOT EXISTS experiment IN nessie FROM main
       Correct: USE REFERENCE experiment IN nessie
    
    2. DATA commands (SELECT, DELETE, UPDATE) MUST use the full table name 'nessie.traffic_violations'.
       Correct: DELETE FROM nessie.traffic_violations WHERE fine_amount < 50
       Correct: SELECT COUNT(*) FROM nessie.traffic_violations

    The user command is: "{question}"
    Write the Spark SQL queries to execute this. Separate multiple commands with a semicolon (;).
    IMPORTANT: Return ONLY the raw SQL. Do not include markdown formatting or explanations.
    """
    
    sql_query = ask_llm(sql_prompt)
    
    sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
    
    # Python Interceptors to catch Llama's literal hallucinations
    sql_query = sql_query.replace("IN nessie.traffic_violations", "IN nessie FROM main")
    sql_query = sql_query.replace("ALTER TABLE nessie.traffic_violations", "")
    sql_query = sql_query.replace("DELETE FROM nessie WHERE", "DELETE FROM nessie.traffic_violations WHERE")
    sql_query = sql_query.replace("SELECT COUNT(*) FROM nessie;", "SELECT COUNT(*) FROM nessie.traffic_violations;")
    
    if "Error" in sql_query:
        print(f"❌ AGENT FAILED: \n{sql_query}")
        continue

    queries = [q.strip() for q in sql_query.split(';') if q.strip()]
    
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
        I executed these SQL commands: {sql_query}
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