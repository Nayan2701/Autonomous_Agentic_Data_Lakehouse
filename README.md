# Autonomous Agentic Data Lakehouse: Real-Time V2X Telemetry

An end-to-end, real-time streaming Data Lakehouse built to ingest, manage, and analyze Cooperative Sensing (V2X) telemetry from a simulated fleet of autonomous vehicles. 

This project integrates a local Large Language Model (Llama 3.1) directly into the database infrastructure to act as an autonomous Database Administrator, capable of translating natural language into complex Spark SQL, executing queries, and performing critical database maintenance on live streaming data.

## 🏗️ Architecture & Tech Stack

* **Streaming Engine:** Apache Spark (Structured Streaming)
* **Table Format:** Apache Iceberg
* **Data Catalog & Versioning:** Project Nessie (Git-for-Data)
* **Storage Layer:** MinIO (S3-Compatible Object Storage)
* **AI Integration:** Local Llama 3.1 (via Ollama) + Python Middleware
* **Telemetry Generator:** Python (Simulated 10-second micro-batch JSON payloads)

## 🚀 Key Engineering Challenges Solved

### 1. Concurrent Streaming & Snapshot Isolation
Streaming continuous 10-second micro-batches of JSON data into a data lake traditionally causes read/write locks. By implementing **Apache Iceberg**, the AI agent can safely query the most recent snapshot of the `main` branch while the Spark streaming engine concurrently builds the next snapshot in the background.

### 2. The "Small Files" Compaction Problem
High-velocity streaming inherently fragments data into thousands of tiny files, destroying query performance. This architecture features custom Python middleware that safely translates natural language commands into Iceberg system procedures, allowing the AI agent to autonomously execute `CALL nessie.system.rewrite_data_files()` to physically merge and optimize the Parquet files on MinIO without interrupting the live stream.

### 3. AI Hallucination Interception
To safely deploy an LLM with direct database execution privileges, a custom Python interceptor was engineered. It utilizes strict syntax whitelisting to block hallucinated system commands (e.g., `optimize_sweep`) and strictly enforces Nessie catalog routing to prevent accidental corruption of the production environment.

## ⚙️ Project Components

1. **`telemetry_streamer.py`**: A standalone producer generating live autonomous vehicle state data (speed, battery drain, LIDAR anomalies).
2. **`telemetry_consumer.py`**: A Spark Structured Streaming application that continuously ingests the landing zone files into the Iceberg table.
3. **`gemini_native_agent.py`**: The interactive AI terminal equipped with Python middleware to safely bridge Ollama and the Spark SQL engine.