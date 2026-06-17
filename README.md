<img width="1045" height="626" alt="Screenshot 2026-06-15 184255" src="https://github.com/user-attachments/assets/6ad27b4f-c9b9-44fd-b811-afc0a4186d2f" />

# 📊 End-to-End Data Lake Pipeline (CDC + Kafka + Spark + MinIO)
## 📌 Project Overview

This project implements a real-time end-to-end Data Engineering pipeline simulating an e-commerce order analytics system.

It captures transactional data from PostgreSQL, streams it using Debezium CDC + Kafka, stores raw events in MinIO (Data Lake), processes data using Apache Spark, generates curated and analytics datasets, and then store is back to MinIO(Data Lake).

# 🏗️ Architecture Diagram

# 🔄 Data Flow

Python Data Generator
        │
        ▼
PostgreSQL (orders_db)
        │
        ▼
Debezium CDC (Change Data Capture)
        │
        ▼
Apache Kafka
        │
        ▼
Kafka Connect Sink
        │
        ▼
MinIO (Raw Data Lake Zone)
        │
        ▼
Apache Spark (ETL Processing)
        │
        ▼
MinIO (Curated Zone - Parquet)
        │
        ▼
Analytics Layer (Revenue, Customers)


## ⚙️ Setup Instructions
# 1️⃣ Start Infrastructure (Docker)
        docker-compose up -d

# 2️⃣ Verify Services
        docker ps
  
  #  Expected services:
        PostgreSQL
        Kafka + Zookeeper
        Debezium
        Kafka Connect
        MinIO

# 3️⃣ Verify Database & Table
        docker exec -it postgres psql -U admin -d orders_db

        \dt

# 4️⃣ Generate Data
        py data_generator\generate_orders.py
    
    Verify:

    SELECT COUNT(*) FROM orders;

# 5️⃣ Start and Verify CDC Pipeline (Debezium → Kafka)

    curl.exe http://localhost:8084
    curl.exe http://localhost:8084/connectors
    
    curl -X POST http://localhost:8083/connectors `
    -H "Content-Type: application/json" `
    --data @connectors/postgres_source.json
    
    curl http://localhost:8083/connectors

6️⃣ Verify Kafka Streaming:

        docker exec -it kafka bash
        kafka-console-consumer `
        --topic orders_db.public.orders `
        --from-beginning `
        --bootstrap-server localhost:9092

#  7️⃣ Configure MinIO (Raw Zone)

    docker exec -it minio mc alias set local http://minio:9000 minioadmin minioadmin
    >> docker exec -it minio mc mb local/datalake

# 8️⃣ Kafka → MinIO Sink Connector

    curl.exe -X POST http://localhost:8083/connectors `
    -H "Content-Type: application/json" `
    -d @connectors/minio_sink.json

    curl.exe http://localhost:8083/connectors/minio-sink/status

Structure of MinIO(Data Lake):
           
            raw/
                orders/
                    year=2026/
                        month=06/
                            day=12/
                                *.json

# 9️⃣ Run Spark ETL Job
    Enter the Spark Bash: 
        docker exec -it spark bash
        In the Spark app, run: 
            python /app/spark/transform_orders.py

# 🔟 Output Validation:
   #   📦 Raw Zone (MinIO)
              raw/
                orders/
                    year=2026/
                        month=06/
                            day=XX/
                                *.json

#        📦 Curated Zone (Parquet)
                curated/
                    orders/
                        year=2026/
                            month=6

 #       📦 Analytics Layer
                analytics/
                    revenue_by_product

                analytics/
                    top_customers    

## 📊 Features Implemented

✔ Real-time CDC pipeline (Debezium)
✔ Kafka streaming architecture
✔ Data Lake storage using MinIO
✔ Spark ETL transformations
✔ Partitioned Parquet storage
✔ Analytics layer generation

## ⚠️ Challenges Faced
    1. Kafka + Debezium Connector Configuration
        Issue: Incorrect topic mapping and schema mismatch
            Solution: Fixed PostgreSQL table mapping in the connector JSON
    2. MinIO Sink Integration
        Issue: Kafka Connect failed to write structured CDC events
            Solution: Corrected bucket path structure and serialization format
    3. Spark JSON Parsing Complexity
        Issue: Nested CDC JSON structure
            Solution: Flattened schema using Spark DataFrame transformations
    4. Data Consistency Across Pipeline
        Issue: Duplicate / delayed Kafka messages
            Solution: Used offset tracking and idempotent Spark processing logic


## 🚀 How to Run the Entire Project (Quick Start)
    docker-compose up -d 
    docker ps
    python data_generator/generate_orders.py
    

## 🧠 Final Outcome

This project demonstrates a complete modern data engineering pipeline:

👉 OLTP Database → Real-time Streaming → Data Lake → ETL → Analytics

# 📎 Author
Rahul Yadav

## ✅ Done

