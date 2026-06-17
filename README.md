# 📊 End-to-End Data Lake Pipeline (CDC + Kafka + Spark + MinIO)

<img width="1045" height="626" alt="Screenshot 2026-06-15 184255" src="https://github.com/user-attachments/assets/6ad27b4f-c9b9-44fd-b811-afc0a4186d2f" />

## 📌 Project Overview

This project implements a real-time, end-to-end Data Engineering pipeline that simulates an e-commerce order analytics system.

It captures transactional data from PostgreSQL, streams it using Debezium CDC + Kafka, stores raw events in MinIO (Data Lake), processes data using Apache Spark, generates curated and analytics datasets, and then stores them back to MinIO (Data Lake).

# 🏗️ Architecture Diagram

# 🔄 Data Flow

Python Data Generator  
&emsp;&emsp;&emsp;&emsp;│  
&emsp;&emsp;&emsp;&emsp;▼  
PostgreSQL (orders_db)  
&emsp;&emsp;&emsp;&emsp;│  
&emsp;&emsp;&emsp;&emsp;▼  
Debezium CDC (Change Data Capture)  
&emsp;&emsp;&emsp;&emsp;│  
&emsp;&emsp;&emsp;&emsp;▼  
&emsp;&emsp;&emsp;Kafka  
&emsp;&emsp;&emsp;&emsp;│  
&emsp;&emsp;&emsp;&emsp;▼  
Kafka Connect Sink  
&emsp;&emsp;&emsp;&emsp;│  
&emsp;&emsp;&emsp;&emsp;▼  
MinIO (Raw Data-Lake Zone)  
&emsp;&emsp;&emsp;&emsp;│  
&emsp;&emsp;&emsp;&emsp;▼  
Apache Spark (ETL Processing)  
&emsp;&emsp;&emsp;&emsp;│  
&emsp;&emsp;&emsp;&emsp;▼  
MinIO (Curated Zone(Parquet))  
&emsp;&emsp;&emsp;&emsp;│  
&emsp;&emsp;&emsp;&emsp;▼  
Analytics Layer (Revenue, Customers)  


## ⚙️ Setup Instructions
# 1️⃣ Start Infrastructure (Docker):
        docker-compose up -d
<img width="814" height="156" alt="image" src="https://github.com/user-attachments/assets/a3409f90-e2ec-4f82-9b7a-05bea23ceced" />

# 2️⃣ Verify Services:
        docker ps
  
  #  Services that are running:
        PostgreSQL
        Kafka + Zookeeper
        Debezium
        Kafka Connect
        MinIO
        Spark
<img width="693" height="178" alt="image" src="https://github.com/user-attachments/assets/2df91699-27ed-4459-ab09-c9c7ac596f69" />

# 3️⃣ Verify Database & Table:
        docker exec -it postgres psql -U admin -d orders_db

        \dt
<img width="443" height="271" alt="image" src="https://github.com/user-attachments/assets/71b19b9d-abbc-4c46-bfc6-c54dc4a97564" />

# 4️⃣ Generate Data:
        py data_generator\generate_orders.py
<img width="513" height="160" alt="image" src="https://github.com/user-attachments/assets/6479049f-9b20-4452-8301-a49c7892c2c1" />
                        
        SELECT * FROM orders order by order_id desc limit 5;
<img width="451" height="168" alt="image" src="https://github.com/user-attachments/assets/6ef47d7e-7755-4747-869b-c5116f41e8b8" />

# 5️⃣ Start and Verify CDC Pipeline (Debezium → Kafka):

    curl.exe http://localhost:8084
    curl.exe http://localhost:8084/connectors
    
    curl -X POST http://localhost:8084/connectors `
    -H "Content-Type: application/json" `
    --data @connectors/postgres_source.json
    
    curl http://localhost:8084/connectors
<img width="708" height="178" alt="image" src="https://github.com/user-attachments/assets/ef859b05-70d8-4e7f-8bda-93b6499c7405" />

6️⃣ Verify Kafka Streaming:

        docker exec -it kafka bash
        
        kafka-console-consumer `
        --topic orders_db.public.orders `
        --from-beginning `
        --bootstrap-server localhost:9092

#  7️⃣ Configure MinIO (Raw Zone):

    docker exec -it minio mc alias set local http://minio:9000 minioadmin minioadmin
    >> docker exec -it minio mc mb local/datalake
<img width="712" height="107" alt="image" src="https://github.com/user-attachments/assets/35cfd068-8fa5-49b1-92cc-bede49939e64" />

# 8️⃣ Kafka → MinIO Sink Connector:

    curl.exe -X POST http://localhost:8083/connectors `
    -H "Content-Type: application/json" `
    -d @connectors/minio_sink.json

    curl.exe http://localhost:8083/connectors/minio-sink/status
<img width="836" height="198" alt="image" src="https://github.com/user-attachments/assets/f85ebb1f-963c-461d-9fe2-8f8d151751fc" />

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
<img width="665" height="368" alt="image" src="https://github.com/user-attachments/assets/6837c952-081e-4415-adc9-a41ab203be6c" />
<img width="556" height="382" alt="image" src="https://github.com/user-attachments/assets/f253a6fc-4a76-447a-bc2b-0d24da596c25" />

# 🔟 Output Validation:
   #   📦 Raw Zone (MinIO):
              raw/
                orders/
                    year=2026/
                        month=06/
                            day=XX/
                                *.json

#        📦 Curated Zone (Parquet):
                curated/
                    orders/
                        year=2026/
                            month=6/
                                    *.parquet

 #       📦 Analytics Layer:
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
5. Data generation and insertion  
   Issue: Duplicate error message for order_id when inserted in PostgreSQL  
   Solution: used the Serial datatype in PostgreSQL   


## 🚀 How to Run the Entire Project (Quick Start)
    Start Docker:
    docker-compose up -d 
    
    Check service running status:
    docker ps
    
    Configure Debezium connector:
    curl -X POST http://localhost:8083/connectors `
    -H "Content-Type: application/json" `
    --data @connectors/postgres_source.json
    
    Generate Data:
    py data_generator/generate_orders.py
    
    Create a data lake in MinIO:
    docker exec -it minio mc alias set local http://minio:9000 minioadmin minioadmin
    >> docker exec -it minio mc mb local/datalake

    Configure Kafka connector:
    curl.exe -X POST http://localhost:8083/connectors `
    -H "Content-Type: application/json" `
    -d @connectors/minio_sink.json



## 🧠 Final Outcome

This project demonstrates a complete modern data engineering pipeline:

👉 OLTP Database → Real-time Streaming → Data Lake → ETL → Analytics

# 📎 Author
Rahul Yadav

## ✅ Done

