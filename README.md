# Uber-Project
Uber Project in Azure
End-to-end Azure + Databricks project that captures **real-time ride events** from a web app via **Azure Event Hubs** and combines them with **batch reference / bulk ride data** ingested from **GitHub APIs** using **Azure Data Factory (ADF)**. The pipeline outputs both:
1) a **business-ready Silver “One Big Table” (OBT)** for direct analytics, and  
2) a **Gold Star Schema** (dimensions + fact) for scalable BI and semantic modeling.

## Project Architecture
<img width="1161" height="625" alt="image" src="https://github.com/user-attachments/assets/0afd5906-3426-4f9e-a80b-78faa5a22eec" />

### High-level Flow
- **Streaming path:** Web app → Event Hubs → Databricks (Kafka stream) → `rides_raw` → `stg_rides` → `silver_obt`
- **Batch path:** GitHub API → ADF pipeline (`API_to_ADLS`) → ADLS → Databricks → `stg_rides` → `silver_obt`
- **Modeling path:** `silver_obt` → Gold star schema (dims + fact) with DLT CDC/SCD

## ADF pipeline for data extraction
<img width="1904" height="902" alt="image" src="https://github.com/user-attachments/assets/1bd15c5b-a20f-4da9-9b6f-64faa04483b6" />

1) Batch ingestion (GitHub/API → ADLS via ADF)
- ADF pipeline API_to_ADLS reads a list of JSON files (Lookup), loops through each item (ForEach), and copies each GitHub JSON into ADLS as raw landing data.


## Databricks pipeline for Processing and Modeling layer
<img width="2242" height="1550" alt="image" src="https://github.com/user-attachments/assets/76a4ba7a-d1e9-45a2-925b-c9ddf8151d28" />


<img width="2288" height="660" alt="image" src="https://github.com/user-attachments/assets/fe92bfa5-363e-4315-bc75-d01703be44ae" />

2) Real-time ingestion (Event Hubs → Databricks)
- Databricks reads Event Hubs through Kafka options and creates a streaming table rides_raw (DLT).

3) Converged Silver staging table (Batch + Stream)
A unified streaming staging table stg_rides appends:
- Bulk/batch rides data
- Real-time rides events (JSON payload parsed using schema)

4) Silver OBT (Business-ready analytics table)
silver_obt is built as a streaming table with:
- Enrichment joins to mapping tables (vehicle types, cities, statuses, payment methods, cancellation reasons)
- A watermark on booking time with late-arrival handling

5) Gold Layer (Star Schema + CDC/SCD)
From silver_obt, the project builds:
- Dimensions: passenger, driver, vehicle, payments, booking, location
- Fact: ride-level fact table
CDC/SCD is handled using DLT create_auto_cdc_flow:
- SCD Type 1 for most dims + fact
- SCD Type 2 for location changes over time
