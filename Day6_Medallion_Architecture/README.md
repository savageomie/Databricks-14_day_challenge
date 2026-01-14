## ✅ DAY 6 (14/01/26) — Medallion Architecture (Bronze → Silver → Gold)

### 📌 Overview
On **Day 6**, I implemented the **Medallion Architecture** in Databricks using **Delta Lake**.  
The goal of this architecture is to build a clean and scalable data pipeline by dividing the data flow into three layers:

- **Bronze Layer** → Raw ingested data (as-is)
- **Silver Layer** → Cleaned and validated data
- **Gold Layer** → Aggregated business-ready data for analytics/reporting

---

## 🎯 What I Learned

### ✅ Medallion Architecture Concept
I learned how Medallion Architecture helps in:
- improving data quality
- maintaining traceability (raw → cleaned → analytics)
- creating reusable datasets for multiple analytics use cases
- simplifying debugging and maintenance

### ✅ Bronze Layer (Raw Ingestion)
Bronze layer stores the **raw dataset without transformations**.  
Only ingestion metadata is added to track data properly.

Best practices followed:
- store raw form of dataset
- add ingestion timestamp (`ingestion_ts`)
- avoid cleaning at this stage

### ✅ Silver Layer (Cleaning & Validation)
Silver layer contains **cleaned and validated dataset** after applying transformations and data quality rules.

Cleaning & validation tasks performed:
- cleaned price columns (removed currency symbols and unwanted characters)
- handled empty and malformed values safely
- created numeric price columns (`final_price_num`, `initial_price_num`)
- removed invalid prices (NULL or <= 0)
- removed duplicate records
- generated `price_tier` column (`budget`, `mid`, `premium`)

### ✅ Gold Layer (Business Aggregates)
Gold layer contains **aggregated business insights**, directly usable for dashboards and reporting.

Gold outputs generated:
- product performance aggregation (grouped by category/brand depending on schema)
- total product counts
- average, minimum, maximum price analysis
- tier wise distribution of products

---

## 🛠️ Implementation Summary

### ✅ Bronze Table Created
- `workspace.default.bronze_amazon_products`  
Raw ingested dataset + ingestion timestamp.

### ✅ Silver Table Created
- `workspace.default.silver_amazon_products`  
Cleaned dataset with numeric prices + validation + tier classification.

### ✅ Gold Table Created
- `workspace.default.gold_product_performance`  
Business-ready aggregated dataset for analytics.

---

## ✅ Output Tables Created

| Layer  | Table Name |
|--------|------------|
| Bronze | `workspace.default.bronze_amazon_products` |
| Silver | `workspace.default.silver_amazon_products` |
| Gold   | `workspace.default.gold_product_performance` |

---

## 📌 Key Takeaways
- Bronze preserves raw data for audit and replay
- Silver ensures clean, validated, structured data
- Gold provides analytics-ready business insights
- Medallion Architecture makes pipelines scalable and maintainable using Delta Lake

