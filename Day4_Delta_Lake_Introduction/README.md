# Day 4 (12/01/26) — Delta Lake Introduction ✅

## 📌 Overview
On Day 4, I learned and implemented **Delta Lake** concepts in Databricks using my Day 3 dataset (`workspace.default.amazon_products`).  
Since my workspace has **Public DBFS root disabled**, I used **Managed Delta Tables** (Unity Catalog/Hive Metastore) instead of saving Delta files to `/delta/...` paths.

---

## ✅ Topics Learned
- What is **Delta Lake**
- **ACID transactions** in Data Lakes
- **Schema enforcement**
- Delta vs Parquet (why Delta is better for reliability)
- Handling duplicates in Delta

---

## 🛠️ Tasks Completed

### ✅ 1) Convert Day 3 Data to Delta Format
Loaded Day 3 dataset:
- `workspace.default.amazon_products`

Converted to Delta (Managed Delta table):
- `workspace.default.amazon_products_day4_delta`

✅ Proof: Table preview + total row count

---

### ✅ 2) Create Delta Tables (PySpark + SQL)

#### PySpark Managed Delta Table
Created using:
- `saveAsTable()`  
Table:
- `workspace.default.amazon_products_day4_delta`

#### SQL Delta Table
Created using SQL:
- `workspace.default.amazon_products_day4_delta_sql`

✅ Proof: `SHOW TABLES` + row count query

---

### ✅ 3) Test Schema Enforcement
Created a wrong schema DataFrame and tried appending into Delta table.  
Delta rejected the write due to schema mismatch.

✅ Proof: Schema enforcement error message output

---

### ✅ 4) Handle Duplicate Inserts
Steps followed:
1. Inserted duplicate data (append same dataset again)
2. Checked duplicates using `groupBy()` + `count`
3. Removed duplicates using `dropDuplicates()`
4. Overwrote Delta table with cleaned data

✅ Proof:
- Duplicates found output
- Duplicates removed output

---

## 🧾 Delta Lake Extra (History)
Used Delta history to verify Delta table versions/logs.

✅ Proof: `DESCRIBE HISTORY` output

---

## 📂 Tables Created
| Table Name | Type | Description |
|-----------|------|-------------|
| `workspace.default.amazon_products_day4_delta` | Delta Managed Table | Main Delta table created from Day 3 dataset |
| `workspace.default.amazon_products_day4_delta_sql` | Delta Table (SQL) | Delta table created using SQL from the managed table |



---

## ✅ Conclusion
Day 4 successfully covered **Delta Lake fundamentals** with hands-on tasks including:
- Delta conversion
- Table creation via PySpark + SQL
- Schema enforcement validation
- Duplicate handling

All tasks completed successfully ✅

