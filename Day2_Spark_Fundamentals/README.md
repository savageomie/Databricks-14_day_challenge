# Day 2 – Apache Spark Fundamentals (Databricks)

## ✅ Topics Covered
- Spark Architecture: Driver, Executors, DAG
- DataFrames vs RDDs
- Lazy Evaluation (Transformations vs Actions)
- Databricks Notebook Magic Commands:
  - `%sql`
  - `%python`
  - `%fs`

---

## 🛠 Dataset Used
Sample e-commerce dataset (CSV uploaded as Databricks table)

**Columns:**
- order_date
- country
- order_id
- product
- qty
- price

---

## ✅ Tasks Implemented

### 1) Loaded CSV Table into DataFrame
- Read data using `spark.table()`
- Verified schema using `printSchema()` and `show()`

### 2) DataFrame Operations
Performed:
- `select()` → selected required columns
- `filter()` → filtered expensive orders
- `groupBy()` → grouped by country and product
- `orderBy()` → sorted results by revenue / count

### 3) Revenue Calculation
Created new column:

`total_amount = qty * price`

### 4) SQL Queries using `%sql`
- Created a temporary view for SQL querying
- Ran queries to get:
  - orders per country
  - revenue per product

### 5) Export Results
Due to restricted DBFS access in this environment, results were exported as a managed table:

`default.day2_top_products`

Verified with:
```sql
SELECT * FROM default.day2_top_products;
