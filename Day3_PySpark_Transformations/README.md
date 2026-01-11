# Day 3 (11/01/26) – PySpark Transformations Deep Dive

## Overview
On Day 3, I worked on deeper PySpark transformations in Databricks using the Amazon Products dataset. The focus was on understanding how Spark handles large-scale data processing and applying advanced transformation techniques used in real-world data engineering.

---

## Topics Learned
### 1) PySpark vs Pandas
- Pandas works on a single machine and is best for small datasets.
- PySpark works in a distributed environment (cluster computing) and is suitable for big data.
- Spark uses lazy execution, which means operations run only when an action is triggered.

### 2) Joins
I learned and implemented different join types:
- Inner Join (only matching rows)
- Left Join (keeps all rows from left side)
- Right Join (keeps all rows from right side)
- Outer Join (keeps all rows from both sides)

I also learned why joins are important in combining facts + dimension tables in real ETL pipelines.

### 3) Window Functions
I practiced window functions to perform advanced analytics without losing row-level details:
- Ranking products within a brand
- Selecting top products per group
- Running totals / cumulative calculations

Window functions helped me understand how to calculate group-based metrics efficiently.

### 4) User Defined Functions (UDF)
I learned how to create UDFs for custom logic when built-in Spark functions are not enough.  
I used a UDF to create a simplified category column from the raw category text.

### 5) Derived Features (Feature Engineering)
I created derived columns useful for analytics and ML readiness, such as:
- Discount percentage
- Price bucket (low / mid / high)
- Review level (new / popular / viral)
- Main category extracted from category text

---

## Dataset Used
- Amazon Products Dataset
- Loaded in Databricks as a table: `workspace.default.amazon_products`

---

## Challenges Faced & Fix
During this task, I faced a casting issue where numeric columns contained malformed string values (example: values stored with quotes).  
I learned how to clean and safely convert data types in PySpark, ensuring the pipeline doesn’t fail.

---

## Output
Final processed dataset with new derived features was saved as:
- `workspace.default.day3_amazon_features`

---

## Summary
Day 3 helped me build strong fundamentals in PySpark transformations with hands-on practice on joins, window functions, UDFs, and feature engineering, which are essential skills for data engineering and analytics workflows.


