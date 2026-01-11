# Day 3 (11/01/26) – PySpark Transformations Deep Dive 🔥

## ✅ What I Learned Today
- Difference between **PySpark vs Pandas**
- Performed **joins** (inner, left, outer)
- Used **window functions** for ranking and running totals
- Created a **UDF (User Defined Function)**
- Built **derived features** for analytics/ML use

---

## 📂 Dataset Used
Amazon Products Dataset loaded as Databricks table:

- `workspace.default.amazon_products`

```python
df = spark.table("workspace.default.amazon_products")
display(df.limit(5))
print("Rows:", df.count())


🧹 Data Cleaning (Casting Fix)

Some numeric columns had malformed values like "57.79" so normal casting failed.
I fixed it using regexp_replace + try_cast.

from pyspark.sql import functions as F

products = df \
    .withColumn("final_price_clean", F.regexp_replace(F.col("final_price"), '"', '')) \
    .withColumn("initial_price_clean", F.regexp_replace(F.col("initial_price"), '"', '')) \
    .withColumn("final_price_num", F.expr("try_cast(final_price_clean as double)")) \
    .withColumn("initial_price_num", F.expr("try_cast(initial_price_clean as double)")) \
    .withColumn("reviews_count_num", F.expr("try_cast(reviews_count as int)")) \
    .withColumn("rating_num", F.expr("try_cast(rating as double)"))


