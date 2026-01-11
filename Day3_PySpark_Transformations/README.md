# Day 3 (11/01/26) – PySpark Transformations Deep Dive

## 📌 Topics Covered
- PySpark vs Pandas comparison
- Joins (inner, left, right, outer)
- Window functions (ranking, running totals)
- User-Defined Functions (UDFs)
- Derived Feature Engineering

---

## 📂 Dataset Used
- Amazon Products Dataset (`amazon-products.csv`)
- Loaded as Databricks table:
  - `workspace.default.amazon_products`




🧠 Key Learnings
✅ PySpark vs Pandas
Feature	Pandas	PySpark
Execution	Eager	Lazy
Works on	Single machine	Distributed cluster
Best for	Small datasets	Large datasets

PySpark is designed for scalable big data processing using cluster computing.

🛠️ Data Cleaning (Important Fix)
Some numeric columns contained malformed string values like "57.79", causing cast() to fail.

✅ Solution: remove quotes using regexp_replace and safely convert using try_cast.

python
Copy code
products = df \
    .withColumn("final_price_clean", F.regexp_replace(F.col("final_price"), '"', '')) \
    .withColumn("initial_price_clean", F.regexp_replace(F.col("initial_price"), '"', '')) \
    .withColumn("final_price_num", F.expr("try_cast(final_price_clean as double)")) \
    .withColumn("initial_price_num", F.expr("try_cast(initial_price_clean as double)")) \
    .withColumn("reviews_count_num", F.expr("try_cast(reviews_count as int)")) \
    .withColumn("rating_num", F.expr("try_cast(rating as double)"))
✅ Learned: try_cast() avoids job failure and converts invalid values to NULL.

🔗 Joins (Inner, Left, Outer)
Created a dimension table brand_dim and joined it with the products dataset.

python
Copy code
brand_dim = products.select("brand").dropna().dropDuplicates() \
    .withColumn("brand_id", F.monotonically_increasing_id())

inner_joined = products.join(brand_dim, on="brand", how="inner")
left_joined  = products.join(brand_dim, on="brand", how="left")
outer_joined = products.join(brand_dim, on="brand", how="outer")
✅ Learned:

Inner Join → keeps matching rows only

Left Join → keeps all left rows

Outer Join → keeps all rows from both sides

🪟 Window Functions
✅ Ranking products within each brand
python
Copy code
w_brand_price = Window.partitionBy("brand").orderBy(F.desc("final_price_num"))

ranked = products.withColumn(
    "price_rank_in_brand",
    F.dense_rank().over(w_brand_price)
)

top3_per_brand = ranked.filter(F.col("price_rank_in_brand") <= 3)
✅ Running total of product price per brand
python
Copy code
w_running = Window.partitionBy("brand") \
                  .orderBy("timestamp") \
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

running_total = products.withColumn(
    "running_total_price_brand",
    F.sum("final_price_num").over(w_running)
)
✅ Learned:
Window functions help in advanced analytics like ranking, cumulative sums, rolling metrics etc. while keeping row-level detail.

👨‍💻 UDF (User Defined Function)
Extracted a simplified main_category from the categories column.

python
Copy code
from pyspark.sql.types import StringType

def get_main_category(cat):
    if cat is None:
        return None
    return str(cat).split(",")[0].strip()

main_category_udf = F.udf(get_main_category, StringType())

features = products.withColumn(
    "main_category",
    main_category_udf(F.col("categories"))
)
✅ Learned: UDF enables custom transformations but is slower than Spark built-in functions.

🧪 Derived Features (Feature Engineering)
Created useful derived columns for analytics / ML readiness.

✅ Discount percentage
python
Copy code
features = features.withColumn(
    "discount_percent",
    F.when(
        (F.col("initial_price_num") > 0) & (F.col("final_price_num").isNotNull()),
        ((F.col("initial_price_num") - F.col("final_price_num")) / F.col("initial_price_num")) * 100
    ).otherwise(None)
)
✅ Price bucket
python
Copy code
features = features.withColumn(
    "price_bucket",
    F.when(F.col("final_price_num") < 20, "low")
     .when((F.col("final_price_num") >= 20) & (F.col("final_price_num") < 100), "mid")
     .otherwise("high")
)
✅ Review level bucket
python
Copy code
features = features.withColumn(
    "review_level",
    F.when(F.col("reviews_count_num") >= 1000, "viral")
     .when(F.col("reviews_count_num") >= 100, "popular")
     .otherwise("new")
)
📊 Practice Output
✅ Top 5 products by revenue
python
Copy code
top5 = features.groupBy("asin", "title") \
    .agg(F.sum("final_price_num").alias("revenue")) \
    .orderBy(F.desc("revenue")) \
    .limit(5)

display(top5)
💾 Saving Final Output Table
python
Copy code
features.write.mode("overwrite").saveAsTable("workspace.default.day3_amazon_features")
✅ Summary
Day 3 helped me understand and implement:

joins for combining datasets

window functions for ranking and running totals

UDF for custom transformations

derived feature engineering for better analysis & ML readiness
