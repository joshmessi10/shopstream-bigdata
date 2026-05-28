from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys

# =====================================================
# 1. SPARK SESSION (optimized for EMR)
# =====================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("ShopStreamAnalytics")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

# =====================================================
# 2. LOAD DATA (SAFE JSONL)
# =====================================================
def load_raw(spark, path):
    return (
        spark.read
        .option("multiLine", "true")
        .json(path)
    )

# =====================================================
# 3. DATA QUALITY LAYER (enterprise‑style)
# =====================================================
def clean_data(df):
    # ---------------------------
    # 3.1 schema hygiene
    # ---------------------------
    df = df.dropDuplicates()

    # normalize timestamp FIRST
    df = df.withColumn("timestamp", to_timestamp("timestamp"))

    # ---------------------------
    # 3.2 fill safe categorical nulls
    # ---------------------------
    df = df.fillna({
        "country": "unknown",
        "device_type": "unknown",
        "referrer": "direct",
        "page_type": "unknown"
    })

    # ---------------------------
    # 3.3 safe numeric columns
    # ---------------------------
    df = df.withColumn(
        "time_on_page_seconds",
        col("time_on_page_seconds").cast("double")
    )
    df = df.withColumn(
        "price",
        col("price").cast("double")
    )

    # fill missing numerics with 0 (safe baseline)
    df = df.fillna({
        "time_on_page_seconds": 0,
        "price": 0
    })

    # ---------------------------
    # 3.4 z‑score (robust)
    # ---------------------------
    stats = df.filter(col("event_type") == "page_view").select(
        mean("time_on_page_seconds").alias("mean"),
        stddev("time_on_page_seconds").alias("std")
    ).first()
    mean_val = stats["mean"] or 0
    std_val = stats["std"] or 1
    df = df.withColumn(
        "time_z",
        (col("time_on_page_seconds") - mean_val) / std_val
    )

    # ---------------------------
    # 3.5 min‑max normalization safe
    # ---------------------------
    price_stats = df.agg(
        min("price").alias("min_p"),
        max("price").alias("max_p")
    ).first()
    min_p = price_stats["min_p"] or 0
    max_p = price_stats["max_p"] or 1
    denom = (max_p - min_p) or 1
    df = df.withColumn(
        "price_norm",
        (col("price") - min_p) / denom
    )

    return df

# =====================================================
# 4. METRICS ENGINE (optimized logic)
# =====================================================
def compute_metrics(df):
    page_views = df.filter(col("event_type") == "page_view")
    product_views = df.filter(col("event_type") == "product_view")
    cart_events = df.filter(col("event_type") == "cart_event")

    # -------------------------------------------------
    # 4.1 TOP PAGES (no null risk)
    # -------------------------------------------------
    top_pages = (
        page_views
        .filter(col("page_url").isNotNull())
        .groupBy("page_url")
        .agg(avg("time_on_page_seconds").alias("avg_time"))
        .orderBy(desc("avg_time"))
        .limit(20)
    )

    # -------------------------------------------------
    # 4.2 BOUNCE RATE (correct session logic)
    # -------------------------------------------------
    sessions = (
        page_views
        .groupBy("session_id")
        .agg(
            count("*").alias("pv"),
            first("page_type").alias("page_type")
        )
    )
    bounce_rate = (
        sessions.groupBy("page_type")
        .agg(
            (sum(when(col("pv") == 1, 1).otherwise(0)) /
            count("*")).alias("bounce_rate")
        )
    )

    # -------------------------------------------------
    # 4.3 FUNNEL (real conversion flow)
    # -------------------------------------------------
    funnel = (
        df.groupBy("event_type")
        .agg(countDistinct("user_id").alias("users"))
    )

    # -------------------------------------------------
    # 4.4 PRODUCT PERFORMANCE
    # -------------------------------------------------
    views = product_views.groupBy("product_id").agg(count("*").alias("views"))
    carts = cart_events.groupBy("product_id").agg(count("*").alias("carts"))
    product_perf = (
        views.join(carts, "product_id", "full_outer")
        .na.fill(0)
        .withColumn("gap", col("views") - col("carts"))
        .orderBy(desc("gap"))
    )

    # -------------------------------------------------
    # 4.5 NAVIGATION PATHS (ORDERED PROPERLY)
    # -------------------------------------------------
    w = Window.partitionBy("session_id").orderBy("timestamp")
    ordered = (
        page_views
        .withColumn("page", col("page_url"))
        .withColumn("rn", row_number().over(w))
    )
    paths = (
        ordered.groupBy("session_id")
        .agg(collect_list("page").alias("path"))
    )
    top_paths = (
        paths.groupBy("path")
        .count()
        .orderBy(desc("count"))
        .limit(10)
    )

    # -------------------------------------------------
    # 4.6 DEVICE + COUNTRY
    # -------------------------------------------------
    device_country = (
        page_views
        .groupBy("device_type", "country")
        .agg(avg("time_on_page_seconds").alias("avg_time"))
    )

    # -------------------------------------------------
    # 4.7 ANOMALIES (ROBUST)
    # -------------------------------------------------
    anomalies = (
        page_views
        .filter(abs(col("time_z")) > 3)
    )

    return {
        "top_pages": top_pages,
        "bounce_rate": bounce_rate,
        "funnel": funnel,
        "product_perf": product_perf,
        "top_paths": top_paths,
        "device_country": device_country,
        "anomalies": anomalies
    }

# =====================================================
# 5. OUTPUT LAYER (partitioned + clean)
# =====================================================
def write_outputs(metrics, base="s3://tienda-shopstream/analytics/"):
    for name, df in metrics.items():
        df = df.withColumn("event_date", to_date(col("timestamp")))
        (
            df.write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(base + name + "/")
        )
        print(f"[OK] {name} -> {base}{name}/")

# =====================================================
# 6. MAIN PIPELINE
# =====================================================
def main(path):
    spark = get_spark()
    df = load_raw(spark, path)
    df = clean_data(df)
    metrics = compute_metrics(df)
    write_outputs(metrics)
    print("PIPELINE SUCCESS")

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "s3://tienda-shopstream/raw/"
    main(path)