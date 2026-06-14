"""
MapReduce Job 5: YOE vs Salary Correlation
============================================
Phuong phap MapReduce:
  - MAP   : Gan moi job vao 1 bucket kinh nghiem (0-1, 2, 3-4, 5-6, 7-9, 10+)
  - REDUCE: groupBy(bucket).agg(count, avg_salary, percentile, avg_yoe)
  - WINDOW: Tinh muc tang luong so voi nhom truoc (LAG)
  - SQL   : Dung Spark SQL TempView de query
Input  : Data_ITJOB_Cleaned.csv
Output : data/parquet/yoe_salary_correlation/ + yoe_salary_correlation.txt
"""

import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse
import sys

MONGO_URI = (
    "mongodb+srv://khangnguyen2x0_db_user:khangnguyen2x0_db_user"
    "@cluster0.yyrcrds.mongodb.net/"
)
MONGO_DB         = "BigDataJobMarket"
MONGO_COL_INPUT  = "Jobs"
MONGO_COL_OUTPUT = "yoe_salary_correlation"

OUTPUT_TXT = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/yoe_salary_correlation.txt"
OUTPUT_PARQUET = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/yoe_salary_correlation.parquet"

spark = SparkSession.builder \
    .appName("MR_YOE_Salary") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.mongodb.read.connection.uri",  MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Doc & lam sach ────────────────────────────
parser = argparse.ArgumentParser(description="YOE Salary Correlation")
parser.add_argument("--source", type=str, default="HDFS", choices=["HDFS", "MONGODB"], help="Data source: HDFS or MONGODB")
# parser.parse_known_args used to avoid issues if spark-submit passes extra args
args, _ = parser.parse_known_args()
READ_SOURCE = args.source

if READ_SOURCE == "HDFS":
    print("[INFO] Reading from HDFS: hdfs://localhost:9000/project/jobs")
    df = spark.read.csv("hdfs://localhost:9000/project/jobs", header=True, inferSchema=True)
else:
    print(f"[INFO] Reading from MongoDB: {MONGO_DB}.{MONGO_COL_INPUT}")
    df = spark.read \
        .format("mongodb") \
        .option("database",   MONGO_DB) \
        .option("collection", MONGO_COL_INPUT) \
        .load()

df_clean = df \
    .filter(
        F.col("salary_final_vnd").isNotNull() &
        (F.col("salary_final_vnd").cast("double") > 0) &
        F.col("yoe_extracted").isNotNull()
    ) \
    .withColumn("salary",    F.col("salary_final_vnd").cast("double")) \
    .withColumn("yoe",       F.col("yoe_extracted").cast("double")) \
    .withColumn("skill_cnt", F.col("skill_count").cast("int")) \
    .filter(F.col("yoe") >= 0)

print(f"[INFO] Valid rows: {df_clean.count()}")

# ── MAP: Phan loai (bucket) moi job theo yoe ──
df_bucketed = df_clean.withColumn(
    "yoe_bucket",
    F.when(F.col("yoe") <= 1, "00-01yr (Fresher)")
     .when(F.col("yoe") <= 2, "02yr    (Junior)")
     .when(F.col("yoe") <= 4, "03-04yr (Mid-level)")
     .when(F.col("yoe") <= 6, "05-06yr (Senior)")
     .when(F.col("yoe") <= 9, "07-09yr (Lead)")
     .otherwise(              "10+yr   (Expert/Mgr)")
).withColumn(
    "bucket_order",
    F.when(F.col("yoe") <= 1, 1)
     .when(F.col("yoe") <= 2, 2)
     .when(F.col("yoe") <= 4, 3)
     .when(F.col("yoe") <= 6, 4)
     .when(F.col("yoe") <= 9, 5)
     .otherwise(6)
)

# Dang ky TempView de dung Spark SQL
df_bucketed.createOrReplaceTempView("job_yoe")

# ── REDUCE: Tinh thong ke luong theo bucket bang Spark SQL ──
result_df = spark.sql("""
    SELECT
        yoe_bucket,
        bucket_order,
        COUNT(*)                                              AS job_count,
        ROUND(AVG(yoe), 1)                                   AS avg_yoe,
        ROUND(MIN(salary) / 1e6, 1)                          AS min_salary_M,
        ROUND(AVG(salary) / 1e6, 1)                          AS avg_salary_M,
        ROUND(PERCENTILE_APPROX(salary, 0.5) / 1e6, 1)      AS median_salary_M,
        ROUND(MAX(salary) / 1e6, 1)                          AS max_salary_M,
        ROUND(AVG(skill_cnt), 1)                             AS avg_skill_count
    FROM job_yoe
    GROUP BY yoe_bucket, bucket_order
    ORDER BY bucket_order ASC
""")

# ── WINDOW: Tinh muc tang luong so voi nhom kinh nghiem truoc ──
window_spec = Window.orderBy("bucket_order")
result_df = result_df.withColumn(
    "salary_increase_M",
    F.round(
        F.col("avg_salary_M") - F.lag("avg_salary_M", 1).over(window_spec),
        1
    )
).drop("bucket_order")

print(f"[INFO] Writing results to Parquet: {OUTPUT_PARQUET}")
result_df.write.mode("overwrite").parquet("file:///" + OUTPUT_PARQUET)
print(f"[OK] Da ghi ket qua Parquet vao: {OUTPUT_PARQUET}")

# ── Ghi vao MongoDB ──────────────────────────────────
print(f"[INFO] Writing results to MongoDB: {MONGO_DB}.{MONGO_COL_OUTPUT}")
result_df.write \
    .format("mongodb") \
    .option("database",   MONGO_DB) \
    .option("collection", MONGO_COL_OUTPUT) \
    .mode("overwrite") \
    .save()
print(f"[OK] MongoDB written: {MONGO_DB}.{MONGO_COL_OUTPUT}")

# ── Ghi TXT UTF-8 ────────────────────────────
rows = result_df.collect()
with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write("=" * 88 + "\n")
    f.write("    TUONG QUAN NAM KINH NGHIEM (YOE) VA MUC LUONG THI TRUONG IT\n")
    f.write("=" * 88 + "\n")
    hdr = (f"{'Nhom KN':<22} {'Jobs':>5} {'AvgYOE':>7} "
           f"{'Min(M)':>7} {'Avg(M)':>7} {'Median(M)':>10} {'Max(M)':>7} "
           f"{'AvgSkills':>10} {'Tang(M)':>8}")
    f.write(hdr + "\n")
    f.write("-" * 88 + "\n")
    for row in rows:
        inc = (f"{float(row['salary_increase_M']):>+8.1f}"
               if row['salary_increase_M'] is not None else f"{'N/A':>8}")
        f.write(
            f"{str(row['yoe_bucket']):<22} {row['job_count']:>5} "
            f"{float(row['avg_yoe']):>7.1f} {float(row['min_salary_M']):>7.1f} "
            f"{float(row['avg_salary_M']):>7.1f} {float(row['median_salary_M']):>10.1f} "
            f"{float(row['max_salary_M']):>7.1f} {float(row['avg_skill_count']):>10.1f} "
            f"{inc}\n"
        )
    f.write("=" * 88 + "\n")

print("[OK] TXT written: " + OUTPUT_TXT)

# ── Upload len HDFS ──────────────────────────────────────────────────────────
HDFS_OUTPUT_DIR = "/project/output/"
print(f"[INFO] Uploading TXT and Parquet to HDFS: {HDFS_OUTPUT_DIR}")
os.system(f"hdfs dfs -mkdir -p {HDFS_OUTPUT_DIR}")
os.system(f"hdfs dfs -rm -r -f {HDFS_OUTPUT_DIR}" + os.path.basename(OUTPUT_TXT))
os.system(f"hdfs dfs -rm -r -f {HDFS_OUTPUT_DIR}" + os.path.basename(OUTPUT_PARQUET))
os.system(f"hdfs dfs -put -f {OUTPUT_TXT} {HDFS_OUTPUT_DIR}")
os.system(f"hdfs dfs -put -f {OUTPUT_PARQUET} {HDFS_OUTPUT_DIR}")
print("[OK] HDFS upload commands executed.")

spark.stop()
