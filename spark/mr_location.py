import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession

MONGO_URI = (
    "mongodb+srv://khangnguyen2x0_db_user:khangnguyen2x0_db_user"
    "@cluster0.yyrcrds.mongodb.net/"
)
MONGO_DB         = "BigDataJobMarket"
MONGO_COL_INPUT  = "Jobs"
MONGO_COL_OUTPUT = "location_result"

OUTPUT_TXT = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/location_result.txt"
OUTPUT_PARQUET = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/location_result.parquet"

spark = SparkSession.builder \
    .appName("MR_Location") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.mongodb.read.connection.uri",  MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("[INFO] Reading from HDFS: hdfs://localhost:9000/project/jobs")
df = spark.read.csv("hdfs://localhost:9000/project/jobs", header=True, inferSchema=True)

result = df.groupBy("location_clean") \
           .count() \
           .orderBy("count", ascending=False)

# Ghi kết quả ra file txt (UTF-8) thay vì in ra terminal để tránh lỗi encoding
with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    rows = result.collect()
    f.write(f"{'location_clean':<45} {'count':>6}\n")
    f.write("-" * 53 + "\n")
    for row in rows:
        f.write(f"{str(row['location_clean']):<45} {row['count']:>6}\n")

print(f"[OK] Da ghi ket qua vao: {OUTPUT_TXT}")

print(f"[INFO] Writing results to Parquet: {OUTPUT_PARQUET}")
result.write.mode("overwrite").parquet("file:///" + OUTPUT_PARQUET)
print(f"[OK] Da ghi ket qua Parquet vao: {OUTPUT_PARQUET}")

print(f"[INFO] Writing results to MongoDB: {MONGO_DB}.{MONGO_COL_OUTPUT}")
result.write \
    .format("mongodb") \
    .option("database",   MONGO_DB) \
    .option("collection", MONGO_COL_OUTPUT) \
    .mode("overwrite") \
    .save()
print(f"[OK] MongoDB written: {MONGO_DB}.{MONGO_COL_OUTPUT}")

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