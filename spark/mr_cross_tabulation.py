import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONGO_URI = (
    "mongodb+srv://khangnguyen2x0_db_user:khangnguyen2x0_db_user"
    "@cluster0.yyrcrds.mongodb.net/"
)
MONGO_DB         = "BigDataJobMarket"
MONGO_COL_OUTPUT = "cross_tabulation_result"

OUTPUT_TXT = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/cross_tabulation_result.txt"
OUTPUT_PARQUET = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/cross_tabulation_result.parquet"

def main():
    spark = SparkSession.builder \
        .appName("JobMarket_Cross_Tabulation") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.mongodb.read.connection.uri",  MONGO_URI) \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    print("[INFO] Reading from HDFS: hdfs://localhost:9000/project/jobs")
    df = spark.read.csv("hdfs://localhost:9000/project/jobs", header=True, inferSchema=True)
    df = df.withColumn("salary_final_vnd", F.col("salary_final_vnd").cast("double"))
    
    # Standardize locations to main hubs
    df = df.withColumn("loc", F.when(F.col("location_clean").like("%Hồ Chí Minh%"), "HCM")
                              .when(F.col("location_clean").like("%Hà Nội%"), "HN")
                              .when(F.col("location_clean").like("%Đà Nẵng%"), "DN")
                              .otherwise("Other"))
    
    # Filter out empty skills and salary
    df = df.filter(F.col("skills_clean").isNotNull() & F.col("salary_final_vnd").isNotNull() & (F.col("loc") != "Other"))
    
    # Map: Explode skills
    mapped_df = df.withColumn("skill", F.explode(F.split(F.lower(F.col("skills_clean")), ",")))
    mapped_df = mapped_df.withColumn("skill", F.trim(F.col("skill")))
    mapped_df = mapped_df.filter(F.col("skill") != "")
    
    # Reduce: Pivot cross-tabulation
    # Cung 1 skill, luong trung binh o cac dia diem ra sao?
    pivot_df = mapped_df.groupBy("skill").pivot("loc", ["HCM", "HN", "DN"]).agg(
        F.round(F.avg("salary_final_vnd") / 1000000, 1).alias("avg_sal_M")
    ).fillna(0)
    
    # Calculate Total Salary (for sorting)
    final_df = pivot_df.withColumn("total_avg", (F.col("HCM") + F.col("HN") + F.col("DN")) / 3) \
                       .filter(F.col("HCM") > 0) \
                       .orderBy(F.desc("total_avg")).limit(30)
                       
    print(f"[INFO] Writing results to Parquet: {OUTPUT_PARQUET}")
    final_df.drop("total_avg").write.mode("overwrite").parquet("file:///" + OUTPUT_PARQUET)
    print(f"[OK] Da ghi ket qua Parquet vao: {OUTPUT_PARQUET}")
    
    results = final_df.collect()
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("=== CROSS-TABULATION: AVERAGE SALARY (M) BY SKILL & LOCATION ===\n\n")
        f.write(f"{'SKILL':<20} | {'HCM':<10} | {'HN':<10} | {'DN':<10}\n")
        f.write("-" * 55 + "\n")
        for row in results:
            f.write(f"{row['skill']:<20} | {row['HCM']:<10.1f} | {row['HN']:<10.1f} | {row['DN']:<10.1f}\n")
            
    print(f"[OK] Da ghi ket qua vao: {OUTPUT_TXT}")

    print(f"[INFO] Writing results to MongoDB: {MONGO_DB}.{MONGO_COL_OUTPUT}")
    final_df.drop("total_avg").write \
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

if __name__ == "__main__":
    main()
