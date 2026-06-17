import os
os.environ["PYTHONIOENCODING"] = "utf-8"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# CONFIG
MONGO_URI = ("mongodb+srv://admin:admin@cluster0.fyjsmyq.mongodb.net/?appName=Cluster0")
MONGO_DB = "BigDataJobMarket"
MONGO_COL_OUTPUT = "skill_demand_salary"
INPUT_CSV = ("hdfs://localhost:9000/project/jobs/Data_ITJOB_Cleaned.csv")
OUTPUT_TXT = ("D:/JobMarket/Data/skill_demand_salary.txt")
LOCAL_PARQUET_DIR = ("D:/JobMarket/Data/skill_demand_salary")
HDFS_PARQUET_DIR = ("/project/output/skill_demand_salary")
HDFS_OUTPUT_DIR = ("/project/output")

# SPARK
spark = SparkSession.builder \
    .appName("MR_SkillDemandSalary") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# READ
print("[INFO] Reading CSV from HDFS...")
df = spark.read.csv(INPUT_CSV, header=True, inferSchema=True)
print(df.columns)
df.printSchema()

# CLEAN
df = df.filter(
    F.col("skills_clean").isNotNull() &
    F.col("salary_final_vnd").isNotNull()
)
df = df.withColumn("salary",F.col("salary_final_vnd").cast("double"))

# MAP
print("[INFO] Exploding skills...")
skill_df = df.withColumn(
    "skill",
    F.explode(
        F.split(
            F.lower(
                F.col("skills_clean")),","
            )
        )
    )
skill_df = skill_df.withColumn("skill", F.trim(F.col("skill")))

# REDUCE
result_df = skill_df.groupBy("skill").agg(
    F.count("*").alias("demand_count"),
    F.round(F.avg("salary") / 1000000, 1).alias("avg_salary_M"),
    F.round(F.percentile_approx("salary", 0.5) / 1000000, 1).alias("median_salary_M"),
    F.round(F.max("salary") / 1000000, 1).alias("max_salary_M")
)
result_df = result_df.filter(F.col("demand_count") >= 5)
result_df = result_df.orderBy(F.desc("avg_salary_M"))

# PARQUET LOCAL
print("[INFO] Writing parquet to local...")
result_df.write \
    .mode("overwrite") \
    .parquet(
        "file:///" + LOCAL_PARQUET_DIR
    )
print("[OK] Local parquet saved")
# PARQUET HDFS
print("[INFO] Writing parquet to HDFS...")
result_df.write \
    .mode("overwrite") \
    .parquet(HDFS_PARQUET_DIR)
print("[OK] HDFS parquet saved")

# MONGODB
print(f"[INFO] Writing results to MongoDB: " f"{MONGO_DB}.{MONGO_COL_OUTPUT}")
result_df.write \
    .format("mongodb") \
    .option("database", MONGO_DB) \
    .option("collection", MONGO_COL_OUTPUT) \
    .mode("overwrite") \
    .save()
print("[OK] MongoDB written")

# TXT
results = result_df.collect()
print(f"[INFO] Total skills analysed: {len(results)}")
os.makedirs(os.path.dirname(OUTPUT_TXT), exist_ok=True)
with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write("=" * 90 + "\n")
    f.write("SKILL DEMAND VS SALARY ANALYSIS\n")
    f.write("=" * 90 + "\n\n")
    f.write(
        f"{'Skill':<30}"
        f"{'Jobs':>10}"
        f"{'Avg(M)':>12}"
        f"{'Median(M)':>12}"
        f"{'Max(M)':>12}\n")
    f.write("-" * 90 + "\n")
    for row in results:
        f.write(
            f"{row['skill']:<30}"
            f"{row['demand_count']:>10}"
            f"{row['avg_salary_M']:>12.1f}"
            f"{row['median_salary_M']:>12.1f}"
            f"{row['max_salary_M']:>12.1f}\n")
print("[OK] TXT written: " + OUTPUT_TXT)

# TXT -> HDFS
print(f"[INFO] Uploading TXT to HDFS: " f"{HDFS_OUTPUT_DIR}")
os.system("hdfs dfs -mkdir -p /project/output")
os.system(f"hdfs dfs -put -f {OUTPUT_TXT} /project/output/")
print("[OK] TXT uploaded to HDFS")
spark.stop()