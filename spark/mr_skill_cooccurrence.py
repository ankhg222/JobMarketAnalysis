import os
os.environ["PYTHONIOENCODING"] = "utf-8"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONGO_URI = ("mongodb+srv://admin:admin@cluster0.fyjsmyq.mongodb.net/?appName=Cluster0")
MONGO_DB = "BigDataJobMarket"
MONGO_COL_OUTPUT = "skill_cooccurrence"
INPUT_CSV = ("hdfs://localhost:9000/project/jobs/Data_ITJOB_Cleaned.csv")
OUTPUT_TXT = ("D:/JobMarket/Data/skill_cooccurrence.txt")
HDFS_PARQUET_DIR = ("/project/output/skill_cooccurrence")
HDFS_OUTPUT_DIR = ("/project/output")

# MAIN
spark = SparkSession.builder \
    .appName("MR_SkillCooccurrence") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("[INFO] Reading CSV from HDFS...")
df = spark.read.csv(INPUT_CSV,header=True,inferSchema=True)
print(df.columns)
df.printSchema()
df = df.fillna({"skills_clean": ""})
total_jobs = df.count()
print(f"[INFO] Total jobs: {total_jobs}")

# MAP PHASE
print("[INFO] Generating skill pairs...")
# MAP: Tach skills thanh mang
df_pairs = df \
    .filter(F.col("skills_clean") != "") \
    .withColumn(
        "skills",
        F.array_distinct(
            F.split(
                F.lower(F.col("skills_clean")),
                ","
            )
        )
    )

# Tao ID cho moi job
df_pairs = df_pairs.withColumn("job_id",F.monotonically_increasing_id())
# explode lan 1
left_df = df_pairs.select("job_id", F.posexplode("skills").alias("pos1", "skill1"))
# explode lan 2
right_df = df_pairs.select("job_id", F.posexplode("skills").alias("pos2", "skill2"))
# Tao cap skill trong cung 1 job
pair_df = left_df.join(right_df, on="job_id").filter(F.col("pos1") < F.col("pos2"))
# Chuan hoa skill
pair_df = pair_df.select(F.trim(F.col("skill1")).alias("skill1"), F.trim(F.col("skill2")).alias("skill2"))
# Tao ten pair: tránh trùng
pair_df = pair_df.withColumn("skill_pair",
    F.concat_ws(" + ",
        F.least("skill1", "skill2"),
        F.greatest("skill1", "skill2")))

# REDUCE
final_df = pair_df \
    .groupBy("skill_pair") \
    .agg(F.count("*").alias("support_count")) \
    .orderBy(F.desc("support_count")) \
    .limit(50)

print(f"[INFO] Writing to MongoDB: {MONGO_DB}.{MONGO_COL_OUTPUT}")
final_df.write \
    .format("mongodb") \
    .option("database", MONGO_DB) \
    .option("collection", MONGO_COL_OUTPUT) \
    .mode("overwrite") \
    .save()
print("[OK] MongoDB written")

print("[INFO] Writing Spark output to HDFS...")
final_df.write \
    .mode("overwrite") \
    .parquet(HDFS_PARQUET_DIR)
print("[OK] HDFS parquet saved")

# WRITE TXT
results = final_df.collect()
print(f"[INFO] Top skill pairs found: {len(results)}")
with open(
    OUTPUT_TXT,
    "w",
    encoding="utf-8"
) as f:
    f.write("=" * 80 + "\n")
    f.write("TOP 50 SKILL CO-OCCURRENCE (MARKET BASKET ANALYSIS)\n")
    f.write("=" * 80 + "\n\n")
    f.write("Cac cap ky nang thuong xuyen xuat hien cung nhau trong tin tuyen dung\n\n")
    f.write(
        f"{'Rank':<6}"
        f"{'Skill Pair':<55}"
        f"{'Support':>10}\n"
    )
    f.write("-" * 80 + "\n")
    for i, row in enumerate(results, 1):
        f.write(
            f"{i:<6}"
            f"{row['skill_pair']:<55}"
            f"{row['support_count']:10}\n"
        )
    f.write("\n")
    f.write(f"Total jobs analysed: {total_jobs}\n")
    f.write(f"Top skill pairs : {len(results)}\n")
print("[OK] TXT written: " + OUTPUT_TXT)

# UPLOAD TXT TO HDFS
print(f"[INFO] Uploading TXT to HDFS: " f"{HDFS_OUTPUT_DIR}")
os.system("hdfs dfs -mkdir -p /project/output")
os.system(f"hdfs dfs -put -f {OUTPUT_TXT} /project/output/")
print("[OK] TXT uploaded to HDFS")
spark.stop()