import os
os.environ["PYTHONIOENCODING"] = "utf-8"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MONGO_URI = ("mongodb+srv://admin:admin@cluster0.fyjsmyq.mongodb.net/?appName=Cluster0")
MONGO_DB = "BigDataJobMarket"
MONGO_COL_OUTPUT = "job_level_distribution"
INPUT_CSV = ("hdfs://localhost:9000/project/jobs/Data_ITJOB_Cleaned.csv")
OUTPUT_TXT = ("D:/JobMarket/Data/job_level_distribution.txt")
HDFS_PARQUET_DIR = ("/project/output/job_level_distribution")

spark = SparkSession.builder \
    .appName("MR_JobLevelDistribution") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.mongodb.read.connection.uri",  MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.csv(INPUT_CSV,header=True,inferSchema=True)
# Kiem tra schema va so luong record
print(df.columns)
df.printSchema()

# MAP: Chon & cast cac truong can thiet
df_mapped = df \
    .filter(
        F.col("job_level").isNotNull() & (F.col("job_level") != "") &
        F.col("source").isNotNull()    & (F.col("source") != "")
    ) \
    .withColumn("salary",    F.col("salary_final_vnd").cast("double")) \
    .withColumn("yoe",       F.col("yoe_extracted").cast("double")) \
    .withColumn("skill_cnt", F.col("skill_count").cast("int"))

total = df_mapped.count()
print(f"[INFO] Total valid rows: {total}")
# Dang ky TempView
df_mapped.createOrReplaceTempView("level_dist")

# REDUCE 1: Phan bo tong the theo job_level
level_overall = spark.sql("""
    SELECT job_level, COUNT(*) AS job_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_total,
        ROUND(AVG(CASE WHEN salary > 0 THEN salary END)/1e6, 1) AS avg_salary_M,
        ROUND(PERCENTILE_APPROX(CASE WHEN salary > 0 THEN salary END, 0.5)/1e6, 1)
                                                            AS median_salary_M,
        ROUND(AVG(yoe), 1) AS avg_yoe, ROUND(AVG(skill_cnt), 1) AS avg_skills
    FROM level_dist
    GROUP BY job_level ORDER BY job_count DESC""")

# REDUCE 2: Pivot source x job_level 
pivot_df = df_mapped \
    .groupBy("source") \
    .pivot("job_level") \
    .count() \
    .fillna(0)
# Tinh total_jobs per source de sort
level_cols = [c for c in pivot_df.columns if c != "source"]
pivot_with_total = pivot_df \
    .withColumn("total_jobs", sum(F.col(c) for c in level_cols)) \
    .orderBy(F.col("total_jobs").desc())

#  REDUCE 3: Thong ke theo (source, job_level) composite key 
source_level_df = spark.sql("""
    SELECT source, job_level, COUNT(*) AS job_count,
        ROUND(AVG(CASE WHEN salary > 0 THEN salary END)/1e6, 1) AS avg_salary_M,
        ROUND(AVG(yoe), 1) AS avg_yoe
    FROM level_dist
    GROUP BY source, job_level ORDER BY job_count DESC""").limit(15)

print("[INFO] Writing Spark output to HDFS...")
level_overall.write \
    .mode("overwrite") \
    .parquet(HDFS_PARQUET_DIR)
print("[OK] HDFS parquet saved")

# Ghi ket qua vao MongoDB
print(f"[INFO] Writing results to MongoDB: {MONGO_DB}.{MONGO_COL_OUTPUT}")
level_overall.write \
    .format("mongodb") \
    .option("database",   MONGO_DB) \
    .option("collection", MONGO_COL_OUTPUT) \
    .mode("overwrite") \
    .save()
print(f"[OK] MongoDB written: {MONGO_DB}.{MONGO_COL_OUTPUT}")

# Ghi TXT UTF-8
level_rows  = level_overall.collect()
pivot_rows  = pivot_with_total.collect()
src_lv_rows = source_level_df.collect()
with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write("=" * 75 + "\n")
    f.write("       PHAN BO CAP BAC CONG VIEC - THI TRUONG IT VIET NAM\n")
    f.write("=" * 75 + "\n\n")
    f.write("-- PHAN BO TONG THE THEO CAP BAC --\n")
    hdr = f"{'Cap bac':<16} {'So job':>7} {'Ti le':>7} {'Avg(M)':>8} {'Median(M)':>10} {'AvgYOE':>7} {'AvgSkills':>10}"
    f.write(hdr + "\n")
    f.write("-" * 75 + "\n")
    for row in level_rows:
        avg = f"{float(row['avg_salary_M']):.1f}" if row['avg_salary_M'] else "N/A"
        med = f"{float(row['median_salary_M']):.1f}" if row['median_salary_M'] else "N/A"
        f.write(
            f"{str(row['job_level']):<16} {row['job_count']:>7} "
            f"{float(row['pct_total']):>6.1f}% {avg:>8} {med:>10} "
            f"{float(row['avg_yoe']):>7.1f} {float(row['avg_skills']):>10.1f}\n"
        )
    f.write("\n-- PIVOT: NGUON TUYEN DUNG x CAP BAC --\n")
    col_w = 9
    hdr_p = f"{'Nguon':<22}"
    for c in level_cols:
        hdr_p += f"{str(c)[:8]:>{col_w}}"
    hdr_p += f"{'Total':>{col_w}}"
    f.write(hdr_p + "\n")
    f.write("-" * (22 + col_w * (len(level_cols) + 1)) + "\n")
    for row in pivot_rows:
        line = f"{str(row['source']):<22}"
        for c in level_cols:
            val = row[c] if row[c] is not None else 0
            line += f"{val:>{col_w}}"
        line += f"{row['total_jobs']:>{col_w}}"
        f.write(line + "\n")
    f.write("\n-- TOP 15 CAP (NGUON, CAP BAC) NHIEU VIEC NHAT --\n")
    for row in src_lv_rows:
        avg = f"{float(row['avg_salary_M']):.1f}" if row['avg_salary_M'] else "N/A"
        f.write(f"  [{str(row['source']):<8}] {str(row['job_level']):<12}: "
                f"{row['job_count']} jobs, avg={avg}M, avg_yoe={float(row['avg_yoe']):.1f}yr\n")
    f.write(f"\nTong jobs phan tich: {total}\n")
print("[OK] TXT written: " + OUTPUT_TXT)

# Upload len HDFS
HDFS_OUTPUT_DIR = "/project/output/"
print(f"[INFO] Uploading TXT to HDFS: {HDFS_OUTPUT_DIR}")
os.system("hdfs dfs -mkdir -p /project/output")
os.system(f"hdfs dfs -put -f {OUTPUT_TXT} /project/output/")
print("[OK] TXT uploaded to HDFS")
spark.stop()