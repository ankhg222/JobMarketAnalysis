from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os


# ─────────────────────────────────────────────────────────────
# BƯỚC 1: Khởi tạo SparkSession
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Job4_SalaryBySkill") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 65)
print("JOB 4: SALARY STATISTICS BY SKILL (with Noise Filtering)")
print("=" * 65)


# ─────────────────────────────────────────────────────────────
# BƯỚC 2: Đọc dữ liệu từ HDFS
# ─────────────────────────────────────────────────────────────
HDFS_INPUT = "hdfs://tanyen-master:9000/project/jobs/Data_ITJOB_Cleaned.csv"
LOCAL_DIR  = "/home/tanyen/hadoopyen/project/output"
HDFS_OUT   = "hdfs://tanyen-master:9000/project/output"
os.makedirs(LOCAL_DIR, exist_ok=True)

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv(HDFS_INPUT)

# Lọc bản ghi hợp lệ: có lương VND > 0 và có skills
df_valid = df.filter(
    F.col("salary_final_vnd").isNotNull() &
    (F.col("salary_final_vnd") > 0) &
    F.col("skills_clean").isNotNull() &
    (F.col("skills_clean") != "")
)

N_TOTAL = df_valid.count()
print(f"Tổng bản ghi hợp lệ (có lương + skills): {N_TOTAL}")


# ─────────────────────────────────────────────────────────────
# BƯỚC 3 (MAP PHASE): Explode skills_clean → (url, skill, salary, level)
# ─────────────────────────────────────────────────────────────

df_exploded = df_valid \
    .withColumn(
        "skill",
        F.explode(F.split(F.col("skills_clean"), r",\s*"))
    ) \
    .withColumn(
        "skill",
        F.lower(F.trim(F.col("skill")))  # chuẩn hóa: lowercase + bỏ khoảng trắng
    ) \
    .filter(F.length(F.col("skill")) > 1)  # bỏ ký tự lẻ 1 chữ

print(f"\n[MAP OUTPUT] Tổng cặp (job, skill) sau explode: {df_exploded.count()}")
df_exploded.select("url", "skill", "salary_final_vnd", "job_level") \
           .show(5, truncate=50)


# ─────────────────────────────────────────────────────────────
# BƯỚC 4 (REDUCE PHASE): Thống kê lương theo skill
# ─────────────────────────────────────────────────────────────

df_stats = df_exploded.groupBy("skill") \
    .agg(
        # Đếm số job KHÁC NHAU yêu cầu skill này (dùng url làm key)
        F.countDistinct("url").alias("job_count"),

        # Lương: trung bình, min, max
        F.round(F.avg("salary_final_vnd") / 1e6, 2).alias("avg_salary_trieu"),
        F.round(F.min("salary_final_vnd") / 1e6, 2).alias("min_salary_trieu"),
        F.round(F.max("salary_final_vnd") / 1e6, 2).alias("max_salary_trieu"),

        # Độ lệch chuẩn — đo mức độ phân tán lương
        F.round(F.stddev("salary_final_vnd") / 1e6, 2).alias("stddev_salary_trieu"),

        # Phân vị: Median (P50), P75, P90
        F.round(
            F.percentile_approx("salary_final_vnd", 0.50, 1000) / 1e6, 2
        ).alias("median_p50_trieu"),
        F.round(
            F.percentile_approx("salary_final_vnd", 0.75, 1000) / 1e6, 2
        ).alias("p75_trieu"),
        F.round(
            F.percentile_approx("salary_final_vnd", 0.90, 1000) / 1e6, 2
        ).alias("p90_trieu"),
    )

print(f"\n[REDUCE OUTPUT] Tổng skill trước lọc nhiễu: {df_stats.count()}")


# ─────────────────────────────────────────────────────────────
# BƯỚC 5 (NOISE FILTERING): Loại bỏ kỹ năng quá hiếm
# ─────────────────────────────────────────────────────────────

MIN_JOB_COUNT = 5

df_filtered = df_stats.filter(F.col("job_count") >= MIN_JOB_COUNT)
noise_count  = df_stats.count() - df_filtered.count()
print(f"[NOISE FILTER] Loại bỏ {noise_count} skill hiếm (job_count < {MIN_JOB_COUNT})")
print(f"[NOISE FILTER] Giữ lại: {df_filtered.count()} skill")


# ─────────────────────────────────────────────────────────────
# BƯỚC 6: Tính tỷ lệ % và thêm metadata
# ─────────────────────────────────────────────────────────────

N_lit = F.lit(float(N_TOTAL))

df_result = df_filtered \
    .withColumn(
        "pct_job",
        F.round(F.col("job_count") / N_lit * 100, 2)
    ) \
    .withColumn(
        # Phân loại mức độ phổ biến
        "popularity",
        F.when(F.col("pct_job") >= 10,  "Rất phổ biến (≥10%)")
         .when(F.col("pct_job") >= 5,   "Phổ biến (5-10%)")
         .when(F.col("pct_job") >= 2,   "Trung bình (2-5%)")
         .otherwise(                     "Ít phổ biến (<2%)")
    ) \
    .withColumn(
        # Phân loại mức lương trung bình
        "salary_tier",
        F.when(F.col("avg_salary_trieu") >= 50, "Premium (≥50tr)")
         .when(F.col("avg_salary_trieu") >= 40, "High (40-50tr)")
         .when(F.col("avg_salary_trieu") >= 30, "Mid (30-40tr)")
         .otherwise(                             "Entry (<30tr)")
    )


# ─────────────────────────────────────────────────────────────
# BƯỚC 7: Window Function — Xếp hạng skill theo lương
# ─────────────────────────────────────────────────────────────
win_salary = Window.orderBy(F.desc("avg_salary_trieu"))
win_demand = Window.orderBy(F.desc("job_count"))

df_ranked = df_result \
    .withColumn("rank_by_salary", F.rank().over(win_salary)) \
    .withColumn("rank_by_demand", F.rank().over(win_demand))


# ─────────────────────────────────────────────────────────────
# BƯỚC 8: Hiển thị kết quả
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("TOP 20 SKILL LƯƠNG CAO NHẤT (avg_salary DESC):")
print("=" * 65)
df_ranked.select(
    "rank_by_salary", "skill", "job_count", "pct_job",
    "avg_salary_trieu", "median_p50_trieu", "p90_trieu",
    "salary_tier", "popularity"
).orderBy("rank_by_salary").show(20, truncate=False)

print("\n" + "=" * 65)
print("TOP 20 SKILL PHỔ BIẾN NHẤT (job_count DESC):")
print("=" * 65)
df_ranked.select(
    "rank_by_demand", "skill", "job_count", "pct_job",
    "avg_salary_trieu", "median_p50_trieu", "salary_tier"
).orderBy("rank_by_demand").show(20, truncate=False)

print("\n" + "=" * 65)
print("PHÂN LOẠI THEO popularity × salary_tier:")
print("=" * 65)
df_ranked.groupBy("popularity", "salary_tier") \
    .agg(
        F.count("*").alias("so_skill"),
        F.round(F.avg("avg_salary_trieu"), 2).alias("avg_salary_tb"),
        F.round(F.avg("job_count"), 1).alias("avg_job_count")
    ) \
    .orderBy("popularity", "salary_tier") \
    .show(truncate=False)

# ─────────────────────────────────────────────────────────────
# BƯỚC 8B (BONUS): Phân tích chéo skill × job_level
# ─────────────────────────────────────────────────────────────
# Top 10 skill phổ biến nhất, xem phân bố job_level
print("\n" + "=" * 65)
print("BONUS: TOP 10 SKILL × JOB_LEVEL DISTRIBUTION:")
print("=" * 65)

top10_skills = [row["skill"] for row in
                df_ranked.orderBy("rank_by_demand").limit(10).select("skill").collect()]

df_cross = df_exploded \
    .filter(F.col("skill").isin(top10_skills)) \
    .groupBy("skill", "job_level") \
    .agg(F.countDistinct("url").alias("job_cnt")) \
    .orderBy("skill", F.desc("job_cnt"))

df_cross.show(50, truncate=False)


# ─────────────────────────────────────────────────────────────
# BƯỚC 9: Chuẩn bị output cuối
# ─────────────────────────────────────────────────────────────
final_df = df_ranked.select(
    "rank_by_demand", "rank_by_salary",
    "skill", "job_count", "pct_job",
    "avg_salary_trieu", "median_p50_trieu", "p75_trieu", "p90_trieu",
    "min_salary_trieu", "max_salary_trieu", "stddev_salary_trieu",
    "popularity", "salary_tier"
).orderBy("rank_by_demand")


# ─────────────────────────────────────────────────────────────
# BƯỚC 10: Lưu kết quả
# ─────────────────────────────────────────────────────────────
# Lưu lên HDFS
HDFS_OUTPUT = f"{HDFS_OUT}/job4_salary_by_skill"
final_df.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(HDFS_OUTPUT)
print(f"\nĐã lưu lên HDFS: {HDFS_OUTPUT}")

# Lưu local
LOCAL_OUTPUT = f"{LOCAL_DIR}/job4_salary_by_skill.csv"
final_df.toPandas().to_csv(LOCAL_OUTPUT, index=False, encoding="utf-8-sig")
print(f"Đã lưu local: {LOCAL_OUTPUT}")

# Lưu cross-tab skill × level
LOCAL_CROSS = f"{LOCAL_DIR}/job4_skill_by_level.csv"
df_cross.toPandas().to_csv(LOCAL_CROSS, index=False, encoding="utf-8-sig")
print(f"Đã lưu cross-tab skill×level: {LOCAL_CROSS}")

spark.stop()
print("\nJob 4 hoàn tất!")
