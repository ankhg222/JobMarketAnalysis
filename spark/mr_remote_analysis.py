from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
# ─────────────────────────────────────────────────────────────
# BƯỚC 1: Khởi tạo SparkSession
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Job7_RemoteAnalysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 65)
print("JOB 7: REMOTE WORK ANALYSIS — PIVOT SO SÁNH CHÉO")
print("=" * 65)

# ─────────────────────────────────────────────────────────────
# BƯỚC 2: Đọc & làm sạch dữ liệu
# ─────────────────────────────────────────────────────────────
HDFS_INPUT = "hdfs://tanyen-master:9000/project/jobs/Data_ITJOB_Cleaned.csv"
LOCAL_DIR  = "/home/tanyen/hadoopyen/project/output"
HDFS_OUT   = "hdfs://tanyen-master:9000/project/output"
os.makedirs(LOCAL_DIR, exist_ok=True)

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv(HDFS_INPUT)

print(f"Tổng bản ghi gốc: {df_raw.count()}")

# Lọc bản ghi có lương hợp lệ
df = df_raw.filter(
    F.col("salary_final_vnd").isNotNull() &
    (F.col("salary_final_vnd") > 0)
)

N_TOTAL = df.count()
print(f"Bản ghi có lương hợp lệ: {N_TOTAL}")
print(f"  → Remote (is_remote=1): {df.filter(F.col('is_remote') == 1).count()}")
print(f"  → Non-Remote (is_remote=0): {df.filter(F.col('is_remote') == 0).count()}")

# ─────────────────────────────────────────────────────────────
# BƯỚC 3 (MAP PHASE): Chuẩn hóa dữ liệu cho PIVOT
# ─────────────────────────────────────────────────────────────
df_mapped = df \
    .withColumn(
        "remote_label",
        F.when(F.col("is_remote") == 1, "Remote/Hybrid")
         .otherwise("Tại văn phòng")
    ) \
    .withColumn(
        # Lấy thành phố đầu tiên trong chuỗi "Hồ Chí Minh, Hà Nội"
        # split(",") → getItem(0) → trim
        "city_primary",
        F.trim(F.split(F.col("location_clean"), ",").getItem(0))
    ) \
    .withColumn(
        # Chuẩn hóa location thành 4 vùng rõ ràng để PIVOT gọn
        "location_group",
        F.when(F.col("city_primary").contains("Hồ Chí Minh"), "Hồ Chí Minh")
         .when(F.col("city_primary").contains("Hà Nội"),      "Hà Nội")
         .when(F.col("city_primary").contains("Đà Nẵng"),     "Đà Nẵng")
         .when(F.col("city_primary") == "Remote",             "Remote-only")
         .otherwise("Khác")
    ) \
    .withColumn(
        # Chuẩn hóa level — gộp Fresher & Junior/Fresher
        "level_group",
        F.when(F.col("job_level").isin("Fresher", "Junior/Fresher"), "Fresher/Junior")
         .when(F.col("job_level").isNull(), "Undefined")
         .otherwise(F.col("job_level"))
    )

print("\n[MAP OUTPUT] Mẫu sau chuẩn hóa:")
df_mapped.select(
    "title_clean", "is_remote", "remote_label",
    "location_clean", "location_group", "level_group",
    "salary_final_vnd"
).show(5, truncate=40)


# ═════════════════════════════════════════════════════════════
# PHÂN TÍCH 1: PIVOT — Remote vs Non-Remote THEO JOB_LEVEL
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("PHÂN TÍCH 1: PIVOT Remote vs Non-Remote × Job Level")
print("=" * 65)

# REDUCE: groupBy(level_group) với conditional aggregation
# Đây là kỹ thuật PIVOT bằng F.when() — tương đương SQL:
#   SUM(CASE WHEN is_remote=1 THEN 1 ELSE 0 END) AS remote_count
#   AVG(CASE WHEN is_remote=1 THEN salary END)    AS avg_salary_remote

df_pivot_level = df_mapped.groupBy("level_group") \
    .agg(
        # Tổng số job của level này
        F.count("*").alias("total_jobs"),

        # ── Cột PIVOT: Remote ──
        F.sum(F.when(F.col("is_remote") == 1, 1).otherwise(0))
          .alias("remote_count"),
        F.round(
            F.avg(F.when(F.col("is_remote") == 1, F.col("salary_final_vnd"))),
            0
        ).alias("remote_avg_vnd"),

        # ── Cột PIVOT: Non-Remote ──
        F.sum(F.when(F.col("is_remote") == 0, 1).otherwise(0))
          .alias("nonremote_count"),
        F.round(
            F.avg(F.when(F.col("is_remote") == 0, F.col("salary_final_vnd"))),
            0
        ).alias("nonremote_avg_vnd"),
    ) \
    .withColumn(
        # Tỷ lệ remote trong level này
        "remote_rate_pct",
        F.round(F.col("remote_count") / F.col("total_jobs") * 100, 2)
    ) \
    .withColumn(
        # Chênh lệch lương Remote - Non-Remote (triệu VND)
        # Dương = remote trả cao hơn; Âm = non-remote trả cao hơn
        "salary_delta_trieu",
        F.when(
            F.col("remote_avg_vnd").isNotNull() & F.col("nonremote_avg_vnd").isNotNull(),
            F.round((F.col("remote_avg_vnd") - F.col("nonremote_avg_vnd")) / 1e6, 2)
        ).otherwise(None)
    ) \
    .withColumn(
        "remote_avg_trieu",
        F.round(F.col("remote_avg_vnd") / 1e6, 2)
    ) \
    .withColumn(
        "nonremote_avg_trieu",
        F.round(F.col("nonremote_avg_vnd") / 1e6, 2)
    ) \
    .withColumn(
        # Thứ tự hiển thị cấp bậc
        "level_order",
        F.when(F.col("level_group") == "Fresher/Junior", 1)
         .when(F.col("level_group") == "Junior",         2)
         .when(F.col("level_group") == "Mid-level",      3)
         .when(F.col("level_group") == "Senior",         4)
         .when(F.col("level_group") == "Lead",           5)
         .when(F.col("level_group") == "Manager",        6)
         .otherwise(99)
    )

df_pivot_level_out = df_pivot_level.select(
    "level_group", "total_jobs",
    "remote_count", "nonremote_count", "remote_rate_pct",
    "remote_avg_trieu", "nonremote_avg_trieu", "salary_delta_trieu"
).orderBy("level_order")

print("\n[PIVOT 1] Remote vs Non-Remote theo Job Level:")
df_pivot_level_out.show(truncate=False)


# ═════════════════════════════════════════════════════════════
# PHÂN TÍCH 2: PIVOT — Remote vs Non-Remote THEO LOCATION
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("PHÂN TÍCH 2: PIVOT Remote vs Non-Remote × Location")
print("=" * 65)

df_pivot_loc = df_mapped.groupBy("location_group") \
    .agg(
        F.count("*").alias("total_jobs"),
        F.sum(F.when(F.col("is_remote") == 1, 1).otherwise(0))
          .alias("remote_count"),
        F.sum(F.when(F.col("is_remote") == 0, 1).otherwise(0))
          .alias("nonremote_count"),
        F.round(
            F.avg(F.when(F.col("is_remote") == 1, F.col("salary_final_vnd"))) / 1e6, 2
        ).alias("remote_avg_trieu"),
        F.round(
            F.avg(F.when(F.col("is_remote") == 0, F.col("salary_final_vnd"))) / 1e6, 2
        ).alias("nonremote_avg_trieu"),
        F.round(
            F.avg("salary_final_vnd") / 1e6, 2
        ).alias("overall_avg_trieu"),
    ) \
    .withColumn(
        "remote_rate_pct",
        F.round(F.col("remote_count") / F.col("total_jobs") * 100, 2)
    ) \
    .withColumn(
        "salary_delta_trieu",
        F.when(
            F.col("remote_avg_trieu").isNotNull() & F.col("nonremote_avg_trieu").isNotNull(),
            F.round(F.col("remote_avg_trieu") - F.col("nonremote_avg_trieu"), 2)
        ).otherwise(None)
    ) \
    .orderBy(F.desc("total_jobs"))

print("\n[PIVOT 2] Remote vs Non-Remote theo Location:")
df_pivot_loc.show(truncate=False)


# ═════════════════════════════════════════════════════════════
# PHÂN TÍCH 3: Remote × SKILL — Kỹ năng nào gắn với remote?
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("PHÂN TÍCH 3: Remote × Skill — Kỹ năng nào gắn với Remote nhất?")
print("=" * 65)

# MAP: explode skills
df_skill_remote = df_mapped \
    .filter(F.col("skills_clean").isNotNull() & (F.col("skills_clean") != "")) \
    .withColumn("skill", F.explode(F.split(F.col("skills_clean"), r",\s*"))) \
    .withColumn("skill", F.lower(F.trim(F.col("skill")))) \
    .filter(F.length(F.col("skill")) > 1)

# REDUCE: groupBy(skill) → PIVOT remote columns
df_skill_pivot = df_skill_remote.groupBy("skill") \
    .agg(
        F.countDistinct("url").alias("total_job_count"),
        F.countDistinct(
            F.when(F.col("is_remote") == 1, F.col("url"))
        ).alias("remote_job_count"),
        F.round(
            F.avg(F.when(F.col("is_remote") == 1, F.col("salary_final_vnd"))) / 1e6, 2
        ).alias("remote_avg_trieu"),
        F.round(
            F.avg(F.when(F.col("is_remote") == 0, F.col("salary_final_vnd"))) / 1e6, 2
        ).alias("nonremote_avg_trieu"),
    ) \
    .filter(F.col("total_job_count") >= 5) \
    .withColumn(
        "remote_rate_pct",
        F.round(F.col("remote_job_count") / F.col("total_job_count") * 100, 2)
    ) \
    .orderBy(F.desc("remote_rate_pct"))

print("\n[PIVOT 3] Top 15 skill có tỷ lệ Remote cao nhất (min 5 job):")
df_skill_pivot.show(15, truncate=False)


# ═════════════════════════════════════════════════════════════
# PHÂN TÍCH 4 (BONUS): Remote × Level × Location — Mini 3D Cube
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("PHÂN TÍCH 4 (BONUS): Remote × Level × Location — Mini Cube")
print("=" * 65)

# Chỉ lấy các combination có đủ dữ liệu (>= 3 job)
df_cube_mini = df_mapped.groupBy("location_group", "level_group", "remote_label") \
    .agg(
        F.count("*").alias("job_count"),
        F.round(F.avg("salary_final_vnd") / 1e6, 2).alias("avg_salary_trieu"),
        F.round(F.min("salary_final_vnd") / 1e6, 2).alias("min_salary_trieu"),
        F.round(F.max("salary_final_vnd") / 1e6, 2).alias("max_salary_trieu"),
    ) \
    .filter(F.col("job_count") >= 3) \
    .orderBy("location_group", "level_group", "remote_label")

print("\n[CUBE MINI] Remote × Level × Location:")
df_cube_mini.show(40, truncate=False)


# ─────────────────────────────────────────────────────────────
# BƯỚC 9: Lưu kết quả
# ─────────────────────────────────────────────────────────────
outputs = {
    "job7_remote_by_level.csv":    df_pivot_level_out,
    "job7_remote_by_location.csv": df_pivot_loc,
    "job7_remote_by_skill.csv":    df_skill_pivot,
    "job7_remote_cube_mini.csv":   df_cube_mini,
}

for fname, df_out in outputs.items():
    local_path = f"{LOCAL_DIR}/{fname}"
    df_out.toPandas().to_csv(local_path, index=False, encoding="utf-8-sig")
    print(f"Đã lưu local: {local_path}")

    hdfs_path = f"{HDFS_OUT}/{fname.replace('.csv', '')}"
    df_out.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(hdfs_path)
    print(f"Đã lưu HDFS : {hdfs_path}")

spark.stop()
print("\nJob 7 hoàn tất!")
