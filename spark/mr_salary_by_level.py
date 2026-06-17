from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ─────────────────────────────────────────────────────────────
# BƯỚC 1: Khởi tạo SparkSession
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Job3_SalaryByLevel") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("JOB 3: SALARY STATISTICS BY JOB LEVEL")
print("=" * 60)


# ─────────────────────────────────────────────────────────────
# BƯỚC 2: Đọc dữ liệu từ HDFS
# ─────────────────────────────────────────────────────────────
HDFS_INPUT = "hdfs://tanyen-master:9000/project/jobs/Data_ITJOB_Cleaned.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv(HDFS_INPUT)

print(f"Tổng bản ghi: {df.count()}")


# ─────────────────────────────────────────────────────────────
# BƯỚC 3 (MAP PHASE): Lọc & chuẩn hóa job_level
# ─────────────────────────────────────────────────────────────
# Ánh xạ (mapping) các giá trị job_level về dạng chuẩn
# "Junior/Fresher" → tách ra hoặc gộp vào Fresher/Junior tùy bài

df_mapped = df.filter(
    F.col("salary_final_vnd").isNotNull() &
    (F.col("salary_final_vnd") > 0) &
    F.col("job_level").isNotNull()
).withColumn(
    # Chuẩn hóa: "Junior/Fresher" → "Fresher/Junior" thống nhất
    "level_clean",
    F.when(F.col("job_level").isin("Junior/Fresher", "Fresher"),
           "Fresher/Junior")
     .otherwise(F.col("job_level"))
).withColumn(
    # Thêm cột thứ tự để sort đúng bậc lương
    "level_order",
    F.when(F.col("job_level") == "Fresher", 1)
     .when(F.col("job_level") == "Junior/Fresher", 1)
     .when(F.col("job_level") == "Junior", 2)
     .when(F.col("job_level") == "Mid-level", 3)
     .when(F.col("job_level") == "Senior", 4)
     .when(F.col("job_level") == "Lead", 5)
     .when(F.col("job_level") == "Manager", 6)
     .otherwise(99)  # Undefined → cuối
)

print("\n[MAP OUTPUT] Mẫu sau khi chuẩn hóa:")
df_mapped.select("job_level", "level_clean", "salary_final_vnd").show(8, truncate=False)
print(f"Phân bố job_level:")
df_mapped.groupBy("level_clean").count().orderBy("count", ascending=False).show()


# ─────────────────────────────────────────────────────────────
# BƯỚC 4 (REDUCE PHASE): Thống kê đầy đủ theo level
# ─────────────────────────────────────────────────────────────
# percentile_approx(col, [p1, p2, ...]) → tính phân vị xấp xỉ
# Rất hiệu quả vì không cần sort toàn bộ dataset

df_stats = df_mapped.groupBy("level_clean", "level_order") \
    .agg(
        F.count("*").alias("so_luong_job"),

        # Thống kê cơ bản
        F.round(F.min("salary_final_vnd") / 1e6, 2).alias("min_trieu"),
        F.round(F.max("salary_final_vnd") / 1e6, 2).alias("max_trieu"),
        F.round(F.avg("salary_final_vnd") / 1e6, 2).alias("trung_binh_trieu"),
        F.round(F.stddev("salary_final_vnd") / 1e6, 2).alias("do_lech_chuan_trieu"),

        # Phân vị: [P25, P50(Median), P75, P90]
        # Tham số accuracy=1000: độ chính xác, càng cao càng đúng nhưng chậm hơn
        F.round(
            F.percentile_approx("salary_final_vnd", 0.25, accuracy=1000) / 1e6, 2
        ).alias("p25_trieu"),
        F.round(
            F.percentile_approx("salary_final_vnd", 0.50, accuracy=1000) / 1e6, 2
        ).alias("median_p50_trieu"),
        F.round(
            F.percentile_approx("salary_final_vnd", 0.75, accuracy=1000) / 1e6, 2
        ).alias("p75_trieu"),
        F.round(
            F.percentile_approx("salary_final_vnd", 0.90, accuracy=1000) / 1e6, 2
        ).alias("p90_trieu"),
    ) \
    .orderBy("level_order")

print("\n[REDUCE OUTPUT] Thống kê lương theo cấp bậc (đơn vị: triệu VND):")
df_stats.drop("level_order").show(truncate=False)


# ─────────────────────────────────────────────────────────────
# BƯỚC 5 (BONUS): Tính IQR và phát hiện outlier
# ─────────────────────────────────────────────────────────────
# IQR (Interquartile Range) = P75 - P25
# Outlier: ngoài [P25 - 1.5*IQR, P75 + 1.5*IQR]

# Tính percentile cho toàn bộ dataset
p25_val = df_mapped.selectExpr("percentile_approx(salary_final_vnd, 0.25, 1000)").collect()[0][0]
p75_val = df_mapped.selectExpr("percentile_approx(salary_final_vnd, 0.75, 1000)").collect()[0][0]
iqr = p75_val - p25_val
lower_fence = p25_val - 1.5 * iqr
upper_fence = p75_val + 1.5 * iqr

print(f"\n[BONUS] IQR Analysis (toàn bộ dataset):")
print(f"  P25 = {p25_val/1e6:.1f}M VND")
print(f"  P75 = {p75_val/1e6:.1f}M VND")
print(f"  IQR = {iqr/1e6:.1f}M VND")
print(f"  Ngưỡng dưới (outlier) = {lower_fence/1e6:.1f}M VND")
print(f"  Ngưỡng trên (outlier) = {upper_fence/1e6:.1f}M VND")

outlier_count = df_mapped.filter(
    (F.col("salary_final_vnd") < lower_fence) |
    (F.col("salary_final_vnd") > upper_fence)
).count()
print(f"  Số bản ghi outlier: {outlier_count} ({outlier_count/df_mapped.count()*100:.1f}%)")


# ─────────────────────────────────────────────────────────────
# BƯỚC 6 (BONUS): Window Function - xếp hạng theo level
# ─────────────────────────────────────────────────────────────
# Thêm cột rank lương trong từng level
window_spec = Window.partitionBy("level_clean").orderBy(F.desc("salary_final_vnd"))

df_ranked = df_mapped.withColumn("rank_in_level", F.rank().over(window_spec))

print("\n[BONUS] Top 3 lương cao nhất trong mỗi cấp bậc:")
df_ranked.filter(F.col("rank_in_level") <= 3) \
    .select("level_clean", "rank_in_level", "title_clean", "company",
            F.round(F.col("salary_final_vnd") / 1e6, 1).alias("luong_trieu")) \
    .orderBy("level_order", "rank_in_level") \
    .show(20, truncate=40)


# ─────────────────────────────────────────────────────────────
# BƯỚC 7: Lưu kết quả
# ─────────────────────────────────────────────────────────────
# Lưu lên HDFS
HDFS_OUTPUT = "hdfs://tanyen-master:9000/project/output/job3_salary_by_level"
df_stats.drop("level_order").coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(HDFS_OUTPUT)
print(f"\n✅ Đã lưu lên HDFS: {HDFS_OUTPUT}")

# Lưu local
LOCAL_OUTPUT = "/home/tanyen/hadoopyen/project/output/job3_salary_by_level.csv"
df_stats.drop("level_order").toPandas().to_csv(LOCAL_OUTPUT, index=False, encoding="utf-8-sig")
print(f"✅ Đã lưu local: {LOCAL_OUTPUT}")

spark.stop()
print("\nJob 3 hoàn tất!")
