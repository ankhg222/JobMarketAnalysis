"""
=============================================================================
Job 12: mr_salary_bracketing.py
Người 3 - Nhóm Thống kê & Thuật toán
Mức độ: DỄ
Kỹ thuật: F.when(), Bucketing/Binning tự động
=============================================================================

MỤC ĐÍCH:
  Phân loại (bracket) mức lương theo các khoảng cố định và thống kê
  số lượng, tỉ lệ % và trung bình lương trong mỗi nhóm.

LUỒNG XỬ LÝ (MapReduce logic):
  MAP    → Gán mỗi bản ghi vào một "bracket" (khoảng lương) bằng F.when()
  REDUCE → groupBy(bracket) → count, avg, tỉ lệ phần trăm

CÁC CỘT SỬ DỤNG:
  - salary_final_vnd: lương (VND)
  - job_level: cấp bậc
  - location_clean: địa điểm
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ─────────────────────────────────────────────────────────────
# BƯỚC 1: Khởi tạo SparkSession
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Job12_SalaryBracketing") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("JOB 12: SALARY BRACKETING")
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

print(f"Tổng số bản ghi đọc được: {df.count()}")
df.printSchema()


# ─────────────────────────────────────────────────────────────
# BƯỚC 3: Lọc và làm sạch dữ liệu
# ─────────────────────────────────────────────────────────────
df_clean = df.filter(
    F.col("salary_final_vnd").isNotNull() &
    (F.col("salary_final_vnd") > 0)
)

print(f"Số bản ghi hợp lệ (có lương): {df_clean.count()}")


# ─────────────────────────────────────────────────────────────
# BƯỚC 4 (MAP PHASE): Tạo cột salary_bracket bằng F.when()
# ─────────────────────────────────────────────────────────────
# F.when() hoạt động như mệnh đề CASE WHEN trong SQL
# Mỗi bản ghi → được ánh xạ vào 1 nhóm lương (bucket)
# Đây chính là bước MAP: key = bracket, value = salary record

df_mapped = df_clean.withColumn(
    "salary_bracket",
    F.when(F.col("salary_final_vnd") < 10_000_000,
           "1. Dưới 10 triệu")
     .when(F.col("salary_final_vnd") < 20_000_000,
           "2. 10–20 triệu")
     .when(F.col("salary_final_vnd") < 30_000_000,
           "3. 20–30 triệu")
     .when(F.col("salary_final_vnd") < 50_000_000,
           "4. 30–50 triệu")
     .when(F.col("salary_final_vnd") < 70_000_000,
           "5. 50–70 triệu")
     .when(F.col("salary_final_vnd") < 100_000_000,
           "6. 70–100 triệu")
     .otherwise("7. Trên 100 triệu")
)

# Xem thử kết quả ánh xạ
print("\n[MAP OUTPUT] Mẫu dữ liệu sau khi gán bracket:")
df_mapped.select("title_clean", "salary_final_vnd", "salary_bracket") \
         .show(10, truncate=40)


# ─────────────────────────────────────────────────────────────
# BƯỚC 5 (REDUCE PHASE): Thống kê theo từng bracket
# ─────────────────────────────────────────────────────────────
# groupBy = shuffle + sort (giống reduce step trong MapReduce thuần)
# Mỗi bracket được gom lại → tính count, avg, min, max

total_count = df_mapped.count()

df_result = df_mapped.groupBy("salary_bracket") \
    .agg(
        F.count("*").alias("so_luong_viec"),                          # Số job trong bracket
        F.round(F.avg("salary_final_vnd"), 0).alias("luong_tb_vnd"),  # Lương trung bình
        F.round(F.min("salary_final_vnd"), 0).alias("luong_min_vnd"), # Lương nhỏ nhất
        F.round(F.max("salary_final_vnd"), 0).alias("luong_max_vnd"), # Lương lớn nhất
    ) \
    .withColumn(
        "ti_le_phan_tram",
        F.round(F.col("so_luong_viec") / total_count * 100, 2)
    ) \
    .orderBy("salary_bracket")

print("\n[REDUCE OUTPUT] Thống kê theo khoảng lương:")
df_result.show(truncate=False)


# ─────────────────────────────────────────────────────────────
# BƯỚC 6 (BONUS): Cross-tab bracket × job_level
# ─────────────────────────────────────────────────────────────
print("\n[BONUS] Phân bố bracket × job_level:")
df_cross = df_mapped.groupBy("salary_bracket", "job_level") \
    .agg(F.count("*").alias("count")) \
    .orderBy("salary_bracket", F.desc("count"))

df_cross.show(30, truncate=False)


# ─────────────────────────────────────────────────────────────
# BƯỚC 7: Lưu kết quả
# ─────────────────────────────────────────────────────────────

# --- Lưu trên HDFS (định dạng CSV) ---
HDFS_OUTPUT = "hdfs://tanyen-master:9000/project/output/job12_salary_brackets"
df_result.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(HDFS_OUTPUT)
print(f"\n✅ Đã lưu kết quả lên HDFS: {HDFS_OUTPUT}")

# --- Lưu local (dùng pandas để ghi 1 file duy nhất) ---
LOCAL_OUTPUT = "/home/tanyen/hadoopyen/project/output/job12_salary_brackets.csv"
df_result.toPandas().to_csv(LOCAL_OUTPUT, index=False, encoding="utf-8-sig")
print(f"✅ Đã lưu kết quả local: {LOCAL_OUTPUT}")

# Lưu cross-tab
LOCAL_CROSS = "/home/tanyen/hadoopyen/project/output/job12_cross_bracket_level.csv"
df_cross.toPandas().to_csv(LOCAL_CROSS, index=False, encoding="utf-8-sig")
print(f"✅ Đã lưu cross-tab local: {LOCAL_CROSS}")

spark.stop()
print("\nJob 12 hoàn tất!")
