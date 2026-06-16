"""
=============================================================================
Job 13: mr_skill_rarity.py
Người 3 - Nhóm Thống kê & Thuật toán
Mức độ: KHÓ
Kỹ thuật: Custom Formula - Log-based TF-IDF cho skill rarity scoring
=============================================================================

MỤC ĐÍCH:
  Tính điểm "độ hiếm" (rarity score) của từng kỹ năng IT dựa trên:
  - TF  (Term Frequency)   = Skill xuất hiện nhiều → phổ biến → ít hiếm
  - IDF (Inverse Document Frequency) = Biến thể: skill hiếm → IDF cao
  
  CÔNG THỨC CUSTOM:
    rarity_score = log(N / df_skill) × (1 / tf_normalized)
    
    Trong đó:
      N          = Tổng số job
      df_skill   = Số job có kỹ năng này (document frequency)
      tf_norm    = df_skill / N  (tần suất xuất hiện, 0→1)
      
    → Skill càng hiếm → df_skill nhỏ → log(N/df) lớn → score cao

LUỒNG XỬ LÝ (MapReduce logic):
  MAP    → Explode skills_clean: mỗi (job, skill) → 1 bản ghi riêng
  REDUCE → groupBy(skill) → đếm df, tính log-IDF, tính avg lương kèm skill
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
import math


# ─────────────────────────────────────────────────────────────
# BƯỚC 1: Khởi tạo SparkSession
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Job13_SkillRarity") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("JOB 13: SKILL RARITY SCORING (Log/TF-IDF)")
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

# Lọc chỉ giữ các bản ghi có skills
df_with_skills = df.filter(
    F.col("skills_clean").isNotNull() &
    (F.col("skills_clean") != "") &
    F.col("salary_final_vnd").isNotNull() &
    (F.col("salary_final_vnd") > 0)
)

N = df_with_skills.count()  # Tổng số job (N)
print(f"Tổng số job có skills hợp lệ (N): {N}")


# ─────────────────────────────────────────────────────────────
# BƯỚC 3 (MAP PHASE): Explode skills
# ─────────────────────────────────────────────────────────────
# skills_clean có dạng: "python, sql, docker, aws"
# → Cần split thành array, rồi explode: mỗi skill = 1 hàng riêng

# Bước 3a: Split chuỗi skills thành array
df_skills_array = df_with_skills.withColumn(
    "skills_array",
    F.split(F.col("skills_clean"), r",\s*")  # tách bằng dấu phẩy + khoảng trắng
)

# Bước 3b: Explode array → mỗi skill thành 1 dòng
# Đây là MAP chính: (job_id, [skill1, skill2]) → (job_id, skill1), (job_id, skill2)
df_exploded = df_skills_array.withColumn(
    "skill",
    F.explode(F.col("skills_array"))  # 1 hàng → nhiều hàng
).withColumn(
    "skill",
    F.lower(F.trim(F.col("skill")))   # lowercase + trim whitespace
).filter(
    F.length(F.col("skill")) > 1      # bỏ ký tự rác 1 chữ
)

print(f"\n[MAP OUTPUT] Tổng cặp (job, skill) sau explode: {df_exploded.count()}")
df_exploded.select("title_clean", "skill", "salary_final_vnd").show(8, truncate=40)


# ─────────────────────────────────────────────────────────────
# BƯỚC 4 (REDUCE PHASE): Tính Document Frequency (df_skill)
# ─────────────────────────────────────────────────────────────
# df_skill = số job khác nhau chứa skill này
# countDistinct("url") = đếm số job (tránh đếm trùng nếu 1 job có nhiều dòng)

df_skill_stats = df_exploded.groupBy("skill") \
    .agg(
        F.countDistinct("url").alias("df_skill"),           # Document frequency
        F.round(F.avg("salary_final_vnd") / 1e6, 2).alias("avg_salary_trieu"),  # Lương TB
        F.round(F.min("salary_final_vnd") / 1e6, 2).alias("min_salary_trieu"),
        F.round(F.max("salary_final_vnd") / 1e6, 2).alias("max_salary_trieu"),
    )

print(f"\n[REDUCE OUTPUT] Số skill unique: {df_skill_stats.count()}")
df_skill_stats.orderBy(F.desc("df_skill")).show(10, truncate=False)


# ─────────────────────────────────────────────────────────────
# BƯỚC 5: Tính Rarity Score (Custom Log/TF-IDF Formula)
# ─────────────────────────────────────────────────────────────
# Công thức:
#   tf_normalized = df_skill / N              (tần suất: 0 → 1)
#   idf = log(N / (df_skill + 1))            (IDF chuẩn, +1 tránh chia 0)
#   rarity_score = idf / (tf_normalized + 1e-9)
#   → Skill hiếm: df_skill thấp → idf cao, tf thấp → score rất cao
#
# Thêm popularity_score ngược lại:
#   popularity_score = tf_normalized × log(avg_salary + 1)
#   → Skill vừa phổ biến vừa lương cao → giá trị thực tiễn

N_lit = F.lit(float(N))  # Convert N thành literal để dùng trong F.expr

df_with_scores = df_skill_stats \
    .filter(F.col("df_skill") >= 2) \
    .withColumn(
        "tf_normalized",
        F.round(F.col("df_skill") / N_lit, 6)
    ) \
    .withColumn(
        # IDF = log_e(N / (df + 1))
        # Spark không có F.log2 riêng; F.log(base, col) với base=math.e → ln
        "idf_score",
        F.round(F.log(N_lit / (F.col("df_skill") + F.lit(1))), 4)
    ) \
    .withColumn(
        # Rarity score = IDF / TF (skill hiếm → IDF cao, TF thấp)
        "rarity_score",
        F.round(F.col("idf_score") / (F.col("tf_normalized") + F.lit(1e-9)), 2)
    ) \
    .withColumn(
        # Popularity score: phổ biến và lương cao → có giá trị
        "popularity_score",
        F.round(
            F.col("tf_normalized") * F.log(F.col("avg_salary_trieu") + F.lit(1)),
            4
        )
    ) \
    .withColumn(
        # Phân loại rarity
        "rarity_category",
        F.when(F.col("tf_normalized") < 0.02, "⭐ Rất hiếm (<2%)")
         .when(F.col("tf_normalized") < 0.05, "🔹 Hiếm (2-5%)")
         .when(F.col("tf_normalized") < 0.15, "🔸 Trung bình (5-15%)")
         .when(F.col("tf_normalized") < 0.30, "🟡 Phổ biến (15-30%)")
         .otherwise("🟢 Rất phổ biến (>30%)")
    )


# ─────────────────────────────────────────────────────────────
# BƯỚC 6: Hiển thị kết quả phân tích
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("TOP 20 SKILL HIẾM NHẤT (rarity_score cao nhất):")
print("=" * 60)
df_with_scores.select(
    "skill", "df_skill", "tf_normalized", "idf_score",
    "rarity_score", "avg_salary_trieu", "rarity_category"
).orderBy(F.desc("rarity_score")).show(20, truncate=False)

print("\n" + "=" * 60)
print("TOP 20 SKILL PHỔ BIẾN NHẤT (df_skill cao nhất):")
print("=" * 60)
df_with_scores.select(
    "skill", "df_skill", "tf_normalized", "rarity_score",
    "avg_salary_trieu", "rarity_category"
).orderBy(F.desc("df_skill")).show(20, truncate=False)

print("\n" + "=" * 60)
print("TOP 15 SKILL CÓ GIÁ TRỊ CAO (popularity_score - lương cao + phổ biến):")
print("=" * 60)
df_with_scores.select(
    "skill", "df_skill", "avg_salary_trieu", "popularity_score"
).orderBy(F.desc("popularity_score")).show(15, truncate=False)

print("\n" + "=" * 60)
print("PHÂN BỐ THEO rarity_category:")
print("=" * 60)
df_with_scores.groupBy("rarity_category") \
    .agg(
        F.count("*").alias("so_skill"),
        F.round(F.avg("avg_salary_trieu"), 2).alias("avg_salary_tb_trieu")
    ) \
    .orderBy("rarity_category").show(truncate=False)


# ─────────────────────────────────────────────────────────────
# BƯỚC 7: Lưu kết quả
# ─────────────────────────────────────────────────────────────
final_df = df_with_scores.select(
    "skill", "df_skill", "tf_normalized", "idf_score",
    "rarity_score", "popularity_score", "avg_salary_trieu",
    "min_salary_trieu", "max_salary_trieu", "rarity_category"
).orderBy(F.desc("rarity_score"))

# Lưu lên HDFS
HDFS_OUTPUT = "hdfs://tanyen-master:9000/project/output/job13_skill_rarity"
final_df.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(HDFS_OUTPUT)
print(f"\n✅ Đã lưu lên HDFS: {HDFS_OUTPUT}")

# Lưu local
LOCAL_OUTPUT = "/home/tanyen/hadoopyen/project/output/job13_skill_rarity.csv"
final_df.toPandas().to_csv(LOCAL_OUTPUT, index=False, encoding="utf-8-sig")
print(f"✅ Đã lưu local: {LOCAL_OUTPUT}")

spark.stop()
print("\nJob 13 hoàn tất!")
