from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os


# ─────────────────────────────────────────────────────────────
# BƯỚC 1: Khởi tạo SparkSession
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Job15_OLAPCube") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 65)
print("JOB 15: OLAP CUBE — ROLLUP/CUBE 3 CHIỀU")
print("Dimensions: Level × Location × YOE Band")
print("Measure: salary_final_vnd")
print("=" * 65)


# ─────────────────────────────────────────────────────────────
# BƯỚC 2: Đọc dữ liệu
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

df = df_raw.filter(
    F.col("salary_final_vnd").isNotNull() &
    (F.col("salary_final_vnd") > 0) &
    F.col("job_level").isNotNull()
)

N_TOTAL = df.count()
print(f"Dataset hợp lệ: {N_TOTAL} bản ghi")


# ─────────────────────────────────────────────────────────────
# BƯỚC 3 (MAP PHASE): Chuẩn hóa 3 chiều OLAP
# ─────────────────────────────────────────────────────────────
# Mục tiêu: mỗi chiều có nhãn rõ ràng, có thứ tự, không null

df_dims = df \
    .withColumn(
        # ── CHIỀU 1: DIM_LEVEL ──
        # Gộp Fresher variants, gán thứ tự 1→7
        "dim_level",
        F.when(F.col("job_level").isin("Fresher", "Junior/Fresher"), "1_Fresher")
         .when(F.col("job_level") == "Junior",    "2_Junior")
         .when(F.col("job_level") == "Mid-level", "3_Mid-level")
         .when(F.col("job_level") == "Senior",    "4_Senior")
         .when(F.col("job_level") == "Lead",      "5_Lead")
         .when(F.col("job_level") == "Manager",   "6_Manager")
         .otherwise(                              "7_Undefined")
    ) \
    .withColumn(
        # ── CHIỀU 2: DIM_LOCATION ──
        # Lấy thành phố chính, gộp các thành phố nhỏ vào "Khác"
        "dim_location",
        F.when(
            F.col("location_clean").contains("Hồ Chí Minh"), "HCM"
        ).when(
            F.col("location_clean").contains("Hà Nội"),      "HN"
        ).when(
            F.col("location_clean").contains("Đà Nẵng"),     "DN"
        ).otherwise("Khác")
    ) \
    .withColumn(
        # ── CHIỀU 3: DIM_YOE_BAND ──
        # Bucketize yoe_extracted thành 5 dải kinh nghiệm
        # Dải được thiết kế theo tiến trình career thực tế:
        #   0-1y: Fresher/Intern
        #   1-3y: Junior đến chín
        #   3-5y: Mid/Senior entry
        #   5-10y: Senior/Lead
        #   10y+: Principal/Expert
        "dim_yoe_band",
        F.when(F.col("yoe_extracted") <= 1,  "A_0-1y")
         .when(F.col("yoe_extracted") <= 3,  "B_1-3y")
         .when(F.col("yoe_extracted") <= 5,  "C_3-5y")
         .when(F.col("yoe_extracted") <= 10, "D_5-10y")
         .otherwise(                         "E_10y+")
    )

print("\n[MAP OUTPUT] Phân bố 3 chiều:")
print("--- dim_level ---")
df_dims.groupBy("dim_level").count().orderBy("dim_level").show()
print("--- dim_location ---")
df_dims.groupBy("dim_location").count().orderBy("dim_location").show()
print("--- dim_yoe_band ---")
df_dims.groupBy("dim_yoe_band").count().orderBy("dim_yoe_band").show()


# ─────────────────────────────────────────────────────────────
# HELPER: Hàm tính aggregation chuẩn
# ─────────────────────────────────────────────────────────────
def agg_measures():
    """Trả về list các aggregation functions cho OLAP measures."""
    return [
        F.count("*").alias("job_count"),
        F.round(F.avg("salary_final_vnd") / 1e6, 2).alias("avg_salary_trieu"),
        F.round(F.min("salary_final_vnd") / 1e6, 2).alias("min_salary_trieu"),
        F.round(F.max("salary_final_vnd") / 1e6, 2).alias("max_salary_trieu"),
        F.round(F.stddev("salary_final_vnd") / 1e6, 2).alias("stddev_trieu"),
        F.round(
            F.percentile_approx("salary_final_vnd", 0.50, 1000) / 1e6, 2
        ).alias("median_trieu"),
        F.round(
            F.percentile_approx("salary_final_vnd", 0.75, 1000) / 1e6, 2
        ).alias("p75_trieu"),
        F.round(
            F.percentile_approx("salary_final_vnd", 0.90, 1000) / 1e6, 2
        ).alias("p90_trieu"),
    ]


# ═════════════════════════════════════════════════════════════
# CUBE LEVEL 0 — DRILL-DOWN: Full 3D Cell (level × location × yoe)
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("CUBE L0 — DRILL-DOWN: Tất cả 3 chiều (level × location × yoe)")
print("=" * 65)

# REDUCE: groupBy 3 chiều đầy đủ → tính measures
# Đây là lõi của OLAP Cube: mỗi hàng = 1 cell trong khối 3D
df_cube_l0 = df_dims.groupBy("dim_level", "dim_location", "dim_yoe_band") \
    .agg(*agg_measures()) \
    .withColumn("rollup_level", F.lit("L0: Level×Location×YOE"))  # metadata OLAP
    
# Lọc bỏ cell ít dữ liệu (< 3 job) để tránh kết quả không đáng tin
df_cube_l0 = df_cube_l0.filter(F.col("job_count") >= 3)

print(f"[L0] Tổng số cell đầy đủ (≥3 job): {df_cube_l0.count()}")
df_cube_l0.orderBy("dim_level", "dim_location", "dim_yoe_band").show(30, truncate=False)


# ═════════════════════════════════════════════════════════════
# CUBE LEVEL 1 — ROLLUP-1: (level × location) — cuộn YOE lên
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("CUBE L1 — ROLLUP: Level × Location (cuộn YOE → ALL)")
print("=" * 65)

# ROLLUP: groupBy 2 chiều, gán dim_yoe_band = "ALL" để đánh dấu đã rollup
df_cube_l1 = df_dims.groupBy("dim_level", "dim_location") \
    .agg(*agg_measures()) \
    .withColumn("dim_yoe_band", F.lit("ALL")) \
    .withColumn("rollup_level", F.lit("L1: Level×Location"))

print(f"[L1] Số cell (level × location): {df_cube_l1.count()}")
df_cube_l1.orderBy("dim_level", "dim_location").show(30, truncate=False)


# ═════════════════════════════════════════════════════════════
# CUBE LEVEL 2 — ROLLUP-2: (level × yoe) — cuộn Location lên
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("CUBE L2 — ROLLUP: Level × YOE (cuộn Location → ALL)")
print("=" * 65)

df_cube_l2 = df_dims.groupBy("dim_level", "dim_yoe_band") \
    .agg(*agg_measures()) \
    .withColumn("dim_location", F.lit("ALL")) \
    .withColumn("rollup_level", F.lit("L2: Level×YOE"))

print(f"[L2] Số cell (level × yoe): {df_cube_l2.count()}")
df_cube_l2.orderBy("dim_level", "dim_yoe_band").show(30, truncate=False)


# ═════════════════════════════════════════════════════════════
# CUBE LEVEL 3 — ROLLUP-3: (location × yoe) — cuộn Level lên
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("CUBE L3 — ROLLUP: Location × YOE (cuộn Level → ALL)")
print("=" * 65)

df_cube_l3 = df_dims.groupBy("dim_location", "dim_yoe_band") \
    .agg(*agg_measures()) \
    .withColumn("dim_level",    F.lit("ALL")) \
    .withColumn("rollup_level", F.lit("L3: Location×YOE"))

print(f"[L3] Số cell (location × yoe): {df_cube_l3.count()}")
df_cube_l3.orderBy("dim_location", "dim_yoe_band").show(25, truncate=False)


# ═════════════════════════════════════════════════════════════
# CUBE LEVEL 4 — ROLLUP-4: Subtotals 1 chiều
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("CUBE L4 — ROLLUP: Subtotals 1 chiều")
print("=" * 65)

# Subtotal theo Level
df_sub_level = df_dims.groupBy("dim_level") \
    .agg(*agg_measures()) \
    .withColumn("dim_location", F.lit("ALL")) \
    .withColumn("dim_yoe_band", F.lit("ALL")) \
    .withColumn("rollup_level", F.lit("L4a: Level subtotal"))

# Subtotal theo Location
df_sub_loc = df_dims.groupBy("dim_location") \
    .agg(*agg_measures()) \
    .withColumn("dim_level",    F.lit("ALL")) \
    .withColumn("dim_yoe_band", F.lit("ALL")) \
    .withColumn("rollup_level", F.lit("L4b: Location subtotal"))

# Subtotal theo YOE
df_sub_yoe = df_dims.groupBy("dim_yoe_band") \
    .agg(*agg_measures()) \
    .withColumn("dim_level",    F.lit("ALL")) \
    .withColumn("dim_location", F.lit("ALL")) \
    .withColumn("rollup_level", F.lit("L4c: YOE subtotal"))

print("\n[L4a] Subtotal theo Level:")
df_sub_level.orderBy("dim_level").show(truncate=False)

print("\n[L4b] Subtotal theo Location:")
df_sub_loc.orderBy("dim_location").show(truncate=False)

print("\n[L4c] Subtotal theo YOE Band:")
df_sub_yoe.orderBy("dim_yoe_band").show(truncate=False)


# ═════════════════════════════════════════════════════════════
# CUBE LEVEL 5 — GRAND TOTAL
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("CUBE L5 — GRAND TOTAL (toàn bộ dataset)")
print("=" * 65)

df_grand = df_dims.agg(*agg_measures()) \
    .withColumn("dim_level",    F.lit("ALL")) \
    .withColumn("dim_location", F.lit("ALL")) \
    .withColumn("dim_yoe_band", F.lit("ALL")) \
    .withColumn("rollup_level", F.lit("L5: Grand Total"))

print("\n[L5] Grand Total:")
df_grand.show(truncate=False)


# ═════════════════════════════════════════════════════════════
# FULL OLAP CUBE: Union tất cả levels thành 1 bảng phẳng
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("FULL OLAP CUBE: Union tất cả rollup levels")
print("=" * 65)

# Đảm bảo cùng schema trước khi union
cols = ["dim_level", "dim_location", "dim_yoe_band", "rollup_level",
        "job_count", "avg_salary_trieu", "median_trieu", "p75_trieu", "p90_trieu",
        "min_salary_trieu", "max_salary_trieu", "stddev_trieu"]

df_full_cube = df_cube_l0.select(cols) \
    .union(df_cube_l1.select(cols)) \
    .union(df_cube_l2.select(cols)) \
    .union(df_cube_l3.select(cols)) \
    .union(df_sub_level.select(cols)) \
    .union(df_sub_loc.select(cols)) \
    .union(df_sub_yoe.select(cols)) \
    .union(df_grand.select(cols))

print(f"[FULL CUBE] Tổng số hàng trong OLAP Cube: {df_full_cube.count()}")


# ═════════════════════════════════════════════════════════════
# OLAP SLICE — Cắt theo 1 chiều cố định
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("OLAP SLICE 1: Cố định Location = 'HCM' → Phân tích Level × YOE")
print("=" * 65)

# SLICE: filter location = HCM, groupBy(level, yoe)
df_slice_hcm = df_dims.filter(F.col("dim_location") == "HCM") \
    .groupBy("dim_level", "dim_yoe_band") \
    .agg(*agg_measures()) \
    .filter(F.col("job_count") >= 3) \
    .orderBy("dim_level", "dim_yoe_band")

print(f"[SLICE HCM] Số cell: {df_slice_hcm.count()}")
df_slice_hcm.show(25, truncate=False)


print("\n" + "=" * 65)
print("OLAP SLICE 2: Cố định Level = 'Senior' → Phân tích Location × YOE")
print("=" * 65)

df_slice_senior = df_dims.filter(F.col("dim_level") == "4_Senior") \
    .groupBy("dim_location", "dim_yoe_band") \
    .agg(*agg_measures()) \
    .filter(F.col("job_count") >= 3) \
    .orderBy("dim_location", "dim_yoe_band")

print(f"[SLICE Senior] Số cell: {df_slice_senior.count()}")
df_slice_senior.show(25, truncate=False)


# ═════════════════════════════════════════════════════════════
# OLAP DICE — Lọc nhiều chiều đồng thời
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("OLAP DICE: Senior+Lead × HCM+HN × 3-10y kinh nghiệm")
print("(Phân tích 'Senior IT ở 2 thành phố lớn, kinh nghiệm vừa đủ')")
print("=" * 65)

df_dice = df_dims.filter(
    F.col("dim_level").isin("4_Senior", "5_Lead") &
    F.col("dim_location").isin("HCM", "HN") &
    F.col("dim_yoe_band").isin("C_3-5y", "D_5-10y")
).groupBy("dim_level", "dim_location", "dim_yoe_band") \
 .agg(*agg_measures()) \
 .orderBy("dim_level", "dim_location", "dim_yoe_band")

print(f"[DICE] Số cell sau DICE filter: {df_dice.count()}")
df_dice.show(truncate=False)


# ═════════════════════════════════════════════════════════════
# WINDOW FUNCTION: Phần trăm đóng góp so với subtotal
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("WINDOW: % lương so với subtotal theo Level (L1 cube)")
print("=" * 65)

# Tính tổng job_count theo level để tính %
win_level = Window.partitionBy("dim_level")

df_l1_with_pct = df_cube_l1 \
    .withColumn(
        "total_jobs_in_level",
        F.sum("job_count").over(win_level)
    ) \
    .withColumn(
        "pct_of_level",
        F.round(F.col("job_count") / F.col("total_jobs_in_level") * 100, 2)
    ) \
    .withColumn(
        # Chênh lệch lương location so với avg của level đó
        "delta_vs_level_avg",
        F.round(
            F.col("avg_salary_trieu") -
            F.round(F.avg("avg_salary_trieu").over(win_level), 2),
            2
        )
    )

print("[WINDOW] % theo level và delta lương so với level avg:")
df_l1_with_pct.select(
    "dim_level", "dim_location",
    "job_count", "pct_of_level",
    "avg_salary_trieu", "delta_vs_level_avg"
).orderBy("dim_level", F.desc("avg_salary_trieu")).show(30, truncate=False)


# ═════════════════════════════════════════════════════════════
# INSIGHT: Tìm "Sweet Spot" — cell lương cao nhất
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("INSIGHT: TOP 10 'Sweet Spots' — (Level × Location × YOE)")
print("theo lương trung bình (min 5 job)")
print("=" * 65)

df_sweet_spot = df_cube_l0 \
    .filter(F.col("job_count") >= 5) \
    .orderBy(F.desc("avg_salary_trieu")) \
    .limit(10)

df_sweet_spot.select(
    "dim_level", "dim_location", "dim_yoe_band",
    "job_count", "avg_salary_trieu", "median_trieu",
    "p75_trieu", "p90_trieu"
).show(truncate=False)

print("\nTOP 10 'Value Spots' — cao nhất theo Median (ổn định hơn mean):")
df_cube_l0.filter(F.col("job_count") >= 5) \
    .orderBy(F.desc("median_trieu")) \
    .limit(10) \
    .select(
        "dim_level", "dim_location", "dim_yoe_band",
        "job_count", "avg_salary_trieu", "median_trieu", "p75_trieu"
    ).show(truncate=False)

outputs = {
    "job15_olap_full_cube.csv":     df_full_cube.orderBy(
                                        "rollup_level", "dim_level",
                                        "dim_location", "dim_yoe_band"),
    "job15_olap_l0_3d_cell.csv":    df_cube_l0.orderBy(
                                        "dim_level", "dim_location", "dim_yoe_band"),
    "job15_olap_l1_level_loc.csv":  df_cube_l1.drop("rollup_level").orderBy(
                                        "dim_level", "dim_location"),
    "job15_olap_l2_level_yoe.csv":  df_cube_l2.drop("rollup_level").orderBy(
                                        "dim_level", "dim_yoe_band"),
    "job15_olap_slice_hcm.csv":     df_slice_hcm,
    "job15_olap_dice_senior.csv":   df_dice,
    "job15_olap_sweet_spots.csv":   df_cube_l0.filter(F.col("job_count") >= 5)
                                        .orderBy(F.desc("avg_salary_trieu")),
}

for fname, df_out in outputs.items():
    local_path = f"{LOCAL_DIR}/{fname}"
    df_out.toPandas().to_csv(local_path, index=False, encoding="utf-8-sig")
    print(f"Lưu local: {local_path}")

    hdfs_path = f"{HDFS_OUT}/{fname.replace('.csv', '')}"
    df_out.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(hdfs_path)
    print(f"Lưu HDFS : {hdfs_path}")

spark.stop()
print("\nJob 15 hoàn tất!")
