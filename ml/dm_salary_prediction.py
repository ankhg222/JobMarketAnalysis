"""
=============================================================================
Job 3: dm_salary_prediction.py
Người 1 - Quy luật & Dự đoán
Mức độ: KHÓ
Kỹ thuật: So sánh mô hình dự đoán: LinearRegression vs RandomForest vs XGBoost
=============================================================================

MỤC ĐÍCH:
  Xây dựng và so sánh 3 mô hình dự đoán mức lương (salary_final_vnd) dựa trên:
  - job_level, yoe_extracted (năm kinh nghiệm)
  - location_clean, is_remote
  - skill_count, top skills (TF-IDF features)
  
  PIPELINE:
    1. Feature Engineering (Spark): StringIndexer → OneHotEncoder → VectorAssembler
    2. Tách train/test (80/20)
    3. Train 3 mô hình: LinearRegression, RandomForestRegressor, GBTRegressor (XGBoost-like)
    4. Đánh giá: RMSE, MAE, R²
    5. Cross-validation + Feature Importance
    6. Lưu model và kết quả

LUỒNG XỬ LÝ:
  MAP    → Feature engineering, encode categorical features
  REDUCE → Training, aggregation metrics, comparison
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler,
    StandardScaler, Imputer
)
from pyspark.ml.regression import (
    LinearRegression,
    RandomForestRegressor,
    GBTRegressor  # Gradient Boosted Trees ≈ XGBoost trong Spark MLlib
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import pandas as pd
import json


# ─────────────────────────────────────────────────────────────
# BƯỚC 1: Khởi tạo SparkSession
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Job3_SalaryPrediction") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("JOB 3: SALARY PREDICTION - LR vs RandomForest vs GBT(XGBoost)")
print("=" * 70)


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

df_raw = df.filter(
    F.col("salary_final_vnd").isNotNull() &
    (F.col("salary_final_vnd") > 0)
)
print(f"Tổng bản ghi có lương: {df_raw.count()}")


# ─────────────────────────────────────────────────────────────
# BƯỚC 3 (MAP PHASE): Feature Engineering
# ─────────────────────────────────────────────────────────────

# TOP SKILLS quan trọng nhất (dùng làm binary features)
TOP_SKILLS = [
    "python", "java", "javascript", "sql", "react", "nodejs",
    "docker", "aws", "git", "agile", "linux", "typescript",
    "spring", "mongodb", "kubernetes", "machine learning", "ai",
    "figma", "c++", "flutter"
]

# ── 3a: Tạo các feature từ dữ liệu gốc ──
df_features = df_raw \
    .withColumn(
        # Chuẩn hóa job_level
        "level_norm",
        F.when(F.col("job_level").isin("Junior/Fresher", "Fresher"), "Fresher")
         .when(F.col("job_level").isNull(), "Undefined")
         .otherwise(F.col("job_level"))
    ) \
    .withColumn(
        # Log transform salary → phân phối bình thường hơn (giúp LR)
        "log_salary",
        F.log(F.col("salary_final_vnd"))
    ) \
    .withColumn(
        # YoE: thay null bằng 0
        "yoe_clean",
        F.when(F.col("yoe_extracted").isNull(), 0.0)
         .otherwise(F.col("yoe_extracted"))
    ) \
    .withColumn(
        # Nhóm location
        "location_group",
        F.when(F.col("location_clean").contains("Hà Nội"), "Hanoi")
         .when(F.col("location_clean").contains("Hồ Chí Minh"), "HCM")
         .when(F.col("location_clean").contains("Đà Nẵng"), "Danang")
         .otherwise("Other")
    ) \
    .withColumn(
        # Skill count (đã có sẵn trong dataset)
        "skill_count_clean",
        F.when(F.col("skill_count").isNull(), 0)
         .otherwise(F.col("skill_count"))
    )

# ── 3b: Tạo binary feature cho mỗi top skill ──
# Mỗi cột = 1 nếu job yêu cầu skill đó, 0 nếu không
skills_clean_lower = F.lower(F.col("skills_clean"))

for skill in TOP_SKILLS:
    col_name = "has_" + skill.replace(" ", "_").replace("+", "plus")
    df_features = df_features.withColumn(
        col_name,
        F.when(
            skills_clean_lower.contains(skill), 1.0
        ).otherwise(0.0)
    )

# ── 3c: Các cột sẽ dùng làm features ──
SKILL_COLS = ["has_" + s.replace(" ", "_").replace("+", "plus") for s in TOP_SKILLS]

CATEGORICAL_COLS = ["level_norm", "location_group"]  # cần StringIndexer + OHE
NUMERIC_COLS = ["yoe_clean", "skill_count_clean", "is_remote"] + SKILL_COLS

TARGET_COL = "salary_final_vnd"  # biến mục tiêu

print(f"\n[MAP OUTPUT] Feature engineering xong!")
print(f"  Categorical features: {CATEGORICAL_COLS}")
print(f"  Numeric features: {len(NUMERIC_COLS)} cột")
df_features.select("level_norm", "location_group", "yoe_clean",
                   "skill_count_clean", TARGET_COL).show(5)


# ─────────────────────────────────────────────────────────────
# BƯỚC 4: Tách Train/Test
# ─────────────────────────────────────────────────────────────
# 80% train, 20% test; seed=42 để tái lập kết quả
train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)
print(f"\nTrain size: {train_df.count()} | Test size: {test_df.count()}")


# ─────────────────────────────────────────────────────────────
# BƯỚC 5: Xây dựng Spark ML Pipeline
# ─────────────────────────────────────────────────────────────
# Pipeline = chuỗi các Transformer + Estimator
# Đảm bảo các bước encode LUÔN nhất quán giữa train và test

# ── Stage 1: StringIndexer (chuỗi → số nguyên) ──
indexers = [
    StringIndexer(
        inputCol=col,
        outputCol=col + "_idx",
        handleInvalid="keep"   # giá trị mới khi test → gán index = cuối
    )
    for col in CATEGORICAL_COLS
]

# ── Stage 2: OneHotEncoder (số nguyên → vector one-hot) ──
encoders = [
    OneHotEncoder(
        inputCol=col + "_idx",
        outputCol=col + "_ohe"
    )
    for col in CATEGORICAL_COLS
]

# ── Stage 3: VectorAssembler (gộp tất cả features thành 1 vector) ──
OHE_COLS = [col + "_ohe" for col in CATEGORICAL_COLS]
assembler = VectorAssembler(
    inputCols=OHE_COLS + NUMERIC_COLS,
    outputCol="features_raw",
    handleInvalid="skip"
)

# ── Stage 4: StandardScaler (chuẩn hóa: mean=0, std=1 — giúp LR) ──
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=False   # Sparse vector không dùng withMean=True
)

# ─────────────────────────────────────────────────────────────
# BƯỚC 6 (REDUCE PHASE): Train 3 mô hình
# ─────────────────────────────────────────────────────────────
evaluator_rmse = RegressionEvaluator(
    labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse"
)
evaluator_mae = RegressionEvaluator(
    labelCol=TARGET_COL, predictionCol="prediction", metricName="mae"
)
evaluator_r2 = RegressionEvaluator(
    labelCol=TARGET_COL, predictionCol="prediction", metricName="r2"
)

results = []  # Lưu kết quả các model để so sánh


# ════════════════════════════════════════════
# MODEL 1: Linear Regression
# ════════════════════════════════════════════
print("\n" + "─" * 50)
print("MODEL 1: Linear Regression")
print("─" * 50)

lr = LinearRegression(
    labelCol=TARGET_COL,
    featuresCol="features",
    maxIter=100,
    regParam=0.1,     # L2 regularization (Ridge)
    elasticNetParam=0.0  # 0=Ridge, 1=Lasso
)

pipeline_lr = Pipeline(stages=indexers + encoders + [assembler, scaler, lr])
model_lr = pipeline_lr.fit(train_df)
pred_lr = model_lr.transform(test_df)

rmse_lr = evaluator_rmse.evaluate(pred_lr)
mae_lr  = evaluator_mae.evaluate(pred_lr)
r2_lr   = evaluator_r2.evaluate(pred_lr)

print(f"  RMSE: {rmse_lr/1e6:.3f} triệu VND")
print(f"  MAE : {mae_lr/1e6:.3f} triệu VND")
print(f"  R²  : {r2_lr:.4f}")

results.append({
    "model": "Linear Regression",
    "rmse_trieu": round(rmse_lr / 1e6, 3),
    "mae_trieu":  round(mae_lr / 1e6, 3),
    "r2":         round(r2_lr, 4)
})

# Coefficients của LR (top features)
lr_model = model_lr.stages[-1]
print(f"  Số lượng features: {len(lr_model.coefficients)}")
print(f"  Intercept: {lr_model.intercept/1e6:.3f}M VND")


# ════════════════════════════════════════════
# MODEL 2: Random Forest Regressor
# ════════════════════════════════════════════
print("\n" + "─" * 50)
print("MODEL 2: Random Forest Regressor")
print("─" * 50)

rf = RandomForestRegressor(
    labelCol=TARGET_COL,
    featuresCol="features_raw",   # RF không cần scale
    numTrees=100,                  # Số cây trong rừng
    maxDepth=8,                    # Độ sâu tối đa mỗi cây
    minInstancesPerNode=5,         # Tránh overfit
    featureSubsetStrategy="sqrt",  # Sqrt(n_features) cho mỗi split
    seed=42
)

# RF không cần scaler, dùng features_raw
pipeline_rf = Pipeline(stages=indexers + encoders + [assembler, rf])
model_rf = pipeline_rf.fit(train_df)
pred_rf = model_rf.transform(test_df)

rmse_rf = evaluator_rmse.evaluate(pred_rf)
mae_rf  = evaluator_mae.evaluate(pred_rf)
r2_rf   = evaluator_r2.evaluate(pred_rf)

print(f"  RMSE: {rmse_rf/1e6:.3f} triệu VND")
print(f"  MAE : {mae_rf/1e6:.3f} triệu VND")
print(f"  R²  : {r2_rf:.4f}")

results.append({
    "model": "Random Forest",
    "rmse_trieu": round(rmse_rf / 1e6, 3),
    "mae_trieu":  round(mae_rf / 1e6, 3),
    "r2":         round(r2_rf, 4)
})

# Feature Importance (RF native)
rf_model = model_rf.stages[-1]
feature_names = OHE_COLS + NUMERIC_COLS  # tên feature sau OHE
importances = rf_model.featureImportances.toArray()

# Lấy top 15 feature quan trọng nhất
import numpy as np
top_idx = np.argsort(importances)[::-1][:15]
print("\n  Top 15 Feature Importances (RF):")
for i in top_idx:
    if i < len(feature_names):
        print(f"    {feature_names[i]:35s}: {importances[i]:.4f}")


# ════════════════════════════════════════════
# MODEL 3: GBT Regressor (≈ XGBoost)
# ════════════════════════════════════════════
print("\n" + "─" * 50)
print("MODEL 3: GBT Regressor (Gradient Boosted Trees ≈ XGBoost)")
print("─" * 50)

gbt = GBTRegressor(
    labelCol=TARGET_COL,
    featuresCol="features_raw",
    maxIter=100,         # Số boosting rounds
    maxDepth=5,          # Cây nông hơn RF để tránh overfit
    stepSize=0.1,        # Learning rate
    subsamplingRate=0.8, # Tỉ lệ sample mỗi round (giống XGBoost's subsample)
    seed=42
)

pipeline_gbt = Pipeline(stages=indexers + encoders + [assembler, gbt])
model_gbt = pipeline_gbt.fit(train_df)
pred_gbt = model_gbt.transform(test_df)

rmse_gbt = evaluator_rmse.evaluate(pred_gbt)
mae_gbt  = evaluator_mae.evaluate(pred_gbt)
r2_gbt   = evaluator_r2.evaluate(pred_gbt)

print(f"  RMSE: {rmse_gbt/1e6:.3f} triệu VND")
print(f"  MAE : {mae_gbt/1e6:.3f} triệu VND")
print(f"  R²  : {r2_gbt:.4f}")

results.append({
    "model": "GBT (≈XGBoost)",
    "rmse_trieu": round(rmse_gbt / 1e6, 3),
    "mae_trieu":  round(mae_gbt / 1e6, 3),
    "r2":         round(r2_gbt, 4)
})

# Feature Importance (GBT)
gbt_model = model_gbt.stages[-1]
gbt_importances = gbt_model.featureImportances.toArray()
top_gbt_idx = np.argsort(gbt_importances)[::-1][:15]
print("\n  Top 15 Feature Importances (GBT):")
for i in top_gbt_idx:
    if i < len(feature_names):
        print(f"    {feature_names[i]:35s}: {gbt_importances[i]:.4f}")


# ─────────────────────────────────────────────────────────────
# BƯỚC 7: Bảng so sánh kết quả
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("📊 BẢNG SO SÁNH KẾT QUẢ 3 MÔ HÌNH:")
print("=" * 70)
df_results = pd.DataFrame(results)
df_results["rank_rmse"] = df_results["rmse_trieu"].rank().astype(int)
print(df_results.to_string(index=False))

best_model_name = df_results.loc[df_results["r2"].idxmax(), "model"]
best_r2         = df_results["r2"].max()
best_rmse       = df_results.loc[df_results["r2"].idxmax(), "rmse_trieu"]
print(f"\n🏆 Mô hình tốt nhất: {best_model_name}")
print(f"   R² = {best_r2:.4f} | RMSE = {best_rmse:.3f} triệu VND")


# ─────────────────────────────────────────────────────────────
# BƯỚC 8: Dự đoán mẫu với mô hình tốt nhất
# ─────────────────────────────────────────────────────────────
print("\n" + "─" * 60)
print("Kết quả dự đoán vs. thực tế (20 mẫu test - GBT):")
print("─" * 60)
pred_gbt.select(
    "title_clean",
    "level_norm",
    F.round(F.col("salary_final_vnd") / 1e6, 1).alias("thuc_te_trieu"),
    F.round(F.col("prediction") / 1e6, 1).alias("du_doan_trieu"),
    F.round(F.abs(F.col("prediction") - F.col("salary_final_vnd")) / 1e6, 1).alias("sai_so_trieu")
).orderBy("sai_so_trieu").show(20, truncate=40)


# ─────────────────────────────────────────────────────────────
# BƯỚC 9: Lưu kết quả
# ─────────────────────────────────────────────────────────────

# Lưu bảng so sánh lên HDFS
df_results_spark = spark.createDataFrame(df_results)
HDFS_OUTPUT = "hdfs://tanyen-master:9000/project/output/job3_salary_prediction"
df_results_spark.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(HDFS_OUTPUT)
print(f"\n✅ Đã lưu so sánh models lên HDFS: {HDFS_OUTPUT}")

# Lưu local
LOCAL_OUTPUT = "/home/tanyen/hadoopyen/project/output/job3_model_comparison.csv"
df_results.to_csv(LOCAL_OUTPUT, index=False, encoding="utf-8-sig")
print(f"✅ Đã lưu local: {LOCAL_OUTPUT}")

# Lưu predictions của GBT
pred_local = "/home/tanyen/hadoopyen/project/output/job3_gbt_predictions.csv"
pred_gbt.select(
    "title_clean", "company", "level_norm", "yoe_clean",
    F.round("salary_final_vnd", 0).alias("actual_vnd"),
    F.round("prediction", 0).alias("predicted_vnd"),
    F.round(F.abs(F.col("prediction") - F.col("salary_final_vnd")), 0).alias("error_vnd")
).toPandas().to_csv(pred_local, index=False, encoding="utf-8-sig")
print(f"✅ Đã lưu predictions GBT: {pred_local}")

# Lưu model GBT để dùng sau
MODEL_PATH = "hdfs://tanyen-master:9000/project/models/gbt_salary_model"
model_gbt.write().overwrite().save(MODEL_PATH)
print(f"✅ Đã lưu GBT model lên HDFS: {MODEL_PATH}")

spark.stop()
print("\nJob 3 (Salary Prediction) hoàn tất!")
