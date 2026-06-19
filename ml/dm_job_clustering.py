import os
import pymongo
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Cấu hình đồng bộ MongoDB của bạn
MONGO_URI = "mongodb+srv://votin02061998_db_user:votin02061998_db_user@bigdatat>
DB_NAME = "bigdata_project"
COLLECTION_NAME = "Result_ML_Job_Clustering"

def main():
    print("=== PIPELINE MACHINE LEARNING: PHÂN CỤM VIỆC LÀM K-MEANS ===")
    
    spark = SparkSession.builder \
        .appName("Pipeline_ML_KMeans") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://master:9000") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # ── BƯỚC 1: READ TỪ DATA WAREHOUSE (HDFS PARQUET) ──
    print("[1/5] Đang nạp dữ liệu sạch từ HDFS Parquet Warehouse...")
    df = spark.read.parquet("/project/data_warehouse/it_jobs_parquet")
    
    # ── BƯỚC 2: TIỀN XỬ LÝ & TRÍCH XUẤT ĐẶC TRƯNG (FEATURE ENGINEERING) ──
    print("[2/5] Đang xây dựng Vector đặc trưng và chuẩn hóa dữ liệu (StandardS>
    # Đổi lương sang triệu VNĐ để báo cáo in ra đẹp mắt
    df_ml = df.withColumn("salary_M", F.col("salary_final_vnd") / 1000000) \
              .filter((F.col("yoe_extracted").isNotNull()) & (F.col("skill_coun>

    # Gom các cột đặc trưng vào một Vector duy nhất đặt tên là 'raw_features'
    feature_cols = ["salary_M", "yoe_extracted", "skill_count"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features>
    df_vector = assembler.transform(df_ml)

    # Khởi tạo bộ chuẩn hóa StandardScaler để đưa dữ liệu về cùng một thang đo
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", with>
    scaler_model = scaler.fit(df_vector)
    df_scaled = scaler_model.transform(df_vector)

    # ── BƯỚC 3: HUẤN LUYỆN MÔ HÌNH HỌC MÁY K-MEANS (TRAINING) ──
    print("[3/5] Đang tiến hành huấn luyện mô hình K-Means phân tán với K=3 Cụm>
    kmeans = KMeans(featuresCol="features", predictionCol="cluster_id", k=3, se>
    model = kmeans.fit(df_scaled)

    # Dự đoán (Gán nhãn id cụm cho từng dòng công việc)
    df_predictions = model.transform(df_scaled)

    # Đánh giá độ chính xác toán học của mô hình bằng Silhouette Score
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="clus>
    silhouette_score = evaluator.evaluate(df_predictions)
    print(f"\n[BẰNG CHỨNG AI] Chỉ số Silhouette đánh giá chất lượng cụm: {silho>

    # Đọc tâm cụm để suy luận nhãn nghiệp vụ (Độ phức tạp công việc)
    centers = model.clusterCenters()
    print("[INFO] Tọa độ các tâm cụm thu được (Salary_M, YOE, Skill_Count):")
    for i, center in enumerate(centers):
        print(f"   - Cụm {i}: [{center[0]:.1f}M, {center[1]:.1f} năm, {center[2>

    # Thống kê tổng hợp các chỉ số trung bình thực tế của từng cụm để gán nhãn >
    cluster_summary = df_predictions.groupBy("cluster_id").agg(
        F.count("*").alias("so_luong_job"),
        F.round(F.avg("salary_M"), 1).alias("avg_salary_M"),
        F.round(F.avg("yoe_extracted"), 1).alias("avg_yoe"),
        F.round(F.avg("skill_count"), 1).alias("avg_skills")
    ).orderBy("avg_salary_M") # Sắp xếp theo lương để tự động định tầng phân kh>

    summary_rows = cluster_summary.collect()
    
    # Tạo từ điển bản đồ (Mapping dictionary) để ánh xạ ID cụm sang tên nghiệp >
    # Cụm lương thấp nhất = Nhóm 1, Cụm lương cao nhất = Nhóm 3
    mapping_dict = {}
    for index, row in enumerate(summary_rows, 1):
        mapping_dict[row["cluster_id"]] = f"{index}. Độ phức tạp: " + ("Thấp" i>

    # Hàm UDF mini để map nhãn chữ thẳng vào DataFrame kết quả
    map_label_expr = F.create_map([F.lit(x) for row in mapping_dict.items() for>
    df_final_result = df_predictions.withColumn("Complexity_Label", map_label_e>

    # Gom nhóm kết quả cuối cùng sạch sẽ để xuất bản ra các Sinks
    final_summary_df = df_final_result.groupBy("Complexity_Label").agg(
        F.count("*").alias("so_luong_job"),
        F.round(F.avg("salary_M"), 1).alias("avg_salary_M"),
        F.round(F.avg("yoe_extracted"), 1).alias("avg_yoe"),
        F.round(F.avg("skill_count"), 1).alias("avg_skills")
    ).orderBy("Complexity_Label")

    final_rows = final_summary_df.collect()
    mongo_docs = [row.asDict() for row in final_rows]

    # ── BƯỚC 4: SINK 1 & 2 - XUẤT LOCAL TXT BÁO CÁO VÀ JSON HDFS ──

    print("[4/5] Đang xuất báo cáo Học máy ra Local TXT và HDFS sao lưu...")
    local_path = "/home/hadoopvohuutin/data/mining_results/ml_job_clustering_re>
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("           BÁO CÁO KẾT QUẢ PHÂN CỤM THUẬT TOÁN K-MEANS (SPARK >
        f.write(f"           Chỉ số Silhouette đánh giá: {silhouette_score:.4f}>
        f.write("=" * 80 + "\n")
        f.write(f"{'Phân Nhóm Độ Phức Tạp':<25} | {'Số Job':>8} | {'TB Lương (M>
        f.write("-" * 80 + "\n")
        for r in final_rows:
            f.write(f"{r['Complexity_Label']:<25} | {r['so_luong_job']:>8} | {r>
        f.write("=" * 80 + "\n")

    # Ghi bản sao tệp JSON phân tán lên HDFS
    final_summary_df.write.mode("overwrite").json("/project/results/ml_job_clus>

    # ── BƯỚC 5: SINK 3 - ĐỒNG BỘ LÊN MONGODB ATLAS CLOUD ──

    print("[5/5] Đang đẩy kết quả phân cụm thông minh lên MongoDB Atlas Cloud..>
    try:
        client = pymongo.MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        col.delete_many({}) # Reset collection
        if mongo_docs:
            col.insert_many(mongo_docs)
            print(f"[SUCCESS] Đã nạp thành công dữ liệu phân cụm lên MongoDB Cl>
    except Exception as e:
        print(f"[ERROR] Lỗi đồng bộ đám mây: {e}")
    finally:

        if 'client' in locals(): client.close()

    spark.stop()
    print("=== PIPELINE MACHINE LEARNING HOÀN THÀNH XUẤT SẮC ===")

if __name__ == "__main__":
    main()
