import os
import pymongo
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Cấu hình đồng bộ hệ thống của bạn
MONGO_URI = "mongodb+srv://votin02061998_db_user:votin02061998_db_user@bigdatatin.lqwd4d6.mongodb.net/?appName=BigDataTin"
DB_NAME = "bigdata_project"
COLLECTION_NAME = "Job12_Salary_Brackets"

def main():
    print("=== PIPELINE JOB 12: PHÂN BỐ TẦNG LƯƠNG (SALARY BRACKETING) ===")
    
    # Khởi tạo Spark Session cục bộ kết nối HDFS thật
    spark = SparkSession.builder \
        .appName("Pipeline_Job12") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://master:9000") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # ── BƯỚC 1: READ TỪ HDFS PARQUET WAREHOUSE ──
    print("[1/5] Đang nạp dữ liệu tinh sạch từ HDFS Parquet...")
    df = spark.read.parquet("/project/data_warehouse/it_jobs_parquet")
    print(f"[BẰNG CHỨNG] Nhận được {df.count()} dòng từ Data Warehouse.")

    # ── BƯỚC 2: XỬ LÝ BIẾN ĐỔI (TRANSFORM) ──
    print("[2/5] Đang thực hiện thuật toán phân nhóm lương theo logic thực tế...")
    df_bracketed = df.withColumn("salary_bucket",
        F.when(F.col("salary_final_vnd") < 15000000, "1. Dưới 15 Triệu")
         .when((F.col("salary_final_vnd") >= 15000000) & (F.col("salary_final_vnd") < 30000000), "2. Từ 15 - 30 Triệu")
         .when((F.col("salary_final_vnd") >= 30000000) & (F.col("salary_final_vnd") < 50000000), "3. Từ 30 - 50 Triệu")
         .when((F.col("salary_final_vnd") >= 50000000) & (F.col("salary_final_vnd") < 80000000), "4. Từ 50 - 80 Triệu")
         .otherwise("5. Trên 80 Triệu")
    )
    
    # Gom nhóm và đếm số lượng tuyển dụng
    result = df_bracketed.groupBy("salary_bucket").agg(F.count("*").alias("job_count")).orderBy("salary_bucket")
    
    # Gom kết quả về Driver dưới dạng Dictionary phục vụ Multi-Sink
    rows = result.collect()
    mongo_docs = [row.asDict() for row in rows]

    # ── BƯỚC 3: SINK 1 - GHI FILE LOCAL TXT BÁO CÁO ──
    print("[3/5] Đang xử lý ghi tệp văn bản báo cáo nội bộ...")
    local_path = "/home/hadoopvohuutin/data/mining_results/job12_salary_brackets.txt"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        f.write("=" * 55 + "\n")
        f.write("       BÁO CÁO THỐNG KÊ PHÂN BỐ TẦNG LƯƠNG IT\n")
        f.write("=" * 55 + "\n")
        f.write(f"{'Phân Khúc Khoảng Lương':<30} | {'Số Lượng Job':>15}\n")
        f.write("-" * 55 + "\n")
        for row in rows:
            f.write(f"{row['salary_bucket']:<30} | {row['job_count']:>15}\n")
        f.write("=" * 55 + "\n")
    print(f"[SUCCESS] Đã ghi báo cáo văn bản thành công tại: {local_path}")

    # ── BƯỚC 4: SINK 2 - GHI BACKUP JSON LÊN HDFS ──
    print("[4/5] Đang đồng bộ bản sao lưu JSON lên cụm phân tán HDFS...")
    try:
        result.write.mode("overwrite").json("/project/results/job12_salary_brackets")
        print("[SUCCESS] Đã lưu thành công lên HDFS: /project/results/job12_salary_brackets")
    except Exception as e:
        print("[ERROR] Thất bại khi ghi HDFS:", e)

    # ── BƯỚC 5: SINK 3 - ĐẨY DỮ LIỆU LÊN CLOUD MONGODB ──
    print("[5/5] Đang kết nối đồng bộ dữ liệu lên MongoDB Atlas...")
    try:
        client = pymongo.MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        col.delete_many({}) # Dọn sạch dữ liệu cũ
        if mongo_docs:
            col.insert_many(mongo_docs)
            print(f"[SUCCESS] Đã nạp thành công {len(mongo_docs)} records vào Collection NoSQL: {COLLECTION_NAME}")
    except Exception as e:
        print(f"[ERROR] Thất bại khi kết nối Cloud MongoDB: {e}")
    finally:
        if 'client' in locals():
            client.close()

    spark.stop()
    print("=== PIPELINE JOB 12 HOÀN THÀNH XUẤT SẮC ===\n")

if __name__ == "__main__":
    main()
