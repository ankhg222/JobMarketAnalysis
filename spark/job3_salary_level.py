import os
import pymongo
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

MONGO_URI = "mongodb+srv://votin02061998_db_user:votin02061998_db_user@bigdatatin.lqwd4d6.mongodb.net/?appName=BigDataTin"
DB_NAME = "bigdata_project"
COLLECTION_NAME = "Job3_Salary_By_Level"

def main():
    print("=== PIPELINE JOB 3: THỐNG KÊ LƯƠNG THEO CẤP BẬC ===")
    
    # FIX: Đổi địa chỉ kết nối HDFS về hdfs://master:9000
    spark = SparkSession.builder \
        .appName("Pipeline_Job3") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://master:9000") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # ── BƯỚC 1: ĐỌC TỪ KHO PARQUET HDFS ──
    print("[1/5] Đang kết nối và nạp dữ liệu từ HDFS Parquet...")
    df = spark.read.parquet("/project/data_warehouse/it_jobs_parquet")
    print(f"\n[BẰNG CHỨNG 3] Số lượng bản ghi nhận được từ kho: {df.count()} dòng\n")

    # ── BƯỚC 2: THUẬT TOÁN NỘI SUY VÀ TÍNH TOÁN ──
    print("[2/5] Đang chạy logic phân tán nội suy chức danh nghiệp vụ...")
    df = df.withColumn("salary_M", F.col("salary_final_vnd") / 1000000)
    
    level_col = F.col("job_level")
    title_col = F.lower(F.col("title_clean"))
    
    imputed_level = F.when(
        level_col.isin("Undefined", "", "nan") | level_col.isNull(),
        F.when(title_col.rlike("intern|fresher|trainee"), "1. Fresher")
         .when(title_col.rlike("junior"), "2. Junior")
         .when(title_col.rlike("senior"), "4. Senior")
         .when(title_col.rlike("lead"), "5. Lead")
         .when(title_col.rlike("manager|director|head"), "6. Manager")
         .when(title_col.rlike("mid|middle"), "3. Mid-level")
         .otherwise(
             F.when(F.col("yoe_extracted") <= 1, "1. Fresher")
              .when(F.col("yoe_extracted") <= 3, "2. Junior")
              .when(F.col("yoe_extracted") <= 5, "3. Mid-level")
              .when(F.col("yoe_extracted") <= 8, "4. Senior")
              .when(F.col("yoe_extracted") > 8, "6. Manager")
              .otherwise("0. Khác")
         )
    ).otherwise(level_col)

    df_imputed = df.withColumn("job_level_refined", imputed_level)

    result = df_imputed.groupBy("job_level_refined").agg(
        F.count("*").alias("so_luong_job"),
        F.round(F.min("salary_M"), 1).alias("min_salary_M"),
        F.round(F.max("salary_M"), 1).alias("max_salary_M"),
        F.round(F.avg("salary_M"), 1).alias("avg_salary_M")
    ).orderBy("job_level_refined")

    rows = result.collect()
    mongo_docs = [row.asDict() for row in rows]

    # ── BƯỚC 3: XUẤT LOCAL TXT BÁO CÁO ──
    print("[3/5] Đang xử lý ghi file văn bản báo cáo nội bộ...")
    local_path = "/home/hadoopvohuutin/data/mining_results/job3_salary_by_level.txt"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("                 BÁO CÁO MỨC LƯƠNG THEO CẤP BẬC (TRIỆU VNĐ)\n")
        f.write("=" * 80 + "\n")
        f.write(f"{'Cấp Bậc':<20} | {'Số Job':>8} | {'Thấp Nhất':>12} | {'Cao Nhất':>12} | {'Trung Bình':>12}\n")
        f.write("-" * 80 + "\n")
        for r in rows:
            f.write(f"{r['job_level_refined']:<20} | {r['so_luong_job']:>8} | {r['min_salary_M']:>12} | {r['max_salary_M']:>12} | {r['avg_salary_M']:>12}\n")
        f.write("=" * 80 + "\n")
    print(f"[SUCCESS] Đã ghi dữ liệu thành công ra file thật tại: {local_path}")

    # ── BƯỚC 4: GHI RA HDFS (JSON BACKUP) ──
    print("[4/5] Đang đẩy tệp sao lưu JSON lên cụm phân tán HDFS...")
    try:
        result.write.mode("overwrite").json("/project/results/job3_salary_by_level")
        print("[SUCCESS] Đã lưu thành công lên HDFS!")
    except Exception as e:
        print("[ERROR] Thất bại khi ghi HDFS:", e)

    # ── BƯỚC 5: ĐẨY LÊN MONGODB ATLAS ──
    print("[5/5] Đang mở cổng kết nối đồng bộ lên đám mây MongoDB Atlas...")
    try:
        client = pymongo.MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        col.delete_many({})
        if mongo_docs:
            col.insert_many(mongo_docs)
            print(f"[SUCCESS] Đã đẩy thành công {len(mongo_docs)} nhóm dữ liệu lên Cloud MongoDB!")
    except Exception as e:
        print(f"[ERROR] Thất bại khi kết nối Cloud Database: {e}")
    finally:
        if 'client' in locals(): client.close()

    spark.stop()
    print("=== TOÀN BỘ PIPELINE HOÀN THÀNH XUẤT SẮC ===")

if __name__ == "__main__":
    main()
