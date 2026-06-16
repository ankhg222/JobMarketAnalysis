import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient

def main():
    print("[INFO] Khởi tạo tiến trình phân tích Top Paying Jobs từ HDFS...")

    # 1. KHỞI TẠO SPARK SESSION 
    spark = SparkSession.builder \
        .appName("MR_TopPayingJobs_HDFS") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hnq-master:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 2. KHAI BÁO CÁC BIẾN CẤU HÌNH
    OUTPUT_TXT = "/home/hadoophnq/data/top_paying_jobs.txt"
    MONGO_URI = "mongodb+srv://quynhquynh748_db_user:123@cluster0.h4o1hpr.mongodb.net/BigDataJobMarket?authSource=admin"
    DB_NAME = "BigDataJobMarket"
    COLLECTION_NAME = "Top_Paying_Jobs_By_Level"

    # 3. ĐỌC DỮ LIỆU TỪ HDFS PARQUET 
    parquet_in_path = "/project/data_warehouse/it_jobs_parquet"
    print(f"[INFO] Đang đọc dữ liệu Parquet từ: {parquet_in_path}")
    df = spark.read.parquet(parquet_in_path)

    # 4. LỌC VÀ CHUẨN HÓA DỮ LIỆU CƠ BẢN
    df_clean = df.filter(
        F.col("job_level").isNotNull() & (F.col("salary_final_vnd").cast("double") > 0)
    ).withColumn("salary", F.col("salary_final_vnd").cast("double"))

    # 5. TÁCH job_level CHỨA "/" THÀNH NHIỀU DÒNG (Junior/Fresher -> Junior, Fresher)
    df_split = df_clean.withColumn(
        "job_level_split",
        F.explode(F.split(F.col("job_level"), "/"))
    ).withColumn(
        "job_level_split",
        F.trim(F.col("job_level_split"))
    )

    # 6. WINDOW PARTITION + ROW_NUMBER (Xếp hạng lương theo từng cấp bậc)
    window_spec = Window.partitionBy("job_level_split").orderBy(F.col("salary").desc())
    ranked_df = df_split.withColumn("rank", F.row_number().over(window_spec))

    # Lấy Top 5 và format lại cột lương (chia cho 1 triệu)
    top_jobs = ranked_df.filter(F.col("rank") <= 5) \
        .select(
            F.col("job_level_split").alias("job_level"),
            "rank", "company", "title_clean", "skills_clean",
            F.round(F.col("salary") / 1e6, 1).alias("salary_M")
        ).orderBy("job_level", "rank")

    # Thu thập dữ liệu về máy Local để xuất báo cáo
    rows = top_jobs.collect()

    # 7. XUẤT BÁO CÁO RA FILE TEXT 
    print("[INFO] Đang ghi file báo cáo TXT...")
    os.makedirs(os.path.dirname(OUTPUT_TXT), exist_ok=True)
    
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("=" * 120 + "\n")
        f.write("       TOP 5 CONG VIEC LUONG CAO NHAT THEO CAP BAC\n")
        f.write("=" * 120 + "\n")
        
        hdr = f"{'Level':<14} {'Rank':>4} {'Company':<40} {'Title':<35} {'Salary(M)':>10}"
        f.write(hdr + "\n")
        f.write("-" * 120 + "\n")
        
        for row in rows:
            f.write(f"{str(row['job_level']):<14} {row['rank']:>4} "
                    f"{str(row['company'])[:40]:<40} {str(row['title_clean'])[:35]:<35} "
                    f"{row['salary_M']:>10}\n")
            
        f.write("=" * 120 + "\n")
        
    print(f"[OK] TXT written to Local: {OUTPUT_TXT}")

    print("\n[INFO] Connecting and uploading to MongoDB Atlas...")
    try:
        client = MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        
        # Xóa dữ liệu cũ trước khi nạp mới
        col.delete_many({})
        
        mongo_docs = [r.asDict() for r in rows]
        if mongo_docs:
            col.insert_many(mongo_docs)
        print(f"[OK] Uploaded {len(mongo_docs)} rows to MongoDB Collection: '{COLLECTION_NAME}'")
    except Exception as e:
        print(f"[ERROR] Failed to write to MongoDB Atlas: {e}")
    finally:
        if 'client' in locals():
            client.close()

    spark.stop()
    print("\n[DONE] Process finished successfully.")

if __name__ == "__main__":
    main()
