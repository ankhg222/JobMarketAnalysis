import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pymongo import MongoClient

# 1. THIẾT LẬP MÔI TRƯỜNG
os.environ["PYTHONIOENCODING"] = "utf-8"

def main():
    print("[INFO] Khởi tạo tiến trình phân tích Top Skills từ HDFS...")

    # 2. KHỞI TẠO SPARK SESSION (Cấu hình HDFS, bỏ Hive)
    spark = SparkSession.builder \
        .appName("MR_TopSkills_Overall_HDFS") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hnq-master:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 3. KHAI BÁO BIẾN MÔI TRƯỜNG & KẾT NỐI
    OUTPUT_TXT = "/home/hadoophnq/data/top_skills.txt"
    MONGO_URI = "mongodb+srv://quynhquynh748_db_user:123@cluster0.h4o1hpr.mongodb.net/BigDataJobMarket?authSource=admin"
    DB_NAME = "BigDataJobMarket"
    COLLECTION_NAME = "Top_Skills"

    # 4. ĐỌC DỮ LIỆU TỪ HDFS PARQUET (Bỏ qua Hive Table)
    parquet_in_path = "/project/data_warehouse/it_jobs_parquet"
    print(f"[INFO] Đang đọc dữ liệu Parquet từ: {parquet_in_path}")
    df = spark.read.parquet(parquet_in_path)

    garbage_skills = [
        "fresher accepted", "intern", "it intern", "content creator intern", "ltd",
        "giao tiếp", "leadership", "tư vấn & bán hàng", "bán hàng", 
        "sales management", "marketing strategy", "lead generation", 
        "account management", "it asset management", "internal it audit", 
        "fire safety operations", "financial products", "financial", 
        "đấu thầu", "công nghệ thông tin", "kiểm soát tuân thủ", 
        "tư vấn pháp lý", "stakeholder management", "vendor management", 
        "soạn thảo hợp đồng", "kinh doanh dự án"
    ]

    # ── LUỒNG XỬ LÝ CHÍNH: EXPLODE VÀ COUNT ──

    # 1. Lọc bỏ dòng null và nổ chuỗi kỹ năng ngăn cách bởi dấu phẩy
    df_exploded = df.filter(F.col("skills_clean").isNotNull()) \
        .withColumn("skill", F.explode(F.split(F.lower(F.col("skills_clean")), ","))) \
        .withColumn("skill", F.trim(F.col("skill")))

    # 2. Loại bỏ các kỹ năng rỗng và các từ nằm trong Blacklist
    df_filtered = df_exploded.filter((F.col("skill") != "") & (~F.col("skill").isin(garbage_skills)))

    # 3. Gom nhóm đếm số lần xuất hiện và lấy Top 30 kỹ năng phổ biến nhất
    top_skills_df = df_filtered.groupBy("skill") \
        .agg(F.count("*").alias("appearance_count")) \
        .orderBy(F.col("appearance_count").desc()) \
        .limit(30)

    rows = top_skills_df.collect()
    
    # Lấy tổng số job để làm dữ liệu phân tích thêm 
    total_jobs = df.count() 

    # ── BƯỚC 3: GHI FILE TXT LOCAL ──
    print("\n[INFO] Đang xuất báo cáo ra file Text...")
    os.makedirs(os.path.dirname(OUTPUT_TXT), exist_ok=True)
    
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("       TOP 30 KỸ NĂNG CÔNG NGHỆ PHỔ BIẾN NHẤT THỊ TRƯỜNG\n")
        f.write("=" * 60 + "\n")
        f.write(f"{'Rank':<6} {'Skill':<30} {'Jobs Count':>12}\n")
        f.write("-" * 60 + "\n")
        for i, row in enumerate(rows, 1):
            f.write(f"{i:<6} {str(row['skill'])[:30]:<30} {row['appearance_count']:>12}\n")
        f.write("=" * 60 + "\n")
    print(f"[OK] TXT written to Local: {OUTPUT_TXT}")

    # ── BƯỚC 4: ĐẨY LÊN HDFS ──
    print("\n[INFO] Uploading Top Skills to HDFS...")
    try:
        top_skills_df.write.mode("overwrite").json("/project/top_skills")
        print("[OK] Saved a backup copy to HDFS: /project/top_skills")
    except Exception as e:
        print("[ERROR] Failed to write to HDFS:", e)

    # ── BƯỚC 5: ĐẨY LÊN MONGODB ATLAS ──
    print("\n[INFO] Connecting and uploading to MongoDB Atlas...")
    try:
        client = MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        
        col.delete_many({})
        mongo_docs = [r.asDict() for r in rows]
        
        if mongo_docs:
            col.insert_many(mongo_docs)
        print(f"[OK] Uploaded {len(mongo_docs)} rows to MongoDB Collection: '{COLLECTION_NAME}'")
    except Exception as e:
        print("[ERROR] Failed to write to MongoDB Atlas:", e)
    finally:
        if 'client' in locals():
            client.close()

    spark.stop()
    print("\n[DONE] Process finished successfully.")

if __name__ == "__main__":
    main()
