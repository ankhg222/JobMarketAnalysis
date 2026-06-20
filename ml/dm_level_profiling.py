import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient

# 1. THIẾT LẬP MÔI TRƯỜNG CHUẨN TRÊN UBUNTU
os.environ["PYTHONIOENCODING"] = "utf-8"

def main():
    print("[INFO] Khởi tạo tiến trình phân tích TF-IDF WordCloud từ HDFS...")

    # 2. KHỞI TẠO SPARK SESSION (Trỏ thẳng về HDFS, cấu hình 4 partitions)
    spark = SparkSession.builder \
        .appName("DM_TFIDF_WordCloud_HDFS") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hnq-master:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 3. KHAI BÁO BIẾN MÔI TRƯỜNG & KẾT NỐI
    OUTPUT_TXT = "/home/hadoophnq/data/tfidf_wordcloud.txt"
    MONGO_URI = "mongodb+srv://quynhquynh748_db_user:123@cluster0.h4o1hpr.mongodb.net/BigDataJobMarket?authSource=admin"
    DB_NAME = "BigDataJobMarket"
    COLLECTION_NAME = "TFIDF_WordCloud"

    # 4. ĐỌC DỮ LIỆU TỪ HDFS PARQUET
    parquet_in_path = "/project/data_warehouse/it_jobs_parquet"
    print(f"[INFO] Đang đọc dữ liệu Parquet từ: {parquet_in_path}")
    df = spark.read.parquet(parquet_in_path)

    # ── BƯỚC 1: XỬ LÝ TÁCH CÁC LEVEL BỊ DÍNH DẤU "/" ──
    df_clean_level = df.filter(F.col("job_level").isNotNull() & F.col("skills_clean").isNotNull()) \
        .withColumn("job_level", F.explode(F.split(F.col("job_level"), "/"))) \
        .withColumn("job_level", F.trim(F.col("job_level"))) \
        .filter(F.col("job_level") != "")

    # ── BƯỚC 2: TÁCH KỸ NĂNG VÀ LỌC TỪ RÁC (BLACKLIST) ──
    garbage_skills = [
        "fresher accepted", "intern", "it intern", "content creator intern", "ltd",
        "giao tiếp", "leadership", "tư vấn & bán hàng", "bán hàng", 
        "sales management", "marketing strategy", "lead generation", 
        "account management", "fire safety operations", "it asset management", 
        "internal it audit", "financial products", "financial", "đấu thầu", 
        "công nghệ thông tin", "kiểm soát tuân thủ", "tư vấn pháp lý", 
        "stakeholder management", "vendor management", "soạn thảo hợp đồng", 
        "kinh doanh dự án"
    ]

    df_skills = df_clean_level \
        .withColumn("skill", F.explode(F.split(F.lower(F.col("skills_clean")), ","))) \
        .withColumn("skill", F.trim(F.col("skill"))) \
        .filter((F.col("skill") != "") & (~F.col("skill").isin(garbage_skills)))

    print("[INFO] Bắt đầu tính toán trọng số TF-IDF...")
    
    # 2.1 Tính TF (Term Frequency)
    skill_level_count = df_skills.groupBy("job_level", "skill").count()
    level_total_skills = df_skills.groupBy("job_level").agg(F.count("*").alias("total_skills_in_level"))
    
    tf_df = skill_level_count.join(level_total_skills, "job_level") \
        .withColumn("tf", F.col("count") / F.col("total_skills_in_level"))

    # 2.2 Tính IDF (Inverse Document Frequency)
    total_levels = df_skills.select("job_level").distinct().count()
    skill_doc_freq = df_skills.groupBy("skill").agg(F.countDistinct("job_level").alias("doc_freq"))
    
    idf_df = skill_doc_freq.withColumn("idf", F.log(F.lit(total_levels) / F.col("doc_freq")))

    # 2.3 Tính điểm TF-IDF
    tfidf_result = tf_df.join(idf_df, "skill").withColumn(
        "tf_idf_score", F.round(F.col("tf") * F.col("idf"), 4)
    )

    # 2.4 Lấy Top 15 kỹ năng đặc trưng nhất mỗi level
    window_tfidf = Window.partitionBy("job_level").orderBy(F.col("tf_idf_score").desc())
    
    wordcloud_df = tfidf_result.withColumn("rank", F.row_number().over(window_tfidf)) \
        .filter(F.col("rank") <= 15) \
        .select("job_level", "skill", "tf_idf_score") \
        .orderBy("job_level", F.col("tf_idf_score").desc())

    rows = wordcloud_df.collect()

    # ── BƯỚC 3: GHI FILE TXT LOCAL ──
    print("\n[INFO] Đang xuất báo cáo ra file Text...")
    os.makedirs(os.path.dirname(OUTPUT_TXT), exist_ok=True)
    
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("       TOP 15 KY NANG NOI BAT (TF-IDF) THEO CAP BAC (ĐÃ CLEAN NHIỄU)\n")
        f.write("=" * 70 + "\n")
        hdr = f"{'Level':<14} {'Skill':<30} {'TF-IDF':>10}"
        f.write(hdr + "\n")
        f.write("-" * 70 + "\n")
        for row in rows:
            f.write(f"{str(row['job_level']):<14} {str(row['skill'])[:30]:<30} {row['tf_idf_score']:>10}\n")
        f.write("=" * 70 + "\n")
    print(f"[OK] TXT written to Local: {OUTPUT_TXT}")

    # ── BƯỚC 4: ĐẨY LÊN HDFS ──
    print("\n[INFO] Uploading TF-IDF WordCloud to HDFS in JSON format...")
    try:
        wordcloud_df.write.mode("overwrite").json("/project/tfidf_wordcloud")
        print("[OK] Saved a backup copy to HDFS: /project/tfidf_wordcloud")
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
