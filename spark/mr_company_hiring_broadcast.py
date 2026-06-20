import os

# 1. THIẾT LẬP MÔI TRƯỜNG CHUẨN TRÊN UBUNTU
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient

def main():
    # 2. KHỞI TẠO SPARK SESSION
    spark = SparkSession.builder \
        .appName("MR_CompanyHiring_MultiDimensional_HDFS") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hnq-master:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 3. KHAI BÁO BIẾN MÔI TRƯỜNG & KẾT NỐI
    OUTPUT_TXT = "/home/hadoophnq/data/company_hiring_broadcast.txt"
    MONGO_URI = "mongodb+srv://quynhquynh748_db_user:123@cluster0.h4o1hpr.mongodb.net/BigDataJobMarket?authSource=admin"
    DB_NAME = "BigDataJobMarket"
    COLLECTION_NAME = "Company_Hiring_Broadcast"

    # 4. ĐỌC DỮ LIỆU TRỰC TIẾP TỪ HDFS PARQUET
    parquet_in_path = "/project/data_warehouse/it_jobs_parquet"
    print(f"[INFO] Đang đọc dữ liệu Parquet từ: {parquet_in_path}")
    df = spark.read.parquet(parquet_in_path)
    
    # Lọc dữ liệu thô
    df_valid = df.filter(F.col("company").isNotNull() & (F.col("company") != ""))

    # 5. CHUẨN HÓA TÊN CÔNG TY 
    df_normalized = df_valid.withColumn(
        "company_clean",
        F.when(F.col("company").rlike("(?i)MB|Quân Đội"), "MB Bank")
         .when(F.col("company").rlike("(?i)Navigos"), "Navigos Search")
         .otherwise(F.col("company"))
    )

    # 6. TÍNH TOÁN CÁC CHỈ SỐ TOÀN THỊ TRƯỜNG (BROADCAST MODE)
    market_stats = df_normalized.select(
        F.count("*").alias("total_jobs"),
        F.avg("salary_final_vnd").alias("avg_salary")
    ).collect()[0]

    total_market_jobs = market_stats["total_jobs"]
    market_avg_salary_M = round((market_stats["avg_salary"] or 0) / 1e6, 1)

    # Phát sóng (Broadcast) biến toàn cục cho các Worker Nodes
    broadcast_market_jobs = spark.sparkContext.broadcast(total_market_jobs)
    broadcast_market_avg = spark.sparkContext.broadcast(market_avg_salary_M)

    print(f"[INFO] Total Market Jobs: {broadcast_market_jobs.value}")
    print(f"[INFO] Market Avg Salary: {broadcast_market_avg.value}M VND")

    # 7. THUẬT TOÁN NÂNG CAO: TÌM TOP SKILL CỦA TỪNG CÔNG TY

    garbage_skills = [
        "fresher accepted", "intern", "it intern", "content creator intern", "ltd",
        "giao tiếp", "leadership", "tư vấn & bán hàng", "bán hàng", 
        "sales management", "marketing strategy", "lead generation", 
        "fire safety operations", "it asset management", 
        "internal it audit", "financial products", "financial", "đấu thầu", 
        "công nghệ thông tin", "kiểm soát tuân thủ", "tư vấn pháp lý", 
        "stakeholder management", "vendor management", "soạn thảo hợp đồng", 
        "kinh doanh dự án"
    ]

    company_skills = df_normalized.filter(F.col("skills_clean").isNotNull()) \
        .withColumn("skill", F.explode(F.split(F.lower(F.col("skills_clean")), ","))) \
        .withColumn("skill", F.trim(F.col("skill"))) \
        .filter((F.col("skill") != "") & (~F.col("skill").isin(garbage_skills))) \
        .groupBy("company_clean", "skill").count()

    window_skill = Window.partitionBy("company_clean").orderBy(F.col("count").desc())
    top_skill_per_company = company_skills.withColumn("rk", F.row_number().over(window_skill)) \
        .filter(F.col("rk") == 1).select("company_clean", F.col("skill").alias("top_skill"))

    # 8. AGGREGATION ĐA CHIỀU CHO TỪNG CÔNG TY
    company_metrics = df_normalized.groupBy("company_clean").agg(
        F.count("*").alias("jobs"),
        F.round(F.avg("salary_final_vnd") / 1e6, 1).alias("avg_sal_M"),
        F.round(F.max("salary_final_vnd") / 1e6, 1).alias("max_sal_M"),
        F.round(F.avg("yoe_extracted"), 1).alias("avg_yoe"),
        F.round(F.avg(F.when(F.col("is_remote") == "true", 100).otherwise(0)), 1).alias("remote_pct"),
        F.collect_set("job_level").alias("levels_arr")
    )

    # 9. TÍNH % THỊ PHẦN VÀ % CHÊNH LỆCH LƯƠNG VS THỊ TRƯỜNG
    final_df = company_metrics \
        .withColumn("share_pct", F.round((F.col("jobs") / F.lit(broadcast_market_jobs.value)) * 100, 2)) \
        .withColumn("vs_mkt_pct", F.round(((F.col("avg_sal_M") - F.lit(broadcast_market_avg.value)) / F.lit(broadcast_market_avg.value)) * 100, 1)) \
        .withColumn("levels_hired", F.concat_ws(", ", F.col("levels_arr")))

    # Join với bảng Top Skill và sắp xếp Top 20
    final_df = final_df.join(top_skill_per_company, "company_clean", "left") \
        .select("company_clean", "jobs", "share_pct", "avg_sal_M", "vs_mkt_pct", 
                "max_sal_M", "avg_yoe", "remote_pct", "top_skill", "levels_hired") \
        .orderBy(F.col("jobs").desc()).limit(20)

    rows = final_df.collect()

    # 10. GHI FILE TXT LOCAL 
    print("\n[INFO] Đang xuất báo cáo ra file Text...")
    os.makedirs(os.path.dirname(OUTPUT_TXT), exist_ok=True)
    
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("=" * 145 + "\n")
        f.write("    TOP 20 CONG TY TUYEN DUNG NHIEU NHAT (Broadcast Mode - Phan tich da chieu)\n")
        f.write(f"    (Luong trung binh thi truong: {broadcast_market_avg.value} trieu VND)\n")
        f.write("=" * 145 + "\n")
        hdr = f"{'Company':<30} {'Jobs':>5} {'Share%':>8} {'AvgSal(M)':>10} {'vsMkt%':>8} {'MaxSal(M)':>10} {'AvgYoE':>7} {'Remote%':>8} {'TopSkill':<18} {'Levels Hired':<30}"
        f.write(hdr + "\n")
        f.write("-" * 145 + "\n")
        
        for r in rows:
            tskill = r['top_skill'] if r['top_skill'] else "N/A"
            f.write(f"{str(r['company_clean'])[:30]:<30} "
                    f"{r['jobs']:>5} "
                    f"{r['share_pct']:>8} "
                    f"{r['avg_sal_M']:>10} "
                    f"{r['vs_mkt_pct']:>8} "
                    f"{r['max_sal_M']:>10} "
                    f"{r['avg_yoe']:>7} "
                    f"{r['remote_pct']:>8} "
                    f"{str(tskill)[:18]:<18} "
                    f"{str(r['levels_hired'])[:30]:<30}\n")
        f.write("=" * 145 + "\n")

    print("[OK] Multi-Dimensional TXT written to Local: " + OUTPUT_TXT)

    # 11. ĐẨY LÊN MONGODB ATLAS
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
