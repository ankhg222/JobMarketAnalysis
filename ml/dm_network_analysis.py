import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pymongo import MongoClient
import networkx as nx

# 1. THIẾT LẬP MÔI TRƯỜNG CHUẨN TRÊN UBUNTU
os.environ["PYTHONIOENCODING"] = "utf-8"

def main():
    print("[INFO] Khởi tạo tiến trình phân tích Network Graph PageRank từ HDFS...")

    # 2. KHỞI TẠO SPARK SESSION (Trỏ thẳng về HDFS, cấu hình 4 partitions)
    spark = SparkSession.builder \
        .appName("DM_GraphPageRank_HDFS") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hnq-master:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 3. KHAI BÁO BIẾN MÔI TRƯỜNG & KẾT NỐI
    OUTPUT_TXT = "/home/hadoophnq/data/skill_pagerank.txt"
    MONGO_URI = "mongodb+srv://quynhquynh748_db_user:123@cluster0.h4o1hpr.mongodb.net/BigDataJobMarket?authSource=admin"
    DB_NAME = "BigDataJobMarket"
    COLLECTION_NAME = "Skill_PageRank"

    # 4. ĐỌC DỮ LIỆU TỪ HDFS PARQUET
    parquet_in_path = "/project/data_warehouse/it_jobs_parquet"
    print(f"[INFO] Đang đọc dữ liệu Parquet từ: {parquet_in_path}")
    df = spark.read.parquet(parquet_in_path)

    # ── BƯỚC 1: XỬ LÝ VÀ TÁCH KỸ NĂNG ──
    df_skills = df.withColumn("job_id", F.monotonically_increasing_id()) \
                  .filter(F.col("skills_clean").isNotNull()) \
                  .withColumn("skill", F.explode(F.split(F.lower(F.col("skills_clean")), ","))) \
                  .withColumn("skill", F.trim(F.col("skill"))).filter(F.col("skill") != "") \
                  .select("job_id", "skill")

    # ── BƯỚC 2: TẠO CÁC CẠNH (EDGES) CHO GRAPH ──
    edges_df = df_skills.alias("a").join(df_skills.alias("b"), "job_id") \
        .filter(F.col("a.skill") < F.col("b.skill")) \
        .groupBy(F.col("a.skill").alias("source"), F.col("b.skill").alias("target")) \
        .agg(F.count("*").alias("weight")) \
        .filter(F.col("weight") > 5)

    edges = edges_df.collect()
    print(f"[INFO] Collected {len(edges)} edges for graph building.")

    # ── BƯỚC 3: TÍNH TOÁN PAGERANK BẰNG NETWORKX ──
    print("[INFO] Building Graph and calculating PageRank...")
    G = nx.Graph()
    for row in edges:
        G.add_edge(row['source'], row['target'], weight=row['weight'])

    pagerank_scores = nx.pagerank(G, weight='weight')
    sorted_pagerank = sorted(pagerank_scores.items(), key=lambda x: x[1], reverse=True)[:30]

    # ── BƯỚC 4: GHI FILE TXT LOCAL ──
    print("\n[INFO] Đang xuất báo cáo ra file Text...")
    os.makedirs(os.path.dirname(OUTPUT_TXT), exist_ok=True)
    
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("=" * 50 + "\n")
        f.write("       TOP 30 SKILL THEO PAGERANK (Network Centrality)\n")
        f.write("=" * 50 + "\n")
        f.write(f"{'Rank':<6} {'Skill':<30} {'Score':>10}\n")
        f.write("-" * 50 + "\n")
        for i, (k, v) in enumerate(sorted_pagerank, 1):
            f.write(f"{i:<6} {k[:30]:<30} {v:>10.5f}\n")
        f.write("=" * 50 + "\n")
    print(f"[OK] TXT written to Local: {OUTPUT_TXT}")

    # Chuẩn bị dữ liệu cho HDFS và MongoDB
    mongo_docs = [{"skill": k, "pagerank_score": round(float(v), 5)} for k, v in sorted_pagerank]

    # ── BƯỚC 5: ĐẨY LÊN HDFS ──
    print("\n[INFO] Uploading PageRank results to HDFS in JSON format...")
    try:
        # Create DataFrame from top 30 pagerank scores
        pagerank_df = spark.createDataFrame(mongo_docs)
        pagerank_df.write.mode("overwrite").json("/project/skill_pagerank")
        print("[OK] Saved a backup copy to HDFS: /project/skill_pagerank")
    except Exception as e:
        print("[ERROR] Failed to write to HDFS:", e)

    # ── BƯỚC 6: ĐẨY LÊN MONGODB ATLAS ──
    print("\n[INFO] Connecting and uploading to MongoDB Atlas...")
    try:
        client = MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        
        col.delete_many({})
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
