import os
import pymongo
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

MONGO_URI = "mongodb+srv://votin02061998_db_user:votin02061998_db_user@bigdatatin.lqwd4d6.mongodb.net/?appName=BigDataTin"
DB_NAME = "bigdata_project"
COLLECTION_NAME = "Result_Job13_Skill_Market"

def main():
    print("=== PIPELINE JOB 13: KHAI PHÁ GIÁ TRỊ KỸ NĂNG BẰNG THUẬT TOÁN TF-IDF ===")
    
    spark = SparkSession.builder \
        .appName("Pipeline_Job13") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://master:9000") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # ── BƯỚC 1: READ TỪ HDFS PARQUET WAREHOUSE ──
    print("[1/5] Đang nạp dữ liệu từ HDFS Parquet Warehouse...")
    df = spark.read.parquet("/project/data_warehouse/it_jobs_parquet")
    
    # ── BƯỚC 2: THUẬT TOÁN ĐỒNG THỜI MAPREDUCE & TF-IDF ──
    print("[2/5] Đang chạy thuật toán phân tán Explode và tính toán Balanced Score...")
    df = df.filter(F.col("skills_clean").isNotNull())
    df = df.withColumn("salary_M", F.col("salary_final_vnd") / 1000000)
    
    # Tạo mã định danh tài liệu duy nhất (Chống va chạm chuỗi kỹ năng)
    df = df.withColumn("doc_id", F.concat_ws("|", F.col("title_clean"), F.col("company")))
    total_docs = df.count() # Tổng số lượng tin tuyển dụng (N)

    # Tách chuỗi kỹ năng phân tách bằng dấu phẩy thành từng hàng độc lập
    df_exploded = df.withColumn("skill", F.explode(F.split(F.lower(F.col("skills_clean")), ",")))
    df_exploded = df_exploded.withColumn("skill_clean", F.trim(F.col("skill"))).filter(F.col("skill_clean") != "")

    # Tính toán Document Frequency (DF) và Lương trung bình cho từng kỹ năng
    skill_stats = df_exploded.groupBy("skill_clean").agg(
        F.countDistinct("doc_id").alias("df"),
        F.avg("salary_M").alias("avg_salary")
    ).filter(F.col("df") >= 15) # Lọc nhiễu: Chỉ lấy kỹ năng xuất hiện >= 15 lần

    # Áp dụng công thức: IDF = ln(N/DF) | Balanced_Score = Avg_Salary * sqrt(DF) * IDF
    result = skill_stats.withColumn("idf", F.round(F.log(F.lit(total_docs) / F.col("df")), 4)) \
                        .withColumn("balanced_score", F.round(F.col("avg_salary") * F.sqrt(F.col("df")) * F.col("idf"), 2)) \
                        .withColumn("avg_salary", F.round(F.col("avg_salary"), 1))

    # Sắp xếp lấy Top 20 kỹ năng giá trị nhất thị trường IT
    top_20 = result.select("skill_clean", "df", "avg_salary", "idf", "balanced_score") \
                   .orderBy(F.desc("balanced_score")).limit(20)

    rows = top_20.collect()
    data_dicts = [row.asDict() for row in rows]

    # ── BƯỚC 3: SINK 1 - GHI FILE LOCAL TXT BÁO CÁO ──
    print("[3/5] Đang xuất báo cáo phân tích ra file văn bản nội bộ...")
    local_path = "/home/hadoopvohuutin/data/mining_results/job13_skill_market.txt"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        f.write("=" * 85 + "\n")
        f.write("             BẢNG XẾP HẠNG GIÁ TRỊ THỊ TRƯỜNG CỦA CÁC KỸ NĂNG CNTT\n")
        f.write(f"             (Tổng số tin tuyển dụng mẫu trong hệ thống N = {total_docs})\n")
        f.write("=" * 85 + "\n")
        f.write(f"{'Tên Kỹ Năng':<18} | {'Số Job (DF)':<10} | {'Lương TB (M)':<12} | {'Độ Hiếm (IDF)':<12} | {'Balanced Score':<15}\n")
        f.write("-" * 85 + "\n")
        for r in rows:
            f.write(f"{r['skill_clean'].upper():<18} | {r['df']:<10} | {r['avg_salary']:<12} | {r['idf']:<12} | {r['balanced_score']:<15}\n")
        f.write("=" * 85 + "\n")
    print(f"[SUCCESS] Đã xuất báo cáo văn bản thành công tại: {local_path}")

    # ── BƯỚC 4: SINK 2 - GHI BACKUP JSON LÊN HDFS ──
    print("[4/5] Đang đẩy tệp sao lưu JSON thuật toán lên HDFS...")
    try:
        top_20.write.mode("overwrite").json("/project/results/job13_top_skills")
        print("[SUCCESS] Đã lưu thành công tệp JSON phân tán lên HDFS!")
    except Exception as e:
        print("[ERROR] Thất bại khi ghi HDFS:", e)

    # ── BƯỚC 5: SINK 3 - ĐẨY DỮ LIỆU LÊN CLOUD MONGODB ──
    print("[5/5] Đang mở cổng đẩy bảng xếp hạng lên MongoDB Atlas Cloud...")
    try:
        client = pymongo.MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        col.delete_many({})
        if data_dicts:
            col.insert_many(data_dicts)
            print(f"[SUCCESS] Đã đồng bộ thành công {len(data_dicts)} tài liệu lên MongoDB Atlas!")
    except Exception as e:
        print(f"[ERROR] Lỗi kết nối đám mây NoSQL: {e}")
    finally:
        if 'client' in locals():
            client.close()

    spark.stop()
    print("=== TOÀN BỘ PIPELINE HOÀN THÀNH XUẤT SẮC ===\n")

if __name__ == "__main__":
    main()
