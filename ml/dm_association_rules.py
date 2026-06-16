"""
=============================================================================
Job 1: dm_association_rules.py
Người 1 - Quy luật & Dự đoán
Mức độ: DỄ
Kỹ thuật: Apriori Algorithm - Association Rules Mining
=============================================================================

MỤC ĐÍCH:
  Tìm các quy luật kết hợp (association rules) giữa các kỹ năng IT:
  VD: "Nếu job yêu cầu Python thì thường cũng yêu cầu SQL" (support cao)
  
  CÁC CHỈ SỐ:
    Support    = P(A ∩ B) = Tỉ lệ job có CẢ A và B
    Confidence = P(B|A)   = Trong các job có A, bao nhiêu % cũng có B?
    Lift       = P(A∩B) / (P(A)×P(B)) > 1 = A và B hay đi cùng nhau hơn ngẫu nhiên

LUỒNG XỬ LÝ:
  BƯỚC 1: Tạo transaction list (mỗi job = 1 transaction gồm các skills)
  BƯỚC 2: Apriori → tìm frequent itemsets (min_support)
  BƯỚC 3: Sinh association rules (min_confidence)
  BƯỚC 4: Filter theo lift, sort kết quả
  
LƯU Ý: Dùng thư viện mlxtend (cài qua pip), chạy trên Spark worker
        hoặc dùng pandas thuần nếu dataset nhỏ (<10K rows)
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd


# ─────────────────────────────────────────────────────────────
# BƯỚC 1: Khởi tạo SparkSession
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Job1_AssociationRules") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("JOB 1: ASSOCIATION RULES MINING (Apriori)")
print("=" * 60)


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

df_skills = df.filter(
    F.col("skills_clean").isNotNull() &
    (F.col("skills_clean") != "")
).select("url", "skills_clean", "job_level", "salary_final_vnd")

print(f"Số job có skills: {df_skills.count()}")


# ─────────────────────────────────────────────────────────────
# BƯỚC 3: Tạo transaction list (Spark → Pandas)
# ─────────────────────────────────────────────────────────────
# Dataset 2000 rows → dùng pandas là đủ nhanh
# Nếu dataset lớn hơn (>100K), cần dùng Spark MLlib FPGrowth

df_pd = df_skills.toPandas()

# Parse skills_clean thành list: "python, sql, docker" → ["python", "sql", "docker"]
def parse_skills(skills_str):
    if pd.isna(skills_str) or skills_str.strip() == "":
        return []
    return [s.strip().lower() for s in skills_str.split(",") if s.strip()]

df_pd["skills_list"] = df_pd["skills_clean"].apply(parse_skills)

# Lọc job có ít nhất 2 skills (cần ≥2 items để có association)
df_pd = df_pd[df_pd["skills_list"].apply(len) >= 2].reset_index(drop=True)
print(f"Số transaction (job có ≥2 skills): {len(df_pd)}")

transactions = df_pd["skills_list"].tolist()

# Thống kê nhanh
from collections import Counter
all_skills = [skill for t in transactions for skill in t]
skill_counts = Counter(all_skills)
print(f"\nTop 15 skills phổ biến nhất:")
for skill, cnt in skill_counts.most_common(15):
    print(f"  {skill:35s} → {cnt:4d} jobs ({cnt/len(transactions)*100:.1f}%)")


# ─────────────────────────────────────────────────────────────
# BƯỚC 4: Apriori Algorithm
# ─────────────────────────────────────────────────────────────
# Cài mlxtend: pip install mlxtend
try:
    from mlxtend.frequent_patterns import apriori, association_rules
    from mlxtend.preprocessing import TransactionEncoder

    # Encode transaction list → one-hot DataFrame (True/False matrix)
    te = TransactionEncoder()
    te_array = te.fit(transactions).transform(transactions)
    df_encoded = pd.DataFrame(te_array, columns=te.columns_)
    print(f"\n[Apriori] Ma trận one-hot: {df_encoded.shape}")

    # ── Apriori: tìm frequent itemsets ──
    # min_support = 0.05 → itemset phải xuất hiện trong ít nhất 5% số job
    # Giảm nếu muốn nhiều rules hơn, tăng nếu muốn rules chặt hơn
    MIN_SUPPORT = 0.05
    frequent_itemsets = apriori(
        df_encoded,
        min_support=MIN_SUPPORT,
        use_colnames=True,      # dùng tên cột thay vì index
        max_len=3               # itemset tối đa 3 skills (tránh bùng tổ hợp)
    )
    frequent_itemsets["length"] = frequent_itemsets["itemsets"].apply(len)
    print(f"\n[Apriori] Số frequent itemsets (min_support={MIN_SUPPORT}): {len(frequent_itemsets)}")
    print(frequent_itemsets.sort_values("support", ascending=False).head(15).to_string())

    # ── Sinh Association Rules ──
    # min_threshold=0.5 → confidence ≥ 50%
    MIN_CONFIDENCE = 0.5
    rules = association_rules(
        frequent_itemsets,
        metric="confidence",
        min_threshold=MIN_CONFIDENCE
    )

    # Thêm cột "leverage" và làm sạch
    rules = rules.sort_values("lift", ascending=False)
    rules["antecedents_str"] = rules["antecedents"].apply(lambda x: ", ".join(sorted(x)))
    rules["consequents_str"] = rules["consequents"].apply(lambda x: ", ".join(sorted(x)))

    print(f"\n[Rules] Tổng số rules (confidence≥{MIN_CONFIDENCE}): {len(rules)}")

    # ── Lọc rules chất lượng cao ──
    # Lift > 1.2: A và B hay đồng xuất hiện hơn ngẫu nhiên 20%
    good_rules = rules[rules["lift"] > 1.2].copy()
    print(f"[Rules] Rules chất lượng (lift>1.2): {len(good_rules)}")

    print("\n" + "=" * 70)
    print("TOP 20 ASSOCIATION RULES (sort by lift DESC):")
    print("=" * 70)
    cols = ["antecedents_str", "consequents_str", "support", "confidence", "lift"]
    print(good_rules[cols].head(20).round(4).to_string(index=False))

    # ── Lưu kết quả ──
    result_rules = good_rules[cols].rename(columns={
        "antecedents_str": "neu_co_ky_nang",
        "consequents_str": "thi_co_ky_nang",
        "support": "support",
        "confidence": "confidence",
        "lift": "lift"
    })

    # Convert itemsets column to string for saving to HDFS via Spark
    result_spark = spark.createDataFrame(result_rules)

    # Lưu lên HDFS
    HDFS_OUTPUT = "hdfs://tanyen-master:9000/project/output/job1_association_rules"
    result_spark.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(HDFS_OUTPUT)
    print(f"\n✅ Đã lưu lên HDFS: {HDFS_OUTPUT}")

    # Lưu local
    LOCAL_OUTPUT = "/home/tanyen/hadoopyen/project/output/job1_association_rules.csv"
    result_rules.to_csv(LOCAL_OUTPUT, index=False, encoding="utf-8-sig")
    print(f"✅ Đã lưu local: {LOCAL_OUTPUT}")

    # Lưu frequent itemsets
    freq_output = frequent_itemsets.copy()
    freq_output["itemsets"] = freq_output["itemsets"].apply(lambda x: ", ".join(sorted(x)))
    freq_local = "/home/tanyen/hadoopyen/project/output/job1_frequent_itemsets.csv"
    freq_output.to_csv(freq_local, index=False, encoding="utf-8-sig")
    print(f"✅ Đã lưu frequent itemsets: {freq_local}")

except ImportError:
    print("\n⚠ mlxtend chưa được cài!")
    print("Chạy lệnh sau để cài:")
    print("  pip install mlxtend")
    print("\nFallback: Tính manual pairwise co-occurrence...")

    # ── FALLBACK: tính thủ công không cần mlxtend ──
    from itertools import combinations

    pair_counts = Counter()
    skill_totals = Counter()

    for transaction in transactions:
        unique_skills = list(set(transaction))
        for skill in unique_skills:
            skill_totals[skill] += 1
        for pair in combinations(sorted(unique_skills), 2):
            pair_counts[pair] += 1

    N = len(transactions)
    print(f"\nTop 20 cặp kỹ năng hay đi cùng nhau nhất:")
    rows = []
    for (a, b), count in pair_counts.most_common(30):
        support = count / N
        conf_ab = count / skill_totals[a]
        conf_ba = count / skill_totals[b]
        lift = support / ((skill_totals[a] / N) * (skill_totals[b] / N))
        if support >= 0.03 and lift > 1.0:
            rows.append({
                "skill_A": a, "skill_B": b,
                "co_occurrence": count,
                "support": round(support, 4),
                "conf_A→B": round(conf_ab, 4),
                "conf_B→A": round(conf_ba, 4),
                "lift": round(lift, 3)
            })

    df_pairs = pd.DataFrame(rows).sort_values("lift", ascending=False)
    print(df_pairs.head(20).to_string(index=False))

    LOCAL_OUTPUT = "/home/tanyen/hadoopyen/project/output/job1_skill_cooccurrence.csv"
    df_pairs.to_csv(LOCAL_OUTPUT, index=False, encoding="utf-8-sig")
    print(f"\n✅ Đã lưu local (fallback): {LOCAL_OUTPUT}")

spark.stop()
print("\nJob 1 hoàn tất!")
