from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import subprocess
import os

spark = SparkSession.builder \
    .appName("Job1_AssociationRules") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("JOB 1: ASSOCIATION RULES MINING (Apriori)")
print("=" * 60)

# ── Đường dẫn ──
HDFS_INPUT   = "hdfs://tanyen-master:9000/project/jobs/Data_ITJOB_Cleaned.csv"
LOCAL_DIR    = "/home/tanyen/hadoopyen/project/output"
HDFS_OUT_DIR = "hdfs://tanyen-master:9000/project/output"

os.makedirs(LOCAL_DIR, exist_ok=True)

# ── Đọc dữ liệu ──
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv(HDFS_INPUT)

df_skills = df.filter(
    F.col("skills_clean").isNotNull() & (F.col("skills_clean") != "")
).select("url", "skills_clean", "job_level", "salary_final_vnd")

print(f"Số job có skills: {df_skills.count()}")

# ── Chuyển sang Pandas (dataset nhỏ ~2000 rows) ──
df_pd = df_skills.toPandas()

# QUAN TRỌNG: Dừng Spark NGAY SAU KHI lấy xong dữ liệu
# Tránh Spark cố serialize bất kỳ thứ gì từ mlxtend/pandas về sau
spark.stop()
print("Spark đã dừng. Tiếp tục xử lý bằng Python thuần...\n")

# ── Parse skills ──
def parse_skills(s):
    if pd.isna(s) or not s.strip():
        return []
    return [x.strip().lower() for x in s.split(",") if x.strip()]

df_pd["skills_list"] = df_pd["skills_clean"].apply(parse_skills)
df_pd = df_pd[df_pd["skills_list"].apply(len) >= 2].reset_index(drop=True)
transactions = df_pd["skills_list"].tolist()
print(f"Số transaction (job ≥2 skills): {len(transactions)}")

from collections import Counter
all_skills = [sk for t in transactions for sk in t]
skill_counts = Counter(all_skills)
print("\nTop 15 skills phổ biến nhất:")
for sk, cnt in skill_counts.most_common(15):
    print(f"  {sk:35s} → {cnt:4d} jobs ({cnt/len(transactions)*100:.1f}%)")


# ════════════════════════════════════════════════════════
# APRIORI (dùng mlxtend thuần Python, không liên quan Spark)
# ════════════════════════════════════════════════════════
def save_local_and_hdfs(df_save, filename):
    """Lưu CSV local rồi put lên HDFS bằng subprocess — không dùng Spark."""
    local_path = os.path.join(LOCAL_DIR, filename)
    df_save.to_csv(local_path, index=False, encoding="utf-8-sig")
    print(f"✅ Đã lưu local: {local_path}")

    hdfs_path = f"{HDFS_OUT_DIR}/{filename}"
    try:
        subprocess.run(
            ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
            check=True, capture_output=True, text=True
        )
        print(f"✅ Đã lưu HDFS : {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"⚠ Lưu HDFS thất bại: {e.stderr.strip()}")


try:
    from mlxtend.frequent_patterns import apriori, association_rules
    from mlxtend.preprocessing import TransactionEncoder

    # One-hot encode
    te = TransactionEncoder()
    te_array = te.fit(transactions).transform(transactions)
    df_encoded = pd.DataFrame(te_array, columns=te.columns_)
    print(f"\n[Apriori] Ma trận one-hot: {df_encoded.shape}")

    # Frequent itemsets
    MIN_SUPPORT = 0.01
    freq_sets = apriori(df_encoded, min_support=MIN_SUPPORT,
                        use_colnames=True, max_len=3)
    freq_sets["length"] = freq_sets["itemsets"].apply(len)
    print(f"[Apriori] Frequent itemsets (support≥{MIN_SUPPORT}): {len(freq_sets)}")
    print(freq_sets.sort_values("support", ascending=False).head(10).to_string())

    # Lưu frequent itemsets — convert frozenset → str TRƯỚC KHI lưu
    freq_save = freq_sets.copy()
    freq_save["itemsets"] = freq_save["itemsets"].apply(
        lambda x: ", ".join(sorted(x))   # frozenset → chuỗi bình thường
    )
    save_local_and_hdfs(freq_save, "job1_frequent_itemsets.csv")

    # Association rules
    MIN_CONFIDENCE = 0.2
    rules = association_rules(freq_sets, metric="confidence",
                              min_threshold=MIN_CONFIDENCE)
    rules = rules.sort_values("lift", ascending=False)

    # Convert frozenset → str (PHẢI làm trước khi chạm vào bất kỳ thứ gì khác)
    rules["neu_co_ky_nang"] = rules["antecedents"].apply(
        lambda x: ", ".join(sorted(x))
    )
    rules["thi_co_ky_nang"] = rules["consequents"].apply(
        lambda x: ", ".join(sorted(x))
    )

    # Chỉ giữ cột thuần (str/float), DROP hoàn toàn cột frozenset
    result = rules[
        ["neu_co_ky_nang", "thi_co_ky_nang", "support", "confidence", "lift"]
    ].copy()
    result = result[result["lift"] > 1.2].reset_index(drop=True)

    print(f"\n[Rules] Tổng rules (confidence≥{MIN_CONFIDENCE}): {len(rules)}")
    print(f"[Rules] Rules chất lượng (lift>1.2)             : {len(result)}")

    if result.empty:
        print("❌ Không có rule nào đạt yêu cầu. Thử giảm MIN_SUPPORT hoặc MIN_CONFIDENCE.")
    else:
        print("\n" + "=" * 70)
        print("TOP 20 ASSOCIATION RULES (lift DESC):")
        print("=" * 70)
        print(result.head(20).round(4).to_string(index=False))
        save_local_and_hdfs(result, "job1_association_rules.csv")

except ImportError:
    print("\n⚠ mlxtend chưa cài. Chạy: pip install mlxtend")
    print("Dùng fallback co-occurrence thủ công...\n")

    from itertools import combinations

    pair_cnt   = Counter()
    skill_tot  = Counter()

    for t in transactions:
        u = list(set(t))
        for sk in u:
            skill_tot[sk] += 1
        for pair in combinations(sorted(u), 2):
            pair_cnt[pair] += 1

    N = len(transactions)
    rows = []
    for (a, b), cnt in pair_cnt.most_common(200):
        sup  = cnt / N
        c_ab = cnt / skill_tot[a]
        c_ba = cnt / skill_tot[b]
        lift = sup / ((skill_tot[a] / N) * (skill_tot[b] / N))
        if sup >= 0.01 and lift > 1.0:
            rows.append({
                "skill_A": a, "skill_B": b,
                "co_occurrence": cnt,
                "support": round(sup, 4),
                "conf_A_to_B": round(c_ab, 4),
                "conf_B_to_A": round(c_ba, 4),
                "lift": round(lift, 3)
            })

    df_pairs = pd.DataFrame(rows).sort_values("lift", ascending=False)
    if df_pairs.empty:
        print("❌ Không tìm thấy cặp nào đạt yêu cầu.")
    else:
        print(df_pairs.head(20).to_string(index=False))
        save_local_and_hdfs(df_pairs, "job1_skill_cooccurrence.csv")

print("\nJob 1 hoàn tất!")
