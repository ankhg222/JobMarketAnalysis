import os
os.environ["PYTHONUTF8"] = "1"
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import hstack
from pyspark.sql import SparkSession
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

def main():
    print("[INFO] Bat dau mo hinh Phan loai cap bac (Job Level Classification)...")
    
    spark = SparkSession.builder \
        .appName("DM_LevelClassification") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    INPUT_CSV = ("hdfs://localhost:9000/project/jobs/Data_ITJOB_Cleaned.csv")
    results_dir = os.path.join(current_dir, "..", "data", "mining_results")
    os.makedirs(results_dir, exist_ok=True)
    report_path = os.path.join(results_dir, "level_classification_report.txt")
    plot_path = os.path.join(results_dir, "level_confusion_matrix.png")

    print("[INFO] Reading CSV from HDFS...")
    spark_df = spark.read.csv(INPUT_CSV, header=True, inferSchema=True)
    print(f"[INFO] Rows: {spark_df.count()}")
    print(spark_df.columns)
    spark_df.printSchema()
    df = spark_df.toPandas()
    
    # Filter valid levels (ignore Undefined or missing)
    df = df[df['job_level'].notna() & (df['job_level'] != 'Undefined')]
    # Fill NAs
    df['yoe_extracted'] = df['yoe_extracted'].fillna(0)
    df['skill_count'] = df['skill_count'].fillna(0)
    df['skills_clean'] = df['skills_clean'].fillna('')
    df['title_clean'] = df['title_clean'].fillna('')
    
    print(f"[INFO] Du lieu hop le de train: {len(df)} dong.")
    if len(df) < 50:
        print("[ERROR] Khong du du lieu de train.")
        spark.stop()
        return
        
    # Text features
    tfidf = TfidfVectorizer(max_features=3000) # stop_words='english' là loại bỏ các từ tiếng anh thông dụng
    text_features = tfidf.fit_transform(df['title_clean'] + " " + df['skills_clean'])
    # Numeric features
    num_features = df[['yoe_extracted', 'skill_count']].values
    # Combine features
    X = hstack([num_features, text_features])
    y = df['job_level']
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y)
    # Train
    print("[INFO] Dang huan luyen mo hinh Random Forest...")
    clf = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    clf.fit(X_train, y_train)
    # Predict
    y_pred = clf.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"[INFO] Accuracy: {accuracy:.4f}")
    # Evaluation
    report = classification_report(y_test, y_pred, zero_division=0)
    cm = confusion_matrix(y_test, y_pred, labels=clf.classes_)

    print("[INFO] Dang luu bao cao va bieu do...")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== BÁO CÁO PHÂN LOẠI CẤP BẬC CÔNG VIỆC (JOB LEVEL) ===\n\n")
        f.write(f"Thuật toán: Random Forest Classifier\n")
        f.write(f"Số lượng mẫu huấn luyện: {X_train.shape[0]}\n")
        f.write(f"Số lượng mẫu kiểm thử: {X_test.shape[0]}\n")
        f.write(f"Accuracy: {accuracy:.4f}\n\n")
        f.write("\n--- CLASSIFICATION REPORT ---\n")
        f.write(report)
        f.write("\n")
        
    # Plot CM
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=clf.classes_, yticklabels=clf.classes_)
    plt.title("Confusion Matrix - Job Level Classification")
    plt.ylabel('Thực tế (True Label)')
    plt.xlabel('Dự đoán (Predicted Label)')
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()
    
    print(f"[OK] Da luu bao cao: {report_path}")
    print(f"[OK] Da luu bieu do: {plot_path}")
    spark.stop()

if __name__ == "__main__":
    main()
