import os
import pandas as pd
import re
from collections import Counter
import matplotlib.pyplot as plt

def main():
    print("[INFO] Bắt đầu mô hình Trích xuất thông tin (Information Extraction)...")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_dir, "..", "data", "processed", "Data_ITJOB_Cleaned.csv")
    results_dir = os.path.join(current_dir, "..", "data", "mining_results")
    os.makedirs(results_dir, exist_ok=True)
    report_path = os.path.join(results_dir, "information_extraction_report.txt")
    plot_path = os.path.join(results_dir, "top_benefits.png")
    
    if not os.path.exists(data_path):
        print(f"[ERROR] Không tìm thấy file: {data_path}")
        return

    df = pd.read_csv(data_path)
    df['description_clean'] = df['description_clean'].fillna('')
    
    # Define benefit patterns
    benefits_dict = {
        "13th_month": r"(13th\s*month|tháng\s*13)",
        "healthcare_insurance": r"(health\s*care|health\s*insurance|bảo\s*hiểm|bao\s*hiem|medical)",
        "bonus_performance": r"(performance\s*bonus|thưởng|thuong|incentive)",
        "hybrid_remote": r"(hybrid|remote|work\s*from\s*home|wfh|flexible)",
        "laptop_macbook": r"(macbook|laptop|device)",
        "annual_leave": r"(annual\s*leave|paid\s*leave|phép\s*năm|nghỉ\s*phép)",
        "team_building": r"(team\s*building|company\s*trip|outing)",
        "training_certificate": r"(training|certificate|sponsorship|đào\s*tạo)"
    }
    
    # Define soft skills patterns
    soft_skills_dict = {
        "communication": r"(communication|giao\s*tiếp)",
        "teamwork": r"(teamwork|team\s*work|làm\s*việc\s*nhóm)",
        "problem_solving": r"(problem\s*solving|giải\s*quyết\s*vấn\s*đề)",
        "english": r"(english|tiếng\s*anh)",
        "japanese": r"(japanese|tiếng\s*nhật)"
    }
    
    benefit_counts = Counter()
    soft_skill_counts = Counter()
    
    print("[INFO] Đang quét mô tả công việc (Rule-based NER)...")
    for desc in df['description_clean']:
        desc_lower = desc.lower()
        
        # Check benefits
        for b_name, b_pattern in benefits_dict.items():
            if re.search(b_pattern, desc_lower):
                benefit_counts[b_name] += 1
                
        # Check soft skills
        for s_name, s_pattern in soft_skills_dict.items():
            if re.search(s_pattern, desc_lower):
                soft_skill_counts[s_name] += 1
                
    total_jobs = len(df)
    
    # Plot Benefits
    benefits_df = pd.DataFrame(benefit_counts.items(), columns=['Benefit', 'Count']).sort_values(by='Count', ascending=True)
    
    plt.figure(figsize=(10, 6))
    plt.barh(benefits_df['Benefit'], benefits_df['Count'], color='coral')
    plt.title("Top Phúc lợi (Benefits) được đề cập trong JD")
    plt.xlabel("Số lượng tin tuyển dụng")
    plt.tight_layout()
    plt.savefig(plot_path, dpi=300)
    plt.close()
    
    print("[INFO] Đang lưu báo cáo...")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== BÁO CÁO TRÍCH XUẤT THÔNG TIN (INFORMATION EXTRACTION) ===\n\n")
        f.write(f"Tổng số job quét: {total_jobs}\n\n")
        
        f.write("--- PHÚC LỢI PHỔ BIẾN (BENEFITS) ---\n")
        for b_name, count in benefit_counts.most_common():
            pct = (count / total_jobs) * 100
            f.write(f"{b_name:<25}: {count:4d} jobs ({pct:.1f}%)\n")
            
        f.write("\n--- KỸ NĂNG MỀM / NGOẠI NGỮ (SOFT SKILLS / LANGUAGES) ---\n")
        for s_name, count in soft_skill_counts.most_common():
            pct = (count / total_jobs) * 100
            f.write(f"{s_name:<25}: {count:4d} jobs ({pct:.1f}%)\n")
            
    print(f"[OK] Đã lưu báo cáo: {report_path}")
    print(f"[OK] Đã lưu biểu đồ: {plot_path}")

if __name__ == "__main__":
    main()
