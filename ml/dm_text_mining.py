import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer, ENGLISH_STOP_WORDS
from sklearn.decomposition import LatentDirichletAllocation
import warnings
warnings.filterwarnings('ignore')

try:
    from underthesea import word_tokenize
    USE_WORD_SEGMENT = True
except ImportError:
    USE_WORD_SEGMENT = False
    print("[WARN] underthesea not found. Vietnamese text will not be segmented.")

try:
    from wordcloud import WordCloud
except ImportError:
    print("Please install wordcloud: pip install wordcloud")
    import sys
    sys.exit(1)

def main():
    # 1. Setup paths
    input_path = "d:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "d:/HDFS/JOB_MARKET_BIGDATA/data/mining_results"
    plots_dir = os.path.join(output_dir, "plots")
    
    os.makedirs(plots_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "text_mining_report.txt")
    
    # 2. Load Data
    print("Loading data...")
    df = pd.read_csv(input_path)
    
    df['description_clean'] = df['description_clean'].fillna('')
    df['job_level'] = df['job_level'].fillna('Unknown')
    
    # Get text data
    text_data = df['description_clean'].tolist()

    # 3. Vietnamese Word Segmentation
    # underthesea ghép âm tiết thành từ có nghĩa: "kinh nghiệm" → "kinh_nghiệm"
    if USE_WORD_SEGMENT:
        print("Segmenting Vietnamese text (this may take a while)...")
        segmented = []
        for i, text in enumerate(text_data):
            if i % 200 == 0:
                print(f"  Segmenting {i}/{len(text_data)}...")
            try:
                tokens = word_tokenize(text, format="text")
                segmented.append(tokens)
            except Exception:
                segmented.append(text)
        text_data = segmented
        print("Segmentation done.")
    else:
        print("Skipping Vietnamese word segmentation (underthesea not installed).")
    
    # 3. Overall Word Cloud
    print("Generating Overall Word Cloud...")
    text_combined = " ".join(text_data)
    wordcloud = WordCloud(width=800, height=400, background_color='white', max_words=100).generate(text_combined)
    
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title('Overall Word Cloud from Job Descriptions')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'text_overall_wordcloud.png'))
    plt.close()
    
    # 4. TF-IDF and LDA
    print("Vectorizing text...")

    # Vietnamese stopwords (function words only — syllable issue solved by word segmentation)
    vi_stopwords = [
        "và", "các", "có", "của", "để", "với", "trong", "là", "được", "cho",
        "tại", "theo", "về", "từ", "bạn", "chúng", "tôi", "họ", "này", "đó",
        "khi", "nếu", "thì", "mà", "hay", "hoặc", "cũng", "đã", "sẽ", "đang",
        "rất", "nhiều", "một", "những", "không", "như", "sau", "trên", "dưới",
        "giữa", "ngoài", "trước", "qua", "lên", "xuống", "cùng"
    ]

    # Combine English + Vietnamese function word stopwords
    combined_stopwords = list(ENGLISH_STOP_WORDS) + vi_stopwords

    # Use CountVectorizer for LDA
    # With word segmentation, Vietnamese multi-syllable words are now single tokens (joined by _)
    vectorizer = CountVectorizer(
        max_features=2000,
        stop_words=combined_stopwords,
        lowercase=True,
        min_df=5,
        token_pattern=r"[a-zA-ZÀ-ỹ][a-zA-ZÀ-ỹ_]{2,}"  # match Vietnamese_compound_words too
    )
    X = vectorizer.fit_transform(text_data)
    feature_names = vectorizer.get_feature_names_out()
    
    num_topics = 5
    print(f"Running LDA with {num_topics} topics...")
    lda = LatentDirichletAllocation(n_components=num_topics, random_state=42)
    lda_output = lda.fit_transform(X)

    
    # Get top words for each topic
    n_top_words = 10
    topics_words = {}
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== TEXT MINING REPORT ===\n\n")
        f.write(f"Total documents: {len(text_data)}\n")
        f.write(f"Number of topics: {num_topics}\n\n")
        
        for topic_idx, topic in enumerate(lda.components_):
            top_features_ind = topic.argsort()[:-n_top_words - 1:-1]
            top_features = [feature_names[i] for i in top_features_ind]
            topics_words[f"Topic {topic_idx}"] = top_features
            
            f.write(f"Topic {topic_idx}:\n")
            f.write(", ".join(top_features) + "\n\n")
            
            # Generate Word Cloud for each topic
            topic_words_freq = {feature_names[i]: topic[i] for i in top_features_ind}
            wc = WordCloud(width=400, height=300, background_color='white').generate_from_frequencies(topic_words_freq)
            
            plt.figure(figsize=(6, 4))
            plt.imshow(wc, interpolation='bilinear')
            plt.axis('off')
            plt.title(f'Word Cloud for Topic {topic_idx}')
            plt.tight_layout()
            plt.savefig(os.path.join(plots_dir, f'text_topic_{topic_idx}_wordcloud.png'))
            plt.close()
            
    # Assign dominant topic to each document
    df['Dominant_Topic'] = np.argmax(lda_output, axis=1)
    
    # 5. Topic distribution by Job Level
    print("Generating Topic Distribution chart...")
    topic_dist = pd.crosstab(df['job_level'], df['Dominant_Topic'], normalize='index')
    
    # Plot stacked bar chart
    topic_dist.plot(kind='bar', stacked=True, figsize=(12, 6), colormap='viridis')
    plt.title('Topic Distribution by Job Level')
    plt.xlabel('Job Level')
    plt.ylabel('Proportion')
    plt.legend(title='Topic', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'text_topic_distribution.png'))
    plt.close()
    
    print(f"Reports saved to {report_path}")
    print("All text mining tasks completed.")

if __name__ == "__main__":
    main()
