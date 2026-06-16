"""
Configuration cho IT Job Market Streamlit App
Chỉnh sửa các giá trị theo môi trường Ubuntu của bạn
"""

import os

# ─── HDFS Config ───────────────────────────────────────────────────────────────
HDFS_HOST = os.getenv("HDFS_HOST", "localhost")
HDFS_PORT = int(os.getenv("HDFS_PORT", "9000"))
HDFS_BASE_PATH = "/user/hive/warehouse"
HDFS_DATA_PATH = f"{HDFS_BASE_PATH}/it_jobs"
HDFS_BACKUP_PATH = "/user/backups/it_jobs"
HDFS_UPLOAD_PATH = "/user/uploads"

# ─── Spark Config ──────────────────────────────────────────────────────────────
SPARK_APP_NAME = "ITJobMarket_CRUD"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_EXECUTOR_MEMORY = "2g"
SPARK_DRIVER_MEMORY = "2g"
SPARK_WAREHOUSE_DIR = f"hdfs://{HDFS_HOST}:{HDFS_PORT}{HDFS_BASE_PATH}"

# Spark/Hive Thrift Server (nếu dùng thrift)
HIVE_THRIFT_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_THRIFT_PORT = int(os.getenv("HIVE_PORT", "10000"))
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "itjobs_db")

# ─── App Config ────────────────────────────────────────────────────────────────
APP_TITLE = "IT Job Market Analytics"
APP_ICON = "💼"
PAGE_SIZE = 20  # records per page
MAX_DISPLAY_ROWS = 500
BACKUP_MAX_VERSIONS = 10  # giữ tối đa 10 backup

# ─── Hive Table Names ──────────────────────────────────────────────────────────
MAIN_TABLE = "it_jobs"
BACKUP_META_TABLE = "it_jobs_backup_meta"
DELETED_TABLE = "it_jobs_deleted"  # soft delete

# ─── Columns Definition ────────────────────────────────────────────────────────
CSV_COLUMNS = [
    "source", "title_clean", "company", "salary", "salary_clean",
    "salary_min_vnd", "salary_max_vnd", "salary_final_vnd",
    "is_AI_predicted", "impute_method", "location_clean", "is_remote",
    "job_level", "yoe_extracted", "skills_clean", "skill_count",
    "description_clean", "url"
]

DISPLAY_COLUMNS = {
    "id": "ID",
    "source": "Nguồn",
    "title_clean": "Vị Trí",
    "company": "Công Ty",
    "salary_final_vnd": "Lương (VND)",
    "location_clean": "Địa Điểm",
    "job_level": "Cấp Độ",
    "yoe_extracted": "Năm KN",
    "skills_clean": "Kỹ Năng",
    "is_remote": "Remote",
}

NUMERIC_COLUMNS = ["salary_min_vnd", "salary_max_vnd", "salary_final_vnd",
                   "yoe_extracted", "skill_count"]

JOB_LEVELS = ["Undefined", "Fresher", "Junior", "Middle", "Senior",
              "Lead", "Manager", "Director", "C-Level"]

LOCATIONS = ["Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Khác"]

# ─── Local Paths ───────────────────────────────────────────────────────────────
LOCAL_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
CSV_DATA_FILE = os.path.join(LOCAL_DATA_DIR, "Data_ITJOB_Cleaned.csv")
TEMP_DIR = os.path.join(os.path.dirname(__file__), "temp")
os.makedirs(TEMP_DIR, exist_ok=True)
