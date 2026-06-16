#!/usr/bin/env python3
"""
Setup Script: Upload CSV lên HDFS và khởi tạo Hive schema
Chạy một lần đầu tiên để setup môi trường
"""

import os
import sys
import subprocess
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def run_cmd(cmd: str, check: bool = True) -> int:
    """Chạy shell command."""
    logger.info(f"$ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        logger.info(result.stdout.strip())
    if result.stderr and result.returncode != 0:
        logger.error(result.stderr.strip())
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\n{result.stderr}")
    return result.returncode


def setup_hdfs():
    """Tạo cấu trúc thư mục HDFS."""
    logger.info("=== Thiết lập HDFS directories ===")

    dirs = [
        "/user/hive/warehouse",
        "/user/backups/it_jobs",
        "/user/backups/meta",
        "/user/uploads",
        f"/user/hive/warehouse/itjobs_db.db",
    ]

    for d in dirs:
        run_cmd(f"hdfs dfs -mkdir -p {d}", check=False)
        run_cmd(f"hdfs dfs -chmod 777 {d}", check=False)
        logger.info(f"✓ {d}")


def upload_csv():
    """Upload CSV data lên HDFS."""
    logger.info("=== Upload CSV lên HDFS ===")

    local_csv = config.CSV_DATA_FILE
    if not os.path.exists(local_csv):
        # Thử tìm trong thư mục data
        alternatives = [
            "/opt/itjob_crud/data/Data_ITJOB_Cleaned.csv",
            os.path.join(os.path.dirname(__file__), "..", "data", "Data_ITJOB_Cleaned.csv"),
        ]
        for alt in alternatives:
            if os.path.exists(os.path.abspath(alt)):
                local_csv = os.path.abspath(alt)
                break
        else:
            logger.warning(f"CSV không tìm thấy: {config.CSV_DATA_FILE}")
            logger.warning("Bỏ qua bước upload CSV.")
            return

    hdfs_upload_path = f"{config.HDFS_UPLOAD_PATH}/Data_ITJOB_Cleaned.csv"
    run_cmd(f"hdfs dfs -put -f '{local_csv}' {hdfs_upload_path}", check=False)
    logger.info(f"✓ Uploaded to {hdfs_upload_path}")


def init_hive_schema():
    """Khởi tạo Hive schema qua PySpark."""
    logger.info("=== Khởi tạo Hive Schema ===")

    from src.hive_schema import init_database
    init_database()
    logger.info("✓ Schema khởi tạo thành công")


def import_data():
    """Import CSV vào Hive."""
    logger.info("=== Import dữ liệu vào Hive ===")

    from src.hive_operations import import_csv_to_hive

    # Thử HDFS path trước
    hdfs_csv = f"hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}{config.HDFS_UPLOAD_PATH}/Data_ITJOB_Cleaned.csv"

    try:
        ok, count = import_csv_to_hive(hdfs_csv, overwrite=False)
        if ok:
            logger.info(f"✓ Import thành công: {count:,} records từ HDFS")
            return
    except Exception:
        pass

    # Fallback: local path
    if os.path.exists(config.CSV_DATA_FILE):
        ok, count = import_csv_to_hive(config.CSV_DATA_FILE, overwrite=False)
        if ok:
            logger.info(f"✓ Import thành công: {count:,} records từ local")
    else:
        logger.warning("Không tìm thấy CSV. Bỏ qua import.")


def create_initial_backup():
    """Tạo backup ban đầu sau khi import."""
    logger.info("=== Tạo Initial Backup ===")

    from src.backup_restore import create_backup
    ok, bid, count = create_backup(
        backup_name="INITIAL_IMPORT",
        description="Backup sau lần import dữ liệu đầu tiên"
    )
    if ok:
        logger.info(f"✓ Backup tạo thành công: ID={bid}, {count:,} records")


def main():
    logger.info("=" * 60)
    logger.info("IT Job Market CRUD - Setup Script")
    logger.info("=" * 60)

    steps = [
        ("Thiết lập HDFS", setup_hdfs),
        ("Upload CSV", upload_csv),
        ("Khởi tạo Hive Schema", init_hive_schema),
        ("Import dữ liệu", import_data),
        ("Tạo Initial Backup", create_initial_backup),
    ]

    for step_name, step_fn in steps:
        logger.info(f"\n>>> {step_name}...")
        try:
            step_fn()
            logger.info(f"✅ {step_name} - Hoàn thành")
        except Exception as e:
            logger.error(f"❌ {step_name} - Lỗi: {e}")
            if step_name in ["Khởi tạo Hive Schema"]:
                logger.error("Dừng setup do lỗi nghiêm trọng.")
                sys.exit(1)

    logger.info("\n" + "=" * 60)
    logger.info("✅ Setup hoàn tất! Chạy app:")
    logger.info("  streamlit run app.py --server.port 8501 --server.address 0.0.0.0")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
