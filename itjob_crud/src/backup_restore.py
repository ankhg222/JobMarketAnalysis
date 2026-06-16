"""
Backup và Restore operations cho Hive tables
Lưu snapshot lên HDFS, quản lý metadata
"""

import logging
import uuid
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

from src.spark_client import get_spark
import config

logger = logging.getLogger(__name__)


def _meta_tbl() -> str:
    return f"{config.HIVE_DATABASE}.{config.BACKUP_META_TABLE}"


def create_backup(
    backup_name: str = "",
    description: str = "",
    include_deleted: bool = False
) -> Tuple[bool, str, int]:
    """
    Tạo backup snapshot của it_jobs table lên HDFS.
    
    Args:
        backup_name: Tên backup (tự động nếu để trống)
        description: Mô tả backup
        include_deleted: Có include soft-deleted records không
    
    Returns:
        (success, backup_id, record_count)
    """
    spark = get_spark()
    spark.sql(f"USE {config.HIVE_DATABASE}")

    now = datetime.now()
    backup_id = str(uuid.uuid4())[:8].upper()
    
    if not backup_name:
        backup_name = f"BACKUP_{now.strftime('%Y%m%d_%H%M%S')}"

    backup_path = f"hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}{config.HDFS_BACKUP_PATH}/{backup_id}"

    try:
        df = spark.table(f"{config.HIVE_DATABASE}.{config.MAIN_TABLE}")
        if not include_deleted:
            df = df.filter(F.col("is_deleted") == 0)

        record_count = df.count()

        # Lưu lên HDFS dưới dạng Parquet
        df.write.mode("overwrite").parquet(backup_path)
        logger.info(f"Backup saved to {backup_path} ({record_count} records)")

        # Tính version
        existing = spark.sql(
            f"SELECT MAX(table_version) as v FROM {_meta_tbl()}"
        ).collect()[0]["v"]
        version = (existing or 0) + 1

        # Lưu metadata
        meta_row = spark.createDataFrame([{
            "backup_id":    backup_id,
            "backup_name":  backup_name,
            "backup_path":  backup_path,
            "record_count": record_count,
            "table_version": version,
            "description":  description,
            "status":       "ACTIVE",
            "created_at":   now,
        }], schema=StructType([
            StructField("backup_id",     StringType(),    False),
            StructField("backup_name",   StringType(),    True),
            StructField("backup_path",   StringType(),    True),
            StructField("record_count",  LongType(),      True),
            StructField("table_version", IntegerType(),   True),
            StructField("description",   StringType(),    True),
            StructField("status",        StringType(),    True),
            StructField("created_at",    TimestampType(), True),
        ]))

        meta_row.createOrReplaceTempView("temp_meta_insert")
        spark.sql(f"INSERT INTO TABLE {_meta_tbl()} SELECT * FROM temp_meta_insert")
        logger.info(f"Backup metadata saved. ID={backup_id}, v{version}")

        # Auto-cleanup: giữ tối đa MAX_VERSIONS
        _cleanup_old_backups()

        return True, backup_id, record_count

    except Exception as e:
        logger.error(f"Backup failed: {e}")
        raise


def list_backups() -> pd.DataFrame:
    """Liệt kê tất cả backups còn active."""
    spark = get_spark()
    spark.sql(f"USE {config.HIVE_DATABASE}")

    df = (spark.table(_meta_tbl())
              .filter(F.col("status") == "ACTIVE")
              .orderBy(F.col("created_at").desc()))

    return df.toPandas()


def preview_backup(backup_id: str, limit: int = 20) -> pd.DataFrame:
    """Preview data trong backup."""
    spark = get_spark()

    # Lấy path từ metadata
    rows = spark.sql(
        f"SELECT backup_path FROM {_meta_tbl()} WHERE backup_id = '{backup_id}'"
    ).collect()

    if not rows:
        raise ValueError(f"Backup '{backup_id}' không tồn tại")

    backup_path = rows[0]["backup_path"]
    df = spark.read.parquet(backup_path)
    return df.limit(limit).toPandas()


def restore_backup(
    backup_id: str,
    restore_mode: str = "replace"
) -> Tuple[bool, int]:
    """
    Restore backup về Hive table.
    
    Args:
        backup_id: ID backup cần restore
        restore_mode: 
            'replace' - Xóa data hiện tại, restore từ backup
            'merge'   - Merge backup vào data hiện tại (không ghi đè)
            'append'  - Append tất cả records từ backup
    
    Returns:
        (success, record_count)
    """
    spark = get_spark()
    spark.sql(f"USE {config.HIVE_DATABASE}")

    rows = spark.sql(
        f"""SELECT backup_path, record_count, backup_name 
            FROM {_meta_tbl()} 
            WHERE backup_id = '{backup_id}' AND status = 'ACTIVE'"""
    ).collect()

    if not rows:
        raise ValueError(f"Backup '{backup_id}' không tồn tại hoặc đã bị xóa")

    backup_path = rows[0]["backup_path"]
    backup_name = rows[0]["backup_name"]

    logger.info(f"Restoring from backup '{backup_name}' ({backup_path})")

    try:
        df_backup = spark.read.parquet(backup_path)
        count = df_backup.count()

        main_tbl = f"{config.HIVE_DATABASE}.{config.MAIN_TABLE}"

        if restore_mode == "replace":
            # Auto backup current state trước khi replace
            create_backup(
                backup_name=f"AUTO_PRE_RESTORE_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                description=f"Auto backup trước khi restore {backup_id}"
            )
            df_backup.createOrReplaceTempView("temp_restore")
            spark.sql(f"INSERT OVERWRITE TABLE {main_tbl} SELECT * FROM temp_restore")
            logger.info(f"Replaced main table with backup. {count} records.")

        elif restore_mode == "merge":
            # Chỉ thêm records chưa tồn tại
            existing_ids = spark.sql(
                f"SELECT id FROM {main_tbl}"
            ).select("id")
            df_new = df_backup.join(existing_ids, on="id", how="left_anti")
            new_count = df_new.count()
            df_new.write.mode("append").insertInto(main_tbl)
            count = new_count
            logger.info(f"Merged {count} new records from backup.")

        elif restore_mode == "append":
            df_backup.write.mode("append").insertInto(main_tbl)
            logger.info(f"Appended {count} records from backup.")

        return True, count

    except Exception as e:
        logger.error(f"Restore failed: {e}")
        raise


def delete_backup(backup_id: str) -> bool:
    """Xóa backup (mark as DELETED, không xóa file HDFS ngay)."""
    spark = get_spark()
    spark.sql(f"USE {config.HIVE_DATABASE}")

    df = spark.table(_meta_tbl())
    df_new = df.withColumn(
        "status",
        F.when(F.col("backup_id") == backup_id, "DELETED")
         .otherwise(F.col("status"))
    )

    rows = df_new.collect()
    df_final = spark.createDataFrame(rows, schema=df_new.schema)
    df_final.createOrReplaceTempView("temp_meta")

    spark.sql(f"INSERT OVERWRITE TABLE {_meta_tbl()} SELECT * FROM temp_meta")

    logger.info(f"Backup {backup_id} marked as DELETED")
    return True


def export_to_hdfs(path: str = "", file_format: str = "csv") -> str:
    """
    Export data từ Hive ra HDFS với format tùy chọn.
    
    Args:
        path: HDFS path (mặc định = HDFS_UPLOAD_PATH/export_timestamp)
        file_format: 'csv', 'parquet', 'json'
    
    Returns:
        Export path
    """
    spark = get_spark()
    spark.sql(f"USE {config.HIVE_DATABASE}")

    now = datetime.now()
    if not path:
        path = (f"hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}"
                f"{config.HDFS_UPLOAD_PATH}/export_{now.strftime('%Y%m%d_%H%M%S')}")

    df = spark.table(f"{config.HIVE_DATABASE}.{config.MAIN_TABLE}") \
              .filter(F.col("is_deleted") == 0)

    if file_format == "csv":
        df.write.mode("overwrite").option("header", "true").csv(path)
    elif file_format == "json":
        df.write.mode("overwrite").json(path)
    else:
        df.write.mode("overwrite").parquet(path)

    logger.info(f"Exported to {path} as {file_format}")
    return path


def _cleanup_old_backups():
    """Tự động xóa backups cũ khi vượt quá MAX_VERSIONS."""
    spark = get_spark()

    old_backups = spark.sql(f"""
        SELECT backup_id FROM (
            SELECT backup_id, 
                   ROW_NUMBER() OVER (ORDER BY created_at DESC) as rn
            FROM {_meta_tbl()}
            WHERE status = 'ACTIVE'
        ) t WHERE rn > {config.BACKUP_MAX_VERSIONS}
    """).collect()

    for row in old_backups:
        delete_backup(row["backup_id"])
        logger.info(f"Auto-cleaned old backup: {row['backup_id']}")


def get_hdfs_storage_info() -> dict:
    """Lấy thông tin storage HDFS."""
    spark = get_spark()
    sc = spark.sparkContext
    try:
        jvm = sc._jvm
        uri = jvm.java.net.URI(
            f"hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}"
        )
        conf = sc._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        status = fs.getStatus()
        return {
            "capacity_gb": round(status.getCapacity() / (1024**3), 2),
            "used_gb":     round(status.getUsed() / (1024**3), 2),
            "remaining_gb": round(status.getRemaining() / (1024**3), 2),
            "used_pct": round(status.getUsed() / status.getCapacity() * 100, 1)
                        if status.getCapacity() > 0 else 0
        }
    except Exception as e:
        logger.error(f"HDFS info error: {e}")
        return {}
