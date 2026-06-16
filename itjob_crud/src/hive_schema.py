"""
Hive Schema initialization
Tạo database + tables trong Hive qua Spark
"""

import logging
from src.spark_client import get_spark
import config

logger = logging.getLogger(__name__)


SCHEMA_SQL = f"""
CREATE DATABASE IF NOT EXISTS {config.HIVE_DATABASE}
COMMENT 'IT Job Market Database'
LOCATION 'hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}{config.HDFS_BASE_PATH}'
"""

MAIN_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {config.HIVE_DATABASE}.{config.MAIN_TABLE} (
    id              BIGINT,
    source          STRING,
    title_clean     STRING,
    company         STRING,
    salary          STRING,
    salary_clean    STRING,
    salary_min_vnd  DOUBLE,
    salary_max_vnd  DOUBLE,
    salary_final_vnd DOUBLE,
    is_AI_predicted INT,
    impute_method   STRING,
    location_clean  STRING,
    is_remote       INT,
    job_level       STRING,
    yoe_extracted   DOUBLE,
    skills_clean    STRING,
    skill_count     INT,
    description_clean STRING,
    url             STRING,
    is_deleted      INT     COMMENT '0=active, 1=soft-deleted',
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}{config.HDFS_DATA_PATH}'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'comment'='Main IT Jobs table'
)
"""

BACKUP_META_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {config.HIVE_DATABASE}.{config.BACKUP_META_TABLE} (
    backup_id       STRING,
    backup_name     STRING,
    backup_path     STRING,
    record_count    BIGINT,
    table_version   INT,
    description     STRING,
    status          STRING  COMMENT 'ACTIVE / DELETED',
    created_at      TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}/user/backups/meta'
"""

DELETED_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {config.HIVE_DATABASE}.{config.DELETED_TABLE} (
    id              BIGINT,
    source          STRING,
    title_clean     STRING,
    company         STRING,
    salary          STRING,
    salary_clean    STRING,
    salary_min_vnd  DOUBLE,
    salary_max_vnd  DOUBLE,
    salary_final_vnd DOUBLE,
    is_AI_predicted INT,
    impute_method   STRING,
    location_clean  STRING,
    is_remote       INT,
    job_level       STRING,
    yoe_extracted   DOUBLE,
    skills_clean    STRING,
    skill_count     INT,
    description_clean STRING,
    url             STRING,
    deleted_at      TIMESTAMP,
    deleted_reason  STRING
)
STORED AS PARQUET
LOCATION 'hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}/user/hive/warehouse/it_jobs_deleted'
"""


def init_database() -> bool:
    """Khởi tạo toàn bộ schema Hive."""
    spark = get_spark()
    try:
        logger.info("Initializing Hive schema...")

        spark.sql(SCHEMA_SQL)
        logger.info(f"Database '{config.HIVE_DATABASE}' ready.")

        spark.sql(f"USE {config.HIVE_DATABASE}")

        spark.sql(MAIN_TABLE_SQL)
        logger.info(f"Table '{config.MAIN_TABLE}' ready.")

        spark.sql(BACKUP_META_TABLE_SQL)
        logger.info(f"Table '{config.BACKUP_META_TABLE}' ready.")

        spark.sql(DELETED_TABLE_SQL)
        logger.info(f"Table '{config.DELETED_TABLE}' ready.")

        return True
    except Exception as e:
        logger.error(f"Schema init failed: {e}")
        raise


def drop_database(confirm: bool = False) -> bool:
    """Xóa toàn bộ database. Cần confirm=True."""
    if not confirm:
        raise ValueError("Phải pass confirm=True để xóa database!")
    spark = get_spark()
    spark.sql(f"DROP DATABASE IF EXISTS {config.HIVE_DATABASE} CASCADE")
    logger.warning(f"Database '{config.HIVE_DATABASE}' dropped!")
    return True


def get_table_info() -> dict:
    """Trả về thông tin các tables."""
    spark = get_spark()
    spark.sql(f"USE {config.HIVE_DATABASE}")
    tables = spark.sql("SHOW TABLES").collect()
    info = {}
    for t in tables:
        tname = t["tableName"]
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {tname}") \
                         .collect()[0]["cnt"]
        except Exception:
            count = -1
        info[tname] = count
    return info
