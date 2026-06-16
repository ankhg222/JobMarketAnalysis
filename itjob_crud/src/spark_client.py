
"""
Spark + Hive session manager
Singleton pattern - tạo 1 lần, dùng lại
"""

import os
import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

import config

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Tạo SparkSession với Hive support."""

    # JAVA_HOME
    if not os.environ.get("JAVA_HOME"):
        java_paths = [
            "/usr/lib/jvm/java-8-openjdk-amd64",
            "/usr/lib/jvm/java-11-openjdk-amd64",
            "/usr/java/default",
        ]
        for p in java_paths:
            if os.path.exists(p):
                os.environ["JAVA_HOME"] = p
                break

    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)

        # Derby JDBC Driver
        .config(
            "spark.jars",
            "/home/hadoop/hive/lib/derby-10.14.1.0.jar"
        )

        # Hive
        .config(
            "spark.sql.warehouse.dir",
            config.SPARK_WAREHOUSE_DIR
        )
        .config(
            "spark.sql.catalogImplementation",
            "hive"
        )

        # HDFS
        .config(
            "spark.hadoop.fs.defaultFS",
            f"hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}"
        )

        # Hive settings
        .config(
            "spark.sql.hive.convertMetastoreParquet",
            "true"
        )
        .config(
            "spark.hadoop.hive.exec.dynamic.partition",
            "true"
        )
        .config(
            "spark.hadoop.hive.exec.dynamic.partition.mode",
            "nonstrict"
        )

        # Performance
        .config(
            "spark.executor.memory",
            config.SPARK_EXECUTOR_MEMORY
        )
        .config(
            "spark.driver.memory",
            config.SPARK_DRIVER_MEMORY
        )
        .config(
            "spark.sql.shuffle.partitions",
            "4"
        )
        .config(
            "spark.default.parallelism",
            "4"
        )

        # UI
        .config(
            "spark.ui.port",
            "4041"
        )

        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created. Version: {spark.version}")

    return spark


_spark_instance: Optional[SparkSession] = None


def get_spark() -> SparkSession:
    global _spark_instance

    if _spark_instance is None or _spark_instance._sc._jsc is None:
        _spark_instance = create_spark_session()

    return _spark_instance


def stop_spark():
    global _spark_instance

    if _spark_instance:
        _spark_instance.stop()
        _spark_instance = None
        logger.info("SparkSession stopped.")


def test_connection() -> dict:
    result = {
        "spark": False,
        "hive": False,
        "hdfs": False,
        "error": None
    }

    try:
        spark = get_spark()
        result["spark"] = True

        spark.sql("SHOW DATABASES").collect()
        result["hive"] = True

        sc = spark.sparkContext
        jvm = sc._jvm

        uri = jvm.java.net.URI(
            f"hdfs://{config.HDFS_HOST}:{config.HDFS_PORT}"
        )

        conf = sc._jsc.hadoopConfiguration()

        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            uri,
            conf
        )

        fs.exists(
            jvm.org.apache.hadoop.fs.Path("/")
        )

        result["hdfs"] = True

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Connection test failed: {e}")

    return result

