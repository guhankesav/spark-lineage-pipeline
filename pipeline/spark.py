import os
import sys

from pyspark.sql import SparkSession


def use_current_python() -> None:
    """Python workers must match the driver's minor version; otherwise Spark may pick the system python3."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def build_session(app_name: str = "spark-lineage-pipeline", shuffle_partitions: int = 8) -> SparkSession:
    """Local session. Shuffle partitions default to 200 in Spark, far too many for one machine."""
    use_current_python()
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
