"""
Shared logging for aircheck Spark jobs.

Usage in a Databricks notebook (repo root is on sys.path for notebooks
run from within a Databricks Repo):

    from common.logging_utils import get_logger, new_batch_id, log_run, log_error

    log = get_logger("batch.device_registry")
    batch_id = new_batch_id()
    log_run(spark, catalog=catalog_name, ops_schema=ops_schema,
            job_name="batch.device_registry", batch_id=batch_id,
            level="INFO", message="start")
    ...
    log_run(spark, catalog=catalog_name, ops_schema=ops_schema,
            job_name="batch.device_registry", batch_id=batch_id,
            level="INFO", message="merge done", rows_affected=n,
            table=f"{bronze_schema}.device_registry_raw")

Errors:

    try:
        ...
    except Exception as e:
        log_error(spark, catalog=catalog_name, ops_schema=ops_schema,
                  job_name="batch.device_registry", batch_id=batch_id,
                  message="device registry build failed", exc=e,
                  table=f"{bronze_schema}.device_registry_raw")
        raise
"""

import logging
import sys
import traceback
import uuid
from datetime import datetime, timezone

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Explicit schema because createDataFrame() can't infer types when rows_affected/table are None.
_LOG_SCHEMA = StructType([
    StructField("job_name", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("rows_affected", LongType(), True),
    StructField("target_table", StringType(), True),
    StructField("logged_at", TimestampType(), True),
])


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:  # avoid duplicate handlers on notebook re-run
        handler = logging.StreamHandler(sys.stdout)
        fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def new_batch_id() -> str:
    return f"run_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:8]}"


def log_run(
    spark,
    *,
    catalog: str,
    ops_schema: str,
    job_name: str,
    batch_id: str,
    level: str,
    message: str,
    rows_affected: int | None = None,
    table: str | None = None,
) -> None:
    """Append one structured row to <catalog>.<ops_schema>.pipeline_log."""
    # dict, not Row because Row(**kwargs) reorders fields alphabetically and would misalign with _LOG_SCHEMA.
    data = {
        "job_name": job_name,
        "batch_id": batch_id,
        "level": level,
        "message": message,
        "rows_affected": int(rows_affected) if rows_affected is not None else None,
        "target_table": table,
        "logged_at": datetime.now(timezone.utc),
    }
    try:
        (
            spark.createDataFrame([data], schema=_LOG_SCHEMA)
            .write.mode("append")
            .saveAsTable(f"{catalog}.{ops_schema}.pipeline_log")
        )
    except Exception as e:
        # Logging must never crash the job it's instrumenting.
        get_logger(job_name).error("failed to write to ops.pipeline_log: %s", e)


def log_error(
    spark,
    *,
    catalog: str,
    ops_schema: str,
    job_name: str,
    batch_id: str,
    message: str,
    exc: BaseException | None = None,
    table: str | None = None,
) -> None:
    full_message = message
    if exc is not None:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        full_message = f"{message}\n{tb}"
    log_run(
        spark,
        catalog=catalog,
        ops_schema=ops_schema,
        job_name=job_name,
        batch_id=batch_id,
        level="ERROR",
        message=full_message,
        table=table,
    )
