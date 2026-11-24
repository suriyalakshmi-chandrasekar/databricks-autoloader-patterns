"""Bronze ingestion with Auto Loader, plus an open-source fallback for local runs and CI.

On Databricks, ``read_stream`` uses Auto Loader (``cloudFiles``):

* ``schemaLocation`` + ``schemaEvolutionMode=addNewColumns``: new columns are
  added to the table instead of breaking the stream (the stream stops once,
  records the new schema, and the next run continues)
* ``rescuedDataColumn``: values that don't fit the schema (wrong type, nested
  where flat was expected, unexpected fields) land in ``_rescued_data`` as
  JSON instead of being dropped. Zero data loss.
* the checkpoint tracks which files were processed: no hand-made
  "processed files" metadata table, no re-reads, no misses

Off Databricks (``engine="spark"``), the same pipeline runs on Spark's built-in
file streaming, with PERMISSIVE parsing feeding ``_rescued_data``. That keeps
the rest of the pipeline testable anywhere; schema evolution is Databricks-only.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .config import Source

RESCUED = "_rescued_data"


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def read_stream(spark: SparkSession, src: Source, landing_root: str, checkpoint_root: str,
                engine: str | None = None) -> DataFrame:
    engine = engine or ("autoloader" if on_databricks() else "spark")
    path = f"{landing_root.rstrip('/')}/{src.path}"

    if engine == "autoloader":
        reader = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", src.format)
                  .option("cloudFiles.schemaLocation", f"{checkpoint_root}/{src.name}/schema")
                  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                  .option("cloudFiles.inferColumnTypes", "true")
                  .option("rescuedDataColumn", RESCUED))
        if src.schema_hints:
            reader = reader.option("cloudFiles.schemaHints", src.schema_hints)
    else:
        # local / CI: infer the schema once from what's already there, keep bad rows in _rescued_data
        sample = spark.read.options(**src.options, recursiveFileLookup="true",
                                    **({"pathGlobFilter": src.path_glob} if src.path_glob else {}))
        schema = sample.format(src.format).load(path).schema
        from pyspark.sql.types import StructType
        schema = StructType([f for f in schema.fields if f.name not in ("_corrupt_record", RESCUED)])
        schema = schema.add(RESCUED, "string")
        reader = spark.readStream.format(src.format).schema(schema).option("mode", "PERMISSIVE")
        if src.format == "json":
            # Plain Spark CSV marks rows with fewer columns (older files, before a column was
            # added) as corrupt, which would be wrong; Auto Loader handles that drift itself.
            reader = reader.option("columnNameOfCorruptRecord", RESCUED)

    reader = reader.options(**src.options).option("recursiveFileLookup", "true")
    if src.path_glob:
        reader = reader.option("pathGlobFilter", src.path_glob)
    return (reader.load(path)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp()))


def write_stream(spark: SparkSession, df: DataFrame, src: Source, checkpoint_root: str) -> None:
    """Run until all available files are processed (trigger availableNow), then stop."""
    writer = (df.writeStream.option("checkpointLocation", f"{checkpoint_root}/{src.name}/stream")
              .option("mergeSchema", "true").trigger(availableNow=True))
    if src.write_mode == "append":
        query = writer.toTable(src.target)
    else:
        query = writer.foreachBatch(lambda batch, _id: merge_batch(spark, batch, src)).start()
    query.awaitTermination()


def merge_batch(spark: SparkSession, batch: DataFrame, src: Source) -> None:
    """Upsert one micro-batch; the newest file wins when a key appears more than once."""
    if batch.isEmpty():
        return
    w = Window.partitionBy(*src.merge_keys).orderBy(F.col("_source_file").desc())
    latest = batch.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    if not spark.catalog.tableExists(src.target):
        latest.write.format("delta").saveAsTable(src.target)
        return
    from delta.tables import DeltaTable
    on = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in src.merge_keys)
    (DeltaTable.forName(spark, src.target).alias("t").merge(latest.alias("s"), on)
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def ingest(spark: SparkSession, src: Source, landing_root: str, checkpoint_root: str,
           engine: str | None = None) -> None:
    write_stream(spark, read_stream(spark, src, landing_root, checkpoint_root, engine), src, checkpoint_root)
