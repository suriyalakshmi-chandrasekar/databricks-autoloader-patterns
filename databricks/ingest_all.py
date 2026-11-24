# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader: all feeds to bronze, then standardize + quarantine
# MAGIC With `schemaEvolutionMode = addNewColumns` the stream **stops once** when a feed adds a column, after
# MAGIC recording the new schema. The job's retry setting restarts it and it continues with the new column.

# COMMAND ----------

import os
import sys
from functools import reduce

repo_root = os.path.abspath("..")
sys.path.insert(0, os.path.join(repo_root, "src"))
dbutils.widgets.text("landing_root", "/Volumes/main/raw/landing")  # noqa: F821
dbutils.widgets.text("checkpoint_root", "/Volumes/main/raw/checkpoints/autoloader")  # noqa: F821
landing = dbutils.widgets.get("landing_root")  # noqa: F821
checkpoints = dbutils.widgets.get("checkpoint_root")  # noqa: F821

# COMMAND ----------

from alpatterns.config import load_config
from alpatterns.ingest import ingest
from alpatterns.standardize import split_quarantine, standardize

cfg = load_config(os.path.join(repo_root, "config", "sources.yaml"))
for src in cfg.sources:
    ingest(spark, src, landing, checkpoints, engine="autoloader")  # noqa: F821

# COMMAND ----------

unified = reduce(lambda a, b: a.unionByName(b),
                 [standardize(spark.table(s.target), s, cfg) for s in cfg.sources])  # noqa: F821
clean, quarantine = split_quarantine(unified)
clean.write.mode("overwrite").saveAsTable("silver.inventory")
quarantine.write.mode("overwrite").saveAsTable("silver.inventory_quarantine")
display(quarantine.groupBy("source", "quarantine_reason").count())  # noqa: F821
